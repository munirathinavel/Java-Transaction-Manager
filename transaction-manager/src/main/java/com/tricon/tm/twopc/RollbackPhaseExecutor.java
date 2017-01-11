package com.tricon.tm.twopc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import javax.transaction.HeuristicCommitException;
import javax.transaction.HeuristicMixedException;
import javax.transaction.Status;
import javax.transaction.xa.XAException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tricon.tm.TransactionImpl;
import com.tricon.tm.internal.exception.TriconHeuristicCommitException;
import com.tricon.tm.internal.exception.TriconHeuristicMixedException;
import com.tricon.tm.internal.exception.TriconSystemException;
import com.tricon.tm.internal.XAResourceHelper;
import com.tricon.tm.internal.XAResourceInfo;
import com.tricon.tm.internal.XAResourceManager;
import com.tricon.tm.util.DecodingUtil;

public class RollbackPhaseExecutor extends AbstractPhaseExecutor {
	private static Logger logger = LoggerFactory.getLogger(RollbackPhaseExecutor.class);

	private List<XAResourceInfo> resources;
	private final List<XAResourceInfo> rolledbackResources = Collections.synchronizedList(new ArrayList<XAResourceInfo>());

	public RollbackPhaseExecutor(ExecutorService executorService) {
		super(executorService);
	}

	public void rollback(final TransactionImpl transaction, final List<XAResourceInfo> resources)
			throws HeuristicMixedException, HeuristicCommitException, TriconSystemException {
		XAResourceManager xaResourceManager = transaction.getXAResourceManager();
		transaction.setStatus(Status.STATUS_ROLLING_BACK);

		this.resources = Collections.unmodifiableList(resources);
		try {
			executePhase(xaResourceManager);
		} catch (PhaseException ex) {
			logFailedResources(ex);
			transaction.setStatus(Status.STATUS_UNKNOWN);
			throwException("Transaction failed during rollback of " + transaction, ex, resources.size());
		}
		logger.debug("Rollback executed on resources: {}",
				XAResourceHelper.getXAResourceInfosString(rolledbackResources));
		transaction.setStatus(Status.STATUS_ROLLEDBACK);
	}

	protected boolean isParticipating(XAResourceInfo xaResourceInfo) {
		if (resources != null && resources.size() > 0) {
			for (final XAResourceInfo xaResourceInfoTmp : resources) {
				if (xaResourceInfoTmp == xaResourceInfo) {
					return true;
				}
			}
		}
		return false;
	}

	protected Job createJob(XAResourceInfo xaResourceInfo) {
		return new RollbackJob(xaResourceInfo);
	}

	private class RollbackJob extends Job {

		public RollbackJob(XAResourceInfo xaResourceInfo) {
			super(xaResourceInfo);
		}

		public void execute() {
			try {
				rollbackResource(getXAResourceInfo());
			} catch (RuntimeException ex) {
				runtimeException = ex;
			} catch (XAException ex) {
				xaException = ex;
			}
		}

		private void rollbackResource(XAResourceInfo xaResourceInfo) throws XAException {
			try {
				logger.debug("Trying to rollback resource {}", xaResourceInfo);
				xaResourceInfo.getXAResource().rollback(xaResourceInfo.getXid());
				rolledbackResources.add(xaResourceInfo);
				logger.debug("Rolled back resource {}", xaResourceInfo);
			} catch (XAException ex) {
				handleXAException(xaResourceInfo, ex);
			}
		}

		private void handleXAException(XAResourceInfo failedXAResourceInfo, XAException xaException) throws XAException {
			switch (xaException.errorCode) {
				case XAException.XA_HEURRB:
					forgetHeuristicRollback(failedXAResourceInfo);
					return;

				case XAException.XA_HEURCOM:
				case XAException.XA_HEURHAZ:
				case XAException.XA_HEURMIX:
					logger.error("Heuristic rollback is incompatible with the global state of this transaction on resource: "
							+ failedXAResourceInfo);
					throw xaException;

				default:
					logger.warn("During rollback, resource {} reported {} - ex: {}." +
							" Transaction is prepared and will be rolled back through recovery.",
							new Object[] { failedXAResourceInfo.getXAResource(),
									DecodingUtil.decodeXAExceptionErrorCode(xaException), xaException });
			}
		}

		private void forgetHeuristicRollback(XAResourceInfo failedXAResourceInfo) {
			try {
				logger.debug("Handling heuristic rollback on resource: {}", failedXAResourceInfo.getXAResource());
				failedXAResourceInfo.getXAResource().forget(failedXAResourceInfo.getXid());
				logger.debug("Forgotten heuristically rolled back resource {}", failedXAResourceInfo.getXAResource());
			} catch (XAException ex) {
				logger.error("Cannot forget transaction: {} assigned to resource: {}, error: {}, ex: {}",
						new Object[] { failedXAResourceInfo.getXid(), failedXAResourceInfo.getXAResource(),
								DecodingUtil.decodeXAExceptionErrorCode(ex), ex });
			}
		}

		public String toString() {
			return "RollbackJob with resource: " + getXAResourceInfo();
		}
	}

	@SuppressWarnings("rawtypes")
	private void throwException(String message, PhaseException phaseException, int totalResourceCount)
			throws HeuristicMixedException, HeuristicCommitException {

		final List<XAResourceInfo> heuristicResources = new ArrayList<XAResourceInfo>();
		final List<XAResourceInfo> errorResources = new ArrayList<XAResourceInfo>();

		boolean hazard = false, heurRollback = false;
		Iterator itr = phaseException.getResourceExceptionMap().entrySet().iterator();
		while (itr.hasNext()) {
			Map.Entry mapEntry = (Map.Entry) itr.next();
			XAResourceInfo xaResourceInfo = (XAResourceInfo) mapEntry.getKey();
			Throwable ex = (Throwable) mapEntry.getValue();
			if (ex instanceof XAException) {
				XAException xaEx = (XAException) ex;
				switch (xaEx.errorCode) {
					case XAException.XA_HEURHAZ:
						hazard = true;
					case XAException.XA_HEURCOM:
					case XAException.XA_HEURRB:
						heurRollback = true;
					case XAException.XA_HEURMIX:
						heuristicResources.add(xaResourceInfo);
						break;

					default:
						errorResources.add(xaResourceInfo);
				}
			} else {
				errorResources.add(xaResourceInfo);
			}
		}

		if (!hazard && !heurRollback && heuristicResources.size() == totalResourceCount) {
			throw new TriconHeuristicCommitException(message + ": "
					+ " resource(s) " + XAResourceHelper.getXAResourceInfosString(heuristicResources)
					+ " are heuristically committed", phaseException);
		} else {
			final StringBuffer sb = new StringBuffer(message + ": ");
			if (errorResources.size() > 0) {
				sb.append(" resource(s) ").append(XAResourceHelper.getXAResourceInfosString(errorResources));
				sb.append(" thrown unexpected exception");
			}
			if (errorResources.size() > 0 && heuristicResources.size() > 0) {
				sb.append(" and ");
			}
			if (heuristicResources.size() > 0) {
				sb.append(" resource(s) ").append(XAResourceHelper.getXAResourceInfosString(heuristicResources));
				sb.append(" heuristically mixed (committed and/or rolled back)");
			}
			sb.append(hazard ? " (or hazard happened)" : "");
			throw new TriconHeuristicMixedException(sb.toString(), phaseException);
		}
	}

}
