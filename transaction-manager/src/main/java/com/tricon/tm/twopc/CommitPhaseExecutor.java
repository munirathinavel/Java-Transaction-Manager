package com.tricon.tm.twopc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.Status;
import javax.transaction.xa.XAException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tricon.tm.TransactionImpl;
import com.tricon.tm.internal.exception.TriconHeuristicMixedException;
import com.tricon.tm.internal.exception.TriconHeuristicRollbackException;
import com.tricon.tm.internal.exception.TriconSystemException;
import com.tricon.tm.internal.exception.TriconXAException;
import com.tricon.tm.internal.XAResourceHelper;
import com.tricon.tm.internal.XAResourceInfo;
import com.tricon.tm.internal.XAResourceManager;
import com.tricon.tm.util.DecodingUtil;

public class CommitPhaseExecutor extends AbstractPhaseExecutor {
	private static Logger logger = LoggerFactory.getLogger(CommitPhaseExecutor.class);

	private boolean onePhase;
	private List<XAResourceInfo> resources;
	private final List<XAResourceInfo> committedResources = Collections.synchronizedList(new ArrayList<XAResourceInfo>());

	public CommitPhaseExecutor(ExecutorService executorService) {
		super(executorService);
	}

	public void commit(final TransactionImpl transaction, final List<XAResourceInfo> resources)
			throws HeuristicMixedException, HeuristicRollbackException, TriconSystemException {
		XAResourceManager xaResourceManager = transaction.getXAResourceManager();
		transaction.setStatus(Status.STATUS_COMMITTING);
		if (xaResourceManager.resourceCount() == 0) {
			transaction.setStatus(Status.STATUS_COMMITTED);
			logger.debug("Phase 2 commit succeeded with no prepared resource{}", ".");
			return;
		}
		this.resources = Collections.unmodifiableList(resources);
		this.onePhase = xaResourceManager.resourceCount() == 1;
		try {
			executePhase(xaResourceManager);
		} catch (PhaseException ex) {
			logFailedResources(ex);
			transaction.setStatus(Status.STATUS_UNKNOWN);
			throwException("Transaction failed during commit of " + transaction, ex, resources.size());
		}
		logger.debug("Phase 2 commit executed on resources: {}",
				XAResourceHelper.getXAResourceInfosString(committedResources));
		transaction.setStatus(Status.STATUS_COMMITTED);
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
		return new CommitJob(xaResourceInfo);
	}

	private class CommitJob extends Job {
		public CommitJob(XAResourceInfo xaResourceInfo) {
			super(xaResourceInfo);
		}

		public XAException getXAException() {
			return xaException;
		}

		public RuntimeException getRuntimeException() {
			return runtimeException;
		}

		public void execute() {
			try {
				commitResource(getXAResourceInfo(), onePhase);
			} catch (RuntimeException ex) {
				runtimeException = ex;
			} catch (XAException ex) {
				xaException = ex;
			}
		}

		private void commitResource(final XAResourceInfo xaResourceInfo, boolean onePhase) throws XAException {
			try {
				logger.debug("Committing resource {} {}", xaResourceInfo, (onePhase ? " (with one-phase optimization)" : ""));
				xaResourceInfo.getXAResource().commit(xaResourceInfo.getXid(), onePhase);
				committedResources.add(xaResourceInfo);
				logger.debug("Committed resource {}", xaResourceInfo);
			} catch (XAException ex) {
				handleXAException(xaResourceInfo, ex);
			}
		}

		private void handleXAException(XAResourceInfo failedXAResourceInfo, XAException xaException) throws XAException {
			switch (xaException.errorCode) {
				case XAException.XA_HEURCOM:
					forgetHeuristicCommit(failedXAResourceInfo);
					return;

				case XAException.XAER_NOTA:
					throw new TriconXAException("Unknown heuristic termination, state of this global transaction is unknown on resource: "
							+ failedXAResourceInfo, XAException.XA_HEURHAZ, xaException);

				case XAException.XA_HEURHAZ:
				case XAException.XA_HEURMIX:
				case XAException.XA_HEURRB:
				case XAException.XA_RBCOMMFAIL:
				case XAException.XA_RBDEADLOCK:
				case XAException.XA_RBINTEGRITY:
				case XAException.XA_RBOTHER:
				case XAException.XA_RBPROTO:
				case XAException.XA_RBROLLBACK:
				case XAException.XA_RBTIMEOUT:
				case XAException.XA_RBTRANSIENT:
					logger.error("Heuristic rollback is incompatible with the global state of this transaction on resource: "
							+ failedXAResourceInfo);
					throw xaException;

				default:
					logger.warn("During 2 phase commit, resource {} reported {} - ex: {}." +
							" Transaction is prepared and will be committed through recovery.",
							new Object[] { failedXAResourceInfo.getXAResource(),
									DecodingUtil.decodeXAExceptionErrorCode(xaException), xaException });
			}
		}

		private void forgetHeuristicCommit(XAResourceInfo failedXAResourceInfo) {
			try {
				logger.debug("Handling heuristic commit on resource (during commit): {}", failedXAResourceInfo.getXAResource());
				failedXAResourceInfo.getXAResource().forget(failedXAResourceInfo.getXid());
				logger.debug("Forgotten heuristically committed resource: {}", failedXAResourceInfo.getXAResource());
			} catch (XAException ex) {
				logger.error("Cannot forget transction {} assigned to resource {}, error={}, ex: {}",
						new Object[] { failedXAResourceInfo.getXid(), failedXAResourceInfo.getXAResource(),
								DecodingUtil.decodeXAExceptionErrorCode(ex), ex });
			}
		}

		public String toString() {
			return "CommitJob " + (onePhase ? "(one phase)" : "") + " with resource: " + getXAResourceInfo();
		}
	}

	@SuppressWarnings("rawtypes")
	private void throwException(String message, PhaseException phaseException, int totalResourceCount)
			throws HeuristicMixedException, HeuristicRollbackException {

		final List<XAResourceInfo> heuristicResources = new ArrayList<XAResourceInfo>();
		final List<XAResourceInfo> errorResources = new ArrayList<XAResourceInfo>();

		boolean hazard = false, heurCommit = false;
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
						heurCommit = true;
					case XAException.XA_HEURRB:
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

		if (!hazard && !heurCommit && heuristicResources.size() == totalResourceCount) {
			throw new TriconHeuristicRollbackException(message + ": "
					+ " resource(s) " + XAResourceHelper.getXAResourceInfosString(heuristicResources)
					+ " are heuristically rolled back", phaseException);
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
