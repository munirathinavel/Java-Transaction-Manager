package com.tricon.tm.twopc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tricon.tm.TransactionImpl;
import com.tricon.tm.internal.exception.TriconRollbackException;
import com.tricon.tm.internal.exception.TriconSystemException;
import com.tricon.tm.internal.XAResourceHelper;
import com.tricon.tm.internal.XAResourceInfo;
import com.tricon.tm.internal.XAResourceManager;
import com.tricon.tm.util.DecodingUtil;

public class PreparePhaseExecutor extends AbstractPhaseExecutor {
	private static Logger logger = LoggerFactory.getLogger(PreparePhaseExecutor.class);

	private final List<XAResourceInfo> preparedResources = Collections.synchronizedList(new ArrayList<XAResourceInfo>());

	public PreparePhaseExecutor(ExecutorService executorService) {
		super(executorService);
	}

	public List<XAResourceInfo> prepare(final TransactionImpl transaction) throws RollbackException, TriconSystemException {
		XAResourceManager xaResourceManager = transaction.getXAResourceManager();
		transaction.setStatus(Status.STATUS_PREPARING);

		if (xaResourceManager.resourceCount() == 0) {
			logger.warn("Executing transaction with 0 enlisted resource {}", "..");
			transaction.setStatus(Status.STATUS_PREPARED);
			return preparedResources;
		}

		// 1PC optimization
		if (xaResourceManager.resourceCount() == 1) {
			XAResourceInfo xaResourceInfo = xaResourceManager.getAllXAResourceInfoList().get(0);
			preparedResources.add(xaResourceInfo);
			logger.debug("Only 1 resource is enlisted, so no prepare needed (1PC){}", ".");
			transaction.setStatus(Status.STATUS_PREPARED);
			return preparedResources;
		}

		try {
			executePhase(xaResourceManager);
		} catch (PhaseException ex) {
			logFailedResources(ex);
			throwException("Transaction failed during prepare of " + transaction, ex);
		}

		transaction.setStatus(Status.STATUS_PREPARED);
		logger.debug("Successfully prepared {} resource(s)", preparedResources.size());
		return preparedResources;
	}

	protected boolean isParticipating(XAResourceInfo xaResourceInfo) {
		return true;
	}

	protected Job createJob(XAResourceInfo xaResourceInfo) {
		return new PrepareJob(xaResourceInfo);
	}

	private class PrepareJob extends Job {
		public PrepareJob(XAResourceInfo xaResourceInfo) {
			super(xaResourceInfo);
		}

		public void execute() {
			try {
				XAResourceInfo xaResourceInfo = getXAResourceInfo();
				logger.debug("Preparing resource {} ..", xaResourceInfo);

				int vote = xaResourceInfo.getXAResource().prepare(xaResourceInfo.getXid());
				logger.debug("Voted: {} on resource: {}", DecodingUtil.decodePrepareVote(vote), xaResourceInfo);
				if (vote == XAResource.XA_OK) {
					preparedResources.add(xaResourceInfo);
					logger.debug("Prepared resource (for commit): {}", xaResourceInfo);

				} else if (vote == XAResource.XA_RDONLY) {
					logger.debug("Non-prepared (will not be participated for commit) resource : {}", xaResourceInfo);
				}
			} catch (RuntimeException ex) {
				runtimeException = ex;
			} catch (XAException ex) {
				xaException = ex;
			}
		}

		public String toString() {
			return "PrepareJob with resource: " + getXAResourceInfo();
		}
	}

	@SuppressWarnings("rawtypes")
	private void throwException(String message, PhaseException phaseException) throws TriconRollbackException {
		final List<XAResourceInfo> errorResources = new ArrayList<XAResourceInfo>();

		Iterator entrySet = phaseException.getResourceExceptionMap().entrySet().iterator();
		while (entrySet.hasNext()) {
			Map.Entry mapEntry = (Map.Entry) entrySet.next();
			errorResources.add((XAResourceInfo) mapEntry.getKey());
		}
		if (errorResources.size() > 0) {
			throw new TriconRollbackException(message + ": " +
					" resource(s) " + XAResourceHelper.getXAResourceInfosString(errorResources) +
					" thrown unexpected exception", phaseException);
		}
	}

}
