package com.tricon.tm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.transaction.HeuristicCommitException;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tricon.tm.internal.exception.TriconRollbackException;
import com.tricon.tm.internal.exception.TriconSystemException;
import com.tricon.tm.internal.exception.TriconXAException;
import com.tricon.tm.internal.XAResourceHelper;
import com.tricon.tm.internal.XAResourceInfo;
import com.tricon.tm.internal.XAResourceManager;
import com.tricon.tm.timer.TransactionTimeoutTask;
import com.tricon.tm.twopc.CommitPhaseExecutor;
import com.tricon.tm.twopc.PhaseException;
import com.tricon.tm.twopc.PreparePhaseExecutor;
import com.tricon.tm.twopc.RollbackPhaseExecutor;
import com.tricon.tm.util.DecodingUtil;
import com.tricon.tm.util.EncodingUtil;
import com.tricon.tm.util.XidUtil;

public class TransactionImpl implements Transaction {
	private static Logger logger = LoggerFactory.getLogger(TransactionImpl.class);

	private byte[] globalTransacationId;
	private XAResourceManager xaResourceManager;
	private SyncTransactionKey syncTransactionkey;

	private String threadName;
	private volatile int status = Status.STATUS_NO_TRANSACTION;
	private Date startDate;
	private Date timeoutDate;
	private boolean timeoutExpired = false;

	private final List synchronizationList = Collections.synchronizedList(new ArrayList());

	private PreparePhaseExecutor preparer = new PreparePhaseExecutor(TriconTransactionManagerServices.getExecutorService());
	private CommitPhaseExecutor committer = new CommitPhaseExecutor(TriconTransactionManagerServices.getExecutorService());
	private RollbackPhaseExecutor rollbacker = new RollbackPhaseExecutor(TriconTransactionManagerServices.getExecutorService());

	public TransactionImpl() {
		globalTransacationId = XidUtil.generateUniqueXidDataComponent();
		xaResourceManager = new XAResourceManager(globalTransacationId);
		syncTransactionkey = new SyncTransactionKey(globalTransacationId);

		threadName = Thread.currentThread().getName();
	}

	public byte[] getGlobalTransacationId() {
		return globalTransacationId;
	}

	public XAResourceManager getXAResourceManager() {
		return xaResourceManager;
	}

	public SyncTransactionKey getSyncTransactionkey() {
		return syncTransactionkey;
	}

	public String getThreadName() {
		return threadName;
	}

	public Date getStartDate() {
		return startDate;
	}

	public Date getTimeoutDate() {
		return timeoutDate;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public List getSynchronizationList() {
		return synchronizationList;
	}

	public void setActive(int timeout) throws IllegalStateException, SystemException {
		logger.debug("Inside setActive() - status: {}, timeout: {} seconds", DecodingUtil.decodeStatus(status), timeout);
		if (status != Status.STATUS_NO_TRANSACTION) {
			throw new IllegalStateException("Transaction has already started");
		}
		if (timeout < 0) {
			throw new SystemException("Transaction timeout can't be less than zero");
		}
		setStatusAndLogRecord(Status.STATUS_ACTIVE);
		startDate = new Date();
		timeoutDate = new Date(System.currentTimeMillis() + (timeout * 1000L));

		scheduleTransactionTimeout();
	}

	public void timeoutExpired() {
		timeoutExpired = true;
		setStatus(Status.STATUS_MARKED_ROLLBACK);
		logger.warn("Timeout expired! Marked transaction {} for rollback", this);
	}

	public boolean isTimeoutExpired() {
		return timeoutExpired;
	}

	private void setStatusAndLogRecord(int status) {
		// int oldStatus = this.status;
		this.status = status;
		logTransactionRecord();
	}

	private void logTransactionRecord() {
		// TODO: Need to implement logging transactional record
	}

	public boolean enlistResource(XAResource xaResource) throws RollbackException, IllegalStateException, SystemException {
		logger.debug("Inside enlistResource() - status: {}, isStartedOrFinished: {}, xaResource: {}",
				new Object[] { DecodingUtil.decodeStatus(status), isStartedOrFinished(), xaResource });

		if (status == Status.STATUS_NO_TRANSACTION) {
			throw new IllegalStateException("Transaction hasn't started yet");
		}
		if (status == Status.STATUS_MARKED_ROLLBACK) {
			throw new RollbackException("Transaction has been marked as rollback only");
		}
		if (isStartedOrFinished()) {
			throw new IllegalStateException("Transaction started or finished 2PC, cannot enlist any more resource");
		}

		XAResourceInfo xaResourceInfo = XAResourceHelper.createXAResourceInfo(xaResource, null, timeoutDate);
		try {
			xaResourceManager.enlist(xaResourceInfo);
		} catch (XAException ex) {
			if (TriconXAException.isUnilateralRollback(ex)) {
				// Unilateral rollback found, so mark the transaction for rollback only
				setStatus(Status.STATUS_MARKED_ROLLBACK);
				throw new TriconRollbackException("Resource " + xaResourceInfo + " unilaterally rolled back, error="
						+ DecodingUtil.decodeXAExceptionErrorCode(ex), ex);
			}
			throw new TriconSystemException("Cannot enlist " + xaResourceInfo + ", error="
					+ DecodingUtil.decodeXAExceptionErrorCode(ex), ex);
		}
		return true;
	}

	public boolean delistResource(XAResource xaResource, int flag) throws IllegalStateException, SystemException {
		logger.debug("Inside delistResource() - status: {}, isInProgress: {}, xaResource: {}, flag: {}",
				new Object[] { DecodingUtil.decodeStatus(status), isInProgress(), xaResource,
						DecodingUtil.decodeXAResourceFlag(flag) });

		if (status == Status.STATUS_NO_TRANSACTION) {
			throw new IllegalStateException("Transaction hasn't started yet");
		}
		if (flag != XAResource.TMSUCCESS && flag != XAResource.TMSUSPEND && flag != XAResource.TMFAIL) {
			throw new TriconSystemException("Can only delist the resource with flag SUCCESS, SUSPEND, FAIL - but it is: "
					+ DecodingUtil.decodeXAResourceFlag(flag));
		}
		if (isInProgress()) {
			throw new IllegalStateException("Transaction is being committed or rolled back, cannot delist the resource now");
		}

		XAResourceInfo enlistedXAResourceInfo = xaResourceManager.findEnlistedXAResourceInfo(xaResource);
		if (enlistedXAResourceInfo == null) {
			throw new TriconSystemException("Can't delist resource: " + xaResource
					+ " as it is not enlisted for transaction with gtrid: "
					+ EncodingUtil.convertBytesToHex(getGlobalTransacationId()));
		}
		return performDelistResource(enlistedXAResourceInfo, flag);
	}

	private boolean performDelistResource(XAResourceInfo xaResourceInfo, int flag) throws TriconSystemException {
		logger.debug("Inside performDelistResource() - xaResourceInfo: {}, flag: {}", xaResourceInfo,
				DecodingUtil.decodeXAResourceFlag(flag));
		try {
			return xaResourceManager.delist(xaResourceInfo, flag);
		} catch (XAException ex) {
			// Resource could not be delisted, mark the transaction for rollback only
			if (status != Status.STATUS_MARKED_ROLLBACK) {
				setStatus(Status.STATUS_MARKED_ROLLBACK);
			}
			if (TriconXAException.isUnilateralRollback(ex)) {
				// Unilaterally rollback found, so throw the SystemException
				throw new TriconSystemException("Resource " + xaResourceInfo + " unilaterally rolled back, error="
						+ DecodingUtil.decodeXAExceptionErrorCode(ex), ex);
			}
			throw new TriconSystemException("Cannot delist " + xaResourceInfo + ", error="
					+ DecodingUtil.decodeXAExceptionErrorCode(ex), ex);
		}
	}

	public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
			SecurityException, IllegalStateException, SystemException {
		logger.debug("Inside commit() - status: {}, isStartedOrFinished: {}",
				DecodingUtil.decodeStatus(status), isStartedOrFinished());

		if (status == Status.STATUS_NO_TRANSACTION) {
			throw new IllegalStateException("Transaction hasn't started yet");
		}
		if (isStartedOrFinished()) {
			throw new IllegalStateException("Transaction is started/finished, cannot commit it");
		}

		// Cancel transaction timeout task
		cancelTransactionTimeout();

		// Invoke Synchronization.beforeCompletion()
		invokeSyncronizationBeforeCompletion();

		// Timeout expired, rollback
		if (isTimeoutExpired()) {
			rollback();
			throw new TriconRollbackException("Transaction timed out and has been rolled back");
		}

		// Delist the unclosed resources
		try {
			delistUnclosedResources(XAResource.TMSUCCESS);
		} catch (TriconRollbackException ex) {
			logger.error("Resource delistment caused transaction rollback - ex: ", ex);
			rollback();
			throw new TriconRollbackException("Delistment error caused transaction rollback - ex: " + ex.getMessage());
		}

		// If transaction marked for rollback, rollback
		if (status == Status.STATUS_MARKED_ROLLBACK) {
			logger.debug("Transaction marked as rollback only{}", ".");
			rollback();
			throw new TriconRollbackException("Transaction was marked as rollback only and has been rolled back");
		}

		// Perform two phase commit
		performTwoPhaseCommit();

	}

	private void performTwoPhaseCommit() throws RollbackException, HeuristicMixedException,
			HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {
		try {
			List<XAResourceInfo> preparedResources;

			// Phase I - prepare
			try {
				logger.debug("Phase I - Issuing prepare for {} enlisted resource(s)", xaResourceManager.resourceCount());
				preparedResources = preparer.prepare(this);
			} catch (RollbackException ex) {
				logger.error("Caught rollback exception during prepare, trying to rollback: ", ex);

				rollbackOnPrepareFailures(ex);
				throw new TriconRollbackException("Rolled back the prepare failed transaction: " + this, ex);
			} catch (TriconSystemException tse) {
				logger.error("Caught system exception during prepare: ", tse);
				throw tse;
			}

			// Phase II - commit
			logger.debug("Phase II - Issuing commit for {} prepared resource(s)", preparedResources.size());
			committer.commit(this, preparedResources);

			logger.debug("Successfully committed {}", this);
		} finally {
			// Invoke Synchronization.afterCompletion()
			invokeSyncronizationAfterCompletion();
		}
	}

	private void delistUnclosedResources(int flag) throws TriconRollbackException {
		logger.debug("Inside delistUnclosedResources() - flag: {}", DecodingUtil.decodeXAResourceFlag(flag));

		final List<XAResourceInfo> allResources = xaResourceManager.getAllXAResourceInfoList();
		List<XAResourceInfo> rolledBackResources = new ArrayList<XAResourceInfo>();
		List<XAResourceInfo> failedResources = new ArrayList<XAResourceInfo>();

		for (final XAResourceInfo xaResourceInfo : allResources) {
			if (!xaResourceInfo.isEnded()) {
				logger.debug("Unclosed resource to be delisted: {}", xaResourceInfo);
				try {
					delistResource(xaResourceInfo.getXAResource(), flag);
				} catch (TriconSystemException ex) {
					rolledBackResources.add(xaResourceInfo);
					logger.error("Found unilateral rollback on {} - ex: {}", xaResourceInfo, ex);
				} catch (SystemException ex) {
					failedResources.add(xaResourceInfo);
					logger.error("Error delisting resource {} - ex: {}", xaResourceInfo, ex);
				}
			}
		}
		if (!rolledBackResources.isEmpty() || !failedResources.isEmpty()) {
			throw new TriconRollbackException(prepareExceptionMessage(rolledBackResources, failedResources));
		}
	}

	private String prepareExceptionMessage(final List<XAResourceInfo> rolledBackResources,
			final List<XAResourceInfo> failedResources) {
		if (!rolledBackResources.isEmpty() || !failedResources.isEmpty()) {
			final StringBuffer sb = new StringBuffer();
			if (!rolledBackResources.isEmpty()) {
				sb.append("\nUnilaterally rolled back resource(s): \n");
				sb.append("  ").append(XAResourceHelper.getXAResourceInfosString(rolledBackResources));
			}
			if (!failedResources.isEmpty()) {
				sb.append("\nDelistment failed resource(s): \n");
				sb.append("  ").append(XAResourceHelper.getXAResourceInfosString(failedResources));
			}
			return sb.toString();
		}
		return null;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void rollbackOnPrepareFailures(RollbackException rbEx) throws TriconSystemException {
		logger.debug("Inside rollbackOnPrepareFailures() {}", "..");
		List<XAResourceInfo> resources = xaResourceManager.getAllXAResourceInfoList();
		try {
			rollbacker.rollback(this, resources);
			logger.debug("Rollback after prepare failure succeeded{}", ".");
		} catch (Exception ex) {
			logger.debug("Rollback failed (after prepare failure) - ex: ", ex);
			PhaseException preparePhaseEx = (PhaseException) rbEx.getCause();
			PhaseException rollbackPhaseEx = (PhaseException) ex.getCause();

			Map resourceExceptionMap = new LinkedHashMap();
			resourceExceptionMap.putAll(preparePhaseEx.getResourceExceptionMap());
			resourceExceptionMap.putAll(rollbackPhaseEx.getResourceExceptionMap());

			throw new TriconSystemException("Transaction partially prepared and rolled back. Some resources might be left in doubt!",
					new PhaseException(resourceExceptionMap));
		}
	}

	public void rollback() throws IllegalStateException, SystemException {
		logger.debug("Inside rollback() - status: {}, isStartedOrFinished: {}",
				DecodingUtil.decodeStatus(status), isStartedOrFinished());
		if (status == Status.STATUS_NO_TRANSACTION) {
			throw new IllegalStateException("Transaction hasn't started yet");
		}
		if (isStartedOrFinished()) {
			throw new IllegalStateException("Transaction is started/finished, cannot rollback");
		}

		// Cancel transaction timeout task
		cancelTransactionTimeout();

		// Delist the unclosed resources
		try {
			delistUnclosedResources(XAResource.TMSUCCESS);
		} catch (TriconRollbackException ex) {
			logger.warn("Some resource(s) failed during delistment - ex: ", ex);
		}

		// Perform rollback
		performRollback();
	}

	private void performRollback() throws IllegalStateException, SystemException {
		logger.debug("Rolling back {} enlisted resource(s)", xaResourceManager.resourceCount());
		try {
			rollbacker.rollback(this, getEligibleResourcesForRollback());
			logger.debug("Successfully rolled back {}", this);
		} catch (HeuristicMixedException ex) {
			throw new TriconSystemException("Transaction partially committed and rolled back. Resources are now inconsistent!", ex);
		} catch (HeuristicCommitException ex) {
			throw new TriconSystemException("Transaction committed instead of rolled back. Resources are now inconsistent!", ex);
		} finally {
			// Invoke Synchronization.afterCompletion()
			invokeSyncronizationAfterCompletion();
		}
	}

	private List<XAResourceInfo> getEligibleResourcesForRollback() {
		final List<XAResourceInfo> resourcesToRollback = new ArrayList<XAResourceInfo>();
		final List<XAResourceInfo> allResources = xaResourceManager.getAllXAResourceInfoList();
		for (XAResourceInfo xaResourceInfo : allResources) {
			if (!xaResourceInfo.isFailed()) {
				resourcesToRollback.add(xaResourceInfo);
			}
		}
		return resourcesToRollback;
	}

	public void setRollbackOnly() throws IllegalStateException, SystemException {
		logger.debug("Inside setRollbackOnly() - status: {}, isStartedOrFinished: {}",
				DecodingUtil.decodeStatus(status), isStartedOrFinished());
		if (status == Status.STATUS_NO_TRANSACTION) {
			throw new IllegalStateException("Transaction hasn't started yet");
		}
		if (isStartedOrFinished()) {
			throw new IllegalStateException("Transaction is started/finished, cannot change its status");
		}
		setStatus(Status.STATUS_MARKED_ROLLBACK);
	}

	public int getStatus() throws SystemException {
		logger.debug("Inside getStatus() - status: {}", DecodingUtil.decodeStatus(status));
		return this.status;
	}

	public String getStatusDescription() throws SystemException {
		return DecodingUtil.decodeStatus(getStatus());
	}

	@SuppressWarnings("unchecked")
	public void registerSynchronization(Synchronization synchronization) throws RollbackException,
			IllegalStateException, SystemException {
		logger.debug("Inside registerSynchronization() - status: {}, isStartedOrFinished: {}, synchronization: {}",
				new Object[] { DecodingUtil.decodeStatus(status), isStartedOrFinished(), synchronization });
		if (status == Status.STATUS_NO_TRANSACTION) {
			throw new IllegalStateException("Tansaction hasn't started yet");
		}
		if (status == Status.STATUS_MARKED_ROLLBACK) {
			throw new TriconRollbackException("Transaction has been marked as rollback only");
		}
		if (isStartedOrFinished()) {
			throw new IllegalStateException("Transaction is started/finished, cannot register this synchronization");
		}
		synchronizationList.add(synchronization);
		logger.debug("Registered synchronization: {}", synchronization);
	}

	@Override
	public int hashCode() {
		return xaResourceManager.getGlobalTransactionId().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof TransactionImpl)) {
			return false;
		}
		TransactionImpl transaction = (TransactionImpl) obj;
		return Arrays.equals(xaResourceManager.getGlobalTransactionId(),
				transaction.xaResourceManager.getGlobalTransactionId());
	}

	@Override
	public String toString() {
		return new StringBuffer(this.getClass().getName()).append("[")
				.append("globalTransacationId=").append(EncodingUtil.convertBytesToHex(xaResourceManager.getGlobalTransactionId()))
				.append(", status=").append(DecodingUtil.decodeStatus(status))
				.append(", enlistedResourceCount=").append(xaResourceManager.resourceCount())
				.append(", startDate=").append(startDate)
				.append("]").toString();
	}

	private void scheduleTransactionTimeout() {
		logger.debug("Scheduling transaction timeout{}", "..");
		TriconTransactionManagerServices.getTaskScheduler()
				.schedule(new TransactionTimeoutTask(this, this.getTimeoutDate()));
	}

	private void cancelTransactionTimeout() {
		logger.debug("Cancelling transaction timeout{}", "..");
		if (!TriconTransactionManagerServices.getTaskScheduler()
				.cancelByTaskTypeAndObject(TransactionTimeoutTask.class, this)) {
			logger.warn("No TransactionTimeoutTask found based on object {} for cancel. So ignoring ..", this);
		}
	}

	private boolean isStartedOrFinished() {
		switch (status) {
			case Status.STATUS_PREPARING:
			case Status.STATUS_PREPARED:
			case Status.STATUS_COMMITTING:
			case Status.STATUS_COMMITTED:
			case Status.STATUS_ROLLING_BACK:
			case Status.STATUS_ROLLEDBACK:
				return true;
		}
		return false;
	}

	private boolean isInProgress() {
		switch (status) {
			case Status.STATUS_PREPARING:
			case Status.STATUS_PREPARED:
			case Status.STATUS_COMMITTING:
			case Status.STATUS_ROLLING_BACK:
				return true;
		}
		return false;
	}

	@SuppressWarnings("rawtypes")
	private void invokeSyncronizationBeforeCompletion() {
		logger.debug("before completion, {} synchronization(s) to execute", synchronizationList.size());
		synchronized (synchronizationList) {
			Iterator it = synchronizationList.iterator();
			while (it.hasNext()) {
				Synchronization synchronization = (Synchronization) it.next();
				try {
					logger.debug("executing synchronization: {}", synchronization);
					synchronization.beforeCompletion();
				} catch (RuntimeException ex) {
					logger.error("Synchronization.beforeCompletion() call failed on {}, marking transaction for rollback - ex: {}",
							synchronization, ex);
					setStatus(Status.STATUS_MARKED_ROLLBACK);
					throw ex;
				}
			}
		}
	}

	@SuppressWarnings("rawtypes")
	private void invokeSyncronizationAfterCompletion() {
		logger.debug("after completion, clearing resources for globalTransacationId: {}",
				EncodingUtil.convertBytesToHex(globalTransacationId));

		xaResourceManager.clearXAResourceInfos(globalTransacationId);

		logger.debug("after completion, {} synchronization(s) to execute", synchronizationList.size());
		synchronized (synchronizationList) {
			Iterator it = synchronizationList.iterator();
			while (it.hasNext()) {
				Synchronization synchronization = (Synchronization) it.next();
				try {
					logger.debug("executing synchronization: {} with status: {}", synchronization, DecodingUtil.decodeStatus(status));
					synchronization.afterCompletion(status);
				} catch (Exception ex) {
					logger.error("Synchronization.afterCompletion() call failed on {} - ex: {}", synchronization, ex);
				}
			}
		}
	}

}
