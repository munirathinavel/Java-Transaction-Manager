package com.tricon.tm;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;
import javax.transaction.xa.XAException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tricon.tm.internal.TransactionContext;
import com.tricon.tm.internal.exception.TriconSystemException;
import com.tricon.tm.internal.XAResourceManager;
import com.tricon.tm.util.DecodingUtil;
import com.tricon.tm.util.EncodingUtil;

public class TriconTransactionManager implements TransactionManager, UserTransaction, Referenceable, Service {
	private static Logger logger = LoggerFactory.getLogger(TriconTransactionManager.class);

	// Thread specific transaction context
	private final ThreadLocal<TransactionContext> threadTransactionContext = new ThreadLocal<TransactionContext>();

	// Map holds hex string of global transaction id as key and transaction as value
	private final Map<String, TransactionImpl> activeTransactions =
			Collections.synchronizedMap(new HashMap<String, TransactionImpl>());

	private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

	public TriconTransactionManager() {
		logger.info("Starting TriconTransactionManager{}", "..");
		shuttingDown.set(false);

		logVersion();

		// Generate TM vendor name and server id byte array starting itself
		ConfigurationHelper configurationHelper = TriconTransactionManagerServices.getConfigurationHelper();
		configurationHelper.buildTMVendorNameByteArray();
		configurationHelper.buildServerIdByteArray();

		// TODO: Schedule recovery task

		logger.info("TriconTransactionManager is started{}", ".");
	}

	@SuppressWarnings({ "unchecked" })
	public void begin() throws NotSupportedException, SystemException {
		logger.debug("Inside begin() {}", "..");
		if (isShuttingDown()) {
			throw new TriconSystemException("Cannot start a new transaction, transaction manager is shutting down..");
		}
		TransactionImpl transaction = getCurrentTransaction();
		if (transaction != null) {
			throw new NotSupportedException("Nested transactions are not supported!");
		}
		transaction = createNewTransaction();

		TransactionContextSynchronization txContextSync = new TransactionContextSynchronization(transaction);
		try {
			transaction.getSynchronizationList().add(txContextSync);
			transaction.setActive(getOrCreateTransactionContext().getTimeout());
		} catch (RuntimeException ex) {
			logger.error("Inside begin() exception: ", ex);
			txContextSync.afterCompletion(Status.STATUS_NO_TRANSACTION);
			throw ex;
		} catch (SystemException ex) {
			logger.error("Inside begin() exception: ", ex);
			txContextSync.afterCompletion(Status.STATUS_NO_TRANSACTION);
			throw ex;
		}
	}

	public Transaction getTransaction() throws SystemException {
		TransactionImpl transaction = getCurrentTransaction();
		logger.debug("Inside getTransaction() - transaction: {}", transaction);
		return transaction;
	}

	public int getStatus() throws SystemException {
		TransactionImpl transaction = getCurrentTransaction();
		logger.debug("Inside getStatus() - transaction: {}", transaction);
		if (transaction == null) {
			return Status.STATUS_NO_TRANSACTION;
		}
		return transaction.getStatus();
	}

	public void setTransactionTimeout(int timeout) throws SystemException {
		logger.debug("Inside setTransactionTimeout() - timeout: {} seconds", timeout);
		if (timeout < 0) {
			throw new SystemException("Transaction timeout can't be less than zero");
		}
		getOrCreateTransactionContext().setTimeout(timeout);
	}

	public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
			SecurityException, IllegalStateException, SystemException {
		TransactionImpl transaction = getCurrentTransaction();
		logger.debug("Inside commit() - transaction: {}", transaction);
		if (transaction == null) {
			throw new IllegalStateException("No transaction started on this thread");
		}
		transaction.commit();
	}

	public void rollback() throws IllegalStateException, SecurityException, SystemException {
		TransactionImpl transaction = getCurrentTransaction();
		logger.debug("Inside rollback() - transaction: {}", transaction);
		if (transaction == null) {
			throw new IllegalStateException("No transaction started on this thread");
		}
		transaction.rollback();
	}

	public void setRollbackOnly() throws IllegalStateException, SystemException {
		TransactionImpl transaction = getCurrentTransaction();
		logger.debug("Inside setRollbackOnly() - transaction: {}", transaction);
		if (transaction == null) {
			throw new IllegalStateException("No transaction started on this thread");
		}
		transaction.setRollbackOnly();
	}

	public Transaction suspend() throws SystemException {
		TransactionImpl transaction = getCurrentTransaction();
		logger.debug("Inside suspend() - transaction: {}", transaction);
		if (transaction == null) {
			return null;
		}
		try {
			transaction.getXAResourceManager().suspend();
			clearCurrentTransactionContext();
			return transaction;
		} catch (XAException ex) {
			throw new TriconSystemException("Cannot suspend transaction " + transaction
					+ ", error=" + DecodingUtil.decodeXAExceptionErrorCode(ex), ex);
		}
	}

	public void resume(Transaction transaction) throws InvalidTransactionException, IllegalStateException,
			SystemException {
		logger.debug("Inside resume() - transaction: {}", transaction);
		if (transaction == null) {
			throw new InvalidTransactionException("Resumed transaction cannot be null.");
		}
		if (!(transaction instanceof TransactionImpl)) {
			throw new InvalidTransactionException("Resumed transaction must be an instance of "
					+ TransactionImpl.class.getName());
		}
		TransactionImpl currentTransaction = getCurrentTransaction();
		if (currentTransaction != null) {
			throw new IllegalStateException("Another transaction is already running on this thread");
		}
		TransactionImpl transactionImpl = (TransactionImpl) transaction;
		try {
			XAResourceManager resourceManager = transactionImpl.getXAResourceManager();
			resourceManager.resume();
			createNewTransactionContextAndAssociate(transactionImpl);
		} catch (XAException ex) {
			throw new TriconSystemException("Cannot resume transaction " + transactionImpl
					+ ", error=" + DecodingUtil.decodeXAExceptionErrorCode(ex), ex);
		}
	}

	public TransactionImpl getCurrentTransaction() {
		logger.debug("Inside getCurrentTransaction() {}", "..");
		if (threadTransactionContext.get() == null) {
			return null;
		}
		return getOrCreateTransactionContext().getTransaction();
	}

	public Map<String, TransactionImpl> getActiveTransactions() {
		return activeTransactions;
	}

	public boolean isShuttingDown() {
		return shuttingDown.get();
	}

	public synchronized void shutdown() {
		logger.info("Shutting down TriconTransactionManager{}", "..");
		internalShutdown();

		TriconTransactionManagerServices.getTaskScheduler().shutdown();
		TriconTransactionManagerServices.shutdownExecutorService();

		TriconTransactionManagerServices.dispose();
		logger.info("TriconTransactionManager is shutdown{}", "..");
	}

	private void internalShutdown() {
		shuttingDown.set(true);
		long shutdownIntervalSeconds = TriconTransactionManagerServices.getConfigurationHelper().getShutdownInterval();
		logger.debug("shutdownIntervalSeconds: {} seconds", shutdownIntervalSeconds);
		int txCount = activeTransactions != null ? activeTransactions.size() : 0;
		while (shutdownIntervalSeconds > 0 && txCount > 0) {
			try {
				Thread.sleep(1000l);
			} catch (InterruptedException ie) {
				// Ignore
			}
			shutdownIntervalSeconds--;
			txCount = activeTransactions != null ? activeTransactions.size() : 0;
		}
	}

	public Reference getReference() throws NamingException {
		logger.debug("Inside getReference() {}", "..");
		return new Reference(TriconTransactionManager.class.getName(),
				new StringRefAddr("TransactionManager", "TriconTransactionManager"),
				TriconTransactionManagerObjectFactory.class.getName(), null);
	}

	private void setCurrentTransactionContext(final TransactionContext transactionContext) {
		if (transactionContext == null) {
			throw new IllegalArgumentException("Null transaction context can't be associated with the current thread");
		}
		threadTransactionContext.set(transactionContext);
	}

	private TransactionImpl createNewTransaction() {
		final TransactionImpl transaction = new TransactionImpl();
		getOrCreateTransactionContext().setTransaction(transaction);

		String gtridString = EncodingUtil.convertBytesToHex(transaction.getXAResourceManager().getGlobalTransactionId());
		activeTransactions.put(gtridString, transaction);
		return transaction;
	}

	private TransactionContext getOrCreateTransactionContext() {
		TransactionContext transactionContext = threadTransactionContext.get();
		if (transactionContext == null) {
			transactionContext = new TransactionContext();
			setCurrentTransactionContext(transactionContext);
		}
		return transactionContext;
	}

	private TransactionContext createNewTransactionContextAndAssociate(final TransactionImpl transaction) {
		final TransactionContext transactionContext = new TransactionContext();
		transactionContext.setTransaction(transaction);
		setCurrentTransactionContext(transactionContext);
		return transactionContext;
	}

	private void clearCurrentTransactionContext() {
		logger.debug("Inside clearCurrentTransactionContext() - clearing thread transaction context{}", "..");
		threadTransactionContext.set(null);
	}

	private void logVersion() {
		logger.info("TriconTransactionManager version: {}", Version.getVersion());
		logger.info("JVM version: {}", System.getProperty("java.version"));
	}

	private class TransactionContextSynchronization implements Synchronization {
		private TransactionImpl currentTx;

		public TransactionContextSynchronization(TransactionImpl currentTx) {
			this.currentTx = currentTx;
		}

		public void beforeCompletion() {
		}

		public void afterCompletion(int status) {
			final TransactionContext txContext = threadTransactionContext.get();
			if (txContext != null && txContext.getTransaction() == currentTx) {
				logger.debug("afterCompletion() - clearing thread transaction context: {}", txContext);
				threadTransactionContext.set(null);
			}

			String gtridString = EncodingUtil.convertBytesToHex(currentTx.getXAResourceManager().getGlobalTransactionId());
			activeTransactions.remove(gtridString);
		}

		public String toString() {
			return TransactionContextSynchronization.class.getName() + "[currentTx=" + currentTx + "]";
		}
	}

}
