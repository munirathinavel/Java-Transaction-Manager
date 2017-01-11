package com.tricon.tm;

import java.util.HashMap;
import java.util.Map;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.TransactionSynchronizationRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tricon.tm.internal.exception.TriconRuntimeException;

public class TriconTransactionSynchronizationRegistry implements TransactionSynchronizationRegistry, Referenceable, Service {
	private static Logger logger = LoggerFactory.getLogger(TriconTransactionSynchronizationRegistry.class);

	private static TriconTransactionManager transactionManager;

	private static final ThreadLocal<Map<Object, Object>> resources = new ThreadLocal<Map<Object, Object>>() {
		protected Map<Object, Object> initialValue() {
			return new HashMap<Object, Object>();
		}
	};

	public TriconTransactionSynchronizationRegistry() {
		transactionManager = TriconTransactionManagerServices.getTransactionManager();
	}

	public Object getTransactionKey() {
		if (getCurrentTransaction() == null || getCurrentTransactionStatus() == Status.STATUS_NO_TRANSACTION) {
			return null;
		}
		return getCurrentTransaction().getSyncTransactionkey();
	}

	public void putResource(Object key, Object value) {
		if (key == null) {
			throw new NullPointerException("key cannot be null");
		}
		if (getCurrentTransaction() == null || getCurrentTransactionStatus() == Status.STATUS_NO_TRANSACTION) {
			throw new IllegalStateException("no transaction started on current thread");
		}
		Object oldValue = getResources().put(key, value);
		if (oldValue == null && getResources().size() == 1) {
			getCurrentTransaction().getSynchronizationList().add(new RegistryResourcesSynchronization());
		}
	}

	public Object getResource(Object key) {
		if (key == null) {
			throw new NullPointerException("Resource key cannot be null");
		}
		if (getCurrentTransaction() == null || getCurrentTransactionStatus() == Status.STATUS_NO_TRANSACTION) {
			throw new IllegalStateException("No transaction started on current thread");
		}
		return getResources().get(key);
	}

	public void registerInterposedSynchronization(Synchronization sync) {
		if (getCurrentTransaction() == null || getCurrentTransactionStatus() == Status.STATUS_NO_TRANSACTION) {
			throw new IllegalStateException("No transaction started on current thread");
		}
		if (isCurrentTransactionStartedOrFinished()) {
			throw new IllegalStateException("Transaction is started/finished, cannot register an interposed synchronization");
		}
		// TODO: Putting the sync object in a desired position needs to be implemented
		getCurrentTransaction().getSynchronizationList().add(sync);
	}

	public int getTransactionStatus() {
		if (getCurrentTransaction() == null) {
			return Status.STATUS_NO_TRANSACTION;
		}
		return getCurrentTransactionStatus();
	}

	public void setRollbackOnly() {
		if (getCurrentTransaction() == null || getCurrentTransactionStatus() == Status.STATUS_NO_TRANSACTION) {
			throw new IllegalStateException("No transaction started on current thread");
		}
		getCurrentTransaction().setStatus(Status.STATUS_MARKED_ROLLBACK);
	}

	public boolean getRollbackOnly() {
		if (getCurrentTransaction() == null || getCurrentTransactionStatus() == Status.STATUS_NO_TRANSACTION) {
			throw new IllegalStateException("No transaction started on current thread");
		}
		return (getCurrentTransactionStatus() == Status.STATUS_MARKED_ROLLBACK);
	}

	public Reference getReference() throws NamingException {
		return new Reference(TriconTransactionManager.class.getName(),
				new StringRefAddr("TransactionSynchronizationRegistry", "TriconTransactionSynchronizationRegistry"),
				TriconTransactionSynchronizationRegistryObjectFactory.class.getName(), null);
	}
	
	public void shutdown() {
		
	}

	private Map<Object, Object> getResources() {
		return resources.get();
	}

	private TransactionImpl getCurrentTransaction() {
		return transactionManager.getCurrentTransaction();
	}

	private int getCurrentTransactionStatus() {
		try {
			return getCurrentTransaction().getStatus();
		} catch (SystemException ex) {
			throw new TriconRuntimeException("Cannot get current transaction status", ex);
		}
	}

	private boolean isCurrentTransactionStartedOrFinished() {
		int status = getCurrentTransactionStatus();
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

	private class RegistryResourcesSynchronization implements Synchronization {
		public void beforeCompletion() {
		}

		public void afterCompletion(int status) {
			logger.debug("after completion - clearing synchronization registry resources {}", "..");
			getResources().clear();
		}
	}

}
