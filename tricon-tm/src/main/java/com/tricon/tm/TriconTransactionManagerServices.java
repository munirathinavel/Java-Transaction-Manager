package com.tricon.tm;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.transaction.UserTransaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tricon.tm.timer.TaskScheduler;

public class TriconTransactionManagerServices {
	private static Logger logger = LoggerFactory.getLogger(TriconTransactionManagerServices.class);

	private static TriconTransactionManager transactionManager;
	private static ConfigurationHelper configurationHelper;
	private static TaskScheduler taskScheduler;
	private static ExecutorService executorService;
	private static TriconTransactionSynchronizationRegistry transactionSynchronizationRegistry;

	public synchronized static TriconTransactionManager getTransactionManager() {
		if (transactionManager == null) {
			transactionManager = new TriconTransactionManager();
		}
		return transactionManager;
	}

	public synchronized static UserTransaction getUserTransaction() {
		if (transactionManager == null) {
			transactionManager = new TriconTransactionManager();
		}
		return (UserTransaction) transactionManager;
	}

	public synchronized static ConfigurationHelper getConfigurationHelper() {
		if (configurationHelper == null) {
			configurationHelper = new ConfigurationHelper();
		}
		return configurationHelper;
	}

	public synchronized static TaskScheduler getTaskScheduler() {
		if (taskScheduler == null) {
			taskScheduler = new TaskScheduler();
		}
		return taskScheduler;
	}

	public synchronized static ExecutorService getExecutorService() {
		if (executorService == null) {
			if (getConfigurationHelper().isAsynchronous2pc()) {
				executorService = Executors.newCachedThreadPool();
			} else {
				executorService = Executors.newSingleThreadExecutor();
			}
		}
		return executorService;
	}

	public synchronized static TriconTransactionSynchronizationRegistry getTransactionSynchronizationRegistry() {
		if (transactionSynchronizationRegistry == null) {
			transactionSynchronizationRegistry = new TriconTransactionSynchronizationRegistry();
		}
		return transactionSynchronizationRegistry;
	}

	public synchronized static boolean isTransactionManagerRunning() {
		return (transactionManager != null);
	}

	protected synchronized static void shutdownExecutorService() {
		logger.info("Shutting down ExecutorService{}", "..");
		if (executorService != null && !executorService.isShutdown()) {
			executorService.shutdown();
		}
		logger.info("ExecutorService is shutdown{}", ".");
	}
	
	protected synchronized static void dispose() {
		logger.info("Disposing transaction manager related all references{}", "..");
		transactionManager = null;
		configurationHelper = null;
		taskScheduler = null;
		executorService = null;
		transactionSynchronizationRegistry = null;
		logger.info("Disposed transaction manager related all references{}", ".");
	}

}
