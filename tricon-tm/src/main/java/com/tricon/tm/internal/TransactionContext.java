package com.tricon.tm.internal;

import java.io.Serializable;

import com.tricon.tm.TransactionImpl;
import com.tricon.tm.TriconTransactionManagerServices;

public class TransactionContext implements Serializable {
	private static final long serialVersionUID = 8292793970963356298L;

	private TransactionImpl transaction;
	private int timeout = TriconTransactionManagerServices.getConfigurationHelper().getDefaultTransactionTimeout();

	public TransactionContext() {
	}

	public TransactionContext(TransactionImpl transaction) {
		this.transaction = transaction;
	}

	public TransactionImpl getTransaction() {
		return transaction;
	}

	public void setTransaction(TransactionImpl transaction) {
		this.transaction = transaction;
	}

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	@Override
	public String toString() {
		return new StringBuffer(this.getClass().getName()).append("[")
				.append(", transaction=").append(transaction.toString())
				.append(", timeout=").append(timeout).append(" seconds")
				.append("]").toString();
	}

}
