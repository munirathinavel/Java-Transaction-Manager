package com.tricon.tm.timer;

import com.tricon.tm.TransactionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class TransactionTimeoutTask extends ScheduledTask {
	private static Logger logger = LoggerFactory.getLogger(TransactionTimeoutTask.class);

	private TransactionImpl transaction;

	public TransactionTimeoutTask(TransactionImpl transaction, Date executionTime) {
		super(executionTime);
		this.transaction = transaction;
	}

	public Object getObject() {
		return transaction;
	}

	public void execute() throws TaskException {
		try {
			transaction.timeoutExpired();
		} catch (Exception ex) {
			logger.error("Failed to timeout on transaction: {}", transaction);
			throw new TaskException("Failed to timeout " + transaction, ex);
		}
	}

	@Override
	public String toString() {
		return new StringBuffer(this.getClass().getName()).append("[")
				.append("transction=").append(transaction)
				.append(", executionTime=").append(getExecutionTime())
				.append("]").toString();
	}

}
