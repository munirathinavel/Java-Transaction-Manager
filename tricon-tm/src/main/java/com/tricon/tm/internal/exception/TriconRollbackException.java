package com.tricon.tm.internal.exception;

import javax.transaction.RollbackException;

public class TriconRollbackException extends RollbackException {

	public TriconRollbackException(String string) {
		super(string);
	}

	public TriconRollbackException(String string, Throwable t) {
		super(string);
		initCause(t);
	}

}
