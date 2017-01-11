package com.tricon.tm.internal.exception;

import javax.transaction.xa.XAException;

public class TriconXAException extends XAException {

	public TriconXAException(String message, int errorCode) {
		super(message);
		this.errorCode = errorCode;
	}

	public TriconXAException(String message, int errorCode, Throwable t) {
		super(message);
		this.errorCode = errorCode;
		initCause(t);
	}

	public static boolean isUnilateralRollback(XAException ex) {
		return (ex.errorCode >= XAException.XA_RBBASE && ex.errorCode <= XAException.XA_RBEND)
				|| ex.errorCode == XAException.XAER_NOTA;
	}

}
