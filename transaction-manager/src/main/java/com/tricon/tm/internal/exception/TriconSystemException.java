package com.tricon.tm.internal.exception;

import javax.transaction.SystemException;

public class TriconSystemException extends SystemException {

    public TriconSystemException(int errorCode) {
        super(errorCode);
    }

    public TriconSystemException(String string) {
        super(string);
    }

    public TriconSystemException(String string, Throwable t) {
        super(string);
        initCause(t);
    }

}
