package com.tricon.tm.internal.exception;

import javax.transaction.HeuristicRollbackException;

public class TriconHeuristicRollbackException extends HeuristicRollbackException {
    
    public TriconHeuristicRollbackException(String string) {
        super(string);
    }

    public TriconHeuristicRollbackException(String string, Throwable t) {
        super(string);
        initCause(t);
    }

}
