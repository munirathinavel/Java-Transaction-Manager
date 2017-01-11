package com.tricon.tm.internal.exception;

public class TransactionTimeoutException extends Exception {
    public TransactionTimeoutException(String message) {
        super(message);
    }

    public TransactionTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
