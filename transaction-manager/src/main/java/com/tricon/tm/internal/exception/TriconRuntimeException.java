package com.tricon.tm.internal.exception;

public class TriconRuntimeException extends RuntimeException {
    public TriconRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
    public TriconRuntimeException(String message) {
        super(message);
    }
}
