package com.github.cwilper.bigtrouble;

public class FaultException extends RuntimeException {

    FaultException(Throwable cause) {
        super(cause);
    }

    FaultException(String message, Throwable cause) {
        super(message, cause);
    }
}
