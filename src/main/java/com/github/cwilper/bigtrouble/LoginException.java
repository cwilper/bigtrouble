package com.github.cwilper.bigtrouble;

import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.AuthorizationException;

/**
 * Signals a failure to authenticate or authorize when attempting to
 * connect to a node.
 */
public class LoginException extends Exception {

    public LoginException(AuthenticationException cause) {
        super(cause);
    }

    public LoginException(AuthorizationException cause) {
        super(cause);
    }
}
