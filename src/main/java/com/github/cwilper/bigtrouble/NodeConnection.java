package com.github.cwilper.bigtrouble;

import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.AuthorizationException;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import javax.annotation.PreDestroy;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * A connection to a single node in a Cassandra cluster.
 */
public class NodeConnection implements Connection {

    private final String host;
    private final int port;
    private final ConnectionConfig config;

    private final TTransport transport;
    private final Cassandra.Client client;
    private final ConsistencyLevel readConsistency;
    private final ConsistencyLevel writeConsistency;

    /**
     * Creates a connection.
     *
     * @param config the connection configuration to use.
     * @param host the remote host name.
     * @param port the remote port number.
     * @throws LoginException if the connection configuration specifies
     *         credentials that are either incorrect or don't provide
     *         access to the keyspace.
     */
    public NodeConnection(ConnectionConfig config,
                          String host,
                          int port)
            throws LoginException {
        this.host = host;
        this.port = port;
        this.config = config;
        transport = new TFramedTransport(new TSocket(host, port));
        client = new Cassandra.Client(new TBinaryProtocol(transport));
        try {
            client.set_keyspace(config.getKeyspace());
            if (config.getUsername() != null) {
                Map<String, String> creds = new HashMap<String, String>();
                creds.put("username", config.getUsername());
                creds.put("password", config.getPassword());
                AuthenticationRequest authRequest = new AuthenticationRequest();
                authRequest.setCredentials(creds);
                client.login(authRequest);
            }
        } catch (AuthenticationException e) {
            throw new LoginException(e);
        } catch (AuthorizationException e) {
            throw new LoginException(e);
        } catch (InvalidRequestException e) {
            throw new RuntimeException(e);
        } catch (TException e) {
            throw new RuntimeException(e);
        }
        this.readConsistency = config.getReadConsistency().getConsistencyLevel();
        this.writeConsistency = config.getWriteConsistency().getConsistencyLevel();
    }

    /**
     * Gets the remote host name.
     *
     * @return the remote host name.
     */
    public String getHost() {
        return host;
    }

    /**
     * Gets the remote port number.
     *
     * @return the remote port number.
     */
    public int getPort() {
        return port;
    }

    @Override
    public boolean addFile(String columnFamily, String key, InputStream in, Map<String, String> metadata) {
        return false;
    }

    @Override
    public InputStream getFileContent(String columnFamily, String key) {
        return null;
    }

    @Override
    public boolean addRecord(String columnFamily, String key, Map<String, String> columns) {
        return false;
    }

    @Override
    public Map<String, String> getRecord(String columnFamily, String key) {
        return null;
    }

    @Override
    public boolean exists(String columnFamily, String key) {
        return false;
    }

    @Override
    public void delete(String columnFamily, String key) {
    }

    @Override
    @PreDestroy
    public void close() {
    }
}
