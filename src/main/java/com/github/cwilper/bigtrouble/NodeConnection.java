package com.github.cwilper.bigtrouble;

import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.AuthorizationException;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import javax.annotation.PreDestroy;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A connection to a single node in a Cassandra cluster.
 */
public class NodeConnection implements Connection {

    private static final SlicePredicate ALL_COLUMNS = new SlicePredicate();

    static {
        SliceRange range = new SliceRange();
        range.setStart(new byte[0]);
        range.setFinish(new byte[0]);
        range.setCount(Integer.MAX_VALUE);
        ALL_COLUMNS.setSlice_range(range);
    }

    private final String host;
    private final int port;
    private final ConnectionConfig config;

    private final TTransport transport;
    private final Cassandra.Client client;
    private final ConsistencyLevel readConsistency;
    private final ConsistencyLevel writeConsistency;

    private boolean keyspaceIsSet = false;

    /**
     * Creates a connection.
     *
     * @param config the connection configuration to use.
     * @param host the remote host name or IP address.
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
        try {
            transport.open();
            client = new Cassandra.Client(new TBinaryProtocol(transport));
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
        } catch (TException e) {
            throw new RuntimeException(e);
        }
        this.readConsistency = config.getReadConsistency().getConsistencyLevel();
        this.writeConsistency = config.getWriteConsistency().getConsistencyLevel();
    }

    /**
     * Gets the remote host name or IP address.
     *
     * @return the remote host name or IP address.
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
    public boolean keyspaceExists() {
        try {
            client.describe_keyspace(config.getKeyspace());
            return true;
        } catch (NotFoundException e) {
            return false;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addKeyspace(ReplicationStrategy strategy,
                            Map<String, String> options) {
        KsDef def = new KsDef();
        def.setStrategy_class(strategy.getClassName());
        def.setName(config.getKeyspace());
        def.setCf_defs(new ArrayList<CfDef>());
        if (options != null) {
            def.setStrategy_options(options);
        }
        try {
            client.system_add_keyspace(def);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteKeyspace() {
        try {
            client.system_drop_keyspace(config.getKeyspace());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean columnFamilyExists(String name) {
        setKeyspace();
        try {
            KsDef ksDef = client.describe_keyspace(config.getKeyspace());
            for (CfDef cfDef: ksDef.getCf_defs()) {
                if (cfDef.name.equals(name)) {
                    return true;
                }
            }
            return false;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addColumnFamily(String name, String... binaryColumns) {
        setKeyspace();
        CfDef cfDef = new CfDef();
        cfDef.setKeyspace(config.getKeyspace());
        cfDef.setName(name);
        cfDef.setComparator_type("UTF8Type");
        cfDef.setKey_validation_class("UTF8Type");
        cfDef.setDefault_validation_class("UTF8Type");
        try {
            if (binaryColumns.length > 0) {
                List<ColumnDef> cDefs = new ArrayList<ColumnDef>();
                for (String columnName: binaryColumns) {
                    ColumnDef cDef = new ColumnDef();
                    cDef.setName(buffer(columnName));
                    cDef.setValidation_class("BytesType");
                    cDefs.add(cDef);
                }
                cfDef.setColumn_metadata(cDefs);
            }
            client.system_add_column_family(cfDef);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteColumnFamily(String name) {
        setKeyspace();
        try {
            client.system_drop_column_family(name);
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    @Override
    public void addFile(String columnFamily, String key, InputStream in, Map<String, String> metadata) {
        setKeyspace();
    }

    @Override
    public InputStream getFileContent(String columnFamily, String key) {
        setKeyspace();
        return null;
    }

    @Override
    public void deleteFile(String columnFamily, String key) {
        setKeyspace();
        try {

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addRecord(String columnFamily, String key, Map<String, String> columns) {
        setKeyspace();
        ByteBuffer keyBuf = buffer(key);
        long timestamp = System.currentTimeMillis();
        for (String name: columns.keySet()) {
            Column column = new Column();
            column.setName(buffer(name));
            column.setValue(buffer(columns.get(name)));
            column.setTimestamp(timestamp);
            try {
                client.insert(keyBuf, parent(columnFamily), column,
                        config.getWriteConsistency().getConsistencyLevel());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public Map<String, String> getRecord(String columnFamily, String key) {
        setKeyspace();
        try {
            Map<String, String> map = new HashMap<String, String>();
            List<ColumnOrSuperColumn> list = client.get_slice(
                    buffer(key), parent(columnFamily), ALL_COLUMNS,
                    config.getReadConsistency().getConsistencyLevel());
            for (ColumnOrSuperColumn c: list) {
                Column column = c.getColumn();
                map.put(string(column.getName()), string(column.getValue()));
            }
            return map;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean exists(String columnFamily, String key) {
        setKeyspace();
        try {
            Map<String, String> map = new HashMap<String, String>();
            return client.get_count(
                    buffer(key), parent(columnFamily), ALL_COLUMNS,
                    config.getReadConsistency().getConsistencyLevel()) > 0;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteRecord(String columnFamily, String key) {
        setKeyspace();
        try {
            client.remove(buffer(key),
                    path(columnFamily),
                    System.currentTimeMillis(),
                    config.getWriteConsistency().getConsistencyLevel());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @PreDestroy
    public void close() {
        try {
            transport.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            transport.close();
        }
    }

    private void setKeyspace() {
        if (!keyspaceIsSet) {
            try {
                client.set_keyspace(config.getKeyspace());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            keyspaceIsSet = true;
        }
    }

    private static ByteBuffer buffer(String value) {
        try {
            return ByteBuffer.wrap(value.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException wontHappen) {
            throw new RuntimeException(wontHappen);
        }
    }

    private static String string(byte[] bytes) {
        try {
            return new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException wontHappen) {
            throw new RuntimeException(wontHappen);
        }
    }

    private static ColumnParent parent(String columnFamily) {
        ColumnParent parent = new ColumnParent();
        parent.setColumn_family(columnFamily);
        return parent;
    }

    private static ColumnPath path(String columnFamily) {
        ColumnPath path = new ColumnPath();
        path.setColumn_family(columnFamily);
        return path;
    }
}
