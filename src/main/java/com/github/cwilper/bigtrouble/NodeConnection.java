package com.github.cwilper.bigtrouble;

import com.github.cwilper.ttff.AbstractSource;
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
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.commons.io.IOUtils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A connection to a single node in a Cassandra cluster.
 */
public class NodeConnection implements Connection {

    private static final SlicePredicate ALL_COLUMNS = new SlicePredicate();

    private static final Logger logger = LoggerFactory.getLogger(NodeConnection.class);

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

    private boolean keyspaceIsSet = false;

    private boolean closed = false;

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
        logger.trace("Instantiating");
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
            throw new FaultException(e);
        }
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
    public Set<String> keyspaces() {
        logger.trace("Listing keyspaces");
        Set<String> set = new HashSet<String>();
        try {
            for (KsDef ksDef: client.describe_keyspaces()) {
                set.add(ksDef.getName());
            }
            return set;
        } catch (Exception e) {
            throw new FaultException(e);
        }
    }

    @Override
    public boolean addKeyspace(ReplicationStrategy strategy,
                               Map<String, String> options) {
        logger.trace("Adding keyspace");
        KsDef def = new KsDef();
        def.setStrategy_class(strategy.getClassName());
        def.setName(config.getKeyspace());
        def.setCf_defs(new ArrayList<CfDef>());
        if (options != null) {
            def.setStrategy_options(options);
        }
        try {
            client.system_add_keyspace(def);
            return true;
        } catch (InvalidRequestException e) {
            if (e.getWhy().indexOf("already exists") != -1) {
                return false;
            }
            throw new FaultException(e);
        } catch (Exception e) {
            throw new FaultException(e);
        }
    }

    @Override
    public boolean deleteKeyspace() {
        logger.trace("Deleting keyspace");
        try {
            client.system_drop_keyspace(config.getKeyspace());
            return true;
        } catch (InvalidRequestException e) {
            if (e.getWhy().indexOf("does not exist") != -1) {
                return false;
            }
            throw new FaultException(e);
        } catch (Exception e) {
            throw new FaultException(e);
        }
    }

    @Override
    public Set<String> columnFamilies() {
        logger.trace("Listing column families");
        setKeyspace();
        Set<String> set = new HashSet<String>();
        try {
            KsDef ksDef = client.describe_keyspace(config.getKeyspace());
            for (CfDef cfDef: ksDef.getCf_defs()) {
                set.add(cfDef.getName());
            }
            return set;
        } catch (Exception e) {
            throw new FaultException(e);
        }
    }

    @Override
    public boolean addColumnFamily(String name, String... binaryColumns) {
        logger.trace("Adding column family: {}", name);
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
            return true;
        } catch (InvalidRequestException e) {
            if (e.getWhy().indexOf("already exists") != -1) {
                return false;
            }
            throw new FaultException(e);
        } catch (Exception e) {
            throw new FaultException(e);
        }
    }

    @Override
    public boolean deleteColumnFamily(String name) {
        logger.trace("Deleting column family: {}", name);
        setKeyspace();
        try {
            client.system_drop_column_family(name);
            return true;
        } catch (InvalidRequestException e) {
            if (e.getWhy().indexOf("not defined") != -1) {
                return false;
            }
            throw new FaultException(e);
        } catch (Exception e) {
            throw new FaultException(e);
        }
    }

    @Override
    public void putFile(String columnFamily, String key, InputStream in,
                        Map<String, String> columns) {
        logger.trace("Putting file: {} in column family: {}", key, columnFamily);
        setKeyspace();
        if (exists(columnFamily, key)) {
            deleteFile(columnFamily, key);
        }
        if (columns == null) {
            columns = new HashMap<String, String>();
        }
        long timestamp = System.currentTimeMillis();
        long byteCount = 0;
        try {
            // first add the chunks as rows (key = fileKey-chunk-0, data=byte[])
            byte[] buffer = new byte[config.getFileChunkSize()];
            int bytesRead = 0;
            int chunkNum = 0;
            do {
                bytesRead = in.read(buffer, 0, buffer.length);
                if (bytesRead > 0) {
                    Column column = new Column();
                    column.setName(buffer("bytes"));
                    column.setValue(ByteBuffer.wrap(buffer, 0, bytesRead));
                    column.setTimestamp(timestamp);
                    String chunkKey = key + "-chunk-" + chunkNum;
                    logger.trace("Adding chunk: {} to column family: {}", chunkKey, columnFamily);
                    client.insert(buffer(chunkKey),
                            parent(columnFamily),
                            column,
                            config.getWriteConsistency().getConsistencyLevel());
                    chunkNum++;
                    byteCount += bytesRead;
                }
            } while (bytesRead >= 0);
            // then add a row for file metadata
            columns.put("byteCount", "" + byteCount);
            columns.put("chunkSize", "" + config.getFileChunkSize());
            putRecord(columnFamily, key, columns);
        } catch (Exception e) {
            throw new FaultException(e);
        } finally{
            IOUtils.closeQuietly(in);
        }
    }

    @Override
    public InputStream getFileContent(final String columnFamily, final String key) {
        logger.trace("Getting file: {} from column family: {}", key, columnFamily);
        setKeyspace();
        long[] info = getFileInfo(columnFamily, key);
        if (info == null) {
            return null;
        }
        final long chunkCount = info[2];
        return new ChunkedInputStream(new AbstractSource<byte[]>() {
            private long chunkNum;
            @Override
            protected byte[] computeNext() throws IOException {
                if (chunkNum == chunkCount) {
                    return endOfData();
                } else {
                    long i = chunkNum;
                    chunkNum++;
                    return getColumn(columnFamily,
                                     key + "-chunk-" + i,
                                     "bytes").getValue();
                }
            }
        });
    }

    private Column getColumn(String columnFamily, String key, String name) {
        try {
            ColumnPath path = path(columnFamily);
            path.setColumn(buffer(name));
            ColumnOrSuperColumn result = client.get(buffer(key),
                    path, config.getReadConsistency().getConsistencyLevel());
            return result.getColumn();
        } catch (Exception e) {
            throw new FaultException(e);
        }
    }

    @Override
    public void deleteFile(String columnFamily, String key) {
        logger.trace("Deleting file: {} from column family: {}", key, columnFamily);
        setKeyspace();
        long[] info = getFileInfo(columnFamily, key);
        if (info == null) {
            return;
        }
        for (int i = 0; i < info[2]; i++) {
            deleteRecord(columnFamily, key + "-chunk-" + i);
        }
        deleteRecord(columnFamily, key);
    }

    // [0] = byteCount, [1] = chunkSize, [2] = chunkCount
    private long[] getFileInfo(String columnFamily, String key) {
        Map<String, String> columns = getRecord(columnFamily, key);
        if (columns == null) {
            return null;
        }
        long byteCount = Long.parseLong(columns.get("byteCount"));
        long chunkSize = Long.parseLong(columns.get("chunkSize"));
        long chunkCount = byteCount / chunkSize;
        if (byteCount % chunkSize != 0) {
            chunkCount++;
        }
        return new long[] { byteCount, chunkSize, chunkCount };
    }

    @Override
    public void putRecord(String columnFamily, String key, Map<String, String> columns) {
        logger.trace("Putting record: {} in column family: {}", key, columnFamily);
        setKeyspace();
        ByteBuffer keyBuf = buffer(key);
        long timestamp = System.currentTimeMillis();
        for (Map.Entry<String, String> entry: columns.entrySet()) {
            Column column = new Column();
            column.setName(buffer(entry.getKey()));
            column.setValue(buffer(entry.getValue()));
            column.setTimestamp(timestamp);
            try {
                client.insert(keyBuf, parent(columnFamily), column,
                        config.getWriteConsistency().getConsistencyLevel());
            } catch (Exception e) {
                throw new FaultException(e);
            }
        }
    }

    @Override
    public Map<String, String> getRecord(String columnFamily, String key) {
        logger.trace("Getting record: {} from column family: {}", key, columnFamily);
        setKeyspace();
        try {
            return map(client.get_slice(
                    buffer(key), parent(columnFamily), ALL_COLUMNS,
                    config.getReadConsistency().getConsistencyLevel()));
        } catch (Exception e) {
            throw new FaultException(e);
        }
    }

    private static Map<String, String> map(List<ColumnOrSuperColumn> list) {
        Map<String, String> map = new HashMap<String, String>();
        for (ColumnOrSuperColumn c: list) {
            Column column = c.getColumn();
            map.put(string(column.getName()), string(column.getValue()));
        }
        if (map.isEmpty()) {
            return null;
        }
        return map;
    }

    @Override
    public long forEachRecord(String columnFamily, RecordFunction function) {
        logger.trace("Iterating records in column family: {}", columnFamily);
        setKeyspace();
        long count = 0;
        try {
            List<KeySlice> list;
            String lastKey = "";
            boolean doNext = false;
            do {
                KeyRange keyRange = new KeyRange();
                keyRange.setStart_key(buffer(lastKey));
                keyRange.setEnd_key(new byte[0]);
                keyRange.setCount(config.getRecordBatchSize());
                list = client.get_range_slices(
                        parent(columnFamily),
                        ALL_COLUMNS,
                        keyRange,
                        config.getReadConsistency().getConsistencyLevel());
                for (KeySlice keySlice: list) {
                    String key = string(keySlice.getKey());
                    if (!lastKey.equals(key)) {
                        lastKey = string(keySlice.getKey());
                        if (lastKey.indexOf("-chunk-") == -1) { // skip file chunk rows
                            List<ColumnOrSuperColumn> columns = keySlice.getColumns();
                            if (columns.size() > 0) { // skip deleted rows
                                doNext = function.execute(lastKey, map(columns));
                                count++;
                            }
                        }
                    }
                }
            } while (list.size() > 1 && doNext);
            return count;
        } catch (Exception e) {
            throw new FaultException(e);
        }
    }

    @Override
    public boolean exists(String columnFamily, String key) {
        logger.trace("Checking for: {} in column family: {}", key, columnFamily);
        setKeyspace();
        try {
            return client.get_count(
                    buffer(key), parent(columnFamily), ALL_COLUMNS,
                    config.getReadConsistency().getConsistencyLevel()) > 0;
        } catch (Exception e) {
            throw new FaultException(e);
        }
    }

    @Override
    public void deleteRecord(String columnFamily, String key) {
        logger.trace("Deleting record: {} in column family: {}", key, columnFamily);
        setKeyspace();
        try {
            client.remove(buffer(key),
                    path(columnFamily),
                    System.currentTimeMillis(),
                    config.getWriteConsistency().getConsistencyLevel());
        } catch (Exception e) {
            throw new FaultException(e);
        }
    }

    @Override
    @PreDestroy
    public void close() {
        logger.trace("Closing");
        if (!closed) {
            closed = true;
            try {
                transport.flush();
            } catch (TTransportException e) {
                logger.error("Error flushing transport", e);
            } finally {
                transport.close();
            }
        }
    }

    private void setKeyspace() {
        if (!keyspaceIsSet) {
            try {
                client.set_keyspace(config.getKeyspace());
            } catch (Exception e) {
                throw new FaultException(e);
            }
            keyspaceIsSet = true;
        }
    }

    private static ByteBuffer buffer(String value) {
        try {
            return ByteBuffer.wrap(value.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException wontHappen) {
            throw new FaultException(wontHappen);
        }
    }

    private static String string(byte[] bytes) {
        try {
            return new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException wontHappen) {
            throw new FaultException(wontHappen);
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
