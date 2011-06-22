package com.github.cwilper.bigtrouble;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for <code>NodeConnection</code>. An open Cassandra node must be
 * running at localhost, port 9160 (the default Thrift port) to run.
 */
public class NodeConnectionTest {

    private static final String TEST_COLUMN_FAMILY = "TestColumnFamily";
    private static final String TEST_FILE = "testFile";
    private static final String TEST_KEYSPACE = "TestKeyspace";
    private static final String TEST_RECORD = "testRecord";

    private static final String CHAR_ENCODING = "UTF-8";

    private static final String HOST = "localhost";
    private static final int PORT = 9160;

    private static final int RECORD_BATCH_SIZE = 5;
    private static final int FILE_CHUNK_SIZE = 1024 * 1024;

    private NodeConnection newInstance() {
        ConnectionConfig config = new ConnectionConfig(TEST_KEYSPACE);
        config.setRecordBatchSize(RECORD_BATCH_SIZE);
        config.setFileChunkSize(FILE_CHUNK_SIZE);
        try {
            NodeConnection c = new NodeConnection(config, HOST, PORT);
            // clean up if needed
            if (c.keyspaces().contains(TEST_KEYSPACE)) {
                c.deleteKeyspace();
            }
            return c;
        } catch (LoginException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void instantiate() throws Exception {
        NodeConnection c = newInstance();
        c.close();
    }

    @Test
    public void addAndDeleteKeyspace() throws Exception {
        NodeConnection c = newInstance();
        // initially, keyspace does not exist
        Assert.assertFalse(c.keyspaces().contains(TEST_KEYSPACE));
        addKeyspace(c);
        // now it should exist
        Assert.assertTrue(c.keyspaces().contains(TEST_KEYSPACE));
        try {
            c.deleteKeyspace();
            // now it should not exist
            Assert.assertFalse(c.keyspaces().contains(TEST_KEYSPACE));
        } finally {
            c.close();
        }
    }

    @Test
    public void addAndDeleteColumnFamily() throws Exception {
        NodeConnection c = newInstance();
        addKeyspace(c);
        // column family doesn't exist yet
        Assert.assertFalse(c.columnFamilies().contains(TEST_COLUMN_FAMILY));
        c.addColumnFamily(TEST_COLUMN_FAMILY);
        // now it does
        Assert.assertTrue(c.columnFamilies().contains(TEST_COLUMN_FAMILY));
        c.deleteColumnFamily(TEST_COLUMN_FAMILY);
        // shouldn't exist after deletion
        Assert.assertFalse(c.columnFamilies().contains(TEST_COLUMN_FAMILY));
    }

    @Test
    public void addGetDeleteRecord() throws Exception {
        NodeConnection c = newInstance();
        addKeyspace(c);
        c.addColumnFamily(TEST_COLUMN_FAMILY);
        // record doesn't exist yet
        Assert.assertFalse(c.exists(TEST_COLUMN_FAMILY, TEST_RECORD));
        Map<String, String> columns = getColumns(1);
        c.addRecord(TEST_COLUMN_FAMILY, TEST_RECORD, columns);
        // now it does
        Assert.assertTrue(c.exists(TEST_COLUMN_FAMILY, TEST_RECORD));
        // and it should have same data as input
        Assert.assertEquals(columns, c.getRecord(TEST_COLUMN_FAMILY, TEST_RECORD));
        c.deleteRecord(TEST_COLUMN_FAMILY, TEST_RECORD);
        // shouldn't exist after deletion
        Assert.assertFalse(c.exists(TEST_COLUMN_FAMILY, TEST_RECORD));
    }

    @Test
    public void addGetDeleteFile() throws Exception {
        NodeConnection c = newInstance();
        addKeyspace(c);
        c.addColumnFamily(TEST_COLUMN_FAMILY, "bytes");
        // file doesn't exist yet
        Assert.assertFalse(c.exists(TEST_COLUMN_FAMILY, TEST_FILE));
        String content = "Here's the content";
        c.addFile(TEST_COLUMN_FAMILY, TEST_FILE, getStream(content), null);
        // now it does
        Assert.assertTrue(c.exists(TEST_COLUMN_FAMILY, TEST_FILE));
        Assert.assertTrue(c.exists(TEST_COLUMN_FAMILY, TEST_FILE + "-chunk-0"));
        InputStream in = c.getFileContent(TEST_COLUMN_FAMILY, TEST_FILE);
        // what we get back should be exactly what we stored
        Assert.assertEquals(content, IOUtils.toString(in, CHAR_ENCODING));
        c.deleteFile(TEST_COLUMN_FAMILY, TEST_FILE);
        // now it doesn't
        Assert.assertFalse(c.exists(TEST_COLUMN_FAMILY, TEST_FILE));
        Assert.assertFalse(c.exists(TEST_COLUMN_FAMILY, TEST_FILE + "-chunk-0"));
        // now do a few multi-chunk file tests
        long twoChunksOfBytes = FILE_CHUNK_SIZE * 2;
        String key;

        key = TEST_FILE + "1";
        fileTest(c, key, twoChunksOfBytes - 1);
        Assert.assertTrue(c.exists(TEST_COLUMN_FAMILY, key + "-chunk-0"));
        Assert.assertTrue(c.exists(TEST_COLUMN_FAMILY, key + "-chunk-1"));
        Assert.assertFalse(c.exists(TEST_COLUMN_FAMILY, key + "-chunk-2"));

        key = TEST_FILE + "2";
        fileTest(c, key, twoChunksOfBytes);
        Assert.assertTrue(c.exists(TEST_COLUMN_FAMILY, key + "-chunk-0"));
        Assert.assertTrue(c.exists(TEST_COLUMN_FAMILY, key + "-chunk-1"));
        Assert.assertFalse(c.exists(TEST_COLUMN_FAMILY, key + "-chunk-2"));

        key = TEST_FILE + "3";
        fileTest(c, key, twoChunksOfBytes + 1);
        Assert.assertTrue(c.exists(TEST_COLUMN_FAMILY, key + "-chunk-0"));
        Assert.assertTrue(c.exists(TEST_COLUMN_FAMILY, key + "-chunk-1"));
        Assert.assertTrue(c.exists(TEST_COLUMN_FAMILY, key + "-chunk-2"));
        Assert.assertFalse(c.exists(TEST_COLUMN_FAMILY, key + "-chunk-3"));
    }

    private void fileTest(Connection c, String key, long numBytes) throws Exception {
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < numBytes; i++) {
            s.append('.');
        }
        String input = s.toString();
        c.addFile(TEST_COLUMN_FAMILY, key, getStream(input), null);
        String output = IOUtils.toString(c.getFileContent(TEST_COLUMN_FAMILY, key));
        Assert.assertEquals(input, output);
    }

    private static InputStream getStream(String string) {
        try {
            return new ByteArrayInputStream(string.getBytes(CHAR_ENCODING));
        } catch (Exception wontHappen) {
            throw new RuntimeException(wontHappen);
        }
    }

    @Test
    public void iterateRecords() throws Exception {
        NodeConnection c = newInstance();
        addKeyspace(c);
        c.addColumnFamily(TEST_COLUMN_FAMILY);
        Map<String, Map<String, String>> gotRecords;
        gotRecords = getRecords(c, TEST_COLUMN_FAMILY, Integer.MAX_VALUE);
        // initially, no records should be iterated
        Assert.assertEquals(0, gotRecords.size());
        Map<String, Map<String, String>> addedRecords;
        addedRecords = addRecords(c, TEST_COLUMN_FAMILY, 1);
        Assert.assertEquals(1, addedRecords.size());
        gotRecords = getRecords(c, TEST_COLUMN_FAMILY, Integer.MAX_VALUE);
        // then one...the one that was just added
        Assert.assertEquals(addedRecords, gotRecords);
        c.deleteRecord(TEST_COLUMN_FAMILY, "testRecord1");
        gotRecords = getRecords(c, TEST_COLUMN_FAMILY, Integer.MAX_VALUE);
        // then none; it was just deleted
        Assert.assertEquals(0, gotRecords.size());
        int numRecords = RECORD_BATCH_SIZE * 2;
        addedRecords = addRecords(c, TEST_COLUMN_FAMILY, numRecords);
        Assert.assertEquals(numRecords, addedRecords.size());
        gotRecords = getRecords(c, TEST_COLUMN_FAMILY, Integer.MAX_VALUE);
        // then a few...the ones just added
        Assert.assertEquals(addedRecords, gotRecords);
        c.deleteColumnFamily(TEST_COLUMN_FAMILY);
        c.addColumnFamily(TEST_COLUMN_FAMILY);
        gotRecords = getRecords(c, TEST_COLUMN_FAMILY, Integer.MAX_VALUE);
        // then none; the column family was just deleted and re-added
        Assert.assertEquals(0, gotRecords.size());
        numRecords = RECORD_BATCH_SIZE * 4 + 2;
        addedRecords = addRecords(c, TEST_COLUMN_FAMILY, numRecords);
        Assert.assertEquals(numRecords, addedRecords.size());
        gotRecords = getRecords(c, TEST_COLUMN_FAMILY, Integer.MAX_VALUE);
        // then more...the ones just added
        Assert.assertEquals(addedRecords, gotRecords);
        // then more, with a maximum of one less than what we inserted
        numRecords--;
        gotRecords = getRecords(c, TEST_COLUMN_FAMILY, numRecords);
        Assert.assertEquals(numRecords, gotRecords.size());
    }

    private static void addKeyspace(Connection c) {
        Map<String, String> options = new HashMap<String, String>();
        options.put("replication_factor", "1");
        c.addKeyspace(ReplicationStrategy.SIMPLE, options);
    }

    private static Map<String, Map<String, String>> addRecords(
            Connection c, String columnFamily, int count) {
        Map<String, Map<String, String>> records =
                new HashMap<String, Map<String, String>>();
        for (int i = 0; i < count; i++) {
            int n = i + 1;
            String key = "testRecord" + n;
            Map<String, String> columns = getColumns(n);
            c.addRecord(columnFamily, key, columns);
            records.put(key, columns);
        }
        return records;
    }

    private static Map<String, String> getColumns(int n) {
        Map<String, String> columns = new HashMap<String, String>();
        columns.put("recordNum", n + "");
        int t = n * 2;
        columns.put("timesTwo", t + "");
        return columns;
    }

    private static Map<String, Map<String, String>> getRecords(
            Connection c, String columnFamily, int maxRecords) {
        TestRecordFunction func = new TestRecordFunction(maxRecords);
        c.forEachRecord(columnFamily, func);
        return func.getRecords();
    }

    static class TestRecordFunction implements RecordFunction {

        private final int maxRecords;
        private final Map<String, Map<String, String>> records =
                new HashMap<String, Map<String, String>>();
        private int numRecords;

        public TestRecordFunction(int maxRecords) {
            this.maxRecords = maxRecords;
        }

        public Map<String, Map<String, String>> getRecords() {
            return records;
        }

        @Override
        public boolean execute(String key, Map<String, String> columns) {
            records.put(key, columns);
            numRecords++;
            return numRecords < maxRecords;
        }

    }

}