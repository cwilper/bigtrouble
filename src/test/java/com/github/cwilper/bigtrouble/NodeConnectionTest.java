package com.github.cwilper.bigtrouble;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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

    private Connection c;

    @Before
    public void init() throws LoginException {
        ConnectionConfig config = new ConnectionConfig(TEST_KEYSPACE);
        config.setRecordBatchSize(RECORD_BATCH_SIZE);
        config.setFileChunkSize(FILE_CHUNK_SIZE);
        c = new NodeConnection(config, HOST, PORT);
        c.deleteKeyspace();
    }

    @After
    public void destroy() {
        c.deleteKeyspace();
        c.close();
    }

    @Test
    public void multiClose() throws Exception {
        c.close();
        c.close();
        init();
    }

    @Test
    public void addAndDeleteKeyspace() throws Exception {
        // initially, keyspace does not exist
        Assert.assertFalse(c.keyspaces().contains(TEST_KEYSPACE));
        Assert.assertTrue(addKeyspace());
        // now it should exist
        Assert.assertTrue(c.keyspaces().contains(TEST_KEYSPACE));
        Assert.assertTrue(c.deleteKeyspace());
        // now it should not exist
        Assert.assertFalse(c.keyspaces().contains(TEST_KEYSPACE));
    }

    @Test
    public void addExistingKeyspace() throws Exception {
        Assert.assertTrue(addKeyspace());
        Assert.assertFalse(addKeyspace());
    }

    @Test
    public void deleteNonExistingKeyspace() throws Exception {
        Assert.assertFalse(c.deleteKeyspace());
    }

    @Test
    public void addAndDeleteColumnFamily() throws Exception {
        Assert.assertTrue(addKeyspace());
        // column family doesn't exist yet
        Assert.assertFalse(c.columnFamilies().contains(TEST_COLUMN_FAMILY));
        Assert.assertTrue(c.addColumnFamily(TEST_COLUMN_FAMILY));
        // now it does
        Assert.assertTrue(c.columnFamilies().contains(TEST_COLUMN_FAMILY));
        Assert.assertTrue(c.deleteColumnFamily(TEST_COLUMN_FAMILY));
        // shouldn't exist after deletion
        Assert.assertFalse(c.columnFamilies().contains(TEST_COLUMN_FAMILY));
    }

    @Test
    public void addExistingColumnFamily() throws Exception {
        Assert.assertTrue(addKeyspace());
        Assert.assertTrue(c.addColumnFamily(TEST_COLUMN_FAMILY));
        Assert.assertFalse(c.addColumnFamily(TEST_COLUMN_FAMILY));
    }

    @Test
    public void deleteNonExistingColumnFamily() throws Exception {
        Assert.assertTrue(addKeyspace());
        Assert.assertFalse(c.deleteColumnFamily(TEST_COLUMN_FAMILY));
    }

    @Test
    public void addGetDeleteRecord() throws Exception {
        Assert.assertTrue(addKeyspace());
        Assert.assertTrue(c.addColumnFamily(TEST_COLUMN_FAMILY));
        // record doesn't exist yet
        Assert.assertFalse(c.exists(TEST_COLUMN_FAMILY, TEST_RECORD));
        Map<String, String> columns = getColumns(1);
        c.putRecord(TEST_COLUMN_FAMILY, TEST_RECORD, columns);
        // now it does
        Assert.assertTrue(c.exists(TEST_COLUMN_FAMILY, TEST_RECORD));
        // and it should have same data as input
        Assert.assertEquals(columns, c.getRecord(TEST_COLUMN_FAMILY, TEST_RECORD));
        c.deleteRecord(TEST_COLUMN_FAMILY, TEST_RECORD);
        // shouldn't exist after deletion
        Assert.assertFalse(c.exists(TEST_COLUMN_FAMILY, TEST_RECORD));
    }

    @Test
    public void putExistingRecord() throws Exception {
        Assert.assertTrue(addKeyspace());
        Assert.assertTrue(c.addColumnFamily(TEST_COLUMN_FAMILY));
        Map<String, String> columns = getColumns(1);
        c.putRecord(TEST_COLUMN_FAMILY, TEST_RECORD, columns);
        columns.put("timesTwo", "huh");
        columns.put("column3", "value3");
        c.putRecord(TEST_COLUMN_FAMILY, TEST_RECORD, columns);
        Map<String, String> outCols = c.getRecord(TEST_COLUMN_FAMILY, TEST_RECORD);
        Assert.assertEquals(columns, outCols);
    }

    @Test
    public void getNonExistingRecord() throws Exception {
        Assert.assertTrue(addKeyspace());
        Assert.assertTrue(c.addColumnFamily(TEST_COLUMN_FAMILY));
        Assert.assertNull(c.getRecord(TEST_COLUMN_FAMILY, TEST_RECORD));
    }

    @Test
    public void deleteNonExistingRecord() throws Exception {
        Assert.assertTrue(addKeyspace());
        Assert.assertTrue(c.addColumnFamily(TEST_COLUMN_FAMILY));
        c.deleteRecord(TEST_COLUMN_FAMILY, TEST_RECORD);
    }

    @Test
    public void addGetDeleteFile() throws Exception {
        Assert.assertTrue(addKeyspace());
        Assert.assertTrue(c.addColumnFamily(TEST_COLUMN_FAMILY, "bytes"));
        // file doesn't exist yet
        Assert.assertFalse(c.exists(TEST_COLUMN_FAMILY, TEST_FILE));
        String content = "Here's the content";
        c.putFile(TEST_COLUMN_FAMILY, TEST_FILE, getStream(content), null);
        // now it does
        Assert.assertTrue(c.exists(TEST_COLUMN_FAMILY, TEST_FILE));
        Assert.assertTrue(c.exists(TEST_COLUMN_FAMILY, TEST_FILE + "-chunk-0"));
        InputStream in = c.getFileContent(TEST_COLUMN_FAMILY, TEST_FILE);
        Assert.assertNotNull(in);
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

    @Test
    public void putExistingFile() throws Exception {
        Assert.assertTrue(addKeyspace());
        Assert.assertTrue(c.addColumnFamily(TEST_COLUMN_FAMILY, "bytes"));
        c.putFile(TEST_COLUMN_FAMILY, TEST_FILE, getStream("a"), null);
        c.putFile(TEST_COLUMN_FAMILY, TEST_FILE, getStream("b"), null);
        InputStream in = c.getFileContent(TEST_COLUMN_FAMILY, TEST_FILE);
        Assert.assertEquals("b", IOUtils.toString(in, CHAR_ENCODING));
    }

    @Test
    public void getNonExistingFileContent() throws Exception {
        Assert.assertTrue(addKeyspace());
        Assert.assertTrue(c.addColumnFamily(TEST_COLUMN_FAMILY));
        Assert.assertNull(c.getFileContent(TEST_COLUMN_FAMILY, TEST_RECORD));
    }

    @Test
    public void deleteNonExistingFile() throws Exception {
        Assert.assertTrue(addKeyspace());
        Assert.assertTrue(c.addColumnFamily(TEST_COLUMN_FAMILY, "bytes"));
        c.deleteFile(TEST_COLUMN_FAMILY, TEST_FILE);
    }

    private void fileTest(Connection c, String key, long numBytes) throws Exception {
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < numBytes; i++) {
            s.append('.');
        }
        String input = s.toString();
        c.putFile(TEST_COLUMN_FAMILY, key, getStream(input), null);
        InputStream in = c.getFileContent(TEST_COLUMN_FAMILY, key);
        Assert.assertNotNull(in);
        String output = IOUtils.toString(in, CHAR_ENCODING);
        Assert.assertEquals(input, output);
    }

    private static InputStream getStream(String string) {
        try {
            return new ByteArrayInputStream(string.getBytes(CHAR_ENCODING));
        } catch (Exception wontHappen) {
            throw new FaultException(wontHappen);
        }
    }

    @Test
    public void iterateRecords() throws Exception {
        Assert.assertTrue(addKeyspace());
        Assert.assertTrue(c.addColumnFamily(TEST_COLUMN_FAMILY));
        Map<String, Map<String, String>> gotRecords;
        gotRecords = getRecords(Integer.MAX_VALUE, 0);
        // initially, no records should be iterated
        Assert.assertEquals(0, gotRecords.size());
        Map<String, Map<String, String>> addedRecords;
        addedRecords = addRecords(c, TEST_COLUMN_FAMILY, 1);
        Assert.assertEquals(1, addedRecords.size());
        gotRecords = getRecords(Integer.MAX_VALUE, 1);
        // then one...the one that was just added
        Assert.assertEquals(addedRecords, gotRecords);
        c.deleteRecord(TEST_COLUMN_FAMILY, "testRecord1");
        gotRecords = getRecords(Integer.MAX_VALUE, 0);
        // then none; it was just deleted
        Assert.assertEquals(0, gotRecords.size());
        int numRecords = RECORD_BATCH_SIZE * 2;
        addedRecords = addRecords(c, TEST_COLUMN_FAMILY, numRecords);
        Assert.assertEquals(numRecords, addedRecords.size());
        gotRecords = getRecords(Integer.MAX_VALUE, numRecords);
        // then a few...the ones just added
        Assert.assertEquals(addedRecords, gotRecords);
        Assert.assertTrue(c.deleteColumnFamily(TEST_COLUMN_FAMILY));
        Assert.assertTrue(c.addColumnFamily(TEST_COLUMN_FAMILY));
        gotRecords = getRecords(Integer.MAX_VALUE, 0);
        // then none; the column family was just deleted and re-added
        Assert.assertEquals(0, gotRecords.size());
        numRecords = RECORD_BATCH_SIZE * 4 + 2;
        addedRecords = addRecords(c, TEST_COLUMN_FAMILY, numRecords);
        Assert.assertEquals(numRecords, addedRecords.size());
        gotRecords = getRecords(Integer.MAX_VALUE, numRecords);
        // then more...the ones just added
        Assert.assertEquals(addedRecords, gotRecords);
        // then more, with a maximum of one less than what we inserted
        numRecords--;
        gotRecords = getRecords(numRecords, numRecords);
        Assert.assertEquals(numRecords, gotRecords.size());
    }

    private boolean addKeyspace() {
        Map<String, String> options = new HashMap<String, String>();
        options.put("replication_factor", "1");
        return c.addKeyspace(ReplicationStrategy.SIMPLE, options);
    }

    private static Map<String, Map<String, String>> addRecords(
            Connection c, String columnFamily, int count) {
        Map<String, Map<String, String>> records =
                new HashMap<String, Map<String, String>>();
        for (int i = 0; i < count; i++) {
            int n = i + 1;
            String key = "testRecord" + n;
            Map<String, String> columns = getColumns(n);
            c.putRecord(columnFamily, key, columns);
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

    private Map<String, Map<String, String>> getRecords(int maxRecords, long expectedCount) {
        TestRecordFunction func = new TestRecordFunction(maxRecords);
        Assert.assertEquals(expectedCount, c.forEachRecord(TEST_COLUMN_FAMILY, func));
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