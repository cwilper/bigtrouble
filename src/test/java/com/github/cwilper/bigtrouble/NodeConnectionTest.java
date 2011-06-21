package com.github.cwilper.bigtrouble;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class NodeConnectionTest {

    private NodeConnection newInstance() {
        ConnectionConfig config = new ConnectionConfig("TestKeyspace");
        try {
            NodeConnection c = new NodeConnection(config, "localhost", 9160);
            // clean up if needed
            if (c.keyspaces().contains("TestKeyspace")) {
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
        Assert.assertFalse(c.keyspaces().contains("TestKeyspace"));
        addKeyspace(c);
        // now it should exist
        Assert.assertTrue(c.keyspaces().contains("TestKeyspace"));
        try {
            c.deleteKeyspace();
            // now it should not exist
            Assert.assertFalse(c.keyspaces().contains("TestKeyspace"));
        } finally {
            c.close();
        }
    }

    @Test
    public void addAndDeleteColumnFamily() throws Exception {
        NodeConnection c = newInstance();
        addKeyspace(c);
        // column family doesn't exist yet
        Assert.assertFalse(c.columnFamilies().contains("TestCF"));
        c.addColumnFamily("TestCF");
        // now it does
        Assert.assertTrue(c.columnFamilies().contains("TestCF"));
        c.deleteColumnFamily("TestCF");
        // shouldn't exist after deletion
        Assert.assertFalse(c.columnFamilies().contains("TestCF"));
    }

    @Test
    public void addAndDeleteRecord() throws Exception {
        NodeConnection c = newInstance();
        addKeyspace(c);
        c.addColumnFamily("TestCF");
        // record doesn't exist yet
        Assert.assertFalse(c.exists("TestCF", "testRecord1"));
        Map<String, String> columns = getColumns(1);
        c.addRecord("TestCF", "testRecord1", columns);
        // now it does
        Assert.assertTrue(c.exists("TestCF", "testRecord1"));
        // and it should have same data as input
        Assert.assertEquals(columns, c.getRecord("TestCF", "testRecord1"));
        c.deleteRecord("TestCF", "testRecord1");
        // shouldn't exist after deletion
        Assert.assertFalse(c.exists("TestCF", "testRecord1"));
    }

    @Test
    public void iterateRecords() throws Exception {
        NodeConnection c = newInstance();
        addKeyspace(c);
        c.addColumnFamily("TestCF");
        Map<String, Map<String, String>> gotRecords;
        gotRecords = getRecords(c, "TestCF", Integer.MAX_VALUE);
        // initially, no records should be iterated
        Assert.assertEquals(0, gotRecords.size());
        Map<String, Map<String, String>> addedRecords;
        addedRecords = addRecords(c, "TestCF", 1);
        Assert.assertEquals(1, addedRecords.size());
        gotRecords = getRecords(c, "TestCF", Integer.MAX_VALUE);
        // then one...the one that was just added
        Assert.assertEquals(addedRecords, gotRecords);
        c.deleteRecord("TestCF", "testRecord1");
        gotRecords = getRecords(c, "TestCF", Integer.MAX_VALUE);
        // then none; it was just deleted
        Assert.assertEquals(0, gotRecords.size());
        addedRecords = addRecords(c, "TestCF", 10);
        Assert.assertEquals(10, addedRecords.size());
        gotRecords = getRecords(c, "TestCF", Integer.MAX_VALUE);
        // then 10...the ones just added
        Assert.assertEquals(addedRecords, gotRecords);
        c.deleteColumnFamily("TestCF");
        c.addColumnFamily("TestCF");
        gotRecords = getRecords(c, "TestCF", Integer.MAX_VALUE);
        // then none; the column family was just deleted and re-added
        Assert.assertEquals(0, gotRecords.size());
        addedRecords = addRecords(c, "TestCF", 22);
        Assert.assertEquals(22, addedRecords.size());
        gotRecords = getRecords(c, "TestCF", Integer.MAX_VALUE);
        // then 22...the ones just added
        Assert.assertEquals(addedRecords, gotRecords);
        // then only 21 of them (for test, execute returns false after 21)
        gotRecords = getRecords(c, "TestCF", 21);
        Assert.assertEquals(21, gotRecords.size());
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