package com.github.cwilper.bigtrouble;

import org.junit.Test;
import org.junit.Assert;

import java.util.HashMap;
import java.util.Map;

public class NodeConnectionTest {

    private NodeConnection newInstance() {
        ConnectionConfig config = new ConnectionConfig("TestKeyspace");
        try {
            NodeConnection c = new NodeConnection(config, "localhost", 9160);
            // clean up if needed
            if (c.keyspaceExists()) {
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
        Assert.assertFalse(c.keyspaceExists());
        addKeyspace(c);
        // now it should exist
        Assert.assertTrue(c.keyspaceExists());
        try {
            c.deleteKeyspace();
            // now it should not exist
            Assert.assertFalse(c.keyspaceExists());
        } finally {
            c.close();
        }
    }

    @Test
    public void addAndDeleteColumnFamily() throws Exception {
        NodeConnection c = newInstance();
        addKeyspace(c);
        // column family doesn't exist yet
        Assert.assertFalse(c.columnFamilyExists("TestCF"));
        c.addColumnFamily("TestCF");
        // now it does
        Assert.assertTrue(c.columnFamilyExists("TestCF"));
        c.deleteColumnFamily("TestCF");
        // shouldn't exist after deletion
        Assert.assertFalse(c.columnFamilyExists("TestCF"));
    }

    @Test
    public void addAndDeleteRecord() throws Exception {
        NodeConnection c = newInstance();
        addKeyspace(c);
        c.addColumnFamily("TestCF");
        // record doesn't exist yet
        Assert.assertFalse(c.exists("TestCF", "testRecord"));
        Map<String, String> inputMap = new HashMap<String, String>();
        inputMap.put("name1", "value1");
        inputMap.put("name2", "value2");
        c.addRecord("TestCF", "testRecord", inputMap);
        // now it does
        Assert.assertTrue(c.exists("TestCF", "testRecord"));
        // and it should have same data as input
        Assert.assertEquals(inputMap, c.getRecord("TestCF", "testRecord"));
        c.deleteRecord("TestCF", "testRecord");
        // shouldn't exist after deletion
        Assert.assertFalse(c.exists("TestCF", "testRecord"));
    }

    private static void addKeyspace(Connection c) {
        Map<String, String> options = new HashMap<String, String>();
        options.put("replication_factor", "1");
        c.addKeyspace(ReplicationStrategy.SIMPLE, options);
    }


}