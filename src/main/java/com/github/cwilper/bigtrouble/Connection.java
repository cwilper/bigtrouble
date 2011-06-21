package com.github.cwilper.bigtrouble;

import java.io.InputStream;
import java.util.Map;
import java.util.Set;

/**
 * A single-threaded connection to a Cassandra node or cluster.
 */
public interface Connection {

    /**
     * Gets all keyspace names.
     *
     * @return the set of names.
     */
    Set<String> keyspaces();

    /**
     * Adds the keyspace.
     *
     * @param strategy the replication strategy to use.
     * @param options the strategy-specific options to use.
     */
    void addKeyspace(ReplicationStrategy strategy,
                     Map<String, String> options);

    /**
     * Deletes the keyspace and all data within.
     */
    void deleteKeyspace();

    /**
     * Gets all column family names in this keyspace.
     *
     * @return the set of names.
     */
    Set<String> columnFamilies();

    /**
     * Adds a column family.
     *
     * The column family will be created with a UTF8Type comparator and a
     * default validation class of UTF8Type for new columns. Columns for
     * which the validation class should instead be BytesType may be passed
     * as trailing arguments to this method.
     *
     * @param name the name of the column family.
     * @param binaryColumns the names of any columns that should have a
     *        BytesType validation class instead of the default, UTF8Type.
     */
    void addColumnFamily(String name, String... binaryColumns);

    /**
     * Deletes a column family and all data within.
     *
     * @param name the name of the column family.
     */
    void deleteColumnFamily(String name);

    /**
     * Adds a file.
     *
     * @param columnFamily the column family in which to store the file.
     * @param key the unique id of the file within the column family.
     * @param in the stream containing the content. It will be closed by the
     *        time this method returns, regardless of success or failure.
     * @param columns zero or more name-value pairs that describe the content
     *        (may be given as <code>null</code>).
     */
    void addFile(String columnFamily, String key, InputStream in,
            Map<String, String> columns);

    /**
     * Gets the content of a file.
     *
     * @param columnFamily the column family in which it is stored.
     * @param key the unique id of the file within the column family.
     * @return a stream positioned at the beginning of the content, or
     *         <code>null</code> if the file does not exist.
     */
    InputStream getFileContent(String columnFamily, String key);

    /**
     * Deletes a file.
     *
     * @param columnFamily the column family in which the file is stored.
     * @param key the unique id of the file within the column family.
     */
    void deleteFile(String columnFamily, String key);

    /**
     * Adds a record.
     *
     * @param columnFamily the column family in which to store the record.
     * @param key the unique id of the record within the column family.
     * @param columns one or more name-value pairs that comprise the record.
     */
    void addRecord(String columnFamily, String key, Map<String, String> columns);

    /**
     * Gets a record.
     *
     * @param columnFamily the column family in which it is stored.
     * @param key the unique id of the record within the column family.
     * @return one or more name-value pairs that comprise the record, or
     *         <code>null</code> if the record does not exist.
     */
    Map<String, String> getRecord(String columnFamily, String key);

    /**
     * Iterates records in a column family, passing each to the given function.
     * All records will be iterated (in no particular order). The function can
     * cause iteration to complete early by returning <code>false</code>.
     *
     * @param columnFamily the column family whose records should be iterated.
     * @param function the function to execute.
     */
    void forEachRecord(String columnFamily, RecordFunction function);

    /**
     * Deletes a record.
     *
     * @param columnFamily the column family in which the record is stored.
     * @param key the unique id of the record within the column family.
     */
    void deleteRecord(String columnFamily, String key);

    /**
     * Tells whether a record or file exists.
     *
     * @param columnFamily the column family in which to look.
     * @param key the unique id of the record or file within the column family.
     * @return true if it exists, false otherwise.
     */
    boolean exists(String columnFamily, String key);

    /**
     * Releases any resources held by this connection.
     */
    void close();

}
