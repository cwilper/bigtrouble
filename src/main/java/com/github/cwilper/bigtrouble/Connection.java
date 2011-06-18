package com.github.cwilper.bigtrouble;

import java.io.InputStream;
import java.util.Map;

/**
 * A single-threaded connection to a Cassandra node or cluster.
 */
public interface Connection {

    /**
     * Adds a file.
     *
     * @param columnFamily the column family in which to store the file.
     * @param key the unique id of the file within the column family.
     * @param in the stream containing the content. It will be closed by the
     *        time this method returns, regardless of success or failure.
     * @param columns zero or more name-value pairs that describe the content
     *        (may be given as <code>null</code>).
     * @return true if the file was successfully added, false if the key
     *         was already in use in the column family.
     */
    boolean addFile(String columnFamily, String key, InputStream in,
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
     * Adds a record.
     *
     * @param columnFamily the column family in which to store the record.
     * @param key the unique id of the record within the column family.
     * @param columns one or more name-value pairs that comprise the record.
     * @return true if the record was successfully added, false if the key
     *         was already in use in the column family.
     */
    boolean addRecord(String columnFamily, String key, Map<String, String> columns);

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
     * Tells whether a record or file exists.
     *
     * @param columnFamily the column family in which to look.
     * @param key the unique id of the record or file within the column family.
     * @return true if it exists, false otherwise.
     */
    boolean exists(String columnFamily, String key);

    /**
     * Deletes a record or file.
     *
     * @param columnFamily the column family in which the record or file is stored.
     * @param key the unique id of the record or file within the column family.
     */
    void delete(String columnFamily, String key);

    /**
     * Releases any resources held by this connection.
     */
    void close();

}
