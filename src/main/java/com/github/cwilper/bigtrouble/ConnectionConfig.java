package com.github.cwilper.bigtrouble;

/**
 * Configuration for node or cluster connections.
 */
public class ConnectionConfig {

    /**
     * The default read consistency level (ONE).
     */
    public static final Consistency DEFAULT_READ_CONSISTENCY = Consistency.ONE;

    /**
     * The default write consistency level (ANY).
     */
    public static final Consistency DEFAULT_WRITE_CONSISTENCY = Consistency.ANY;

    /**
     * The default chunk size to use when writing new files (8mb).
     */
    public static final int DEFAULT_FILE_CHUNK_SIZE = 8 * 1024 * 1024;

    /**
     * The default maximum number of records to retrieve at once via
     * Connection.forEachRecord.
     */
    public static final int DEFAULT_RECORD_BATCH_SIZE = 5;



    private final String keyspace;

    private String username;
    private String password;
    private Consistency readConsistency;
    private Consistency writeConsistency;
    private int fileChunkSize;
    private int recordBatchSize;

    /**
     * Creates a configuration for use with the given keyspace, with default
     * values and undefined credentials.
     *
     * @param keyspace the keyspace to which the connection is tied.
     */
    public ConnectionConfig(String keyspace) {
        this.keyspace = keyspace;
        this.readConsistency = DEFAULT_READ_CONSISTENCY;
        this.writeConsistency = DEFAULT_WRITE_CONSISTENCY;
        this.fileChunkSize = DEFAULT_FILE_CHUNK_SIZE;
        this.recordBatchSize = DEFAULT_RECORD_BATCH_SIZE;
    }

    /**
     * Gets the keyspace.
     *
     * @return the keyspace.
     */
    public String getKeyspace() {
        return keyspace;
    }

    /**
     * Gets the username.
     *
     * @return the username, or <code>null</code> if undefined.
     */
    public String getUsername() {
        return username;
    }

    /**
     * Sets the username.
     *
     * @param username the username.
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * Gets the password.
     *
     * @return the password, or <code>null</code> if undefined.
     */
    public String getPassword() {
        return password;
    }

    /**
     * Sets the password.
     *
     * @param password the password.
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Gets the consistency level to use for all reads on this connection.
     *
     * @return the consistency level.
     */
    public Consistency getReadConsistency() {
        return readConsistency;
    }

    /**
     * Sets the consistency level to use for all reads on this connection.
     *
     * @param readConsistency the consistency level.
     */
    public void setReadConsistency(Consistency readConsistency) {
        if (readConsistency.equals(Consistency.ANY)) {
            throw new IllegalArgumentException("ANY is not a valid read consistency level");
        }
        this.readConsistency = readConsistency;
    }

    /**
     * Gets the consistency level to use for all writes on this connection.
     *
     * @return the consistency level.
     */
    public Consistency getWriteConsistency() {
        return writeConsistency;
    }

    /**
     * Sets the consistency level to use for all writes on this connection.
     *
     * @param writeConsistency the consistency level.
     */
    public void setWriteConsistency(Consistency writeConsistency) {
        this.writeConsistency = writeConsistency;
    }

    /**
     * Gets the file chunk size that will be used when writing files on
     * this connection.
     *
     * @return the file chunk size, in bytes.
     */
    public int getFileChunkSize() {
        return fileChunkSize;
    }

    /**
     * Sets the file chunk size that will be used when writing files on
     * this connection.
     *
     * @param fileChunkSize the file chunk size, in bytes.
     */
    public void setFileChunkSize(int fileChunkSize) {
        this.fileChunkSize = fileChunkSize;
    }

    /**
     * Gets the maximum number of records to that should be retrieved from
     * the cluster at once (in memory) when calling Connection.forEachRecord.
     *
     * @return the maximum number of records to retrieve at once.
     */
    public int getRecordBatchSize() {
        return recordBatchSize;
    }

    /**
     * Sets the maximum number of records to that should be retrieved from
     * the cluster at once (in memory) when calling Connection.forEachRecord.
     *
     * @param recordBatchSize maximum number of records to retrieve at once.
     */
    public void setRecordBatchSize(int recordBatchSize) {
        if (recordBatchSize < 2) {
            throw new IllegalArgumentException("Record batch size must be at least 2");
        }
        this.recordBatchSize = recordBatchSize;
    }

}
