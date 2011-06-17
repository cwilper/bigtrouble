package com.github.cwilper.bigtrouble;

import org.apache.cassandra.thrift.ConsistencyLevel;

/**
 * Tunable consistency settings for read and write operations in the cluster.
 */
public enum Consistency {

    /**
     * For writes, this level ensures that it has been written to at least 1
     * node, including
     * <a href="http://wiki.apache.org/cassandra/HintedHandoff">Hinted
     * Handoff</a> recipients. This level is not applicable to reads.
     */
    ANY,


    /**
     * For writes, this level ensures that it has been written to at least 1
     * replica's commit log and memory table. For reads, this level will
     * return the requested data from the first replica to respond.
     */
    ONE,

    /**
     * For writes, this level ensures that it has been written to
     * ReplicationFactor / 2 + 1 replicas. For reads, this level will cause
     * a query to all replicas and will return the data with the most recent
     * timestamp once a majority have responded.
     */
    QUORUM,

    /**
     * Same as <code>QUORUM</code>, but requires a quorum only within the
     * local datacenter.
     */
    LOCAL_QUORUM,

    /**
     * Same as <code>QUORUM</code>, but requires a quorum within each
     * datacenter.
     */
    EACH_QUORUM,
    ALL;

    /**
     * Gets the equivalent enum value as defined in the cassandra-thrift API.
     *
     * @return the equivalent ConsistencyLevel enum value.
     */
    ConsistencyLevel getConsistencyLevel() {
        if (this.equals(ANY)) {
            return ConsistencyLevel.ANY;
        } else if (this.equals(ONE)) {
            return ConsistencyLevel.ONE;
        } else if (this.equals(QUORUM)) {
            return ConsistencyLevel.QUORUM;
        } else if (this.equals(LOCAL_QUORUM)) {
            return ConsistencyLevel.LOCAL_QUORUM;
        } else if (this.equals(EACH_QUORUM)) {
            return ConsistencyLevel.EACH_QUORUM;
        } else {
            return ConsistencyLevel.ALL;
        }
    }

}
