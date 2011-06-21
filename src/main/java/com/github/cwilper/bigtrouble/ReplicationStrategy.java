package com.github.cwilper.bigtrouble;

/**
 * Specifies how replicas are distributed among nodes.
 */
public enum ReplicationStrategy {

    /**
     * Places the first replica at the node whose token is closest to the
     * key (as determined by the Partitioner), and additional replicas on
     * subsequent nodes along the ring in increasing Token order.
     *
     * Supports a single strategy option <code>replication_factor</code> that
     * specifies the replication factor for the cluster.
     */
    SIMPLE("org.apache.cassandra.locator.SimpleStrategy"),

    /**
     * With this strategy, for each datacenter, you can specify how many
     * replicas you want on a per-keyspace basis. Replicas are placed on
     * different racks within each DC, if possible.
     *
     * Supports strategy options which specify the replication factor for
     * each datacenter, where the datacenter name is the key and its
     * replication factor is the value. The replication factor for the entire
     * cluster is the sum of all per datacenter values. Note that the
     * datacenter names must match those used in
     * <code>conf/cassandra-topology.properties</code>.
     */
    NETWORK_TOPOLOGY("org.apache.cassandra.locator.NetworkTopologyStrategy"),

    /**
     * Places one replica in each of two datacenters, and the third on a
     * different rack in in the first. Additional datacenters are not
     * guaranteed to get a replica. Additional replicas after three are
     * placed in ring order after the third without regard to rack or
     * datacenter.
     *
     * Supports a single strategy option <code>replication_factor</code> that
     * specifies the replication factor for the cluster.
     */
    OLD_NETWORK_TOPOLOGY("org.apache.cassandra.locator.OldNetworkTopologyStrategy");

    private final String className;

    ReplicationStrategy(String className) {
        this.className = className;
    }

    /**
     * Gets the Cassandra class name corresponding to the strategy.
     *
     * @return the fully qualified class name.
     */
    String getClassName() {
        return className;
    }
}
