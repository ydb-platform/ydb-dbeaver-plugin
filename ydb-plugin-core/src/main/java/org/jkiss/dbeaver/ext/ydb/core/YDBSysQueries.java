package org.jkiss.dbeaver.ext.ydb.core;

/**
 * SQL query constants for YDB system views.
 * Used by both the DBeaver plugin (folder classes) and integration tests.
 * Any change to a query here is automatically reflected in both places.
 */
public final class YDBSysQueries {

    /**
     * Query to load all resource pools from the .sys/resource_pools system view.
     * Column order matches {@code YDBResourcePool} constructor arguments.
     */
    public static final String RESOURCE_POOLS_QUERY =
        "SELECT Name, ConcurrentQueryLimit, QueueSize, DatabaseLoadCpuThreshold, " +
        "ResourceWeight, TotalCpuLimitPercentPerNode, QueryCpuLimitPercentPerNode, " +
        "QueryMemoryLimitPercentPerNode FROM `.sys/resource_pools`";

    /**
     * Query to load all resource pool classifiers from the .sys/resource_pool_classifiers system view.
     */
    public static final String RESOURCE_POOL_CLASSIFIERS_QUERY = "SELECT * FROM `.sys/resource_pool_classifiers`";

    /**
     * Query to load all active streaming queries from the .sys/streaming_queries system view.
     */
    public static final String STREAMING_QUERIES_QUERY = "SELECT * FROM `.sys/streaming_queries`";

    private YDBSysQueries() {
    }
}
