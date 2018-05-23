package com.hazelcast.monitor.impl;

/**
 * Provides internal statistics for {@link com.hazelcast.query.impl.Indexes
 * Indexes}.
 */
public interface InternalIndexesStats {

    /**
     * Returns the number of queries performed on the indexes.
     */
    long getQueryCount();

    /**
     * Increments the number of queries performed on the indexes.
     */
    void incrementQueryCount();

    /**
     * Returns the number of indexed queries performed on the indexes.
     */
    long getIndexedQueryCount();

    /**
     * Increments the number of indexed queries performed on the indexes.
     */
    void incrementIndexedQueryCount();

    /**
     * Creates a new instance of internal per-index stats.
     *
     * @param ordered                   {@code true} if the stats are being created
     *                                  for an ordered index, {@code false} otherwise.
     * @param queryableEntriesAreCached {@code true} if the stats are being created
     *                                  for an index for which queryable entries are
     *                                  cached, {@code false} otherwise.
     * @return the created internal per-index stats instance.
     */
    InternalIndexStats createIndexStats(boolean ordered, boolean queryableEntriesAreCached);

    /**
     * Empty no-op internal indexes stats.
     */
    InternalIndexesStats EMPTY = new InternalIndexesStats() {
        @Override
        public long getQueryCount() {
            return 0;
        }

        @Override
        public void incrementQueryCount() {
            // do nothing
        }

        @Override
        public long getIndexedQueryCount() {
            return 0;
        }

        @Override
        public void incrementIndexedQueryCount() {
            // do nothing
        }

        @Override
        public InternalIndexStats createIndexStats(boolean ordered, boolean queryableEntriesAreCached) {
            return InternalIndexStats.EMPTY;
        }
    };

}