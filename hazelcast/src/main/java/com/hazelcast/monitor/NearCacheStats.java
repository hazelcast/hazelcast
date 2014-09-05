package com.hazelcast.monitor;

public interface NearCacheStats extends LocalInstanceStats {

    /**
     * Returns the creation time of this NearCache on this member
     *
     * @return creation time of this NearCache on this member
     */
    long getCreationTime();

    /**
     * Returns the number of entries owned by this member.
     *
     * @return number of entries owned by this member.
     */
    long getOwnedEntryCount();

    /**
     * Returns memory cost (number of bytes) of entries in this cache.
     *
     * @return memory cost (number of bytes) of entries in this cache.
     */
    long getOwnedEntryMemoryCost();

    /**
     * Returns the number of hits (reads) of the locally owned entries.
     *
     * @return number of hits (reads).
     */
    long getHits();

    /**
     * Returns the number of misses  of the locally owned entries.
     *
     * @return number of misses.
     */
    long getMisses();

    /**
     * Returns the hit/miss ratio  of the locally owned entries.
     *
     * @return hit/miss ratio.
     */
    double getRatio();
}
