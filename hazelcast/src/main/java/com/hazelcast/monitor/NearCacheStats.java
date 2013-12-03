package com.hazelcast.monitor;

/**
 * User: eminn
 * Date: 03/12/13
 * Time: 10:48 AM
 */
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


}
