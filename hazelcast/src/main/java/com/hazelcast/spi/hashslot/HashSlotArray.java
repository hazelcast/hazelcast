package com.hazelcast.spi.hashslot;

import com.hazelcast.nio.Disposable;

/** <p>
 * Manages the backbone array of an off-heap open-addressed hashtable. The backbone consists
 * of <i>slots</i>, where each slot has a key part and an optional value part. The key part
 * is a {@code long} value and the value part is a block whose size is a multiple of 8. A block
 * of zero size is also possible.
 * </p><p>
 * The update operations on this class only ensure that a slot for a given key exists/doesn't exist
 * and it is up to the caller to manage the contents of the value part. The caller will be provided
 * with the raw address of the value, suitable for accessing with {@code Unsafe} memory operations.
 * <b>The returned address is valid only up to the next map update operation</b>.
 * </p>
 */
public interface HashSlotArray extends Disposable {

    /**
     * Ensures that there is a mapping from the given key to a slot in the array.
     * The {@code abs} of the returned integer is the address of the slot's value block.
     * The returned integer is positive if a new slot had to be assigned and negative
     * if the slot was already assigned.
     *
     * @param key the key
     * @return address of value block
     */
    long ensure(long key);

    /**
     * Returns the address of the value block mapped by the given key.
     *
     * @param key the key
     * @return address of the value block or {@link com.hazelcast.memory.MemoryAllocator#NULL_ADDRESS}
     * if no mapping for key exists.
     */
    long get(long key);

    /**
     * Removes the mapping for the given key.
     *
     * @param key the key
     * @return true if a mapping existed and was removed, false otherwise
     */
    boolean remove(long key);

    /**
     * After this method returns, no key has a mapping in this hash slot array.
     */
    void clear();

    /** Compact the array if allowed by the current size.
     * @return true if the array was compacted; false if no action was taken. */
    boolean trimToSize();

    /**
     * Returns the number of keys in the hash slot array.
     */
    long size();

    /**
     * Returns a cursor over all assigned slots in this array.
     */
    HashSlotCursor cursor();
}
