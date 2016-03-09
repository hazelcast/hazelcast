package com.hazelcast.spi.hashslot;

/**
 * Cursor over assigned slots in a {@link HashSlotArrayTwinKey}. Initially the cursor's location is
 * before the first map entry and the cursor is invalid.
 */
public interface HashSlotCursorTwinKey {
    /**
     * Advances to the next assigned slot.
     * @return true if the cursor advanced. If false is returned, the cursor is now invalid.
     * @throws IllegalStateException if a previous call to advance() already returned false.
     */
    boolean advance();

    /**
     * @return key part 1 of current slot.
     * @throws IllegalStateException if the cursor is invalid.
     */
    long key1();

    /**
     * @return key part 2 of current slot.
     * @throws IllegalStateException if the cursor is invalid.
     */
    long key2();

    /**
     * @return Address of the current slot's value block.
     * @throws IllegalStateException if the cursor is invalid.
     */
    long valueAddress();
}
