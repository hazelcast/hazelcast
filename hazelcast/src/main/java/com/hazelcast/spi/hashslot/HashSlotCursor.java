package com.hazelcast.spi.hashslot;

/**
 * Cursor over assigned hash slots in a {@link HashSlotArray}.
 * Initially the cursor's location is before the first slot and the cursor is invalid.
 */
public interface HashSlotCursor {
    /**
     * Advance to the next assigned slot.
     * @return {@code true} if the cursor advanced. If {@code false} is returned, the cursor is now invalid.
     * @throws IllegalStateException if a previous call to advance() already returned false.
     */
    boolean advance();

    /**
     * @return the key of the current slot.
     * @throws IllegalStateException if the cursor is invalid.
     */
    long key();

    /**
     * @return Address of the current slot's value block.
     * @throws IllegalStateException if the cursor is invalid.
     */
    long valueAddress();
}
