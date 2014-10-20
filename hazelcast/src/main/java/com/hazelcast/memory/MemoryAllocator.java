package com.hazelcast.memory;

/**
 * Memory Allocator allocates/free memory blocks from/to OS like C malloc()/free()
 *
 * @author mdogan 03/12/13
 */
public interface MemoryAllocator {

    /**
     * TODO: Javadoc needed
     */
    long NULL_ADDRESS = 0L;

    /**
     * Allocates memory directly from OS, like C malloc().
     *
     * <p>
     * Complement of {@link #free(long, long)}.
     *
     * @param size of requested memory block
     * @return address of memory block
     */
    long allocate(long size);

    /**
     * Gives allocated memory block back to OS, like C free().
     *
     * <p>
     * Complement of {@link #allocate(long)}.
     *
     * @param address address of memory block
     * @param size size of memory block
     */
    void free(long address, long size);

}
