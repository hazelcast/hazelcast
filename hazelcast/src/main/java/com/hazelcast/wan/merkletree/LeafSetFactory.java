package com.hazelcast.wan.merkletree;

import com.hazelcast.util.collection.HashAcceptorSet;

/**
 * Interface used by {@link ArrayMerkleTree} for creating the sets act as
 * its data blocks.
 */
public interface LeafSetFactory<T> {

    /**
     * Creates a set
     *
     * @param initialSize The initial size of the set to create
     * @return The created set
     */
    HashAcceptorSet<T> createLeafSet(int initialSize);
}
