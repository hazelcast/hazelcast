package com.hazelcast.partition;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author mdogan 6/12/13
 */
public final class Partitions implements Iterable<PartitionView> {

    private final PartitionImpl[] partitions;

    Partitions(PartitionImpl[] partitions) {
        this.partitions = partitions;
    }

    public PartitionView get(int partitionId) {
        if (partitionId < 0 || partitionId >= partitions.length) {
            throw new IllegalArgumentException();
        }
        return partitions[partitionId];
    }

    public int size() {
        return partitions.length;
    }

    public Iterator<PartitionView> iterator() {
        return new Iterator<PartitionView>() {
            final int max = partitions.length - 1; // always greater than zero
            int pos = -1;

            public boolean hasNext() {
                return pos < max;
            }

            public PartitionView next() {
                if (pos == max) {
                    throw new NoSuchElementException();
                }
                return partitions[++pos];
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
