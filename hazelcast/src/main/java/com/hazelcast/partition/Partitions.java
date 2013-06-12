package com.hazelcast.partition;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @mdogan 6/12/13
 */
public final class Partitions implements Iterable<PartitionInfo> {

    private final PartitionInfo[] partitions;

    Partitions(PartitionInfo[] partitions) {
        this.partitions = partitions;
    }

    public PartitionInfo get(int partitionId) {
        if (partitionId < 0 || partitionId >= partitions.length) {
            throw new IllegalArgumentException();
        }
        return partitions[partitionId];
    }

    public int size() {
        return partitions.length;
    }

    public Iterator<PartitionInfo> iterator() {
        return new Iterator<PartitionInfo>() {
            final int max = partitions.length - 1; // always greater than zero
            int pos = -1;

            public boolean hasNext() {
                return pos < max;
            }

            public PartitionInfo next() {
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
