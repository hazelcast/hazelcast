/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.partition;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author mdogan 6/12/13
 */
public final class Partitions implements Iterable<PartitionView> {

    private final PartitionView[] partitions;

    Partitions(PartitionView[] partitions) {
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
