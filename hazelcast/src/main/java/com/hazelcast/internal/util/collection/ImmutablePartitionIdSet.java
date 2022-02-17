/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.collection;

import java.util.Collection;

/**
 * An immutable {@link PartitionIdSet}.
 */
public final class ImmutablePartitionIdSet extends PartitionIdSet {

    ImmutablePartitionIdSet() {
    }

    public ImmutablePartitionIdSet(int partitionCount, Collection<Integer> initialPartitionIds) {
        super(partitionCount, initialPartitionIds);
    }

    @Override
    public boolean add(Integer partitionId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(int partitionId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addAll(PartitionIdSet other) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(int partitionId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void union(PartitionIdSet other) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeAll(PartitionIdSet other) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void complement() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getClassId() {
        return UtilCollectionSerializerHook.IMMUTABLE_PARTITION_ID_SET;
    }
}
