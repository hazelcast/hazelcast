/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mapreduce.impl.task;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.KeyPredicate;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.spi.partition.IPartitionService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This is the base of all mapping phase implementations. It's responsibility is to test keys
 * prior to mapping the special value. This is used for preselected key-sets and given
 * {@link com.hazelcast.mapreduce.KeyPredicate} implementations.
 *
 * @param <KeyIn>    type of the input key
 * @param <ValueIn>  type of the input value
 * @param <KeyOut>   type of the emitted key
 * @param <ValueOut> type of the emitted value
 */
public abstract class MappingPhase<KeyIn, ValueIn, KeyOut, ValueOut> {

    private final AtomicBoolean cancelled = new AtomicBoolean();

    private final KeyPredicate<? super KeyIn> predicate;
    private final Object[] keys;

    // Precalculate key set (partition based)
    private Object[][] partitionMappedKeys;

    // Cache of selected keys based on partition
    private Object[] partitionKeys;

    public MappingPhase(Collection<? extends KeyIn> keys, KeyPredicate<? super KeyIn> predicate) {
        this.predicate = predicate;
        this.keys = keys != null ? keys.toArray(new Object[0]) : null;
    }

    public void cancel() {
        cancelled.set(true);
    }

    protected boolean isCancelled() {
        return cancelled.get();
    }

    protected boolean processingPartitionNecessary(int partitionId, IPartitionService partitionService) {
        if (partitionId == -1) {
            partitionKeys = null;
            return true;
        }

        // Select correct keyset
        partitionKeys = prepareKeys(partitionId, partitionService);

        if (keys == null || keys.length == 0 || predicate != null) {
            return true;
        }
        return partitionKeys != null && partitionKeys.length > 0;
    }

    protected boolean matches(KeyIn key) {
        if (partitionKeys == null && predicate == null) {
            return true;
        }
        if (partitionKeys != null && partitionKeys.length > 0) {
            for (Object matcher : partitionKeys) {
                if (key == matcher || key.equals(matcher)) {
                    return true;
                }
            }
        }
        return predicate != null && predicate.evaluate(key);
    }

    private Object[] prepareKeys(int partitionId, IPartitionService partitionService) {
        if (keys == null || keys.length == 0) {
            return null;
        }

        if (partitionMappedKeys != null) {
            // Already pre-cached
            return partitionMappedKeys[partitionId];
        }

        partitionMappedKeys = buildCache(partitionService);
        return partitionMappedKeys[partitionId];
    }

    private Object[][] buildCache(IPartitionService partitionService) {
        List<Object>[] mapping = buildMapping(partitionService);
        Object[][] cache = new Object[mapping.length][];
        for (int i = 0; i < cache.length; i++) {
            List<Object> keys = mapping[i];
            if (keys != null) {
                cache[i] = keys.toArray(new Object[0]);
            }
        }
        return cache;
    }

    private List<Object>[] buildMapping(IPartitionService partitionService) {
        List<Object>[] mapping = new List[partitionService.getPartitionCount()];
        for (Object key : keys) {
            int pid = partitionService.getPartitionId(key);
            List<Object> list = mapping[pid];
            if (list == null) {
                list = new ArrayList<Object>();
                mapping[pid] = list;
            }
            list.add(key);
        }
        return mapping;
    }

    protected abstract void executeMappingPhase(KeyValueSource<KeyIn, ValueIn> keyValueSource,
                                                Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper,
                                                Context<KeyOut, ValueOut> context);

}
