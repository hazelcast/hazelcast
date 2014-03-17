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

package com.hazelcast.mapreduce.impl;

import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyPredicate;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.MappingJob;
import com.hazelcast.mapreduce.ReducerFactory;
import com.hazelcast.mapreduce.ReducingJob;
import com.hazelcast.mapreduce.ReducingSubmittableJob;
import com.hazelcast.mapreduce.TopologyChangedStrategy;
import com.hazelcast.util.ValidationUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Base class for all map reduce job implementations
 *
 * @param <KeyIn>   type of the input key
 * @param <ValueIn> type of the input value
 */
public abstract class AbstractJob<KeyIn, ValueIn>
        implements Job<KeyIn, ValueIn> {

    protected final String name;

    protected final JobTracker jobTracker;

    protected final KeyValueSource<KeyIn, ValueIn> keyValueSource;

    protected Mapper<KeyIn, ValueIn, ?, ?> mapper;

    protected CombinerFactory<?, ?, ?> combinerFactory;

    protected ReducerFactory<?, ?, ?> reducerFactory;

    protected Collection<KeyIn> keys;

    protected KeyPredicate<KeyIn> predicate;

    protected int chunkSize = -1;

    protected TopologyChangedStrategy topologyChangedStrategy;

    public AbstractJob(String name, JobTracker jobTracker, KeyValueSource<KeyIn, ValueIn> keyValueSource) {
        this.name = name;
        this.jobTracker = jobTracker;
        this.keyValueSource = keyValueSource;
    }

    @Override
    public <KeyOut, ValueOut> MappingJob<KeyIn, KeyOut, ValueOut> mapper(Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper) {
        ValidationUtil.isNotNull(mapper, "mapper");
        if (this.mapper != null) {
            throw new IllegalStateException("mapper already set");
        }
        this.mapper = mapper;
        return new MappingJobImpl<KeyIn, KeyOut, ValueOut>();
    }

    @Override
    public Job<KeyIn, ValueIn> onKeys(Iterable<KeyIn> keys) {
        addKeys(keys);
        return this;
    }

    @Override
    public Job<KeyIn, ValueIn> onKeys(KeyIn... keys) {
        addKeys(keys);
        return this;
    }

    @Override
    public Job<KeyIn, ValueIn> keyPredicate(KeyPredicate<KeyIn> predicate) {
        setKeyPredicate(predicate);
        return this;
    }

    @Override
    public Job<KeyIn, ValueIn> chunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
        return this;
    }

    @Override
    public Job<KeyIn, ValueIn> topologyChangedStrategy(TopologyChangedStrategy topologyChangedStrategy) {
        this.topologyChangedStrategy = topologyChangedStrategy;
        return this;
    }

    protected <T> JobCompletableFuture<T> submit(Collator collator) {
        prepareKeyPredicate();
        return invoke(collator);
    }

    protected abstract <T> JobCompletableFuture<T> invoke(Collator collator);

    protected void prepareKeyPredicate() {
        if (predicate == null) {
            return;
        }
        if (keyValueSource.isAllKeysSupported()) {
            Collection<KeyIn> allKeys = keyValueSource.getAllKeys();
            for (KeyIn key : allKeys) {
                if (predicate.evaluate(key)) {
                    if (this.keys == null) {
                        this.keys = new HashSet<KeyIn>();
                    }
                    this.keys.add(key);
                }
            }
        }
    }

    private void addKeys(Iterable<KeyIn> keys) {
        if (this.keys == null) {
            this.keys = new HashSet<KeyIn>();
        }
        for (KeyIn key : keys) {
            this.keys.add(key);
        }
    }

    private void addKeys(KeyIn... keys) {
        if (this.keys == null) {
            this.keys = new ArrayList<KeyIn>();
        }
        this.keys.addAll(Arrays.asList(keys));
    }

    private void setKeyPredicate(KeyPredicate<KeyIn> predicate) {
        ValidationUtil.isNotNull(predicate, "predicate");
        this.predicate = predicate;
    }

    private <T> JobCompletableFuture<T> submit() {
        return submit(null);
    }

    /**
     * This class is just used to comply to the public DSL style API
     *
     * @param <EntryKey> type of the original base key
     * @param <Key>      type of the key at that processing state
     * @param <Value>    type of the value at that processing state
     */
    protected class MappingJobImpl<EntryKey, Key, Value>
            implements MappingJob<EntryKey, Key, Value> {

        @Override
        public MappingJob<EntryKey, Key, Value> onKeys(Iterable<EntryKey> keys) {
            addKeys((Iterable<KeyIn>) keys);
            return this;
        }

        @Override
        public MappingJob<EntryKey, Key, Value> onKeys(EntryKey... keys) {
            addKeys((KeyIn[]) keys);
            return this;
        }

        @Override
        public MappingJob<EntryKey, Key, Value> keyPredicate(KeyPredicate<EntryKey> predicate) {
            setKeyPredicate((KeyPredicate<KeyIn>) predicate);
            return this;
        }

        @Override
        public MappingJob<EntryKey, Key, Value> chunkSize(int chunkSize) {
            AbstractJob.this.chunkSize = chunkSize;
            return this;
        }

        @Override
        public MappingJob<EntryKey, Key, Value> topologyChangedStrategy(TopologyChangedStrategy topologyChangedStrategy) {
            AbstractJob.this.topologyChangedStrategy = topologyChangedStrategy;
            return this;
        }

        @Override
        public <ValueOut> ReducingJob<EntryKey, Key, ValueOut> combiner(CombinerFactory<Key, Value, ValueOut> combinerFactory) {
            ValidationUtil.isNotNull(combinerFactory, "combinerFactory");
            if (AbstractJob.this.combinerFactory != null) {
                throw new IllegalStateException("combinerFactory already set");
            }
            AbstractJob.this.combinerFactory = combinerFactory;
            return new ReducingJobImpl<EntryKey, Key, ValueOut>();
        }

        @Override
        public <ValueOut> ReducingSubmittableJob<EntryKey, Key, ValueOut> reducer(
                ReducerFactory<Key, Value, ValueOut> reducerFactory) {
            ValidationUtil.isNotNull(reducerFactory, "reducerFactory");
            if (AbstractJob.this.reducerFactory != null) {
                throw new IllegalStateException("reducerFactory already set");
            }
            AbstractJob.this.reducerFactory = reducerFactory;
            return new ReducingSubmittableJobImpl<EntryKey, Key, ValueOut>();
        }

        @Override
        public JobCompletableFuture<Map<Key, List<Value>>> submit() {
            return AbstractJob.this.submit();
        }

        @Override
        public <ValueOut> JobCompletableFuture<ValueOut> submit(Collator<Map.Entry<Key, List<Value>>, ValueOut> collator) {
            return AbstractJob.this.submit(collator);
        }
    }

    /**
     * This class is just used to comply to the public DSL style API
     *
     * @param <EntryKey> type of the original base key
     * @param <Key>      type of the key at that processing state
     * @param <Value>    type of the value at that processing state
     */
    protected class ReducingJobImpl<EntryKey, Key, Value>
            implements ReducingJob<EntryKey, Key, Value> {

        @Override
        public <ValueOut> ReducingSubmittableJob<EntryKey, Key, ValueOut> reducer(
                ReducerFactory<Key, Value, ValueOut> reducerFactory) {
            ValidationUtil.isNotNull(reducerFactory, "reducerFactory");
            if (AbstractJob.this.reducerFactory != null) {
                throw new IllegalStateException("reducerFactory already set");
            }
            AbstractJob.this.reducerFactory = reducerFactory;
            return new ReducingSubmittableJobImpl<EntryKey, Key, ValueOut>();
        }

        @Override
        public ReducingJob<EntryKey, Key, Value> onKeys(Iterable<EntryKey> keys) {
            addKeys((Iterable<KeyIn>) keys);
            return this;
        }

        @Override
        public ReducingJob<EntryKey, Key, Value> onKeys(EntryKey... keys) {
            addKeys((KeyIn[]) keys);
            return this;
        }

        @Override
        public ReducingJob<EntryKey, Key, Value> keyPredicate(KeyPredicate<EntryKey> predicate) {
            setKeyPredicate((KeyPredicate<KeyIn>) predicate);
            return this;
        }

        @Override
        public ReducingJob<EntryKey, Key, Value> chunkSize(int chunkSize) {
            AbstractJob.this.chunkSize = chunkSize;
            return this;
        }

        @Override
        public ReducingJob<EntryKey, Key, Value> topologyChangedStrategy(TopologyChangedStrategy topologyChangedStrategy) {
            AbstractJob.this.topologyChangedStrategy = topologyChangedStrategy;
            return this;
        }

        @Override
        public JobCompletableFuture<Map<Key, List<Value>>> submit() {
            return AbstractJob.this.submit();
        }

        @Override
        public <ValueOut> JobCompletableFuture<ValueOut> submit(Collator<Map.Entry<Key, List<Value>>, ValueOut> collator) {
            return AbstractJob.this.submit(collator);
        }
    }

    /**
     * This class is just used to comply to the public DSL style API
     *
     * @param <EntryKey> type of the original base key
     * @param <Key>      type of the key at that processing state
     * @param <Value>    type of the value at that processing state
     */
    protected class ReducingSubmittableJobImpl<EntryKey, Key, Value>
            implements ReducingSubmittableJob<EntryKey, Key, Value> {

        @Override
        public ReducingSubmittableJob<EntryKey, Key, Value> onKeys(Iterable<EntryKey> keys) {
            addKeys((Iterable<KeyIn>) keys);
            return this;
        }

        @Override
        public ReducingSubmittableJob<EntryKey, Key, Value> onKeys(EntryKey... keys) {
            addKeys((KeyIn[]) keys);
            return this;
        }

        @Override
        public ReducingSubmittableJob<EntryKey, Key, Value> keyPredicate(KeyPredicate<EntryKey> predicate) {
            setKeyPredicate((KeyPredicate<KeyIn>) predicate);
            return this;
        }

        @Override
        public ReducingSubmittableJob<EntryKey, Key, Value> chunkSize(int chunkSize) {
            AbstractJob.this.chunkSize = chunkSize;
            return this;
        }

        @Override
        public ReducingSubmittableJob<EntryKey, Key, Value> topologyChangedStrategy(
                TopologyChangedStrategy topologyChangedStrategy) {
            AbstractJob.this.topologyChangedStrategy = topologyChangedStrategy;
            return this;
        }

        @Override
        public JobCompletableFuture<Map<Key, Value>> submit() {
            return AbstractJob.this.submit();
        }

        @Override
        public <ValueOut> JobCompletableFuture<ValueOut> submit(Collator<Map.Entry<Key, Value>, ValueOut> collator) {
            return AbstractJob.this.submit(collator);
        }
    }

}
