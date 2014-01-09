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

import com.hazelcast.core.CompletableFuture;
import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyPredicate;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.MappingJob;
import com.hazelcast.mapreduce.ReducerFactory;
import com.hazelcast.mapreduce.ReducingJob;
import com.hazelcast.mapreduce.ReducingSubmittableJob;
import com.hazelcast.util.UuidUtil;
import com.hazelcast.util.ValidationUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public abstract class AbstractJob<KeyIn, ValueIn> implements Job<KeyIn, ValueIn> {

    protected final String name;

    protected final JobTracker jobTracker;

    protected final String jobId;

    protected final KeyValueSource<KeyIn, ValueIn> keyValueSource;

    protected Mapper<KeyIn, ValueIn, ?, ?> mapper;

    protected CombinerFactory<?, ?, ?> combinerFactory;

    protected ReducerFactory<?, ?, ?> reducerFactory;

    protected Collection<KeyIn> keys;

    protected KeyPredicate<KeyIn> predicate;

    protected int chunkSize = -1;

    public AbstractJob(String name, JobTracker jobTracker, KeyValueSource<KeyIn, ValueIn> keyValueSource) {
        this(name, UuidUtil.buildRandomUuidString(), jobTracker, keyValueSource);
    }

    public AbstractJob(String name, String jobId, JobTracker jobTracker, KeyValueSource<KeyIn, ValueIn> keyValueSource) {
        this.name = name;
        this.jobId = jobId;
        this.jobTracker = jobTracker;
        this.keyValueSource = keyValueSource;
    }

    @Override
    public <KeyOut, ValueOut> MappingJob<KeyIn, KeyOut, ValueOut> mapper(Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper) {
        ValidationUtil.isNotNull(mapper, "mapper");
        if (this.mapper != null)
            throw new IllegalStateException("mapper already set");
        this.mapper = mapper;
        return new MappingJobImpl<KeyIn, KeyOut, ValueOut>();
    }

    @Override
    public String getJobId() {
        return jobId;
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

    protected <T> CompletableFuture<T> submit(Collator collator) {
        prepareKeyPredicate();
        return invoke(collator);
    }

    protected abstract <T> CompletableFuture<T> invoke(Collator collator);

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
        for (KeyIn key : keys) {
            this.keys.add(key);
        }
    }

    private void setKeyPredicate(KeyPredicate<KeyIn> predicate) {
        ValidationUtil.isNotNull(predicate, "predicate");
        this.predicate = predicate;
    }

    private <T> CompletableFuture<T> submit() {
        return submit(null);
    }

    protected class MappingJobImpl<EntryKey, Key, Value>
            implements MappingJob<EntryKey, Key, Value> {

        @Override
        public String getJobId() {
            return jobId;
        }

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
        public <ValueOut> ReducingJob<EntryKey, Key, ValueOut> combiner(
                CombinerFactory<Key, Value, ValueOut> combinerFactory) {
            ValidationUtil.isNotNull(combinerFactory, "combinerFactory");
            if (AbstractJob.this.combinerFactory != null)
                throw new IllegalStateException("combinerFactory already set");
            AbstractJob.this.combinerFactory = AbstractJob.this.combinerFactory;
            return new ReducingJobImpl<EntryKey, Key, ValueOut>();
        }

        @Override
        public <ValueOut> ReducingSubmittableJob<EntryKey, Key, ValueOut> reducer(
                ReducerFactory<Key, Value, ValueOut> reducerFactory) {
            ValidationUtil.isNotNull(reducerFactory, "reducerFactory");
            if (AbstractJob.this.reducerFactory != null)
                throw new IllegalStateException("reducerFactory already set");
            AbstractJob.this.reducerFactory = reducerFactory;
            return new ReducingSubmittableJobImpl<EntryKey, Key, ValueOut>();
        }

        @Override
        public CompletableFuture<Map<Key, List<Value>>> submit() {
            return AbstractJob.this.submit();
        }

        @Override
        public <ValueOut> CompletableFuture<ValueOut> submit(
                Collator<Map.Entry<Key, List<Value>>, ValueOut> collator) {
            return AbstractJob.this.submit(collator);
        }
    }

    protected class ReducingJobImpl<EntryKey, Key, Value>
            implements ReducingJob<EntryKey, Key, Value> {

        @Override
        public String getJobId() {
            return jobId;
        }

        @Override
        public <ValueOut> ReducingSubmittableJob<EntryKey, Key, ValueOut> reducer(
                ReducerFactory<Key, Value, ValueOut> reducerFactory) {
            ValidationUtil.isNotNull(reducerFactory, "reducerFactory");
            if (AbstractJob.this.reducerFactory != null)
                throw new IllegalStateException("reducerFactory already set");
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
        public CompletableFuture<Map<Key, List<Value>>> submit() {
            return AbstractJob.this.submit();
        }

        @Override
        public <ValueOut> CompletableFuture<ValueOut> submit(
                Collator<Map.Entry<Key, List<Value>>, ValueOut> collator) {
            return AbstractJob.this.submit(collator);
        }
    }

    protected class ReducingSubmittableJobImpl<EntryKey, Key, Value>
            implements ReducingSubmittableJob<EntryKey, Key, Value> {

        @Override
        public String getJobId() {
            return jobId;
        }

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
        public CompletableFuture<Map<Key, Value>> submit() {
            return AbstractJob.this.submit();
        }

        @Override
        public <ValueOut> CompletableFuture<ValueOut> submit(Collator<Map.Entry<Key, Value>, ValueOut> collator) {
            return AbstractJob.this.submit(collator);
        }
    }

}
