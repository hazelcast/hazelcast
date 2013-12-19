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
import com.hazelcast.mapreduce.*;

import java.util.*;

public abstract class AbstractJob<KeyIn, ValueIn> implements Job<KeyIn, ValueIn> {

    protected final String name;

    protected final KeyValueSource<KeyIn, ValueIn> keyValueSource;

    protected Mapper<KeyIn, ValueIn, ?, ?> mapper;

    protected CombinerFactory<?, ?, ?> combinerFactory;

    protected ReducerFactory<?, ?, ?> reducerFactory;

    protected Collection<KeyIn> keys;

    protected KeyPredicate<KeyIn> predicate;

    public AbstractJob(String name, KeyValueSource<KeyIn, ValueIn> keyValueSource) {
        this.name = name;
        this.keyValueSource = keyValueSource;
    }

    @Override
    public <KeyOut, ValueOut> MappingJob<KeyIn, KeyOut, ValueOut> mapper(Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper) {
        if (mapper == null)
            throw new IllegalStateException("mapper must not be null");
        if (this.mapper != null)
            throw new IllegalStateException("mapper already set");
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

    protected <T> CompletableFuture<T> submit(Collator collator) {
        prepareKeyPredicate();


        return null;
    }

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

    protected abstract void invokeTask() throws Exception;

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
        if (predicate != null) {
            throw new IllegalStateException("predicate already defined");
        }
        this.predicate = predicate;
    }

    private <T> CompletableFuture<T> submit() {
        return submit(null);
    }

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
        public <ValueOut> ReducingJob<EntryKey, Key, ValueOut> combiner(
                CombinerFactory<Key, Value, ValueOut> combinerFactory) {
            if (combinerFactory == null)
                throw new IllegalStateException("combinerFactory must not be null");
            if (AbstractJob.this.combinerFactory != null)
                throw new IllegalStateException("combinerFactory already set");
            AbstractJob.this.combinerFactory = AbstractJob.this.combinerFactory;
            return new ReducingJobImpl<EntryKey, Key, ValueOut>();
        }

        @Override
        public <ValueOut> SubmittableJob<EntryKey, Map<Key, ValueOut>> reducer(
                ReducerFactory<Key, Value, ValueOut> reducerFactory) {
            if (reducerFactory == null)
                throw new IllegalStateException("reducerFactory must not be null");
            if (AbstractJob.this.reducerFactory != null)
                throw new IllegalStateException("reducerFactory already set");
            AbstractJob.this.reducerFactory = reducerFactory;
            return new SubmittableJobImpl<EntryKey, Map<Key, ValueOut>>();
        }

        @Override
        public CompletableFuture<Map<Key, List<Value>>> submit() {
            return AbstractJob.this.submit();
        }

        @Override
        public <ValueOut> CompletableFuture<ValueOut> submit(
                Collator<Map<Key, List<Value>>, ValueOut> collator) {
            return AbstractJob.this.submit(collator);
        }
    }

    protected class ReducingJobImpl<EntryKey, Key, Value>
            implements ReducingJob<EntryKey, Key, Value> {

        @Override
        public <ValueOut> SubmittableJob<EntryKey, Map<Key, ValueOut>> reducer(
                ReducerFactory<Key, Value, ValueOut> reducerFactory) {
            if (reducerFactory == null)
                throw new IllegalStateException("reducerFactory must not be null");
            if (AbstractJob.this.reducerFactory != null)
                throw new IllegalStateException("reducerFactory already set");
            AbstractJob.this.reducerFactory = reducerFactory;
            return new SubmittableJobImpl<EntryKey, Map<Key, ValueOut>>();
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
                Collator<Map<Key, List<Value>>, ValueOut> collator) {
            return AbstractJob.this.submit(collator);
        }
    }

    protected class SubmittableJobImpl<EntryKey, Value>
            implements SubmittableJob<EntryKey, Value> {

        @Override
        public SubmittableJob<EntryKey, Value> onKeys(Iterable<EntryKey> keys) {
            addKeys((Iterable<KeyIn>) keys);
            return this;
        }

        @Override
        public SubmittableJob<EntryKey, Value> onKeys(EntryKey... keys) {
            addKeys((KeyIn[]) keys);
            return this;
        }

        @Override
        public SubmittableJob<EntryKey, Value> keyPredicate(KeyPredicate<EntryKey> predicate) {
            setKeyPredicate((KeyPredicate<KeyIn>) predicate);
            return this;
        }

        @Override
        public CompletableFuture<Value> submit() {
            return AbstractJob.this.submit();
        }

        @Override
        public <ValueOut> CompletableFuture<ValueOut> submit(
                Collator<Value, ValueOut> collator) {
            return AbstractJob.this.submit(collator);
        }
    }

}
