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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.mapreduce.*;

import java.util.List;
import java.util.Map;

public abstract class AbstractJob<KeyIn, ValueIn> implements Job<KeyIn, ValueIn> {

    protected final String name;

    protected final KeyValueSource<KeyIn, ValueIn> keyValueSource;

    protected final HazelcastInstance hazelcastInstance;

    protected Mapper<KeyIn, ValueIn, ?, ?> mapper;

    protected CombinerFactory<?, ?, ?> combinerFactory;

    protected ReducerFactory<?, ?, ?> reducerFactory;

    protected Iterable<KeyIn> keys;

    protected transient KeyPredicate<KeyIn> predicate;

    public AbstractJob(String name,
                       KeyValueSource<KeyIn, ValueIn> keyValueSource,
                       HazelcastInstance hazelcastInstance) {
        this.name = name;
        this.keyValueSource = keyValueSource;
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public <KeyOut, ValueOut> MappingJob<KeyOut, ValueOut> mapper(Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper) {
        if (mapper == null)
            throw new IllegalStateException("mapper must not be null");
        if (this.mapper != null)
            throw new IllegalStateException("mapper already set");
        this.mapper = mapper;
        return new MappingJobImpl<KeyOut, ValueOut>();
    }

    protected abstract <T> T submit();

    protected abstract <T> T submit(Collator collator);

    protected class MappingJobImpl<Key, Value> implements MappingJob<Key, Value> {

        @Override
        public <ValueOut> ReducingJob<Key, ValueOut> combiner(
                CombinerFactory<Key, Value, ValueOut> combinerFactory) {
            if (combinerFactory == null)
                throw new IllegalStateException("combinerFactory must not be null");
            if (AbstractJob.this.combinerFactory != null)
                throw new IllegalStateException("combinerFactory already set");
            AbstractJob.this.combinerFactory = AbstractJob.this.combinerFactory;
            return new ReducingJobImpl<Key, ValueOut>();
        }

        @Override
        public <ValueOut> SubmittableJob<Key, Map<Key, ValueOut>> reducer(
                ReducerFactory<Key, Value, ValueOut> reducerFactory) {
            if (reducerFactory == null)
                throw new IllegalStateException("reducerFactory must not be null");
            if (AbstractJob.this.reducerFactory != null)
                throw new IllegalStateException("reducerFactory already set");
            AbstractJob.this.reducerFactory = reducerFactory;
            return new SubmittableJobImpl<Key, Map<Key, ValueOut>>();
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

    protected class ReducingJobImpl<Key, Value> implements ReducingJob<Key, Value> {

        @Override
        public <ValueOut> SubmittableJob<Key, Map<Key, ValueOut>> reducer(
                ReducerFactory<Key, Value, ValueOut> reducerFactory) {
            if (reducerFactory == null)
                throw new IllegalStateException("reducerFactory must not be null");
            if (AbstractJob.this.reducerFactory != null)
                throw new IllegalStateException("reducerFactory already set");
            AbstractJob.this.reducerFactory = reducerFactory;
            return new SubmittableJobImpl<Key, Map<Key, ValueOut>>();
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

    protected class SubmittableJobImpl<Key, Value> implements SubmittableJob<Key, Value> {

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
