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

package com.hazelcast.mapreduce.impl.task;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.KeyPredicate;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;

import java.util.Collection;
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

    private final Collection<KeyIn> keys;
    private final KeyPredicate<KeyIn> predicate;

    public MappingPhase(Collection<KeyIn> keys, KeyPredicate<KeyIn> predicate) {
        this.keys = keys;
        this.predicate = predicate;
    }

    public void cancel() {
        cancelled.set(true);
    }

    protected boolean isCancelled() {
        return cancelled.get();
    }

    protected boolean matches(KeyIn key) {
        if ((keys == null || keys.isEmpty()) && predicate == null) {
            return true;
        }
        if (keys != null && !keys.isEmpty()) {
            for (KeyIn matcher : keys) {
                if (key.equals(matcher)) {
                    return true;
                }
            }
        }
        return predicate != null && predicate.evaluate(key);
    }

    protected abstract void executeMappingPhase(KeyValueSource<KeyIn, ValueIn> keyValueSource,
                                                Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper,
                                                Context<KeyOut, ValueOut> context);

}
