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
import java.util.Map;

/**
 * This class executes the mapping phase for a given {@link com.hazelcast.mapreduce.KeyValueSource} scope.
 *
 * @param <KeyIn>    type of the input key
 * @param <ValueIn>  type of the input value
 * @param <KeyOut>   type of the emitted key
 * @param <ValueOut> type of the emitted value
 */
public class KeyValueSourceMappingPhase<KeyIn, ValueIn, KeyOut, ValueOut>
        extends MappingPhase<KeyIn, ValueIn, KeyOut, ValueOut> {

    public KeyValueSourceMappingPhase(Collection<KeyIn> keys, KeyPredicate<KeyIn> predicate) {
        super(keys, predicate);
    }

    @Override
    public void executeMappingPhase(KeyValueSource<KeyIn, ValueIn> keyValueSource,
                                    Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper, Context<KeyOut, ValueOut> context) {

        while (keyValueSource.hasNext()) {
            if (matches(keyValueSource.key())) {
                Map.Entry<KeyIn, ValueIn> entry = keyValueSource.element();
                mapper.map(entry.getKey(), entry.getValue(), context);
            }
            if (isCancelled()) {
                return;
            }
        }
    }
}
