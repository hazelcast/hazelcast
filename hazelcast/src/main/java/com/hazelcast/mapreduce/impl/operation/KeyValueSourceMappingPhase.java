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

package com.hazelcast.mapreduce.impl.operation;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.KeyPredicate;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.impl.task.MappingPhase;

import java.util.Collection;
import java.util.Map;

public class KeyValueSourceMappingPhase<KeyIn, ValueIn, KeyOut, ValueOut>
        extends MappingPhase<KeyIn, ValueIn, KeyOut, ValueOut> {

    private final Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper;

    public KeyValueSourceMappingPhase(Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper,
                                      Collection<KeyIn> keys, KeyPredicate<KeyIn> predicate) {
        super(keys, predicate);
        this.mapper = mapper;
    }

    @Override
    public void executeMappingPhase(KeyValueSource<KeyIn, ValueIn> keyValueSource,
                                    Context<KeyOut, ValueOut> context) {

        while (keyValueSource.hasNext()) {
            if (matches(keyValueSource.key())) {
                Map.Entry<KeyIn, ValueIn> entry = keyValueSource.element();
                mapper.map(entry.getKey(), entry.getValue(), context);
            }
        }
    }
}
