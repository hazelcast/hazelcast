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

import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.KeyPredicate;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.impl.MapReduceDataSerializerHook;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;

import java.util.List;

public class KeyValueMapReduceOperationFactory<KeyIn, ValueIn, KeyOut, ValueOut>
        extends AbstractMapReduceOperationFactory<KeyIn, ValueIn, KeyOut, ValueOut>
        implements IdentifiedDataSerializable {

    public KeyValueMapReduceOperationFactory() {
        super();
    }

    public KeyValueMapReduceOperationFactory(String name, String jobId, int chunkSize,
                                             List<KeyIn> keys, KeyPredicate<KeyIn> predicate,
                                             KeyValueSource<KeyIn, ValueIn> keyValueSource,
                                             Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper,
                                             CombinerFactory<KeyOut, ValueOut, ?> combinerFactory) {
        super(name, jobId, chunkSize, keys, predicate, keyValueSource, mapper, combinerFactory);
    }

    @Override
    public Operation createOperation() {
        return new KeyValueMapReduceOperation<KeyIn, ValueIn, KeyOut, ValueOut>(
                name, jobId, chunkSize, keys, predicate, mapper, keyValueSource, combinerFactory);
    }

    @Override
    public int getFactoryId() {
        return MapReduceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapReduceDataSerializerHook.KEY_VALUE_SOURCE_OPERATION_FACTORY;
    }
}
