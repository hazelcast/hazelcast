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

import com.hazelcast.mapreduce.*;
import com.hazelcast.mapreduce.impl.MapReduceDataSerializerHook;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.util.List;
import java.util.Map;

public class KeyValueMapReduceOperation<KeyIn, ValueIn, KeyOut, ValueOut>
        extends AbstractMapReduceOperation<KeyIn, ValueIn, KeyOut, ValueOut>
        implements IdentifiedDataSerializable {

    protected KeyValueSource<KeyIn, ValueIn> keyValueSource;

    public KeyValueMapReduceOperation() {
        super();
    }

    public KeyValueMapReduceOperation(String name, String jobId, int chunkSize,
                                      List<KeyIn> keys, KeyPredicate<KeyIn> predicate,
                                      Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper,
                                      KeyValueSource<KeyIn, ValueIn> keyValueSource,
                                      CombinerFactory<KeyOut, ValueOut, ?> combinerFactory) {
        super(name, jobId, chunkSize, keys, predicate, mapper, combinerFactory);
        this.keyValueSource = keyValueSource;
    }

    @Override
    public void beforeRun() throws Exception {
        super.beforeRun();
        keyValueSource.open(getNodeEngine());
    }

    @Override
    public void afterRun() throws Exception {
        super.afterRun();
        keyValueSource.close();
    }

    @Override
    public void executeMappingPhase(Context<KeyOut, ValueOut> context) {
        while (keyValueSource.hasNext()) {
            if (matches(keyValueSource.key())) {
                Map.Entry<KeyIn, ValueIn> entry = keyValueSource.element();
                mapper.map(entry.getKey(), entry.getValue(), context);
            }
        }
    }

    @Override
    public int getFactoryId() {
        return MapReduceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapReduceDataSerializerHook.KEY_VALUE_SOURCE_OPERATION;
    }

}
