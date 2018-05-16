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

package com.hazelcast.mapreduce.aggregation.impl;

import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.BinaryInterface;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * Base class for all internal aggregation CombinerFactories to easy the implementation of
 * {@link com.hazelcast.nio.serialization.IdentifiedDataSerializable}.
 *
 * @param <KeyIn>    the input key type
 * @param <ValueIn>  the input value type
 * @param <ValueOut> the output value type
 */
@BinaryInterface
abstract class AbstractAggregationCombinerFactory<KeyIn, ValueIn, ValueOut>
        implements CombinerFactory<KeyIn, ValueIn, ValueOut>, IdentifiedDataSerializable {

    @Override
    public int getFactoryId() {
        return AggregationsDataSerializerHook.F_ID;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
    }
}
