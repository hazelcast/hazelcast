/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.kinesis.impl.source;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.hazelcast.jet.impl.JetFactoryIdHelper.JET_KINESIS_DS_FACTORY;
import static com.hazelcast.jet.impl.JetFactoryIdHelper.JET_KINESIS_DS_FACTORY_ID;

public class KinesisDataSerializerHook implements DataSerializerHook {

    public static final int KINESIS_SOURCE_P_SHARD_STATE = 0;
    public static final int INITIAL_SHARD_ITERATORS = 1;

    public static final int FACTORY_ID = FactoryIdHelper.getFactoryId(JET_KINESIS_DS_FACTORY, JET_KINESIS_DS_FACTORY_ID);

    @Override
    public int getFactoryId() {
        return FACTORY_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new Factory();
    }

    private static class Factory implements DataSerializableFactory {
        @SuppressWarnings("checkstyle:returncount")
        @Override
        public IdentifiedDataSerializable create(int typeId) {
            switch (typeId) {
                case KINESIS_SOURCE_P_SHARD_STATE:
                    return new KinesisSourceP.ShardState();
                case INITIAL_SHARD_ITERATORS:
                    return new InitialShardIterators();
                default:
                    throw new IllegalArgumentException("Unknown type id " + typeId);
            }
        }
    }

}
