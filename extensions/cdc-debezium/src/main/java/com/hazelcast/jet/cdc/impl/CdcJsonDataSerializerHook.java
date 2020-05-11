/*
 * Copyright 2020 Hazelcast Inc.
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

package com.hazelcast.jet.cdc.impl;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.hazelcast.jet.impl.JetFactoryIdHelper.JET_CDC_JSON_DS_FACTORY;
import static com.hazelcast.jet.impl.JetFactoryIdHelper.JET_CDC_JSON_DS_FACTORY_ID;

public class CdcJsonDataSerializerHook implements DataSerializerHook {

    public static final int CHANGE_RECORD = 1;
    public static final int RECORD_PART = 2;
    public static final int SOURCE_STATE = 3;

    public static final int FACTORY_ID = FactoryIdHelper.getFactoryId(JET_CDC_JSON_DS_FACTORY, JET_CDC_JSON_DS_FACTORY_ID);

    @Override
    public int getFactoryId() {
        return FACTORY_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new Factory();
    }

    private static class Factory implements DataSerializableFactory {
        @Override
        public IdentifiedDataSerializable create(int typeId) {
            switch (typeId) {
                case CHANGE_RECORD:
                    return new ChangeRecordImpl();
                case RECORD_PART:
                    return new RecordPartImpl();
                case SOURCE_STATE:
                    return new CdcSource.State();
                default:
                    throw new IllegalArgumentException("Unknown type id " + typeId);
            }
        }
    }
}
