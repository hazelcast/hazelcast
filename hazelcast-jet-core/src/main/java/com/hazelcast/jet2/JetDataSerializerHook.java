/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet2;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.jet2.impl.deployment.ResourcePart;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public final class JetDataSerializerHook implements DataSerializerHook {

    public static final String JET_DS_FACTORY = "hazelcast.serialization.ds.jet";
    public static final int JET_DS_FACTORY_ID = -10001;

    public static final int DAG = 0;
    public static final int VERTEX = 1;
    public static final int EDGE = 2;
    public static final int RESOURCE_PART = 3;
    public static final int FACTORY_ID = FactoryIdHelper.getFactoryId(JET_DS_FACTORY, JET_DS_FACTORY_ID);


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
                case DAG:
                    return new DAG();
                case EDGE:
                    return new Edge();
                case VERTEX:
                    return new Vertex();
                case RESOURCE_PART:
                    return new ResourcePart();
                default:
                    throw new IllegalArgumentException("Unknown type id " + typeId);
            }
        }
    }
}
