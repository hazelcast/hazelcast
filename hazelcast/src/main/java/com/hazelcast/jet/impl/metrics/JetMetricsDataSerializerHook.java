/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.metrics;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.hazelcast.jet.impl.JetFactoryIdHelper.JET_METRICS_DS_FACTORY;
import static com.hazelcast.jet.impl.JetFactoryIdHelper.JET_METRICS_DS_FACTORY_ID;

public final class JetMetricsDataSerializerHook implements DataSerializerHook {

    public static final int RAW_JOB_METRICS = 1;

    public static final int FACTORY_ID = FactoryIdHelper.getFactoryId(JET_METRICS_DS_FACTORY, JET_METRICS_DS_FACTORY_ID);

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
                case RAW_JOB_METRICS:
                    return new RawJobMetrics();
                default:
                    throw new IllegalArgumentException("Unknown type id " + typeId);
            }
        }
    }
}
