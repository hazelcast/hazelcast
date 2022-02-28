/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.core.metrics;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.annotation.PrivateApi;

import static com.hazelcast.jet.impl.JetFactoryIdHelper.JET_JOB_METRICS_DS_FACTORY;
import static com.hazelcast.jet.impl.JetFactoryIdHelper.JET_JOB_METRICS_DS_FACTORY_ID;

/**
 * A Java Service Provider hook for Hazelcast's Identified Data Serializable
 * mechanism. This is private API.
 */
@PrivateApi
public class MetricsDataSerializerHook implements DataSerializerHook {

    /**
     * Serialization ID of the {@link JobMetrics} class.
     */
    public static final int JOB_METRICS = 0;

    /**
     * Serialization ID of the {@link Measurement} class.
     */
    public static final int MEASUREMENT = 1;

    static final int FACTORY_ID = FactoryIdHelper.getFactoryId(JET_JOB_METRICS_DS_FACTORY,
            JET_JOB_METRICS_DS_FACTORY_ID);

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
                case JOB_METRICS:
                    return new JobMetrics();
                case MEASUREMENT:
                    return new Measurement();
                default:
                    throw new IllegalArgumentException("Unknown type id " + typeId);
            }
        }
    }
}
