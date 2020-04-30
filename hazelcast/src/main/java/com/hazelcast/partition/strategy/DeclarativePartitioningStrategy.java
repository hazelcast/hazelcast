/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.partition.strategy;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializableByConvention;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.query.impl.getters.Extractors;

/**
 * Partitioning strategy which uses a field with a predefined name as a partitioning key.
 */
@SerializableByConvention
public class DeclarativePartitioningStrategy<K> implements PartitioningStrategy<K>, SerializationServiceAware {

    private static final long serialVersionUID = 8931805590580189420L;

    private String field;
    private transient Extractors extractors;

    @Override
    public Object getPartitionKey(K key) {
        return field != null ? extractors.extract(key, field, null) : key;
    }

    @Override
    public void setSerializationService(SerializationService ss) {
        if (field != null) {
            InternalSerializationService ss0 = (InternalSerializationService) ss;

            extractors = Extractors.newBuilder(ss0).setClassLoader(ss0.getClassLoader()).build();
        }
    }

    public String getField() {
        return field;
    }

    public DeclarativePartitioningStrategy setField(String field) {
        this.field = field;

        return this;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{field=" + field + "}";
    }
}
