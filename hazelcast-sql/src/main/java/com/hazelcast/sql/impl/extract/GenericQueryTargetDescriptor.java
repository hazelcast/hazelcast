/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.sql.impl.extract;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.impl.getters.Extractors;

/**
 * Generic descriptor that imposes no limitations on the underlying target.
 */
public class GenericQueryTargetDescriptor implements QueryTargetDescriptor, IdentifiedDataSerializable {

    public static final GenericQueryTargetDescriptor DEFAULT = new GenericQueryTargetDescriptor();

    public GenericQueryTargetDescriptor() {
        // No-op.
    }

    @Override
    public QueryTarget create(InternalSerializationService serializationService, Extractors extractors, boolean isKey) {
        return new GenericQueryTarget(serializationService, extractors, isKey);
    }

    @Override
    public int getFactoryId() {
        return JetSqlSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.TARGET_DESCRIPTOR_GENERIC;
    }

    @Override
    public void writeData(ObjectDataOutput out) {
        // No-op.
    }

    @Override
    public void readData(ObjectDataInput in) {
        // No-op.
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof GenericQueryTargetDescriptor;
    }
}
