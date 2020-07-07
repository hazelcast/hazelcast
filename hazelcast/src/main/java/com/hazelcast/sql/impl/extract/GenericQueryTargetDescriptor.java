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

package com.hazelcast.sql.impl.extract;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.SqlDataSerializerHook;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Generic descriptor that imposes no limitations on the underlying target.
 */
public class GenericQueryTargetDescriptor implements QueryTargetDescriptor, IdentifiedDataSerializable {

    public static final GenericQueryTargetDescriptor DEFAULT = new GenericQueryTargetDescriptor();

    private Set<String> pathsRequiringConversion;

    public GenericQueryTargetDescriptor() {
        this(Collections.emptySet());
    }

    public GenericQueryTargetDescriptor(Set<String> pathsRequiringConversion) {
        this.pathsRequiringConversion = pathsRequiringConversion;
    }

    @Override
    public QueryTarget create(InternalSerializationService serializationService, Extractors extractors, boolean isKey) {
        return new GenericQueryTarget(serializationService, extractors, isKey, pathsRequiringConversion);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.TARGET_DESCRIPTOR_GENERIC;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(pathsRequiringConversion);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        pathsRequiringConversion = in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GenericQueryTargetDescriptor that = (GenericQueryTargetDescriptor) o;
        return Objects.equals(pathsRequiringConversion, that.pathsRequiringConversion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pathsRequiringConversion);
    }
}
