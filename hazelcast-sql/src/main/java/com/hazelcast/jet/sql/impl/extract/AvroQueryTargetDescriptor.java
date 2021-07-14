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

package com.hazelcast.jet.sql.impl.extract;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;

public final class AvroQueryTargetDescriptor implements QueryTargetDescriptor {

    public static final AvroQueryTargetDescriptor INSTANCE = new AvroQueryTargetDescriptor();

    private AvroQueryTargetDescriptor() {
    }

    @Override
    public QueryTarget create(InternalSerializationService serializationService, Extractors extractors, boolean isKey) {
        return new AvroQueryTarget();
    }

    @Override
    public void writeData(ObjectDataOutput out) {
    }

    @Override
    public void readData(ObjectDataInput in) {
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof AvroQueryTargetDescriptor;
    }

    @Override
    public int hashCode() {
        return 0;
    }
}
