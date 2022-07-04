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

package com.hazelcast.jet.sql.impl.inject;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

public final class HazelcastJsonUpsertTargetDescriptor implements UpsertTargetDescriptor {

    public static final HazelcastJsonUpsertTargetDescriptor INSTANCE = new HazelcastJsonUpsertTargetDescriptor();

    private HazelcastJsonUpsertTargetDescriptor() {
    }

    @Override
    public UpsertTarget create(InternalSerializationService serializationService) {
        return new HazelcastJsonUpsertTarget();
    }

    @Override
    public void writeData(ObjectDataOutput out) {
    }

    @Override
    public void readData(ObjectDataInput in) {
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof HazelcastJsonUpsertTargetDescriptor;
    }

    @Override
    public int hashCode() {
        return 0;
    }
}
