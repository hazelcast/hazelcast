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

package com.hazelcast.jet.sql.impl.inject;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class HazelcastObjectUpsertTargetDescriptor implements UpsertTargetDescriptor {
    public static final HazelcastObjectUpsertTargetDescriptor INSTANCE = new HazelcastObjectUpsertTargetDescriptor();

    public HazelcastObjectUpsertTargetDescriptor() { }

    @Override
    public UpsertTarget create(final InternalSerializationService serializationService) {
        return new HazelcastObjectUpsertTarget();
    }

    @Override
    public void writeData(final ObjectDataOutput out) throws IOException { }

    @Override
    public void readData(final ObjectDataInput in) throws IOException { }
}
