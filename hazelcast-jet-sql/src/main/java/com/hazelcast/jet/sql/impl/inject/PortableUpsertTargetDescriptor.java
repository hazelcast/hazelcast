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

package com.hazelcast.jet.sql.impl.inject;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class PortableUpsertTargetDescriptor implements UpsertTargetDescriptor {

    private int factoryId;
    private int classId;
    private int classVersion;

    @SuppressWarnings("unused")
    private PortableUpsertTargetDescriptor() {
    }

    public PortableUpsertTargetDescriptor(int factoryId, int classId, int classVersion) {
        this.factoryId = factoryId;
        this.classId = classId;
        this.classVersion = classVersion;
    }

    @Override
    public UpsertTarget create(InternalSerializationService serializationService) {
        return new PortableUpsertTarget(serializationService, factoryId, classId, classVersion);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(factoryId);
        out.writeInt(classId);
        out.writeInt(classVersion);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        factoryId = in.readInt();
        classId = in.readInt();
        classVersion = in.readInt();
    }
}
