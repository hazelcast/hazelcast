/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.usercodedeployment.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

/**
 * Bytecode of a class.
 *
 * It's wrapped inside own object as it allows to add additional metadata and maintain compatibility
 * with Hazelcast Rolling Upgrade.
 */
public class ClassData implements IdentifiedDataSerializable {

    private byte[] classDefinition;

    public ClassData() {
    }

    @SuppressFBWarnings({"MS_EXPOSE_REP", "EI_EXPOSE_REP"})
    public ClassData(byte[] classDefinition) {
        this.classDefinition = classDefinition;
    }

    @SuppressFBWarnings({"MS_EXPOSE_REP", "EI_EXPOSE_REP"})
    public byte[] getClassDefinition() {
        return classDefinition;
    }

    @Override
    public int getFactoryId() {
        return UserCodeDeploymentSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return UserCodeDeploymentSerializerHook.CLASS_DATA;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeByteArray(classDefinition);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        classDefinition = in.readByteArray();
    }
}
