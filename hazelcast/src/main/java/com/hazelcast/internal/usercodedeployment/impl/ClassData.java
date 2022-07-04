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

package com.hazelcast.internal.usercodedeployment.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Carries byte code of a class along with its inner classes.
 *
 *
 * It's wrapped inside own object as it allows to add additional metadata and maintain compatibility
 * with Hazelcast Rolling Upgrade.
 */
public class ClassData implements IdentifiedDataSerializable {

    private Map<String, byte[]> innerClassDefinitions = Collections.emptyMap();

    //this field needed for compatibility with 3.8
    private byte[] mainClassDefinition;

    public ClassData() {
    }

    Map<String, byte[]> getInnerClassDefinitions() {
        return innerClassDefinitions;
    }

    public void setInnerClassDefinitions(Map<String, byte[]> innerClassDefinitions) {
        this.innerClassDefinitions = innerClassDefinitions;
    }

    //this method and related field is for compatibility with 3.8
    void setMainClassDefinition(byte[] mainClassDefinition) {
        this.mainClassDefinition = mainClassDefinition;
    }

    //this method and related field is for compatibility with 3.8
    byte[] getMainClassDefinition() {
        return mainClassDefinition;
    }

    @Override
    public int getFactoryId() {
        return UserCodeDeploymentSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return UserCodeDeploymentSerializerHook.CLASS_DATA;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeByteArray(mainClassDefinition);
        out.writeInt(innerClassDefinitions.size());
        for (Map.Entry<String, byte[]> entry : innerClassDefinitions.entrySet()) {
            out.writeString(entry.getKey());
            out.writeByteArray(entry.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mainClassDefinition = in.readByteArray();
        int size = in.readInt();
        innerClassDefinitions = new HashMap<String, byte[]>();
        for (int i = 0; i < size; i++) {
            innerClassDefinitions.put(in.readString(), in.readByteArray());
        }
    }
}
