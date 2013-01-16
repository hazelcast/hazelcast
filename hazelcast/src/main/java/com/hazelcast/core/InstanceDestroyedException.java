/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.core;

import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class InstanceDestroyedException extends Exception implements DataSerializable {
    private Instance.InstanceType type;
    private String name;

    public InstanceDestroyedException() {
    }

    public InstanceDestroyedException(Instance.InstanceType type, String name) {
        this.type = type;
        this.name = name;
    }

    public Instance.InstanceType getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeInt(type.getTypeId());
        out.writeUTF(name);
    }

    public void readData(DataInput in) throws IOException {
        int id = in.readInt();
        type = Instance.InstanceType.valueOf(id);
        name = in.readUTF();
    }
}
