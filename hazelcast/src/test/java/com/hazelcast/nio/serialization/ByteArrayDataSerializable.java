/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Arrays;

/**
* @author mdogan 22/05/14
*/
class ByteArrayDataSerializable implements DataSerializable {
    private byte[] data;

    ByteArrayDataSerializable() {
    }

    ByteArrayDataSerializable(byte[] data) {
        this.data = data;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(data.length);
        out.write(data);
    }

    public void readData(ObjectDataInput in) throws IOException {
        int len = in.readInt();
        data = new byte[len];
        in.readFully(data);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ByteArrayDataSerializable that = (ByteArrayDataSerializable) o;

        if (!Arrays.equals(data, that.data)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return data != null ? Arrays.hashCode(data) : 0;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SimpleDataSerializable{");
        sb.append("data=").append(Arrays.toString(data));
        sb.append('}');
        return sb.toString();
    }
}
