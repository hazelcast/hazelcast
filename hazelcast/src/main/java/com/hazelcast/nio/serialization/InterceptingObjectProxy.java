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

package com.hazelcast.nio.serialization;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Proxy class which encapsulates Hazelcast DataSerializable
 * serialized bytearrays.
 */
public class InterceptingObjectProxy
        implements Externalizable {

    private byte[] data;

    public InterceptingObjectProxy() {
    }

    public InterceptingObjectProxy(byte[] data) {
        this.data = data;
    }

    public byte[] getData() {
        return data;
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {

        int length = data.length;
        out.writeInt(length);
        out.write(data, 0, length);
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {

        int length = in.readInt();
        data = new byte[length];
        in.read(data, 0, length);
    }
}
