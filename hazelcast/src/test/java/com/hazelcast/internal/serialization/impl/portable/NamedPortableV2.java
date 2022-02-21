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

package com.hazelcast.internal.serialization.impl.portable;

import com.hazelcast.internal.serialization.impl.TestSerializationConstants;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.nio.serialization.VersionedPortable;

import java.io.IOException;

class NamedPortableV2 extends NamedPortable implements VersionedPortable {

    public Integer v;

    NamedPortableV2() {
    }

    NamedPortableV2(int v) {
        this.v = v;
    }

    NamedPortableV2(String name, int k, int v) {
        super(name, k);
        this.v = v;
    }

    @Override
    public int getClassVersion() {
        return 2;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        super.writePortable(writer);
        writer.writeInt("v", v);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        super.readPortable(reader);
        if (reader.hasField("v")) {
            v = reader.readInt("v");
        }
    }

    @Override
    public int getFactoryId() {
        return TestSerializationConstants.PORTABLE_FACTORY_ID;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NamedPortableV2{");
        sb.append("name='").append(name).append('\'');
        sb.append(", k=").append(myint);
        sb.append(", v=").append(v);
        sb.append('}');
        return sb.toString();
    }
}
