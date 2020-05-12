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

package com.hazelcast.internal.serialization.impl.portable;

import com.hazelcast.internal.serialization.impl.TestSerializationConstants;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public class NamedPortable implements Portable {

    String name;
    int k;

    public NamedPortable() {
    }

    public NamedPortable(String name, int k) {
        this.name = name;
        this.k = k;
    }

    @Override
    public int getClassId() {
        return TestSerializationConstants.NAMED_PORTABLE;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("name", name);
        writer.writeInt("myint", k);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        k = reader.readInt("myint");
        name = reader.readUTF("name");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof NamedPortable)) {
            return false;
        }

        NamedPortable that = (NamedPortable) o;
        if (k != that.k) {
            return false;
        }
        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + k;
        return result;
    }

    @Override
    public int getFactoryId() {
        return TestSerializationConstants.PORTABLE_FACTORY_ID;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NamedPortable{");
        sb.append("name='").append(name).append('\'');
        sb.append(", k=").append(k);
        sb.append('}');
        return sb.toString();
    }
}
