/*
 * Copyright 2024 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.module;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public class PortablePojo extends Pojo implements Portable {
    public static final int ID = 1;

    public PortablePojo() {
    }

    public PortablePojo(Integer f0, Integer f1, Integer f2) {
        super(f0, f1, f2);
    }

    @Override
    public int getFactoryId() {
        return MyPortableFactory.ID;
    }

    @Override
    public int getClassId() {
        return ID;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeInt("f0", f0);
        writer.writeInt("f1", f1);
        writer.writeInt("f2", f2);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        f0 = reader.readInt("f0");
        f1 = reader.readInt("f1");
        f2 = reader.readInt("f2");
    }
}
