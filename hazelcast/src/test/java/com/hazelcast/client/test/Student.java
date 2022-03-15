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

package com.hazelcast.client.test;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

/**
 * This class is for Non-java clients. Please do not remove or modify.
 */
public class Student implements Portable {
    private long age;
    private float height;

    @Override
    public int getFactoryId() {
        return PortableFactory.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return 6;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeLong("age", age);
        writer.writeFloat("height", height);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        age = reader.readLong("age");
        height = reader.readFloat("height");
    }
}
