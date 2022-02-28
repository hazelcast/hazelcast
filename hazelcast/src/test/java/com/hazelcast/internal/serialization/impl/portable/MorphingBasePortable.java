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
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

/**
 * Portable for version support tests
 */
public class MorphingBasePortable implements Portable {

    byte aByte;
    boolean aBoolean;
    char character;
    short aShort;
    int integer;
    long aLong;
    float aFloat;
    double aDouble;
    String aString;

    public MorphingBasePortable(byte aByte, boolean aBoolean, char character, short aShort, int integer, long aLong,
                                float aFloat, double aDouble, String aString) {
        this.aByte = aByte;
        this.aBoolean = aBoolean;
        this.character = character;
        this.aShort = aShort;
        this.integer = integer;
        this.aLong = aLong;
        this.aFloat = aFloat;
        this.aDouble = aDouble;
        this.aString = aString;
    }

    public MorphingBasePortable() {
    }

    @Override
    public int getFactoryId() {
        return TestSerializationConstants.PORTABLE_FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return TestSerializationConstants.MORPHING_PORTABLE;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeByte("byte", aByte);
        writer.writeBoolean("boolean", aBoolean);
        writer.writeChar("char", character);
        writer.writeShort("short", aShort);
        writer.writeInt("int", integer);
        writer.writeLong("long", aLong);
        writer.writeFloat("float", aFloat);
        writer.writeDouble("double", aDouble);
        writer.writeString("string", aString);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        this.aByte = reader.readByte("byte");
        this.aBoolean = reader.readBoolean("boolean");
        this.character = reader.readChar("char");
        this.aShort = reader.readShort("short");
        this.integer = reader.readInt("int");
        this.aLong = reader.readLong("long");
        this.aFloat = reader.readFloat("float");
        this.aDouble = reader.readDouble("double");
        this.aString = reader.readString("string");
    }
}
