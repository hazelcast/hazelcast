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

package com.hazelcast.nio.serialization.compatibility;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public class AnInnerPortable implements Portable {

    public static final int classId = ReferenceObjects.INNER_PORTABLE_CLASS_ID;
    private int anInt;
    private float aFloat;

    public AnInnerPortable(int anInt, float aFloat) {
        this.anInt = anInt;
        this.aFloat = aFloat;
    }

    public AnInnerPortable() {
    }

    @Override
    public int getFactoryId() {
        return ReferenceObjects.PORTABLE_FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return ReferenceObjects.INNER_PORTABLE_CLASS_ID;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeInt("i", anInt);
        writer.writeFloat("f", aFloat);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        anInt = reader.readInt("i");
        aFloat = reader.readFloat("f");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AnInnerPortable that = (AnInnerPortable) o;
        if (anInt != that.anInt) {
            return false;
        }
        return Float.compare(that.aFloat, aFloat) == 0;
    }

    @Override
    public int hashCode() {
        int result = anInt;
        result = 31 * result + (aFloat != +0.0f ? Float.floatToIntBits(aFloat) : 0);
        return result;
    }

    @Override
    public String toString() {
        return "AnInnerPortable";
    }

    public int getAnInt() {
        return anInt;
    }

    public float getaFloat() {
        return aFloat;
    }
}
