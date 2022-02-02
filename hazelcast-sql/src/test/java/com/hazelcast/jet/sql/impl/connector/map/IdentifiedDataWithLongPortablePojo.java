/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

/**
 * A sample IdentifiedDataSerializable object implementation.
 * With 20 integers in 'numbers' and a long in 'value'.
 */
public class IdentifiedDataWithLongPortablePojo implements Portable {

    public Integer[] numbers;
    public Long value;

    public IdentifiedDataWithLongPortablePojo(){

    }

    public IdentifiedDataWithLongPortablePojo(Integer[] numbers, Long value)
    {
        this.numbers = numbers;
        this.value = value;
    }

    @Override
    public int getFactoryId() {
        return 1;
    }

    @Override
    public int getClassId() {
        return 1;
    }

    @Override
    public void writePortable(PortableWriter portableWriter) throws IOException {
        portableWriter.writeLong("value", value);
        portableWriter.writeInt("numbers-size", numbers.length);
        for (int i = 0; i < numbers.length; i++) {
            Integer number = numbers[i];
            if (number != null) {
                portableWriter.writeBoolean("numbers-present-" + i, true);
                portableWriter.writeInt("numbers-" + i, number);
            } else {
                portableWriter.writeBoolean("numbers-present-" + i, false);
            }
        }
    }

    @Override
    public void readPortable(PortableReader portableReader) throws IOException {
        value = portableReader.readLong("value");
        numbers = new Integer[portableReader.readInt("numbers-size")];
        for (int i = 0; i < numbers.length; i++) {
            if (portableReader.readBoolean("numbers-present-" + i)) {
                numbers[i] = portableReader.readInt("numbers-" + i);
            }
        }
    }
}
