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

package com.hazelcast.aggregation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

public class NumberContainer implements DataSerializable {

    enum ValueType {
        INTEGER,
        LONG,
        FLOAT,
        DOUBLE,
        BIG_DECIMAL,
        BIG_INTEGER,
        NUMBER
    }

    public int intValue;
    public long longValue;

    public float floatValue;
    public double doubleValue;

    public BigDecimal bigDecimal;
    public BigInteger bigInteger;

    public Number numberValue;

    public NumberContainer() {
    }

    public NumberContainer(int intValue) {
        this.intValue = intValue;
    }

    public NumberContainer(long longValue) {
        this.longValue = longValue;
    }

    public NumberContainer(float floatValue) {
        this.floatValue = floatValue;
    }

    public NumberContainer(double doubleValue) {
        this.doubleValue = doubleValue;
    }

    public NumberContainer(BigDecimal bigDecimal) {
        this.bigDecimal = bigDecimal;
    }

    public NumberContainer(BigInteger bigInteger) {
        this.bigInteger = bigInteger;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(intValue);
        out.writeLong(longValue);
        out.writeFloat(floatValue);
        out.writeDouble(doubleValue);
        out.writeObject(bigDecimal);
        out.writeObject(bigInteger);
        out.writeObject(numberValue);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        intValue = in.readInt();
        longValue = in.readLong();
        floatValue = in.readFloat();
        doubleValue = in.readDouble();
        bigDecimal = in.readObject(BigDecimal.class);
        bigInteger = in.readObject(BigInteger.class);
        numberValue = in.readObject(Number.class);
    }
}
