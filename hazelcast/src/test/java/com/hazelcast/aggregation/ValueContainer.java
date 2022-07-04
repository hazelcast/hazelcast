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

package com.hazelcast.aggregation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

public class ValueContainer implements DataSerializable, Comparable<ValueContainer> {

    enum ValueType {
        INTEGER,
        LONG,
        FLOAT,
        DOUBLE,
        BIG_DECIMAL,
        BIG_INTEGER,
        NUMBER,
        STRING
    }

    public ValueType valueType;

    public int intValue;
    public long longValue;

    public float floatValue;
    public double doubleValue;

    public BigDecimal bigDecimal;
    public BigInteger bigInteger;

    public Number numberValue;
    public String stringValue;

    public ValueContainer() {
        this.valueType = ValueType.NUMBER;
    }

    public ValueContainer(int intValue) {
        this.valueType = ValueType.INTEGER;
        this.intValue = intValue;
    }

    public ValueContainer(long longValue) {
        this.valueType = ValueType.LONG;
        this.longValue = longValue;
    }

    public ValueContainer(float floatValue) {
        this.valueType = ValueType.FLOAT;
        this.floatValue = floatValue;
    }

    public ValueContainer(double doubleValue) {
        this.valueType = ValueType.DOUBLE;
        this.doubleValue = doubleValue;
    }

    public ValueContainer(BigDecimal bigDecimal) {
        this.valueType = ValueType.BIG_DECIMAL;
        this.bigDecimal = bigDecimal;
    }

    public ValueContainer(BigInteger bigInteger) {
        this.valueType = ValueType.BIG_INTEGER;
        this.bigInteger = bigInteger;
    }

    public ValueContainer(String stringValue) {
        this.valueType = ValueType.STRING;
        this.stringValue = stringValue;
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
        out.writeString(stringValue);
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
        stringValue = in.readString();
    }

    @Override
    public int compareTo(ValueContainer o) {
        switch (valueType) {
            case INTEGER:
                return (intValue < o.intValue) ? -1 : ((intValue == o.intValue) ? 0 : 1);
            case LONG:
                return (longValue < o.longValue) ? -1 : ((longValue == o.longValue) ? 0 : 1);
            case FLOAT:
                return Float.compare(floatValue, o.floatValue);
            case DOUBLE:
                return Double.compare(doubleValue, o.doubleValue);
            case BIG_DECIMAL:
                return bigDecimal.compareTo(o.bigDecimal);
            case BIG_INTEGER:
                return bigInteger.compareTo(o.bigInteger);
            case STRING:
                return stringValue.compareTo(o.stringValue);
            default:
                return 0;
        }
    }
}
