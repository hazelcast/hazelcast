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

package com.hazelcast.sql.impl.expression;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class ExpressionEndToEndTestBase extends SqlTestSupport {

    public static final String EXPR0 = "EXPR$0";

    protected static SqlService sql;

    private static TestHazelcastInstanceFactory factory;

    private static HazelcastInstance instance;

    @BeforeClass
    public static void beforeClass() {
        Config config = smallInstanceConfig();
        config.getSerializationConfig().addDataSerializableFactory(123, typeId -> {
            assert typeId == 321;
            return new IdentifiedDataSerializableRecord();
        });
        config.getSerializationConfig().addPortableFactory(300, classId -> {
            assert classId == 100;
            return new PortableRecord();
        });

        factory = new TestHazelcastInstanceFactory(2);
        sql = factory.newHazelcastInstance(config).getSql();
        instance = factory.newHazelcastInstance(config);

        IMap<RecordKey, SerializableRecord> serializableRecords = instance.getMap("serializableRecords");
        serializableRecords.put(new RecordKey(0), new SerializableRecord());

        IMap<RecordKey, DataSerializableRecord> dataSerializableRecords = instance.getMap("dataSerializableRecords");
        dataSerializableRecords.put(new RecordKey(0), new DataSerializableRecord());

        IMap<RecordKey, IdentifiedDataSerializableRecord> identifiedDataSerializableRecords =
                instance.getMap("identifiedDataSerializableRecords");
        identifiedDataSerializableRecords.put(new RecordKey(0), new IdentifiedDataSerializableRecord());

        IMap<RecordKey, PortableRecord> portableRecords = instance.getMap("portableRecords");
        portableRecords.put(new RecordKey(0), new PortableRecord());
    }

    @AfterClass
    public static void afterClass() {
        factory.shutdownAll();
    }

    protected String getMapName() {
        return "serializableRecords";
    }

    protected IMap<Object, Object> getMap() {
        return instance.getMap(getMapName());
    }

    protected IMap<Object, Object> getMap(String mapName) {
        return instance.getMap(mapName);
    }

    protected Record getRecord() {
        return new SerializableRecord();
    }

    protected void assertRow(String expression, String expectedColumnName, SqlColumnType expectedType, Object expectedValue,
                             Object... args) {
        assertRow(expression, getMapName(), expectedColumnName, expectedType, expectedValue, args);
    }

    protected void assertRow(String expression, String mapName, String expectedColumnName, SqlColumnType expectedType,
                             Object expectedValue, Object... args) {
        boolean done = false;
        for (SqlRow row : sql.execute("select " + expression + " from " + mapName, args)) {
            if (done) {
                fail("one row expected");
            }
            done = true;

            assertEquals(1, row.getMetadata().getColumnCount());
            assertEquals(expectedColumnName, row.getMetadata().getColumn(0).getName());
            assertEquals(expectedType, row.getMetadata().getColumn(0).getType());
            assertEquals(expectedValue, row.getObject(0));
        }
    }

    protected void assertParsingError(String expression, String message, Object... args) {
        assertError(expression, SqlErrorCode.PARSING, message, args);
    }

    protected void assertDataError(String expression, String message, Object... args) {
        assertError(expression, SqlErrorCode.DATA_EXCEPTION, message, args);
    }

    protected void assertError(String expression, int errorCode, String message, Object... args) {
        assertError(expression, getMapName(), errorCode, message, args);
    }

    protected void assertError(String expression, String mapName, int errorCode, String message, Object... args) {
        try {
            //noinspection StatementWithEmptyBody
            for (SqlRow ignore : sql.execute("select " + expression + " from " + mapName, args)) {
                // do nothing
            }
        } catch (HazelcastSqlException e) {
            assertEquals(errorCode, e.getCode());
            assertTrue("expected message '" + message + "', got '" + e.getMessage() + "'",
                    e.getMessage().toLowerCase().contains(message.toLowerCase()));
            return;
        }
        fail("expected error: " + message);
    }

    public abstract static class Record {

        public boolean booleanTrue = true;

        public byte byte0 = 0;
        public byte byte1 = 1;
        public byte byte2 = 2;
        public byte byteMax = Byte.MAX_VALUE;
        public byte byteMin = Byte.MIN_VALUE;

        public short short0 = 0;
        public short short1 = 1;
        public short short2 = 2;
        public short shortMax = Short.MAX_VALUE;
        public short shortMin = Short.MIN_VALUE;

        public int int0 = 0;
        public int int1 = 1;
        public int int2 = 2;
        public int intMax = Integer.MAX_VALUE;
        public int intMin = Integer.MIN_VALUE;

        public long long0 = 0;
        public long long1 = 1;
        public long long2 = 2;
        public long longMax = Long.MAX_VALUE;
        public long longMin = Long.MIN_VALUE;

        public float float0 = 0;
        public float float1 = 1;
        public float float2 = 2;
        public float floatMax = Float.MAX_VALUE;
        public float floatMin = Float.MIN_VALUE;

        public double double0 = 0;
        public double double1 = 1;
        public double double2 = 2;
        public double doubleMax = Double.MAX_VALUE;
        public double doubleMin = Double.MIN_VALUE;

        public BigDecimal decimal0 = BigDecimal.valueOf(0);
        public BigDecimal decimal1 = BigDecimal.valueOf(1);
        public BigDecimal decimal2 = BigDecimal.valueOf(2);
        public BigDecimal decimalBig = new BigDecimal(Long.MAX_VALUE + "0");
        public BigDecimal decimalBigNegative = new BigDecimal("-" + Long.MAX_VALUE + "0");

        public BigInteger bigInteger0 = BigInteger.valueOf(0);
        public BigInteger bigInteger1 = BigInteger.valueOf(1);
        public BigInteger bigInteger2 = BigInteger.valueOf(2);
        public BigInteger bigIntegerBig = new BigInteger(Long.MAX_VALUE + "0");
        public BigInteger bigIntegerBigNegative = new BigInteger("-" + Long.MAX_VALUE + "0");

        public String string0 = "0";
        public String string1 = "1";
        public String string2 = "2";
        public String stringLongMax = Long.toString(Long.MAX_VALUE);
        public String stringBig = Long.MAX_VALUE + "0";
        public String stringBigNegative = "-" + Long.MAX_VALUE + "0";
        public String stringFoo = "foo";
        public String stringFalse = "FaLsE";
        public char char0 = '0';
        public char char1 = '1';
        public char char2 = '2';
        public char charF = 'f';

        public Object objectBooleanTrue = true;
        public Object objectByte1 = (byte) 1;
        public Object objectShort1 = (short) 1;
        public Object objectInt1 = 1;
        public Object objectLong1 = 1L;
        public Object objectFloat1 = 1.0f;
        public Object objectDouble1 = 1.0;
        public Object objectDecimal1 = BigDecimal.valueOf(1);
        public Object objectBigInteger1 = BigInteger.valueOf(1);
        public Object objectString1 = "1";
        public Object objectChar1 = '1';
        public Object object = new SerializableObject();

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Record record = (Record) o;

            if (booleanTrue != record.booleanTrue) {
                return false;
            }
            if (byte0 != record.byte0) {
                return false;
            }
            if (byte1 != record.byte1) {
                return false;
            }
            if (byte2 != record.byte2) {
                return false;
            }
            if (byteMax != record.byteMax) {
                return false;
            }
            if (byteMin != record.byteMin) {
                return false;
            }
            if (short0 != record.short0) {
                return false;
            }
            if (short1 != record.short1) {
                return false;
            }
            if (short2 != record.short2) {
                return false;
            }
            if (shortMax != record.shortMax) {
                return false;
            }
            if (shortMin != record.shortMin) {
                return false;
            }
            if (int0 != record.int0) {
                return false;
            }
            if (int1 != record.int1) {
                return false;
            }
            if (int2 != record.int2) {
                return false;
            }
            if (intMax != record.intMax) {
                return false;
            }
            if (intMin != record.intMin) {
                return false;
            }
            if (long0 != record.long0) {
                return false;
            }
            if (long1 != record.long1) {
                return false;
            }
            if (long2 != record.long2) {
                return false;
            }
            if (longMax != record.longMax) {
                return false;
            }
            if (longMin != record.longMin) {
                return false;
            }
            if (Float.compare(record.float0, float0) != 0) {
                return false;
            }
            if (Float.compare(record.float1, float1) != 0) {
                return false;
            }
            if (Float.compare(record.float2, float2) != 0) {
                return false;
            }
            if (Float.compare(record.floatMax, floatMax) != 0) {
                return false;
            }
            if (Float.compare(record.floatMin, floatMin) != 0) {
                return false;
            }
            if (Double.compare(record.double0, double0) != 0) {
                return false;
            }
            if (Double.compare(record.double1, double1) != 0) {
                return false;
            }
            if (Double.compare(record.double2, double2) != 0) {
                return false;
            }
            if (Double.compare(record.doubleMax, doubleMax) != 0) {
                return false;
            }
            if (Double.compare(record.doubleMin, doubleMin) != 0) {
                return false;
            }
            if (char0 != record.char0) {
                return false;
            }
            if (char1 != record.char1) {
                return false;
            }
            if (char2 != record.char2) {
                return false;
            }
            if (charF != record.charF) {
                return false;
            }
            if (!decimal0.equals(record.decimal0)) {
                return false;
            }
            if (!decimal1.equals(record.decimal1)) {
                return false;
            }
            if (!decimal2.equals(record.decimal2)) {
                return false;
            }
            if (!decimalBig.equals(record.decimalBig)) {
                return false;
            }
            if (!decimalBigNegative.equals(record.decimalBigNegative)) {
                return false;
            }
            if (!bigInteger0.equals(record.bigInteger0)) {
                return false;
            }
            if (!bigInteger1.equals(record.bigInteger1)) {
                return false;
            }
            if (!bigInteger2.equals(record.bigInteger2)) {
                return false;
            }
            if (!bigIntegerBig.equals(record.bigIntegerBig)) {
                return false;
            }
            if (!bigIntegerBigNegative.equals(record.bigIntegerBigNegative)) {
                return false;
            }
            if (!string0.equals(record.string0)) {
                return false;
            }
            if (!string1.equals(record.string1)) {
                return false;
            }
            if (!string2.equals(record.string2)) {
                return false;
            }
            if (!stringLongMax.equals(record.stringLongMax)) {
                return false;
            }
            if (!stringBig.equals(record.stringBig)) {
                return false;
            }
            if (!stringBigNegative.equals(record.stringBigNegative)) {
                return false;
            }
            if (!stringFoo.equals(record.stringFoo)) {
                return false;
            }
            if (!stringFalse.equals(record.stringFalse)) {
                return false;
            }
            if (!objectBooleanTrue.equals(record.objectBooleanTrue)) {
                return false;
            }
            if (!objectByte1.equals(record.objectByte1)) {
                return false;
            }
            if (!objectShort1.equals(record.objectShort1)) {
                return false;
            }
            if (!objectInt1.equals(record.objectInt1)) {
                return false;
            }
            if (!objectLong1.equals(record.objectLong1)) {
                return false;
            }
            if (!objectFloat1.equals(record.objectFloat1)) {
                return false;
            }
            if (!objectDouble1.equals(record.objectDouble1)) {
                return false;
            }
            if (!objectDecimal1.equals(record.objectDecimal1)) {
                return false;
            }
            if (!objectBigInteger1.equals(record.objectBigInteger1)) {
                return false;
            }
            if (!objectString1.equals(record.objectString1)) {
                return false;
            }
            if (!objectChar1.equals(record.objectChar1)) {
                return false;
            }
            return object.equals(record.object);
        }

        @Override
        public int hashCode() {
            int result;
            long temp;
            result = (booleanTrue ? 1 : 0);
            result = 31 * result + (int) byte0;
            result = 31 * result + (int) byte1;
            result = 31 * result + (int) byte2;
            result = 31 * result + (int) byteMax;
            result = 31 * result + (int) byteMin;
            result = 31 * result + (int) short0;
            result = 31 * result + (int) short1;
            result = 31 * result + (int) short2;
            result = 31 * result + (int) shortMax;
            result = 31 * result + (int) shortMin;
            result = 31 * result + int0;
            result = 31 * result + int1;
            result = 31 * result + int2;
            result = 31 * result + intMax;
            result = 31 * result + intMin;
            result = 31 * result + (int) (long0 ^ (long0 >>> 32));
            result = 31 * result + (int) (long1 ^ (long1 >>> 32));
            result = 31 * result + (int) (long2 ^ (long2 >>> 32));
            result = 31 * result + (int) (longMax ^ (longMax >>> 32));
            result = 31 * result + (int) (longMin ^ (longMin >>> 32));
            result = 31 * result + (float0 != +0.0f ? Float.floatToIntBits(float0) : 0);
            result = 31 * result + (float1 != +0.0f ? Float.floatToIntBits(float1) : 0);
            result = 31 * result + (float2 != +0.0f ? Float.floatToIntBits(float2) : 0);
            result = 31 * result + (floatMax != +0.0f ? Float.floatToIntBits(floatMax) : 0);
            result = 31 * result + (floatMin != +0.0f ? Float.floatToIntBits(floatMin) : 0);
            temp = Double.doubleToLongBits(double0);
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            temp = Double.doubleToLongBits(double1);
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            temp = Double.doubleToLongBits(double2);
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            temp = Double.doubleToLongBits(doubleMax);
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            temp = Double.doubleToLongBits(doubleMin);
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            result = 31 * result + decimal0.hashCode();
            result = 31 * result + decimal1.hashCode();
            result = 31 * result + decimal2.hashCode();
            result = 31 * result + decimalBig.hashCode();
            result = 31 * result + decimalBigNegative.hashCode();
            result = 31 * result + bigInteger0.hashCode();
            result = 31 * result + bigInteger1.hashCode();
            result = 31 * result + bigInteger2.hashCode();
            result = 31 * result + bigIntegerBig.hashCode();
            result = 31 * result + bigIntegerBigNegative.hashCode();
            result = 31 * result + string0.hashCode();
            result = 31 * result + string1.hashCode();
            result = 31 * result + string2.hashCode();
            result = 31 * result + stringLongMax.hashCode();
            result = 31 * result + stringBig.hashCode();
            result = 31 * result + stringBigNegative.hashCode();
            result = 31 * result + stringFoo.hashCode();
            result = 31 * result + stringFalse.hashCode();
            result = 31 * result + (int) char0;
            result = 31 * result + (int) char1;
            result = 31 * result + (int) char2;
            result = 31 * result + (int) charF;
            result = 31 * result + objectBooleanTrue.hashCode();
            result = 31 * result + objectByte1.hashCode();
            result = 31 * result + objectShort1.hashCode();
            result = 31 * result + objectInt1.hashCode();
            result = 31 * result + objectLong1.hashCode();
            result = 31 * result + objectFloat1.hashCode();
            result = 31 * result + objectDouble1.hashCode();
            result = 31 * result + objectDecimal1.hashCode();
            result = 31 * result + objectBigInteger1.hashCode();
            result = 31 * result + objectString1.hashCode();
            result = 31 * result + objectChar1.hashCode();
            result = 31 * result + object.hashCode();
            return result;
        }

    }

    public static final class SerializableRecord extends Record implements Serializable {

    }

    public static final class DataSerializableRecord extends Record implements DataSerializable {

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF("dummy");
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            String dummy = in.readUTF();
            assert dummy.equals("dummy");
        }

    }

    public static final class IdentifiedDataSerializableRecord extends Record implements IdentifiedDataSerializable {

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF("dummy");
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            String dummy = in.readUTF();
            assert dummy.equals("dummy");
        }

        @Override
        public int getFactoryId() {
            return 123;
        }

        @Override
        public int getClassId() {
            return 321;
        }

    }

    public static final class PortableRecord extends Record implements Portable {

        @Override
        public int getFactoryId() {
            return 300;
        }

        @Override
        public int getClassId() {
            return 100;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeBoolean("booleanTrue", booleanTrue);

            writer.writeByte("byte1", byte1);
            writer.writeShort("short1", short1);
            writer.writeInt("int1", int1);
            writer.writeLong("long1", long1);

            writer.writeFloat("float1", float1);
            writer.writeDouble("double1", double1);

            // no decimal
            // no big integer

            writer.writeUTF("string1", string1);
            writer.writeChar("char1", char1);

            // no object
        }

        @Override
        public void readPortable(PortableReader reader) {
            // do nothing
        }

    }

    public static final class BrokenRecord extends Record implements Serializable {

        public String byte1 = "foo";

    }

    public static final class SerializableObject implements Serializable {

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public boolean equals(Object obj) {
            return obj != null && getClass().equals(obj.getClass());
        }

        @Override
        public String toString() {
            return "object";
        }

    }

    public static final class RecordKey implements Serializable {

        public final int id;

        public RecordKey(int id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            RecordKey recordKey = (RecordKey) o;

            return id == recordKey.id;
        }

        @Override
        public int hashCode() {
            return id;
        }

    }

}
