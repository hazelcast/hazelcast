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
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.SqlException;
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
        for (SqlRow row : sql.query("select " + expression + " from " + mapName, args)) {
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
            for (SqlRow ignore : sql.query("select " + expression + " from " + mapName, args)) {
                // do nothing
            }
        } catch (SqlException e) {
            assertEquals(errorCode, e.getCode());
            assertTrue("expected message '" + message + "', got '" + e.getMessage() + "'",
                    e.getMessage().toLowerCase().contains(message.toLowerCase()));
            return;
        }
        fail("expected error: " + message);
    }

    public abstract static class Record {

        public boolean booleanTrue = true;

        public byte byte1 = 1;
        public short short1 = 1;
        public int int1 = 1;
        public long long1 = 1;

        public float float1 = 1;
        public double double1 = 1;

        public BigDecimal decimal1 = BigDecimal.valueOf(1);
        public BigInteger bigInteger1 = BigInteger.valueOf(1);

        public String string1 = "1";
        public char char1 = '1';

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
            if (byte1 != record.byte1) {
                return false;
            }
            if (short1 != record.short1) {
                return false;
            }
            if (int1 != record.int1) {
                return false;
            }
            if (long1 != record.long1) {
                return false;
            }
            if (Float.compare(record.float1, float1) != 0) {
                return false;
            }
            if (Double.compare(record.double1, double1) != 0) {
                return false;
            }
            if (char1 != record.char1) {
                return false;
            }
            if (!decimal1.equals(record.decimal1)) {
                return false;
            }
            if (!bigInteger1.equals(record.bigInteger1)) {
                return false;
            }
            if (!string1.equals(record.string1)) {
                return false;
            }
            return object.equals(record.object);
        }

        @Override
        public int hashCode() {
            int result;
            long temp;
            result = (booleanTrue ? 1 : 0);
            result = 31 * result + (int) byte1;
            result = 31 * result + (int) short1;
            result = 31 * result + int1;
            result = 31 * result + (int) (long1 ^ (long1 >>> 32));
            result = 31 * result + (float1 != +0.0f ? Float.floatToIntBits(float1) : 0);
            temp = Double.doubleToLongBits(double1);
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            result = 31 * result + decimal1.hashCode();
            result = 31 * result + bigInteger1.hashCode();
            result = 31 * result + string1.hashCode();
            result = 31 * result + (int) char1;
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
