/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datastream;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataStreamConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.io.Serializable;
import java.util.Random;

import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static org.junit.Assert.assertEquals;

public class IndexTest extends HazelcastTestSupport {

    @Test
    public void whenSimple() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(
                        new DataStreamConfig("employees")
                                .addIndexField("age")
                                .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<Employee> stream = hz.getDataStream("employees");
        DataStreamPublisher<Employee> publisher = stream.createPublisher();

        publisher.publish(0l, new Employee(1, 1, 1));
    }

    @Test
    public void whenBooleanIndex() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(
                        new DataStreamConfig("foo")
                                .addIndexField("field")
                                .setValueClass(BooleanObject.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<BooleanObject> stream = hz.getDataStream("foo");
        DataStreamPublisher<BooleanObject> publisher = stream.createPublisher();

        publisher.publish(0l, new BooleanObject());
    }

    public static class BooleanObject implements Serializable {
        public boolean field;
    }


    @Test
    public void whenByteIndex() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(
                        new DataStreamConfig("foo")
                                .addIndexField("field")
                                .setValueClass(ByteObject.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<ByteObject> stream = hz.getDataStream("foo");
        DataStreamPublisher<ByteObject> publisher = stream.createPublisher();

        publisher.publish(0l, new ByteObject());
    }

    public static class ByteObject implements Serializable {
        public byte field;
    }

    @Test
    public void whenCharIndex() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(
                        new DataStreamConfig("foo")
                                .addIndexField("field")
                                .setValueClass(CharObject.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<CharObject> stream = hz.getDataStream("foo");
        DataStreamPublisher<CharObject> publisher = stream.createPublisher();

        publisher.publish(0, new CharObject());
    }

    public static class CharObject implements Serializable {
        public char field;
    }

    @Test
    public void whenShortIndex() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(
                        new DataStreamConfig("foo")
                                .addIndexField("field")
                                .setValueClass(ShortObject.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<ShortObject> stream = hz.getDataStream("foo");
        DataStreamPublisher<ShortObject> publisher = stream.createPublisher();

        publisher.publish(0l, new ShortObject());
    }

    public static class ShortObject implements Serializable {
        public short field;
    }

    @Test
    public void whenIntIndex() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "1")
                .addDataStreamConfig(
                        new DataStreamConfig("foo")
                                .addIndexField("field")
                                .setValueClass(IntObject.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<IntObject> stream = hz.getDataStream("foo");
        DataStreamPublisher<IntObject> publisher = stream.createPublisher();
        publisher.publish(0l, new IntObject());
    }

    public static class IntObject implements Serializable {
        public int field;

        public IntObject() {
        }

        public IntObject(int field) {
            this.field = field;
        }
    }


    @Test
    public void whenInsertingManyIntObjects() {
        Config config = new Config();
        config.setProperty(PARTITION_COUNT.getName(), "1");
        config.addDataStreamConfig(new DataStreamConfig("foo")
                .addIndexField("field")
                .setValueClass(IntObject.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<IntObject> stream = hz.getDataStream("foo");
        DataStreamPublisher<IntObject> publisher = stream.createPublisher();

        int itemCount = 100 * 1000;
        Random random = new Random();
        for (int k = 0; k < itemCount; k++) {
            publisher.publish((long) k, new IntObject(random.nextInt()));
            // System.out.println(stream.memoryUsage());
        }

        assertEquals(itemCount, stream.asFrame().count());
    }

    @Test
    public void whenLongIndex() {
        Config config = new Config();
        config.setProperty(PARTITION_COUNT.getName(), "1");
        config.addDataStreamConfig(new DataStreamConfig("foo")
                .addIndexField("field")
                .setValueClass(LongObject.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<LongObject> stream = hz.getDataStream("foo");
        DataStreamPublisher<LongObject> publisher = stream.createPublisher();

        publisher.publish(0l, new LongObject());
    }

    public static class LongObject implements Serializable {
        public long field;
    }

    @Test
    public void whenFloatIndex() {
        Config config = new Config();
        config.setProperty(PARTITION_COUNT.getName(), "1");
        config.addDataStreamConfig(new DataStreamConfig("foo")
                .addIndexField("field")
                .setValueClass(FloatObject.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<FloatObject> stream = hz.getDataStream("foo");
        DataStreamPublisher<FloatObject> publisher = stream.createPublisher();

        publisher.publish(0l, new FloatObject());
    }

    public static class FloatObject implements Serializable {
        public float field;
    }

    @Test
    public void whenDoubleIndex() {
        Config config = new Config();
        config.setProperty(PARTITION_COUNT.getName(), "1");
        config.addDataStreamConfig(new DataStreamConfig("foo")
                .addIndexField("field")
                .setValueClass(DoubleObject.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<DoubleObject> stream = hz.getDataStream("foo");
        DataStreamPublisher<DoubleObject> publisher = stream.createPublisher();
        publisher.publish(0l, new DoubleObject());
    }

    public static class DoubleObject implements Serializable {
        public double field;
    }


    @Test
    public void whenMultipleIndices() {
        Config config = new Config();
        config.setProperty(PARTITION_COUNT.getName(), "1");
        config.addDataStreamConfig(new DataStreamConfig("foo")
                .addIndexField("i")
                .addIndexField("l")
                .addIndexField("d")
                .addIndexField("f")
                .addIndexField("b")
                .addIndexField("c")
                .addIndexField("s")
                .addIndexField("bt")
                .setValueClass(MultipleScalarsObject.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream<MultipleScalarsObject> stream = hz.getDataStream("foo");
        DataStreamPublisher<MultipleScalarsObject> publisher = stream.createPublisher();

        publisher.publish(0l, new MultipleScalarsObject());
    }

    public static class MultipleScalarsObject implements Serializable {
        public int i;
        public long l;
        public double d;
        public float f;
        public boolean b;
        public char c;
        public short s;
        public byte bt;
    }

    @Test
    public void whenInsertingMultipleObject() {
        Config config = new Config();
        config.setProperty(PARTITION_COUNT.getName(), "1");
        config.addDataStreamConfig(new DataStreamConfig("foo")
                .addIndexField("i")
                .addIndexField("l")
                .addIndexField("d")
                .addIndexField("f")
                .addIndexField("b")
                .addIndexField("c")
                .addIndexField("s")
                .addIndexField("bt")
                .setValueClass(MultipleScalarsObject.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataStream< MultipleScalarsObject> stream = hz.getDataStream("foo");
        DataStreamPublisher<MultipleScalarsObject> publisher = stream.createPublisher();

        int itemCount = 100 * 1000;
        Random random = new Random();
        for (int k = 0; k < itemCount; k++) {
            MultipleScalarsObject record = new MultipleScalarsObject();
            record.i = random.nextInt();
            record.l = random.nextLong();
            record.d = random.nextDouble();
            record.f = random.nextFloat();
            record.b = random.nextBoolean();
            record.c = (char) random.nextInt();
            record.s = (short) random.nextInt();
            record.bt = (byte) random.nextInt();
            publisher.publish((long) k, record);
        }

        assertEquals(itemCount, stream.asFrame().count());
    }
}
