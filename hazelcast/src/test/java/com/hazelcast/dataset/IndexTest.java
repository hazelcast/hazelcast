package com.hazelcast.dataset;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataSetConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.io.Serializable;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class IndexTest extends HazelcastTestSupport {

    @Test
    public void whenSimple() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.addDataSetConfig(new DataSetConfig("employees")
                .addIndexField("age")
                .setKeyClass(Long.class)
                .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataSet<Long, Employee> dataSet = hz.getDataSet("employees");
        dataSet.insert(0l, new Employee(1, 1, 1));
    }

    @Test
    public void whenBooleanIndex() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.addDataSetConfig(new DataSetConfig("foo")
                .addIndexField("field")
                .setKeyClass(Long.class)
                .setValueClass(BooleanObject.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataSet<Long, BooleanObject> dataSet = hz.getDataSet("foo");
        dataSet.insert(0l, new BooleanObject());
    }

    public static class BooleanObject implements Serializable {
        public boolean field;
    }


    @Test
    public void whenByteIndex() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.addDataSetConfig(new DataSetConfig("foo")
                .addIndexField("field")
                .setKeyClass(Long.class)
                .setValueClass(ByteObject.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataSet<Long, ByteObject> dataSet = hz.getDataSet("foo");
        dataSet.insert(0l, new ByteObject());
    }

    public static class ByteObject implements Serializable {
        public byte field;
    }

    @Test
    public void whenCharIndex() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.addDataSetConfig(new DataSetConfig("foo")
                .addIndexField("field")
                .setKeyClass(Long.class)
                .setValueClass(CharObject.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataSet<Long, CharObject> dataSet = hz.getDataSet("foo");
        dataSet.insert(0l, new CharObject());
    }

    public static class CharObject implements Serializable {
        public char field;
    }

    @Test
    public void whenShortIndex() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.addDataSetConfig(new DataSetConfig("foo")
                .addIndexField("field")
                .setKeyClass(Long.class)
                .setValueClass(ShortObject.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataSet<Long, ShortObject> dataSet = hz.getDataSet("foo");
        dataSet.insert(0l, new ShortObject());
    }

    public static class ShortObject implements Serializable {
        public short field;
    }

    @Test
    public void whenIntIndex() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.addDataSetConfig(new DataSetConfig("foo")
                .addIndexField("field")
                .setKeyClass(Long.class)
                .setValueClass(IntObject.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataSet<Long, IntObject> dataSet = hz.getDataSet("foo");
        dataSet.insert(0l, new IntObject());
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
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.addDataSetConfig(new DataSetConfig("foo")
                .addIndexField("field")
                .setKeyClass(Long.class)
                .setValueClass(IntObject.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataSet<Long, IntObject> dataSet = hz.getDataSet("foo");
        int itemCount = 100 * 1000;
        Random random = new Random();
        for (int k = 0; k < itemCount; k++) {
            dataSet.insert((long) k, new IntObject(random.nextInt()));
            // System.out.println(dataSet.memoryUsage());
        }

        assertEquals(itemCount, dataSet.count());
    }

    @Test
    public void whenLongIndex() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.addDataSetConfig(new DataSetConfig("foo")
                .addIndexField("field")
                .setKeyClass(Long.class)
                .setValueClass(LongObject.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataSet<Long, LongObject> dataSet = hz.getDataSet("foo");
        dataSet.insert(0l, new LongObject());
    }

    public static class LongObject implements Serializable {
        public long field;
    }

    @Test
    public void whenFloatIndex() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.addDataSetConfig(new DataSetConfig("foo")
                .addIndexField("field")
                .setKeyClass(Long.class)
                .setValueClass(FloatObject.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataSet<Long, FloatObject> dataSet = hz.getDataSet("foo");
        dataSet.insert(0l, new FloatObject());
    }

    public static class FloatObject implements Serializable {
        public float field;
    }

    @Test
    public void whenDoubleIndex() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.addDataSetConfig(new DataSetConfig("foo")
                .addIndexField("field")
                .setKeyClass(Long.class)
                .setValueClass(DoubleObject.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataSet<Long, DoubleObject> dataSet = hz.getDataSet("foo");
        dataSet.insert(0l, new DoubleObject());
    }

    public static class DoubleObject implements Serializable {
        public double field;
    }


    @Test
    public void whenMultipleIndices() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.addDataSetConfig(new DataSetConfig("foo")
                .addIndexField("i")
                .addIndexField("l")
                .addIndexField("d")
                .addIndexField("f")
                .addIndexField("b")
                .addIndexField("c")
                .addIndexField("s")
                .addIndexField("bt")
                .setKeyClass(Long.class)
                .setValueClass(MultipleObject.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataSet<Long, MultipleObject> dataSet = hz.getDataSet("foo");
        dataSet.insert(0l, new MultipleObject());
    }

    public static class MultipleObject implements Serializable {
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
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.addDataSetConfig(new DataSetConfig("foo")
                .addIndexField("i")
                .addIndexField("l")
                .addIndexField("d")
                .addIndexField("f")
                .addIndexField("b")
                .addIndexField("c")
                .addIndexField("s")
                .addIndexField("bt")
                .setKeyClass(Long.class)
                .setValueClass(MultipleObject.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataSet<Long, MultipleObject> dataSet = hz.getDataSet("foo");
        int itemCount = 100 * 1000;
        Random random = new Random();
        for (int k = 0; k < itemCount; k++) {
            MultipleObject record = new MultipleObject();
            record.i = random.nextInt();
            record.l = random.nextLong();
            record.d = random.nextDouble();
            record.f = random.nextFloat();
            record.b = random.nextBoolean();
            record.c = (char) random.nextInt();
            record.s = (short) random.nextInt();
            record.bt = (byte) random.nextInt();
            dataSet.insert((long) k, record);
        }

        assertEquals(itemCount, dataSet.count());
    }
}
