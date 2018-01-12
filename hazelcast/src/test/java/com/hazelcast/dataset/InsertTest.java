package com.hazelcast.dataset;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataSetConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

public class InsertTest extends HazelcastTestSupport {

    @Test
    public void whenSimple() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.addDataSetConfig(new DataSetConfig("employees")
                .setInitialSegmentSize(1024)
                .setKeyClass(Long.class)
                .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataSet<Long, Employee> dataSet = hz.getDataSet("employees");
        dataSet.insert(0l, new Employee(1, 1, 1));

        assertEquals(1, dataSet.count());
        assertEquals(1, dataSet.memoryUsage().getSegmentsInUse());
    }

    @Test
    public void whenGrowingRequired() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.addDataSetConfig(new DataSetConfig("employees")
                .setInitialSegmentSize(1024)
                .setKeyClass(Long.class)
                .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataSet<Long, Employee> dataSet = hz.getDataSet("employees");
        int itemCount = 100 * 1000;
        for (int k = 0; k < itemCount; k++) {
            dataSet.insert((long) k, new Employee(k, k, k));
        }

        assertEquals(itemCount, dataSet.count());
        assertEquals(2, dataSet.memoryUsage().getSegmentsInUse());
        System.out.println(dataSet.memoryUsage());
    }

    @Test
    public void whenMultipleSegmentsNeeded() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.addDataSetConfig(new DataSetConfig("employees")
                .setInitialSegmentSize(1024)
                .setSegmentsPerPartition(Integer.MAX_VALUE)
                .setMaxSegmentSize(1024)
                .setKeyClass(Long.class)
                .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataSet<Long, Employee> dataSet = hz.getDataSet("employees");
        int itemCount = 100 * 1000;
        for (int k = 0; k < itemCount; k++) {
            dataSet.insert((long) k, new Employee(k, k, k));
        }

        assertEquals(itemCount, dataSet.count());
        assertEquals(1961, dataSet.memoryUsage().getSegmentsInUse());
        System.out.println(dataSet.memoryUsage());
    }

    @Test
    public void whenMultipleSegmentsNeeded_andLimitOnSegment() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.addDataSetConfig(new DataSetConfig("employees")
                .setInitialSegmentSize(1024)
                .setMaxSegmentSize(10)
                .setMaxSegmentSize(1024)
                .setKeyClass(Long.class)
                .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataSet<Long, Employee> dataSet = hz.getDataSet("employees");
        int itemCount = 100 * 1000;
        for (int k = 0; k < itemCount; k++) {
            dataSet.insert((long) k, new Employee(k, k, k));
        }

     //   assertEquals(itemCount, dataSet.count());
        assertEquals(10, dataSet.memoryUsage().getSegmentsInUse());
        System.out.println(dataSet.memoryUsage());
    }

    //  @Test
    public void whenTenuring() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.addDataSetConfig(new DataSetConfig("employees")
                .setInitialSegmentSize(1024)
                .setTenuringAgeMillis((int) SECONDS.toMillis(5))
                //   .setMaxSegmentSize(Long.MAX_VALUE)
                .setKeyClass(Long.class)
                .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataSet<Long, Employee> dataSet = hz.getDataSet("employees");
        int itemCount = 100 * 1000;
        for (int k = 0; k < itemCount; k++) {
            dataSet.insert((long) k, new Employee(k, k, k));
            sleepSeconds(1);
            // System.out.println(dataSet.memoryUsage());
        }

        assertEquals(itemCount, dataSet.count());
    }

}
