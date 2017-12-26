package com.hazelcast.dataseries;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataSeriesConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AppendTest extends HazelcastTestSupport {

    @Test
    public void whenSimple() {
        Config config = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "1")
                .addDataSeriesConfig(
                        new DataSeriesConfig("bytes")
                                .setInitialSegmentSize(1024));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataSeries dataSeries = hz.getDataSeries("bytes");
        long sequence = dataSeries.append(0, new byte[]{1, 2, 3, 5});

        assertEquals(1, dataSeries.count());
        assertEquals(1, dataSeries.memoryInfo().segmentsInUse());
    }

//    @Test
//    public void whenAppendMultiplePartitions() {
//        Config config = new Config()
//                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "10")
//                .addDataSeriesConfig(
//                        new DataSeriesConfig("employees")
//                                .setKeyClass(Long.class)
//                                .setValueClass(Employee.class));
//
//        HazelcastInstance hz = createHazelcastInstance(config);
//
//        DataSeries<Long, Employee> dataSeries = hz.getDataSeries("employees");
//
//        long count = 10000;
//        for (long k = 0; k < count; k++) {
//            dataSeries.append(k, new Employee(1, 1, 1));
//        }
//
//        assertEquals(count, dataSeries.count());
//        assertEquals(10, dataSeries.memoryInfo().segmentsInUse());
//    }
//
//    @Test
//    public void whenGrowingRequired() {
//        Config config = new Config()
//                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "1")
//                .addDataSeriesConfig(
//                        new DataSeriesConfig("employees")
//                                .setInitialSegmentSize(1024)
//                                .setKeyClass(Long.class)
//                                .setValueClass(Employee.class));
//
//        HazelcastInstance hz = createHazelcastInstance(config);
//
//        DataSeries<Long, Employee> dataSeries = hz.getDataSeries("employees");
//        int itemCount = 100 * 1000;
//        for (int k = 0; k < itemCount; k++) {
//            dataSeries.append((long) k, new Employee(k, k, k));
//        }
//
//        assertEquals(itemCount, dataSeries.count());
//        assertEquals(2, dataSeries.memoryInfo().segmentsInUse());
//        System.out.println(dataSeries.memoryInfo());
//    }
//
//    @Test
//    public void whenMultipleSegmentsNeeded() {
//        Config config = new Config()
//                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "1")
//                .addDataSeriesConfig(
//                        new DataSeriesConfig("employees")
//                                .setInitialSegmentSize(1024)
//                                .setSegmentsPerPartition(Integer.MAX_VALUE)
//                                .setMaxSegmentSize(1024)
//                                .setKeyClass(Long.class)
//                                .setValueClass(Employee.class));
//
//        HazelcastInstance hz = createHazelcastInstance(config);
//
//        DataSeries<Long, Employee> dataSeries = hz.getDataSeries("employees");
//        int itemCount = 100 * 1000;
//        for (int k = 0; k < itemCount; k++) {
//            dataSeries.append((long) k, new Employee(k, k, k));
//        }
//
//        assertEquals(itemCount, dataSeries.count());
//        assertEquals(1961, dataSeries.memoryInfo().segmentsInUse());
//        System.out.println(dataSeries.memoryInfo());
//    }
//
//    @Test
//    public void whenMultipleSegmentsNeeded_andLimitOnSegment() {
//        Config config = new Config()
//                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "1")
//                .addDataSeriesConfig(
//                        new DataSeriesConfig("employees")
//                                .setInitialSegmentSize(1024)
//                                .setMaxSegmentSize(1024)
//                                .setSegmentsPerPartition(10)
//                                .setKeyClass(Long.class)
//                                .setValueClass(Employee.class));
//
//        HazelcastInstance hz = createHazelcastInstance(config);
//
//        DataSeries<Long, Employee> dataSeries = hz.getDataSeries("employees");
//        int itemCount = 100 * 1000;
//        for (int k = 0; k < itemCount; k++) {
//            dataSeries.append((long) k, new Employee(k, k, k));
//        }
//
//        //   assertEquals(itemCount, dataSeries.count());
//        assertEquals(10, dataSeries.memoryInfo().segmentsInUse());
//        System.out.println(dataSeries.memoryInfo());
//    }
//
//    //  @Test
//    public void whenTenuring() {
//        Config config = new Config()
//                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "1")
//                .addDataSeriesConfig(
//                        new DataSeriesConfig("employees")
//                                .setInitialSegmentSize(1024)
//                                .setTenuringAgeMillis((int) SECONDS.toMillis(5))
//                                //   .setMaxSegmentSize(Long.MAX_VALUE)
//                                .setKeyClass(Long.class)
//                                .setValueClass(Employee.class));
//
//        HazelcastInstance hz = createHazelcastInstance(config);
//
//        DataSeries<Long, Employee> dataSeries = hz.getDataSeries("employees");
//        int itemCount = 100 * 1000;
//        for (int k = 0; k < itemCount; k++) {
//            dataSeries.append((long) k, new Employee(k, k, k));
//            sleepSeconds(1);
//            // System.out.println(dataSeries.memoryUsage());
//        }
//
//        assertEquals(itemCount, dataSeries.count());
//    }

}
