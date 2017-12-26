package com.hazelcast.dataseries;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataSeriesConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CountTest extends HazelcastTestSupport {

//    @Test
//    public void whenNotEmpty() {
//        Config config = new Config()
//                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "1")
//                .addDataSeriesConfig(
//                        new DataSeriesConfig("employees")
//                                .setKeyClass(Long.class)
//                                .setValueClass(Employee.class));
//
//        HazelcastInstance hz = createHazelcastInstance(config);
//
//        DataSeries<Long, Employee> dataSeries = hz.getDataSeries("employees");
//        for (int k = 0; k < 5; k++) {
//            dataSeries.append((long) k, new Employee(k, k, k));
//        }
//
//        assertEquals(5, dataSeries.count());
//    }
//
//    @Test
//    public void whenEmpty() {
//        Config config = new Config()
//                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "1")
//                .addDataSeriesConfig(
//                        new DataSeriesConfig("employees")
//                                .setKeyClass(Long.class)
//                                .setValueClass(Employee.class));
//
//        HazelcastInstance hz = createHazelcastInstance(config);
//
//        DataSeries<Long, Employee> dataSeries = hz.getDataSeries("employees");
//
//        assertEquals(0, dataSeries.count());
//    }
}
