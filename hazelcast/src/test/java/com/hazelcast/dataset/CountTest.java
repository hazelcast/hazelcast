package com.hazelcast.dataset;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataSetConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CountTest extends HazelcastTestSupport {

    @Test
    public void whenNotEmpty() {
        Config config = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "1")
                .addDataSetConfig(
                        new DataSetConfig("employees")
                                .setKeyClass(Long.class)
                                .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataSet<Long, Employee> dataSet = hz.getDataSet("employees");
        for (int k = 0; k < 5; k++) {
            dataSet.insert((long) k, new Employee(k, k, k));
        }

        assertEquals(5, dataSet.count());
    }

    @Test
    public void whenEmpty() {
        Config config = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "1")
                .addDataSetConfig(
                        new DataSetConfig("employees")
                                .setKeyClass(Long.class)
                                .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataSet<Long, Employee> dataSet = hz.getDataSet("employees");

        assertEquals(0, dataSet.count());
    }
}
