package com.hazelcast.dataseries;

import com.hazelcast.aggregation.impl.MaxAggregator;
import com.hazelcast.config.Config;
import com.hazelcast.config.DataSeriesConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;

public class FetchAggregateTest extends HazelcastTestSupport {

    @Test
    public void maxAgeAggregation() {
        Config config = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "10")
                .addDataSeriesConfig(
                        new DataSeriesConfig("employees")
                                .setKeyClass(Long.class)
                                .setValueClass(Employee.class)
                                .addAttachedAggregator("maxAge", () -> new MaxAggregator("age")));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);

        DataSeries<Long, Employee> dataSeries = cluster[0].getDataSeries("employees");
        int maxAge = Integer.MIN_VALUE;
        Random random = new Random();
        for (int k = 0; k < 1000; k++) {
            int age = random.nextInt(100000);
            maxAge = Math.max(maxAge, age);
            dataSeries.append((long) k, new Employee(age, k, k));
        }

        assertEquals(new Integer(maxAge), dataSeries.aggregate("maxAge"));
    }
}
