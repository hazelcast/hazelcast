package com.hazelcast.dataset;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.aggregation.impl.MaxAggregator;
import com.hazelcast.config.Config;
import com.hazelcast.config.DataSetConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.util.function.Supplier;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;

public class FetchAggregateTest extends HazelcastTestSupport {

    @Test
    public void maxAgeAggregation() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "10");
        config.addDataSetConfig(new DataSetConfig("employees")
                .setKeyClass(Long.class)
                .setValueClass(Employee.class)
                .addAttachedAggregator("maxAge", new Supplier<Aggregator>() {
                    @Override
                    public Aggregator get() {
                        return new MaxAggregator("age");
                    }
                }));


        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);

        DataSet<Long, Employee> dataSet = cluster[0].getDataSet("employees");
        int maxAge = Integer.MIN_VALUE;
        Random random = new Random();
        for (int k = 0; k < 1000; k++) {
            int age = random.nextInt(100000);
            maxAge = Math.max(maxAge, age);
            dataSet.insert((long) k, new Employee(age, k, k));
        }

        assertEquals(maxAge, dataSet.aggregate("maxAge"));
    }
}
