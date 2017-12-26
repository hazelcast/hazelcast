package com.hazelcast.dataseries;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.aggregation.impl.LongAverageAggregator;
import com.hazelcast.aggregation.impl.MaxAggregator;
import com.hazelcast.config.Config;
import com.hazelcast.config.DataSeriesConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class AggregationTest extends HazelcastTestSupport {

    @Test
    public void whenForkJoinUsed() {
        Config config = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "1")
                .addDataSeriesConfig(
                        new DataSeriesConfig("employees")
                                .setSegmentsPerPartition(Integer.MAX_VALUE)
                                .setMaxSegmentSize(16*1024)
                                .setKeyClass(Long.class)
                                .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataSeries<Long, Employee> dataSeries = hz.getDataSeries("employees");
        int itemCount = 1000 * 1000;
        long maxAge = Long.MIN_VALUE;
        Random random = new Random();
        for (int k = 0; k < itemCount; k++) {
            int age = random.nextInt(10000000);
            maxAge = Math.max(maxAge, age);
            dataSeries.append((long) k, new Employee(age, k, k));
        }

        Aggregator aggregator = new MaxAggregator();
        PreparedAggregation preparedAggregation = dataSeries.prepare(
                new AggregationRecipe<Long, Age>(Age.class, aggregator, new SqlPredicate("true")));
        Map<String, Object> bindings = new HashMap<String, Object>();
        // bindings.put("age", 200);
        //bindings.put("iq", 100l);
        Object result = preparedAggregation.executeForkJoin(bindings);
        assertEquals(maxAge, result);
    }

    @Test
    public void maxAgeAggregationMultiplePartitions() {
        Config config = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "10")
                .addDataSeriesConfig(
                        new DataSeriesConfig("employees")
                                .setKeyClass(Long.class)
                                .setValueClass(Employee.class));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(1).newInstances(config);

        DataSeries<Long, Employee> dataSeries = cluster[0].getDataSeries("employees");
        long maxAge = Long.MIN_VALUE;
        Random random = new Random();
        for (long k = 0; k < 1000; k++) {
            int age = random.nextInt(100000);
            maxAge = Math.max(maxAge, age);
            dataSeries.append(k, new Employee(age, (int)k, (int)k));
        }

        Aggregator aggregator = new MaxAggregator();

        PreparedAggregation preparedAggregation = dataSeries.prepare(
                new AggregationRecipe<Long, Age>(Age.class, aggregator, new SqlPredicate("true")));
        Map<String, Object> bindings = new HashMap<String, Object>();
        // bindings.put("age", 200);
        //bindings.put("iq", 100l);
        Object result = preparedAggregation.executePartitionThread(bindings);

        System.out.println("max inserted age:" + maxAge);
        assertEquals(maxAge, result);
    }

    @Test
    public void maxAgeAggregationSinglePartition() {
        Config config = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "1")
                .addDataSeriesConfig(
                        new DataSeriesConfig("employees")
                                .setKeyClass(Long.class)
                                .setValueClass(Employee.class));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);

        DataSeries<Long, Employee> dataSeries = cluster[0].getDataSeries("employees");
        long maxAge = Long.MIN_VALUE;
        Random random = new Random();
        for (int k = 0; k < 1000; k++) {
            int age = random.nextInt(100000);
            maxAge = Math.max(maxAge, age);
            dataSeries.append((long) k, new Employee(age, k, k));
        }

        Aggregator aggregator = new MaxAggregator();

        PreparedAggregation preparedAggregation = dataSeries.prepare(
                new AggregationRecipe<Long, Age>(Age.class, aggregator, new SqlPredicate("true")));
        Map<String, Object> bindings = new HashMap<String, Object>();
        // bindings.put("age", 200);
        //bindings.put("iq", 100l);
        Object result = preparedAggregation.executePartitionThread(bindings);
        assertEquals(maxAge, result);
    }

    @Test
    public void averageAgeAggregation() {
        Config config = new Config()
                .addDataSeriesConfig(
                        new DataSeriesConfig("employees")
                                .setKeyClass(Long.class)
                                .setValueClass(Employee.class));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);

        DataSeries<Long, Employee> dataSeries = cluster[0].getDataSeries("employees");
        double totalAge = 0;
        Random random = new Random();
        int count = 1000;
        for (int k = 0; k < count; k++) {
            int age = random.nextInt(100000);
            totalAge += age;
            dataSeries.append((long) k, new Employee(age, k, k));
        }

        Aggregator aggregator = new LongAverageAggregator();

        PreparedAggregation preparedAggregation = dataSeries.prepare(
                new AggregationRecipe<Long, Age>(Age.class, aggregator, new SqlPredicate("true")));
        Map<String, Object> bindings = new HashMap<String, Object>();

        Double result = (Double) preparedAggregation.executePartitionThread(bindings);

        assertEquals(totalAge / count, (double) result, 0.1);
    }
}
