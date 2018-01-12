package com.hazelcast.dataset;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.aggregation.impl.LongAverageAggregator;
import com.hazelcast.aggregation.impl.MaxAggregator;
import com.hazelcast.config.Config;
import com.hazelcast.config.DataSetConfig;
import com.hazelcast.core.Hazelcast;
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
    public void maxAgeAggregation() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "10");
        config.addDataSetConfig(new DataSetConfig("employees").setKeyClass(Long.class).setValueClass(Employee.class));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);

        DataSet<Long, Employee> dataSet = cluster[0].getDataSet("employees");
        long maxAge = Long.MIN_VALUE;
        Random random = new Random();
        for (int k = 0; k < 1000; k++) {
            int age = random.nextInt(100000);
            maxAge = Math.max(maxAge, age);
            dataSet.insert((long) k, new Employee(age, k, k));
        }

        Aggregator aggregator = new MaxAggregator();

        CompiledAggregation compiledAggregation = dataSet.compile(
                new AggregationRecipe<Long, Age>(Age.class, aggregator, new SqlPredicate("true")));
        Map<String, Object> bindings = new HashMap<String, Object>();
        // bindings.put("age", 200);
        //bindings.put("iq", 100l);
        Object result = compiledAggregation.execute(bindings);
        assertEquals(maxAge, result);
    }

    @Test
    public void averageAgeAggregation() {
        Config config = new Config();
        config.addDataSetConfig(new DataSetConfig("employees").setKeyClass(Long.class).setValueClass(Employee.class));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);

        DataSet<Long, Employee> dataSet = cluster[0].getDataSet("employees");
        double totalAge = 0;
        Random random = new Random();
        int count = 1000;
        for (int k = 0; k < count; k++) {
            int age = random.nextInt(100000);
            totalAge+=age;
            dataSet.insert((long) k, new Employee(age, k, k));
        }

        Aggregator aggregator = new LongAverageAggregator();

        CompiledAggregation compiledAggregation = dataSet.compile(
                new AggregationRecipe<Long, Age>(Age.class, aggregator, new SqlPredicate("true")));
        Map<String, Object> bindings = new HashMap<String, Object>();

        Double result = (Double)compiledAggregation.execute(bindings);

        assertEquals(totalAge/count,(double)result, 0.1);
    }
}
