package com.hazelcast.client.mapreduce.aggregation;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiMap;
import com.hazelcast.mapreduce.aggregation.Aggregation;
import com.hazelcast.mapreduce.aggregation.Aggregations;
import com.hazelcast.mapreduce.aggregation.Supplier;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MultiMapAggregationLiteMemberTest extends HazelcastTestSupport {

    private TestHazelcastFactory factory;

    private HazelcastInstance client;

    @Before
    public void before() {
        factory = new TestHazelcastFactory();
        final HazelcastInstance lite = factory.newHazelcastInstance(new Config().setLiteMember(true));
        final HazelcastInstance instance1 = factory.newHazelcastInstance();
        final HazelcastInstance instance2 = factory.newHazelcastInstance();

        assertClusterSizeEventually(3, lite);
        assertClusterSizeEventually(3, instance1);
        assertClusterSizeEventually(3, instance2);

        client = factory.newHazelcastClient();
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test(timeout = 60000)
    public void testMaxAggregation() {
        testMaxAggregation(client);
    }


    public static void testMaxAggregation(final HazelcastInstance instance) {
        final int size = 2000;
        List<Integer> numbers = new ArrayList<Integer>(size);
        for (int i = 0; i < size; i++) {
            numbers.add(i);
        }

        Collections.shuffle(numbers);
        numbers = numbers.subList(0, 1000);
        final Integer expected = Collections.max(numbers);

        final MultiMap<Integer, Integer> map = instance.getMultiMap(randomMapName());
        for (Integer number : numbers) {
            map.put(number / 2, number);
        }

        final Aggregation<Integer, Integer, Integer> maxAggregation = Aggregations.integerMax();
        final Integer max = map.aggregate(new ValueSupplier(), maxAggregation);

        assertEquals(expected, max);
    }

    public static class ValueSupplier extends Supplier<Integer, Integer, Integer> implements Serializable {

        @Override
        public Integer apply(Map.Entry<Integer, Integer> entry) {
            return entry.getValue();
        }
    }

}
