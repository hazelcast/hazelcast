package com.hazelcast.map.merge;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.ThreadLocalRandomProvider;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
public class IndexAwarePredicateSplitBrainTest extends SplitBrainTestSupport {

    private static final List<String> POSSIBLE_VALUES = Arrays.asList("A", "B", "C", "D");

    @Override
    protected int[] brains() {
        return new int[]{1, 2};
    }

    @Override
    protected Config config() {
        System.setProperty("hazelcast.test.use.network","false");

        return super.config().addMapConfig(new MapConfig()
                .setName("test")
                .addMapIndexConfig(new MapIndexConfig()
                        .setAttribute("value").setOrdered(true)));
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        for (long key = 0; key < 10000; key++) {
            String randomValue = POSSIBLE_VALUES.get(ThreadLocalRandomProvider.get().nextInt(POSSIBLE_VALUES.size()));
            instances[0].getMap("test").put(key, new TestObject(key, randomValue));
        }

        int test = instances[0].getMap("test").size();

        for (final HazelcastInstance instance : instances) {
            assertEqualsEventually(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    return instance.getMap("test").size();
                }
            }, 10000);
            assertEquals(10000, instance.getMap("test").keySet(Predicates.not(Predicates.equal("value", "X"))).size());
            assertEquals(10000, instance.getMap("test").keySet(Predicates.in("value", "A", "B", "C", "D")).size());
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        for (final HazelcastInstance instance : instances) {
            assertEqualsEventually(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    return instance.getMap("test").size();
                }
            }, 10000);
            assertEquals(10000, instance.getMap("test").keySet(Predicates.not(Predicates.equal("value", "X"))).size());
            assertEquals(10000, instance.getMap("test").keySet(Predicates.in("value", "A", "B", "C", "D")).size());
        }
    }

    private static class TestObject implements Serializable {

        private Long id;
        private String value;

        public TestObject() {
        }

        public TestObject(Long id, String value) {
            this.id = id;
            this.value = value;
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }
}