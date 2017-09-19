package com.hazelcast.query.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category(QuickTest.class)
public class PredicateHzAwareTest extends HazelcastTestSupport {

    @Parameters(name = "instanceCount:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {1},
                {3}
        });
    }

    @Parameter
    public int instanceCount;

    @Test
    public void testHzAware() {
        final HazelcastInstance[] instances = createHazelcastInstanceFactory(instanceCount).newInstances();
        final IMap<String, Integer> m = instances[0].getMap("mappy");
        m.put("a", 1);
        m.put("b", 2);

        final Collection<Integer> result = m.project(new SimpleProjection(), new SimplePredicate());

        assertTrue(result.size() == 1);
        assertEquals(2, (int) result.iterator().next());
    }

    private static class SimpleProjection
            extends Projection<Map.Entry<String, Integer>, Integer>
            implements HazelcastInstanceAware, Serializable {

        private transient HazelcastInstance instance;

        @Override
        public Integer transform(Map.Entry<String, Integer> input) {
            assertNotNull(instance);
            return input.getValue();
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.instance = hazelcastInstance;
        }
    }

    private static class SimplePredicate implements Predicate<String, Integer>, Serializable, HazelcastInstanceAware {

        private transient HazelcastInstance instance;

        @Override
        public boolean apply(Map.Entry<String, Integer> mapEntry) {
            assertNotNull(instance);
            return mapEntry.getValue() % 2 == 0;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.instance = hazelcastInstance;
        }
    }
}
