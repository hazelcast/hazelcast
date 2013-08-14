package com.hazelcast.instance;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class ManagedContextInstanceAwareTest extends HazelcastTestSupport {

    @Test
    public void test(){
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);

        Config config = new Config();
        ManagedContextImpl managedContext = new ManagedContextImpl();
        config.setManagedContext(managedContext);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        assertNotNull("hazelcastInstance should have been set",managedContext.hz);
    }

    private class ManagedContextImpl implements ManagedContext, HazelcastInstanceAware {
        private HazelcastInstance hz;

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hz= hazelcastInstance;
        }

        @Override
        public Object initialize(Object obj) {
            return null;
        }
    }
}
