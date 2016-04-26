package com.hazelcast.serviceprovider;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;


@RunWith(HazelcastParallelClassRunner.class)
@Category(value = {QuickTest.class, ParallelTest.class})
public class ServiceProviderTest extends HazelcastTestSupport {
    @Test
    public void remoteServiceLoaderTest() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance();
        TestDistributedObject testDistributedObject = instance.getDistributedObject(TestRemoteService.SERVICE_NAME, "test");
        assertEquals(testDistributedObject.getName(), "test");
    }
}
