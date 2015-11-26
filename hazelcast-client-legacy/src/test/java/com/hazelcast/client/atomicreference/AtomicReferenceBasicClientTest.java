package com.hazelcast.client.atomicreference;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.concurrent.atomicreference.AtomicReferenceAbstractTest;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AtomicReferenceBasicClientTest extends AtomicReferenceAbstractTest {

    @Override
    protected HazelcastInstance[] newInstances() {
        TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
        HazelcastInstance server = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        return new HazelcastInstance[]{client, server};
    }
}
