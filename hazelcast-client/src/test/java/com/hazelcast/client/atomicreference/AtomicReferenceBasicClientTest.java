package com.hazelcast.client.atomicreference;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.concurrent.atomicreference.AtomicReferenceBasicTest;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

//too slow for the moment.
@Ignore
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class AtomicReferenceBasicClientTest extends AtomicReferenceBasicTest {

    @Override
    protected HazelcastInstance[] newInstances() {
        HazelcastInstance server = Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        return new HazelcastInstance[]{client, server};
    }
}
