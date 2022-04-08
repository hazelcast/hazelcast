package com.hazelcast.instance.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class LifecycleServiceImplTest extends JetTestSupport {

    @Test
    // test for https://github.com/hazelcast/hazelcast/issues/21123
    public void testConcurrentShutdown() throws Exception {
        for (int i = 0; i < 3; i++) {
            HazelcastInstance inst = createHazelcastInstance();

            Future f1 = spawn(() -> inst.getLifecycleService().terminate());
            Future f2 = spawn(() -> inst.getLifecycleService().shutdown());

            f1.get(5, TimeUnit.SECONDS);
            f2.get(5, TimeUnit.SECONDS);
        }
    }
}