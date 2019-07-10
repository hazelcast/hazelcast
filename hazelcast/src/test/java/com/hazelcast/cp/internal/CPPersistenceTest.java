package com.hazelcast.cp.internal;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CPPersistenceTest extends HazelcastRaftTestSupport {

    @Test
    public void test() {
        HazelcastInstance[] instances = newInstances(3);

        IAtomicLong atomicLong = instances[0].getCPSubsystem().getAtomicLong("test");
        System.err.println(">>>>>>>>>>>>>>>> atomicLong = " + atomicLong.get());
        atomicLong.incrementAndGet();

        IAtomicLong atomicLong2 = instances[0].getCPSubsystem().getAtomicLong("test@group2");
        System.err.println("================= atomicLong2 = " + atomicLong2.incrementAndGet());
    }
}
