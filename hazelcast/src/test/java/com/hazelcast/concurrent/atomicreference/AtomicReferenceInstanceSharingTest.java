package com.hazelcast.concurrent.atomicreference;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class AtomicReferenceInstanceSharingTest extends HazelcastTestSupport {

    private static HazelcastInstance[] instances;
    private static HazelcastInstance local;
    private static HazelcastInstance remote;

    @BeforeClass
    public static void setUp() {
        instances = new TestHazelcastInstanceFactory(2).newInstances();
        warmUpPartitions(instances);
        local = instances[0];
        remote = instances[1];
    }

    @Test
    public void invocationToLocalMember() throws ExecutionException, InterruptedException {
        String localKey = generateKeyOwnedBy(local);
        IAtomicReference<DummyObject> ref = local.getAtomicReference(localKey);

        DummyObject inserted = new DummyObject();
        ref.set(inserted);

        DummyObject get1 = ref.get();
        DummyObject get2 = ref.get();

        assertNotNull(get1);
        assertNotNull(get2);
        assertNotSame(get1, get2);
        assertNotSame(get1, inserted);
        assertNotSame(get2, inserted);
    }

    public static class DummyObject implements Serializable {
    }

    @Test
    public void invocationToRemoteMember() throws ExecutionException, InterruptedException {
        String localKey = generateKeyOwnedBy(remote);
        IAtomicReference<DummyObject> ref = local.getAtomicReference(localKey);

        DummyObject inserted = new DummyObject();
        ref.set(inserted);

        DummyObject get1 = ref.get();
        DummyObject get2 = ref.get();

        assertNotNull(get1);
        assertNotNull(get2);
        assertNotSame(get1, get2);
        assertNotSame(get1, inserted);
        assertNotSame(get2, inserted);
    }
}
