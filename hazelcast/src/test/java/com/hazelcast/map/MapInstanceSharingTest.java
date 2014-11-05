package com.hazelcast.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MapInstanceSharingTest extends HazelcastTestSupport {

    private HazelcastInstance[] instances;
    private HazelcastInstance local;
    private HazelcastInstance remote;

    @Before
    public void setUp() {
        instances = createHazelcastInstanceFactory(2).newInstances();
        warmUpPartitions(instances);
        local = instances[0];
        remote = instances[1];
    }

    @Test
    public void invocationToLocalMember() throws ExecutionException, InterruptedException {
        String localKey = generateKeyOwnedBy(local);
        IMap<String, DummyObject> map = local.getMap(UUID.randomUUID().toString());

        DummyObject inserted = new DummyObject();
        map.put(localKey,inserted);

        DummyObject get1 = map.get(localKey);
        DummyObject get2 = map.get(localKey);

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
        String remoteKey = generateKeyOwnedBy(remote);
        IMap<String, DummyObject> map = local.getMap(UUID.randomUUID().toString());

        DummyObject inserted = new DummyObject();
        map.put(remoteKey, inserted);

        DummyObject get1 = map.get(remoteKey);
        DummyObject get2 = map.get(remoteKey);

        assertNotNull(get1);
        assertNotNull(get2);
        assertNotSame(get1, get2);
        assertNotSame(get1, inserted);
        assertNotSame(get2, inserted);
    }
}
