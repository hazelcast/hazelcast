package com.hazelcast.wan.impl;

import com.hazelcast.map.impl.wan.MapReplicationRemove;
import com.hazelcast.map.impl.wan.MapReplicationUpdate;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.WanReplicationEvent;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class WanDataSerializerHookTest {

    @Test
    public void testExistingTypes() {
        WanDataSerializerHook hook = new WanDataSerializerHook();
        IdentifiedDataSerializable wanReplicationEvent = hook.createFactory()
                .create(WanDataSerializerHook.WAN_REPLICATION_EVENT);
        assertTrue(wanReplicationEvent instanceof WanReplicationEvent);

        IdentifiedDataSerializable mapUpdate = hook.createFactory()
                .create(WanDataSerializerHook.MAP_REPLICATION_UPDATE);
        assertTrue(mapUpdate instanceof MapReplicationUpdate);

        IdentifiedDataSerializable mapRemove = hook.createFactory()
                .create(WanDataSerializerHook.MAP_REPLICATION_REMOVE);
        assertTrue(mapRemove instanceof MapReplicationRemove);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidType() {
        WanDataSerializerHook hook = new WanDataSerializerHook();
        hook.createFactory().create(999);
    }
}
