/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
