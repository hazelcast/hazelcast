/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapInstanceSharingTest extends HazelcastTestSupport {

    private HazelcastInstance local;
    private HazelcastInstance remote;

    @Before
    public void setUp() {
        HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances(getConfig());
        warmUpPartitions(instances);
        local = instances[0];
        remote = instances[1];
    }

    @Test
    public void invocationToLocalMember() throws ExecutionException, InterruptedException {
        String localKey = generateKeyOwnedBy(local);
        IMap<String, DummyObject> map = local.getMap(UUID.randomUUID().toString());

        DummyObject inserted = new DummyObject();
        map.put(localKey, inserted);

        DummyObject get1 = map.get(localKey);
        DummyObject get2 = map.get(localKey);

        assertNotNull(get1);
        assertNotNull(get2);
        assertNotSame(get1, get2);
        assertNotSame(get1, inserted);
        assertNotSame(get2, inserted);
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

    public static class DummyObject implements Serializable {
    }
}
