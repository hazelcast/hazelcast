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

package com.hazelcast.client.executor.durable;

import com.hazelcast.client.executor.tasks.MapPutRunnable;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.cluster.Member;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static com.hazelcast.test.HazelcastTestSupport.assertSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.generateKeyOwnedBy;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientDurableExecutorServiceExecuteTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private HazelcastInstance server;
    private HazelcastInstance client;

    @Before
    public void setup() {
        server = hazelcastFactory.newHazelcastInstance();
        hazelcastFactory.newHazelcastInstance();
        hazelcastFactory.newHazelcastInstance();
        client = hazelcastFactory.newHazelcastClient();
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testExecute() {
        DurableExecutorService service = client.getDurableExecutorService(randomString());
        String mapName = randomString();

        service.execute(new MapPutRunnable(mapName));
        IMap map = client.getMap(mapName);

        assertSizeEventually(1, map);
    }

    @Test(expected = NullPointerException.class)
    @SuppressWarnings("ConstantConditions")
    public void testExecute_whenTaskNull() {
        DurableExecutorService service = client.getDurableExecutorService(randomString());

        service.execute(null);
    }

    @Test
    public void testExecuteOnKeyOwner() {
        DurableExecutorService service = client.getDurableExecutorService(randomString());
        String mapName = randomString();

        Member member = server.getCluster().getLocalMember();
        final UUID targetUuid = member.getUuid();
        String key = generateKeyOwnedBy(server);

        service.executeOnKeyOwner(new MapPutRunnable(mapName), key);

        final IMap map = client.getMap(mapName);

        assertTrueEventually(() -> assertTrue(map.containsKey(targetUuid)));
    }

    @Test(expected = NullPointerException.class)
    public void testExecuteOnKeyOwner_whenKeyNull() {
        DurableExecutorService service = client.getDurableExecutorService(randomString());
        service.executeOnKeyOwner(new MapPutRunnable("map"), null);
    }
}
