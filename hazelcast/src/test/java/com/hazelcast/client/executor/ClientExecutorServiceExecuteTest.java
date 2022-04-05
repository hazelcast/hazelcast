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

package com.hazelcast.client.executor;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.executor.tasks.MapPutRunnable;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.client.test.executor.tasks.SelectAllMembers;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.map.IMap;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MemberSelector;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import static com.hazelcast.test.HazelcastTestSupport.assertSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.generateKeyOwnedBy;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientExecutorServiceExecuteTest {

    private static final int CLUSTER_SIZE = 3;
    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private HazelcastInstance server1;
    private HazelcastInstance server2;
    private HazelcastInstance client;

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Before
    public void setup()
            throws IOException {
        Config config = new XmlConfigBuilder(getClass().getClassLoader().getResourceAsStream("hazelcast-test-executor.xml"))
                .build();
        ClientConfig clientConfig = new XmlClientConfigBuilder("classpath:hazelcast-client-test-executor.xml").build();

        server1 = hazelcastFactory.newHazelcastInstance(config);
        hazelcastFactory.newHazelcastInstance(config);
        server2 = hazelcastFactory.newHazelcastInstance(config);
        client = hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testExecute() {
        IExecutorService service = client.getExecutorService(randomString());
        String mapName = randomString();

        service.execute(new MapPutRunnable(mapName));
        IMap map = client.getMap(mapName);

        assertSizeEventually(1, map);
    }

    @Test
    public void testExecute_withMemberSelector() {
        IExecutorService service = client.getExecutorService(randomString());
        String mapName = randomString();
        MemberSelector selector = new SelectAllMembers();

        service.execute(new MapPutRunnable(mapName), selector);
        IMap map = client.getMap(mapName);

        assertSizeEventually(1, map);
    }

    @Test(expected = NullPointerException.class)
    public void testExecute_whenTaskNull() {
        IExecutorService service = client.getExecutorService(randomString());

        service.execute(null);
    }

    @Test
    public void testExecuteOnKeyOwner() {
        IExecutorService service = client.getExecutorService(randomString());
        String mapName = randomString();

        Member member = server1.getCluster().getLocalMember();
        final UUID targetUuid = member.getUuid();
        String key = generateKeyOwnedBy(server1);

        service.executeOnKeyOwner(new MapPutRunnable(mapName), key);

        final IMap map = client.getMap(mapName);

        assertTrueEventually(() -> assertTrue(map.containsKey(targetUuid)));
    }

    @Test(expected = NullPointerException.class)
    public void testExecuteOnKeyOwner_whenKeyNull() {
        IExecutorService service = client.getExecutorService(randomString());
        service.executeOnKeyOwner(new MapPutRunnable("map"), null);
    }

    @Test
    public void testExecuteOnMember() {
        IExecutorService service = client.getExecutorService(randomString());
        String mapName = randomString();

        Member member = server1.getCluster().getLocalMember();
        final UUID targetUuid = member.getUuid();

        service.executeOnMember(new MapPutRunnable(mapName), member);

        final IMap map = client.getMap(mapName);

        assertTrueEventually(() -> assertTrue(map.containsKey(targetUuid)));
    }

    @Test(expected = NullPointerException.class)
    public void testExecuteOnMember_WhenMemberNull() {
        IExecutorService service = client.getExecutorService(randomString());

        service.executeOnMember(new MapPutRunnable("map"), null);
    }

    @Test
    public void testExecuteOnMembers() {
        IExecutorService service = client.getExecutorService(randomString());
        String mapName = randomString();
        Collection<Member> collection = new ArrayList<Member>();
        final Member member1 = server1.getCluster().getLocalMember();
        final Member member2 = server2.getCluster().getLocalMember();
        collection.add(member1);
        collection.add(member2);

        service.executeOnMembers(new MapPutRunnable(mapName), collection);

        final IMap map = client.getMap(mapName);
        assertTrueEventually(() -> {
            assertTrue(map.containsKey(member1.getUuid()));
            assertTrue(map.containsKey(member2.getUuid()));
        });
    }

    @Test
    public void testExecuteOnMembers_withEmptyCollection() {
        IExecutorService service = client.getExecutorService(randomString());
        String mapName = randomString();
        Collection<Member> collection = new ArrayList<Member>();

        service.executeOnMembers(new MapPutRunnable(mapName), collection);

        IMap map = client.getMap(mapName);
        assertSizeEventually(0, map);
    }

    @Test(expected = NullPointerException.class)
    public void testExecuteOnMembers_WhenCollectionNull() {
        IExecutorService service = client.getExecutorService(randomString());
        Collection<Member> collection = null;

        service.executeOnMembers(new MapPutRunnable("task"), collection);
    }

    @Test
    public void testExecuteOnMembers_withSelector() {
        IExecutorService service = client.getExecutorService(randomString());
        String mapName = randomString();
        MemberSelector selector = new SelectAllMembers();

        service.executeOnMembers(new MapPutRunnable(mapName), selector);

        IMap map = client.getMap(mapName);
        assertSizeEventually(CLUSTER_SIZE, map);
    }

    @Test(expected = NullPointerException.class)
    public void testExecuteOnMembers_whenSelectorNull() {
        IExecutorService service = client.getExecutorService(randomString());
        MemberSelector selector = null;

        service.executeOnMembers(new MapPutRunnable("task"), selector);
    }

    @Test
    public void testExecuteOnAllMembers() {
        IExecutorService service = client.getExecutorService(randomString());
        String mapName = randomString();

        service.executeOnAllMembers(new MapPutRunnable(mapName));

        IMap map = client.getMap(mapName);
        assertSizeEventually(CLUSTER_SIZE, map);
    }
}
