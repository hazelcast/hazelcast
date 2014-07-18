/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.executor.tasks.*;
import com.hazelcast.core.*;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;

import static com.hazelcast.test.HazelcastTestSupport.*;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientExecutorServiceExecuteTest {

    private static final int CLUSTER_SIZE = 3;

    private static HazelcastInstance server1;
    private static HazelcastInstance server2;

    private static HazelcastInstance client;

    @BeforeClass
    public static void beforeClass() {
        server1 = Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();
        server2 = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void afterClass() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
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

        service.execute( new MapPutRunnable(mapName), selector);
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
        final String targetUuid = member.getUuid();
        String key = generateKeyOwnedBy(server1);

        service.executeOnKeyOwner(new MapPutRunnable(mapName), key);

        final IMap map = client.getMap(mapName);

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertTrue(map.containsKey(targetUuid));
            }
        });
    }

    @Test(expected = NullPointerException.class)
    public void testExecuteOnKeyOwner_whenKeyNull() {
        IExecutorService service = client.getExecutorService(randomString());
        service.executeOnKeyOwner(new MapPutRunnable("map"), null);
    }

    @Test
    public void testExecuteOnMember(){
        IExecutorService service = client.getExecutorService(randomString());
        String mapName = randomString();

        Member member = server1.getCluster().getLocalMember();
        final String targetUuid = member.getUuid();

        service.executeOnMember(new MapPutRunnable(mapName), member);

        final IMap map = client.getMap(mapName);

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertTrue(map.containsKey(targetUuid));
            }
        });
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
        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertTrue(map.containsKey(member1.getUuid()));
                assertTrue(map.containsKey(member2.getUuid()));
            }
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

    @Test(expected = IllegalArgumentException.class)
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