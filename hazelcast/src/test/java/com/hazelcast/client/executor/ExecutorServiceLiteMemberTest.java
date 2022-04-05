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
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.map.IMap;
import com.hazelcast.cluster.Member;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import static com.hazelcast.cluster.memberselector.MemberSelectors.LITE_MEMBER_SELECTOR;
import static com.hazelcast.executor.ExecutorServiceTestSupport.CountingDownExecutionCallback;
import static com.hazelcast.executor.ExecutorServiceTestSupport.LocalMemberReturningCallable;
import static com.hazelcast.executor.ExecutorServiceTestSupport.ResultHoldingMultiExecutionCallback;
import static com.hazelcast.executor.ExecutorServiceTestSupport.ResultSettingRunnable;
import static com.hazelcast.executor.ExecutorServiceTestSupport.assertOpenEventually;
import static com.hazelcast.executor.ExecutorServiceTestSupport.assertTrueEventually;
import static com.hazelcast.executor.ExecutorServiceTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExecutorServiceLiteMemberTest {

    private TestHazelcastFactory factory;

    private final String defaultMemberConfigXml = "hazelcast-test-executor.xml";
    private Config liteConfig = new XmlConfigBuilder(getClass().getClassLoader().getResourceAsStream(defaultMemberConfigXml))
            .build().setLiteMember(true);
    private Config config = new XmlConfigBuilder(getClass().getClassLoader().getResourceAsStream(defaultMemberConfigXml)).build();
    private ClientConfig clientConfig = new XmlClientConfigBuilder("classpath:hazelcast-client-test-executor.xml").build();

    public ExecutorServiceLiteMemberTest()
            throws IOException {
    }

    private HazelcastInstance newHazelcastInstance() {
        return factory.newHazelcastInstance(config);
    }

    private HazelcastInstance newHazelcastLiteMember() {
        return factory.newHazelcastInstance(liteConfig);
    }

    private HazelcastInstance newHazelcastClient() {
        return factory.newHazelcastClient(clientConfig);
    }

    @Before
    public void before() {
        factory = new TestHazelcastFactory();
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test(expected = RejectedExecutionException.class)
    public void test_executeRunnable_failsWhenNoLiteMemberExists() {
        newHazelcastInstance();
        final HazelcastInstance client = newHazelcastClient();

        final String name = randomString();
        final IExecutorService executor = client.getExecutorService(name);
        executor.execute(new ResultSettingRunnable(name), LITE_MEMBER_SELECTOR);
    }

    @Test
    public void test_executeRunnable_onLiteMember() {
        final HazelcastInstance lite1 = newHazelcastLiteMember();
        final HazelcastInstance lite2 = newHazelcastLiteMember();
        newHazelcastInstance();
        final HazelcastInstance client = newHazelcastClient();

        final String name = randomString();
        final IExecutorService executor = client.getExecutorService(name);
        executor.execute(new ResultSettingRunnable(name), LITE_MEMBER_SELECTOR);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                final IMap<Object, Object> results = lite1.getMap(name);
                assertEquals(1, results.size());
                final boolean executedOnLite1 = results.containsKey(lite1.getCluster().getLocalMember());
                final boolean executedOnLite2 = results.containsKey(lite2.getCluster().getLocalMember());

                assertTrue(executedOnLite1 || executedOnLite2);
            }
        });
    }

    @Test
    public void test_executeRunnable_onAllLiteMembers() {
        final HazelcastInstance lite1 = newHazelcastLiteMember();
        final HazelcastInstance lite2 = newHazelcastLiteMember();
        newHazelcastInstance();
        final HazelcastInstance client = newHazelcastClient();

        final String name = randomString();
        final IExecutorService executor = client.getExecutorService(name);
        executor.executeOnMembers(new ResultSettingRunnable(name), LITE_MEMBER_SELECTOR);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                final IMap<Object, Object> results = lite1.getMap(name);
                assertEquals(2, results.size());
                assertTrue(results.containsKey(lite1.getCluster().getLocalMember()));
                assertTrue(results.containsKey(lite2.getCluster().getLocalMember()));
            }
        });
    }

    @Test(expected = RejectedExecutionException.class)
    public void test_submitCallable_failsWhenNoLiteMemberExists() {
        newHazelcastInstance();
        final HazelcastInstance client = newHazelcastClient();

        final IExecutorService executor = client.getExecutorService(randomString());
        executor.submit(new LocalMemberReturningCallable(), LITE_MEMBER_SELECTOR);
    }

    @Test
    public void test_submitCallable_onLiteMember()
            throws ExecutionException, InterruptedException {
        final HazelcastInstance lite1 = newHazelcastLiteMember();
        final HazelcastInstance lite2 = newHazelcastLiteMember();
        newHazelcastInstance();
        final HazelcastInstance client = newHazelcastClient();

        final IExecutorService executor = client.getExecutorService(randomString());
        final Future<Member> future = executor.submit(new LocalMemberReturningCallable(), LITE_MEMBER_SELECTOR);
        final Member executedMember = future.get();

        final boolean executedOnLite1 = lite1.getCluster().getLocalMember().equals(executedMember);
        final boolean executedOnLite2 = lite2.getCluster().getLocalMember().equals(executedMember);
        assertTrue(executedOnLite1 || executedOnLite2);
    }

    @Test
    public void test_submitCallable_onLiteMembers()
            throws ExecutionException, InterruptedException {
        final HazelcastInstance lite1 = newHazelcastLiteMember();
        final HazelcastInstance lite2 = newHazelcastLiteMember();
        newHazelcastInstance();
        final HazelcastInstance client = newHazelcastClient();

        final IExecutorService executor = client.getExecutorService(randomString());
        final Map<Member, Future<Member>> results = executor
                .submitToMembers(new LocalMemberReturningCallable(), LITE_MEMBER_SELECTOR);

        assertEquals(2, results.size());

        final Member liteMember1 = lite1.getCluster().getLocalMember();
        final Member liteMember2 = lite2.getCluster().getLocalMember();

        assertEquals(liteMember1, results.get(liteMember1).get());
        assertEquals(liteMember2, results.get(liteMember2).get());
    }

    @Test
    public void test_submitCallableWithCallback_toLiteMember() {
        final HazelcastInstance lite1 = newHazelcastLiteMember();
        final HazelcastInstance lite2 = newHazelcastLiteMember();
        newHazelcastInstance();
        final HazelcastInstance client = newHazelcastClient();

        final CountingDownExecutionCallback<Member> callback = new CountingDownExecutionCallback<Member>(1);
        final IExecutorService executor = client.getExecutorService(randomString());
        executor.submit(new LocalMemberReturningCallable(), LITE_MEMBER_SELECTOR, callback);

        assertOpenEventually(callback.getLatch());
        final Object result = callback.getResult();
        final boolean executedOnLite1 = lite1.getCluster().getLocalMember().equals(result);
        final boolean executedOnLite2 = lite2.getCluster().getLocalMember().equals(result);
        assertTrue(executedOnLite1 || executedOnLite2);
    }

    @Test
    public void test_submitCallableWithCallback_toLiteMembers() {
        final HazelcastInstance lite1 = newHazelcastLiteMember();
        final HazelcastInstance lite2 = newHazelcastLiteMember();
        newHazelcastInstance();
        final HazelcastInstance client = newHazelcastClient();

        final ResultHoldingMultiExecutionCallback callback = new ResultHoldingMultiExecutionCallback();
        final IExecutorService executor = client.getExecutorService(randomString());
        executor.submitToMembers(new LocalMemberReturningCallable(), LITE_MEMBER_SELECTOR, callback);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                final Map<Member, Object> results = callback.getResults();

                assertNotNull(results);
                final Member liteMember1 = lite1.getCluster().getLocalMember();
                final Member liteMember2 = lite2.getCluster().getLocalMember();
                assertEquals(liteMember1, results.get(liteMember1));
                assertEquals(liteMember2, results.get(liteMember2));
            }
        });
    }

}
