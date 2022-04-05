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

package com.hazelcast.executor;

import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import static com.hazelcast.cluster.memberselector.MemberSelectors.LITE_MEMBER_SELECTOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExecutorServiceLiteMemberTest
        extends ExecutorServiceTestSupport {

    private Config liteConfig = smallInstanceConfig().setLiteMember(true);

    @Test(expected = RejectedExecutionException.class)
    public void test_executeRunnable_failsWhenNoLiteMemberExists() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance = factory.newHazelcastInstance(smallInstanceConfig());
        final String name = randomString();
        final IExecutorService executor = instance.getExecutorService(name);
        executor.execute(new ResultSettingRunnable(name), LITE_MEMBER_SELECTOR);
    }

    @Test
    public void test_executeRunnable_onLiteMember() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance lite1 = factory.newHazelcastInstance(liteConfig);
        final HazelcastInstance lite2 = factory.newHazelcastInstance(liteConfig);
        final HazelcastInstance other = factory.newHazelcastInstance(smallInstanceConfig());

        final String name = randomString();
        final IExecutorService executor = other.getExecutorService(name);
        executor.execute(new ResultSettingRunnable(name), LITE_MEMBER_SELECTOR);

        assertTrueEventually(() -> {
            final IMap<Object, Object> results = lite1.getMap(name);
            assertEquals(1, results.size());
            final boolean executedOnLite1 = results.containsKey(lite1.getCluster().getLocalMember());
            final boolean executedOnLite2 = results.containsKey(lite2.getCluster().getLocalMember());

            assertTrue(executedOnLite1 || executedOnLite2);
        });
    }

    @Test
    public void test_executeRunnable_onAllLiteMembers() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance lite1 = factory.newHazelcastInstance(liteConfig);
        final HazelcastInstance lite2 = factory.newHazelcastInstance(liteConfig);
        final HazelcastInstance other = factory.newHazelcastInstance(smallInstanceConfig());

        final String name = randomString();
        final IExecutorService executor = other.getExecutorService(name);
        executor.executeOnMembers(new ResultSettingRunnable(name), LITE_MEMBER_SELECTOR);

        assertTrueEventually(() -> {
            final IMap<Object, Object> results = lite1.getMap(name);
            assertEquals(2, results.size());
            assertTrue(results.containsKey(lite1.getCluster().getLocalMember()));
            assertTrue(results.containsKey(lite2.getCluster().getLocalMember()));
        });

    }

    @Test(expected = RejectedExecutionException.class)
    public void test_submitCallable_failsWhenNoLiteMemberExists() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance = factory.newHazelcastInstance(smallInstanceConfig());
        final IExecutorService executor = instance.getExecutorService(randomString());
        executor.submit(new LocalMemberReturningCallable(), LITE_MEMBER_SELECTOR);
    }

    @Test
    public void test_submitCallable_onLiteMember()
            throws ExecutionException, InterruptedException {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance lite1 = factory.newHazelcastInstance(liteConfig);
        final HazelcastInstance lite2 = factory.newHazelcastInstance(liteConfig);
        final HazelcastInstance other = factory.newHazelcastInstance(smallInstanceConfig());

        final IExecutorService executor = other.getExecutorService(randomString());
        final Future<Member> future = executor.submit(new LocalMemberReturningCallable(), LITE_MEMBER_SELECTOR);
        final Member executedMember = future.get();

        final boolean executedOnLite1 = lite1.getCluster().getLocalMember().equals(executedMember);
        final boolean executedOnLite2 = lite2.getCluster().getLocalMember().equals(executedMember);
        assertTrue(executedOnLite1 || executedOnLite2);
    }

    @Test
    public void test_submitCallable_onLiteMembers()
            throws ExecutionException, InterruptedException {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance lite1 = factory.newHazelcastInstance(liteConfig);
        final HazelcastInstance lite2 = factory.newHazelcastInstance(liteConfig);
        final HazelcastInstance other = factory.newHazelcastInstance(smallInstanceConfig());

        final IExecutorService executor = other.getExecutorService(randomString());
        final Map<Member, Future<Member>> results = executor
                .submitToMembers(new LocalMemberReturningCallable(), LITE_MEMBER_SELECTOR);

        assertEquals(2, results.size());

        final Member liteMember1 = lite1.getCluster().getLocalMember();
        final Member liteMember2 = lite2.getCluster().getLocalMember();

        assertEquals(liteMember1, results.get(liteMember1).get());
        assertEquals(liteMember2, results.get(liteMember2).get());
    }

    @Test
    public void test_submitCallableWithCallback_onLiteMember() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance lite1 = factory.newHazelcastInstance(liteConfig);
        final HazelcastInstance lite2 = factory.newHazelcastInstance(liteConfig);
        final HazelcastInstance other = factory.newHazelcastInstance(smallInstanceConfig());

        final CountingDownExecutionCallback<Member> callback = new CountingDownExecutionCallback<>(1);
        final IExecutorService executor = other.getExecutorService(randomString());
        executor.submit(new LocalMemberReturningCallable(), LITE_MEMBER_SELECTOR, callback);

        assertOpenEventually(callback.getLatch());
        final Object result = callback.getResult();
        final boolean executedOnLite1 = lite1.getCluster().getLocalMember().equals(result);
        final boolean executedOnLite2 = lite2.getCluster().getLocalMember().equals(result);
        assertTrue(executedOnLite1 || executedOnLite2);
    }

    @Test
    public void test_submitCallableWithCallback_onLiteMembers() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance lite1 = factory.newHazelcastInstance(liteConfig);
        final HazelcastInstance lite2 = factory.newHazelcastInstance(liteConfig);
        final HazelcastInstance other = factory.newHazelcastInstance(smallInstanceConfig());

        final ResultHoldingMultiExecutionCallback callback = new ResultHoldingMultiExecutionCallback();
        final IExecutorService executor = other.getExecutorService(randomString());
        executor.submitToMembers(new LocalMemberReturningCallable(), LITE_MEMBER_SELECTOR, callback);

        assertTrueEventually(() -> {
            final Map<Member, Object> results = callback.getResults();

            assertNotNull(results);
            final Member liteMember1 = lite1.getCluster().getLocalMember();
            final Member liteMember2 = lite2.getCluster().getLocalMember();
            assertEquals(liteMember1, results.get(liteMember1));
            assertEquals(liteMember2, results.get(liteMember2));
        });
    }
}
