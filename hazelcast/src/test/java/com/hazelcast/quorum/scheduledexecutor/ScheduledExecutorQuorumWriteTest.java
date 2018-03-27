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

package com.hazelcast.quorum.scheduledexecutor;

import com.hazelcast.config.Config;
import com.hazelcast.core.Member;
import com.hazelcast.quorum.AbstractQuorumTest;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.scheduledexecutor.IScheduledFuture;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.quorum.scheduledexecutor.ScheduledExecutorQuorumWriteTest.ExecRunnable.callable;
import static com.hazelcast.quorum.scheduledexecutor.ScheduledExecutorQuorumWriteTest.ExecRunnable.runnable;
import static com.hazelcast.test.HazelcastTestSupport.generateKeyOwnedBy;
import static com.hazelcast.test.HazelcastTestSupport.getNodeEngineImpl;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class ScheduledExecutorQuorumWriteTest extends AbstractQuorumTest {

    @Parameters(name = "quorumType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{{QuorumType.WRITE}, {QuorumType.READ_WRITE}});
    }

    @Parameter
    public static QuorumType quorumType;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void setUp() {
        initTestEnvironment(new Config(), new TestHazelcastInstanceFactory());
    }

    @AfterClass
    public static void tearDown() {
        shutdownTestEnvironment();
    }

    @Test
    public void schedule_runnable_quorum() throws Exception {
        exec(0).schedule(runnable(), 10, TimeUnit.MILLISECONDS).get();
    }

    @Test(expected = QuorumException.class)
    public void schedule_runnable_noQuorum() throws Exception {
        exec(3).schedule(runnable(), 10, TimeUnit.MILLISECONDS).get();
    }

    @Test
    public void schedule_callable_quorum() throws Exception {
        exec(0).schedule(callable(), 10, TimeUnit.MILLISECONDS).get();
    }

    @Test(expected = QuorumException.class)
    public void schedule_callable_noQuorum() throws Exception {
        exec(3).schedule(callable(), 10, TimeUnit.MILLISECONDS).get();
    }

    @Test
    public void scheduleAtFixedRate_callable_quorum() {
        exec(0).scheduleAtFixedRate(runnable(), 10, 10, TimeUnit.MILLISECONDS).cancel(false);
    }

    @Test(expected = QuorumException.class)
    public void scheduleAtFixedRate_callable_noQuorum() throws Exception {
        exec(3).scheduleAtFixedRate(runnable(), 10, 10, TimeUnit.MILLISECONDS).get();
    }

    @Test
    public void scheduleOnMember_runnable_quorum() throws Exception {
        exec(0).scheduleOnMember(runnable(), member(0), 10, TimeUnit.MILLISECONDS).get();
    }

    @Test(expected = QuorumException.class)
    public void scheduleOnMember_runnable_noQuorum() throws Exception {
        exec(3).scheduleOnMember(runnable(), member(3), 10, TimeUnit.MILLISECONDS).get();
    }

    @Test
    public void scheduleOnMember_callable_quorum() throws Exception {
        exec(0).scheduleOnMember(callable(), member(0), 10, TimeUnit.MILLISECONDS).get();
    }

    @Test(expected = QuorumException.class)
    public void scheduleOnMember_callable_noQuorum() throws Exception {
        exec(3).scheduleOnMember(callable(), member(3), 10, TimeUnit.MILLISECONDS).get();
    }

    @Test
    public void scheduleOnMemberAtFixedRate_runnable_quorum() {
        exec(0).scheduleOnMemberAtFixedRate(runnable(), member(0), 10, 10, TimeUnit.MILLISECONDS).cancel(false);
    }

    @Test(expected = QuorumException.class)
    public void scheduleOnMemberAtFixedRate_runnable_noQuorum() throws Exception {
        exec(3).scheduleOnMemberAtFixedRate(runnable(), member(3), 10, 10, TimeUnit.MILLISECONDS).get();
    }

    @Test
    public void scheduleOnKeyOwner_runnable_quorum() throws Exception {
        exec(0).scheduleOnKeyOwner(runnable(), key(0), 10, TimeUnit.MILLISECONDS).get();
    }

    @Test(expected = QuorumException.class)
    public void scheduleOnKeyOwner_runnable_noQuorum() throws Exception {
        exec(3).scheduleOnKeyOwner(runnable(), key(3), 10, TimeUnit.MILLISECONDS).get();
    }

    @Test
    public void scheduleOnKeyOwner_callable_quorum() throws Exception {
        exec(0).scheduleOnKeyOwner(callable(), key(0), 10, TimeUnit.MILLISECONDS).get();
    }

    @Test(expected = QuorumException.class)
    public void scheduleOnKeyOwner_callable_noQuorum() throws Exception {
        exec(3).scheduleOnKeyOwner(callable(), key(3), 10, TimeUnit.MILLISECONDS).get();
    }

    @Test
    public void scheduleOnKeyOwnerAtFixedRate_runnable_quorum() {
        exec(0).scheduleOnKeyOwnerAtFixedRate(runnable(), key(0), 10, 10, TimeUnit.MILLISECONDS).cancel(false);
    }

    @Test(expected = QuorumException.class)
    public void scheduleOnKeyOwnerAtFixedRate_runnable_noQuorum() throws Exception {
        exec(3).scheduleOnKeyOwnerAtFixedRate(runnable(), key(3), 10, 10, TimeUnit.MILLISECONDS).get();
    }

    @Test
    public void scheduleOnAllMembers_runnable_quorum() throws Exception {
        wait(exec(0).scheduleOnAllMembers(runnable(), 10, TimeUnit.MILLISECONDS));
    }

    @Test(expected = QuorumException.class)
    public void scheduleOnAllMembers_runnable_noQuorum() throws Exception {
        wait(exec(3).scheduleOnAllMembers(runnable(), 10, TimeUnit.MILLISECONDS));
    }

    @Test
    public void scheduleOnAllMembers_callable_quorum() throws Exception {
        wait(exec(0).scheduleOnAllMembers(callable(), 10, TimeUnit.MILLISECONDS));
    }

    @Test(expected = QuorumException.class)
    public void scheduleOnAllMembers_callable_noQuorum() throws Exception {
        wait(exec(3).scheduleOnAllMembers(callable(), 10, TimeUnit.MILLISECONDS));
    }

    @Test
    public void scheduleOnAllMembersAtFixedRate_runnable_quorum() {
        cancel(exec(0).scheduleOnAllMembersAtFixedRate(runnable(), 10, 10, TimeUnit.MILLISECONDS));
    }

    @Test(expected = QuorumException.class)
    public void scheduleOnAllMembersAtFixedRate_runnable_noQuorum() throws Exception {
        wait(exec(3).scheduleOnAllMembersAtFixedRate(runnable(), 10, 10, TimeUnit.MILLISECONDS));
    }

    @Test
    public void scheduleOnMembers_runnable_quorum() throws Exception {
        wait(exec(0).scheduleOnMembers(runnable(), asList(member(0)), 10, TimeUnit.MILLISECONDS));
    }

    @Test(expected = QuorumException.class)
    public void scheduleOnMembers_runnable_noQuorum() throws Exception {
        wait(exec(3).scheduleOnMembers(runnable(), asList(member(3)), 10, TimeUnit.MILLISECONDS));
    }

    @Test
    public void scheduleOnMembers_callable_quorum() throws Exception {
        wait(exec(0).scheduleOnMembers(callable(), asList(member(0)), 10, TimeUnit.MILLISECONDS));
    }

    @Test(expected = QuorumException.class)
    public void scheduleOnMembers_callable_noQuorum() throws Exception {
        wait(exec(3).scheduleOnMembers(callable(), asList(member(3)), 10, TimeUnit.MILLISECONDS));
    }

    @Test
    public void scheduleOnMembersAtFixedRate_runnable_quorum() {
        cancel(exec(0).scheduleOnMembersAtFixedRate(runnable(), asList(member(0)), 10, 10, TimeUnit.MILLISECONDS));
    }

    @Test(expected = QuorumException.class)
    public void scheduleOnMembersAtFixedRate_runnable_noQuorum() throws Exception {
        wait(exec(3).scheduleOnMembersAtFixedRate(runnable(), asList(member(3)), 10, 10, TimeUnit.MILLISECONDS));
    }

    @Test
    public void shutdown_quorum() {
        exec(0, "shutdown").shutdown();
    }

    @Test(expected = QuorumException.class)
    public void shutdown_noQuorum() {
        exec(3, "shutdown").shutdown();
    }

    static class ExecRunnable implements Runnable, Callable, Serializable {
        @Override
        public Object call() throws Exception {
            return "response";
        }

        public void run() {
        }

        public static Runnable runnable() {
            return new ExecRunnable();
        }

        public static Callable callable() {
            return new ExecRunnable();
        }
    }

    private Object key(int index) {
        return generateKeyOwnedBy(cluster.getInstance(index), true);
    }

    private Member member(int index) {
        return getNodeEngineImpl(cluster.getInstance(index)).getLocalMember();
    }

    private void wait(Map<Member, IScheduledFuture<?>> futures) throws Exception {
        for (IScheduledFuture f : futures.values()) {
            f.get();
        }
    }

    private void cancel(Map<Member, IScheduledFuture<?>> futures) {
        for (IScheduledFuture f : futures.values()) {
            f.cancel(false);
        }
    }

    protected IScheduledExecutorService exec(int index) {
        return scheduledExec(index, quorumType);
    }

    protected IScheduledExecutorService exec(int index, String postfix) {
        return scheduledExec(index, quorumType, postfix);
    }
}
