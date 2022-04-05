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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.config.Config;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IndeterminateOperationStateException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.exception.StaleAppendRequestException;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.operation.DefaultRaftReplicateOp;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.exception.CallerNotMemberException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.Accessors.getOperationService;
import static com.hazelcast.version.MemberVersion.UNKNOWN;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RaftInvocationFailureTest extends HazelcastRaftTestSupport {

    private static final AtomicInteger COMMIT_COUNT = new AtomicInteger();

    private HazelcastInstance[] instances;
    private String groupName = "group";
    private int groupSize = 3;

    @Before
    public void setup() {
        instances = newInstances(groupSize);
        COMMIT_COUNT.set(0);
    }

    @Test
    public void test_invocationFailsOnMemberLeftException() throws ExecutionException, InterruptedException {
        CPGroupId groupId = getRaftInvocationManager(instances[0]).createRaftGroup(groupName).get();

        waitAllForLeaderElection(instances, groupId);
        HazelcastInstance leader = getLeaderInstance(instances, groupId);

        Future f = new RaftInvocation(getOperationService(leader).invocationContext,
                getRaftInvocationManager(leader).getRaftInvocationContext(), groupId,
                new DefaultRaftReplicateOp(groupId, new CustomResponseOp()), 10, 50, 60000).invoke();

        try {
            f.get(60, TimeUnit.SECONDS);
            fail();
        } catch (Exception e) {
            assertInstanceOf(IndeterminateOperationStateException.class, e.getCause());
        }

        assertTrue(COMMIT_COUNT.get() <= groupSize);
    }

    @Test
    public void test_invocationFailsWithMemberLeftException_when_thereAreRetryableExceptionsAfterwards() throws ExecutionException, InterruptedException {
        CPGroupId groupId = getRaftInvocationManager(instances[0]).createRaftGroup(groupName).get();

        waitAllForLeaderElection(instances, groupId);
        HazelcastInstance leader = getLeaderInstance(instances, groupId);

        Future f = new RaftInvocation(getOperationService(leader).invocationContext,
                getRaftInvocationManager(leader).getRaftInvocationContext(), groupId,
                new DefaultRaftReplicateOp(groupId, new CustomResponseOp2()), 10, 50, 60000).invoke();

        try {
            f.get(60, TimeUnit.SECONDS);
            fail();
        } catch (Exception e) {
            assertInstanceOf(IndeterminateOperationStateException.class, e.getCause());
        }

        assertTrue(COMMIT_COUNT.get() > groupSize);
    }

    @Test
    public void test_invocationFailsWithStaleAppendRequestException_when_thereAreRetryableExceptionsAfterwards() throws ExecutionException, InterruptedException {
        CPGroupId groupId = getRaftInvocationManager(instances[0]).createRaftGroup(groupName).get();

        waitAllForLeaderElection(instances, groupId);
        HazelcastInstance leader = getLeaderInstance(instances, groupId);

        Future f = new RaftInvocation(getOperationService(leader).invocationContext,
                getRaftInvocationManager(leader).getRaftInvocationContext(), groupId,
                new DefaultRaftReplicateOp(groupId, new CustomResponseOp3()), 100, 500, 60000).invoke();

        try {
            f.get(60, TimeUnit.SECONDS);
            fail();
        } catch (Exception e) {
            assertInstanceOf(IndeterminateOperationStateException.class, e.getCause());
        }

        assertTrue(COMMIT_COUNT.get() > groupSize);
    }

    @Test
    public void test_invocationFailsWithFirstMemberLeftException_when_thereAreIndeterminateOperationStateExceptionsAfterwards() throws ExecutionException, InterruptedException {
        CPGroupId groupId = getRaftInvocationManager(instances[0]).createRaftGroup(groupName).get();

        waitAllForLeaderElection(instances, groupId);
        HazelcastInstance leader = getLeaderInstance(instances, groupId);

        Future f = new RaftInvocation(getOperationService(leader).invocationContext,
                getRaftInvocationManager(leader).getRaftInvocationContext(), groupId,
                new DefaultRaftReplicateOp(groupId, new CustomResponseOp4()), 10, 50, 60000).invoke();

        try {
            f.get(60, TimeUnit.SECONDS);
            fail();
        } catch (Exception e) {
            assertInstanceOf(IndeterminateOperationStateException.class, e.getCause());
        }

        assertTrue(COMMIT_COUNT.get() > groupSize);
    }

    @Test
    public void test_invocationFailsWitNonRetryableException_when_thereAreRetryableExceptionsAfterIndeterminateOperationState() throws ExecutionException, InterruptedException {
        CPGroupId groupId = getRaftInvocationManager(instances[0]).createRaftGroup(groupName).get();

        waitAllForLeaderElection(instances, groupId);
        HazelcastInstance leader = getLeaderInstance(instances, groupId);

        Future f = new RaftInvocation(getOperationService(leader).invocationContext,
                getRaftInvocationManager(leader).getRaftInvocationContext(), groupId,
                new DefaultRaftReplicateOp(groupId, new CustomResponseOp5()), 10, 50, 60000).invoke();

        try {
            f.get(60, TimeUnit.SECONDS);
            fail();
        } catch (Exception e) {
            assertInstanceOf(IllegalStateException.class, e.getCause());
        }

        assertTrue(COMMIT_COUNT.get() > groupSize);
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);

        CPSubsystemConfig cpSubsystemConfig = config.getCPSubsystemConfig();
        cpSubsystemConfig.setFailOnIndeterminateOperationState(true);

        return config;
    }

    public static class CustomResponseOp extends RaftOp {

        @Override
        public Object run(CPGroupId groupId, long commitIndex) throws Exception {
            if (COMMIT_COUNT.incrementAndGet() <= 3) {
                MemberImpl member = new MemberImpl(new Address("localhost", 1111), UNKNOWN, false);
                throw new MemberLeftException(member);
            }

            throw new CallerNotMemberException("");
        }

        @Override
        protected String getServiceName() {
            return RaftService.SERVICE_NAME;
        }

        @Override
        public void writeData(ObjectDataOutput out) {
        }

        @Override
        public void readData(ObjectDataInput in) {
        }
    }

    public static class CustomResponseOp2 extends RaftOp implements IndeterminateOperationStateAware {

        @Override
        public Object run(CPGroupId groupId, long commitIndex) throws Exception {
            if (COMMIT_COUNT.incrementAndGet() <= 3) {
                MemberImpl member = new MemberImpl(new Address("localhost", 1111), UNKNOWN, false);
                throw new MemberLeftException(member);
            }

            throw new CallerNotMemberException("");
        }

        @Override
        public boolean isRetryableOnIndeterminateOperationState() {
            return true;
        }

        @Override
        protected String getServiceName() {
            return RaftService.SERVICE_NAME;
        }

        @Override
        public void writeData(ObjectDataOutput out) {
        }

        @Override
        public void readData(ObjectDataInput in) {
        }
    }

    public static class CustomResponseOp3 extends RaftOp implements IndeterminateOperationStateAware {

        @Override
        public Object run(CPGroupId groupId, long commitIndex) {
            if (COMMIT_COUNT.incrementAndGet() <= 3) {
                throw new StaleAppendRequestException(null);
            }

            throw new CallerNotMemberException("");
        }

        @Override
        public boolean isRetryableOnIndeterminateOperationState() {
            return true;
        }

        @Override
        protected String getServiceName() {
            return RaftService.SERVICE_NAME;
        }

        @Override
        public void writeData(ObjectDataOutput out) {
        }

        @Override
        public void readData(ObjectDataInput in) {
        }
    }

    public static class CustomResponseOp4 extends RaftOp implements IndeterminateOperationStateAware {

        @Override
        public Object run(CPGroupId groupId, long commitIndex) throws Exception {
            COMMIT_COUNT.incrementAndGet();
            MemberImpl member = new MemberImpl(new Address("localhost", 1111), UNKNOWN, false);
            throw new MemberLeftException(member);
        }

        @Override
        public boolean isRetryableOnIndeterminateOperationState() {
            return true;
        }

        @Override
        protected String getServiceName() {
            return RaftService.SERVICE_NAME;
        }

        @Override
        public void writeData(ObjectDataOutput out) {
        }

        @Override
        public void readData(ObjectDataInput in) {
        }
    }

    public static class CustomResponseOp5 extends RaftOp implements IndeterminateOperationStateAware {

        @Override
        public Object run(CPGroupId groupId, long commitIndex) {
            if (COMMIT_COUNT.incrementAndGet() <= 3) {
                throw new StaleAppendRequestException(null);
            }

            throw new IllegalStateException("");
        }

        @Override
        public boolean isRetryableOnIndeterminateOperationState() {
            return true;
        }

        @Override
        protected String getServiceName() {
            return RaftService.SERVICE_NAME;
        }

        @Override
        public void writeData(ObjectDataOutput out) {
        }

        @Override
        public void readData(ObjectDataInput in) {
        }
    }
}
