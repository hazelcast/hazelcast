/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.StaticMemberNodeContext;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.instance.impl.HazelcastInstanceFactory.newHazelcastInstance;
import static com.hazelcast.test.Accessors.getOperationService;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Invocation_OnMemberLeftTest extends HazelcastTestSupport {

    private OperationServiceImpl localOperationService;
    private InvocationMonitor localInvocationMonitor;
    private HazelcastInstance remote;

    private HazelcastInstance master;
    private HazelcastInstance nonMaster;

    private MemberImpl remoteMember;
    private TestHazelcastInstanceFactory instanceFactory;

    @Before
    public void setup() {
        instanceFactory = createHazelcastInstanceFactory();
        Config config = new Config();
        config.setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "5");

        HazelcastInstance[] cluster = instanceFactory.newInstances(config, 2);

        localOperationService = getOperationService(cluster[0]);
        localInvocationMonitor = localOperationService.getInvocationMonitor();

        remote = cluster[1];
        ClusterService localClusterService = Accessors.getClusterService(cluster[0]);
        Address masterAddress = localClusterService.getMasterAddress();
        master = Accessors.getAddress(cluster[0]).equals(masterAddress) ? cluster[0] : cluster[1];
        nonMaster = master == cluster[0] ? cluster[1] : cluster[0];

        remoteMember = (MemberImpl) remote.getCluster().getLocalMember();
    }

    @Test
    public void when_MemberLeaves_withTargetInvocation_then_MemberLeftExceptionIsThrown() throws Exception {
        Future<Object> future =
                localOperationService.invokeOnTarget(null, new UnresponsiveTargetOperation(), remoteMember.getAddress());

        // Unresponsive operation should be executed before shutting down the node
        assertUnresponsiveTargetOperationStarted();

        remote.getLifecycleService().terminate();

        try {
            future.get();
            fail("Invocation should have failed with MemberLeftException!");
        } catch (MemberLeftException e) {
            ignore(e);
        }
    }

    /**
     * We use static field because instance of FailingUnresponsiveMasterOperation is ser/deser, and we want to have one
     * instance of AtomicBoolean. Since the whole test class is run with HazelcastParallelClassRunner that field cannot be reused
     * in other tests then following one.
     */
    private static final AtomicBoolean MASTER_EXCEPTION_HANDLED = new AtomicBoolean();

    @Test
    public void when_MemberLeavesWithMasterInvocation_then_MemberLeftExceptionIsHandledAndThrown() throws Exception {
        MASTER_EXCEPTION_HANDLED.set(false);
        Future<Object> future = getOperationService(nonMaster).invokeOnMaster(null,
                new FailingUnresponsiveMasterOperation());

        // Unresponsive operation should be executed before shutting down the node
        assertUnresponsiveMasterOperationStartedOnMaster();

        master.getLifecycleService().terminate();

        try {
            future.get();
            fail("Invocation should have failed with MemberLeftException!");
        } catch (MemberLeftException e) {
            ignore(e);
        }

        assert MASTER_EXCEPTION_HANDLED.get() : "MemberLeftException wasn't properly handled";
    }

    @Test
    public void when_MemberLeavesWithMasterInvocation_then_OperationIsResubmittedToTheNewMaster() {
        getOperationService(nonMaster).invokeOnMaster(null,
                new UnresponsiveMasterOperation());

        // Unresponsive operation should be executed before shutting down the node
        assertUnresponsiveMasterOperationStartedOnMaster();
        // The operation should not be executed on non-master before termination of master
        assertUnresponsiveMasterOperationNotStartedOnNonMaster();
        master.getLifecycleService().terminate();
        // After master failure the operation should be submitted to non-master node
        assertUnresponsiveMasterOperationStartedOnNonMaster();
    }

    @Test
    public void whenMemberRestarts_withSameAddress() throws Exception {
        whenMemberRestarts(() -> remote = instanceFactory.newHazelcastInstance(remoteMember.getAddress()));
    }

    @Test
    public void whenMemberRestarts_withSameIdentity() throws Exception {
        whenMemberRestarts(() -> {
            StaticMemberNodeContext nodeContext = new StaticMemberNodeContext(instanceFactory, remoteMember);
            remote = newHazelcastInstance(new Config(), remoteMember.toString(), nodeContext);
        });
    }

    private void whenMemberRestarts(Runnable restartAction) throws Exception {
        Future<Object> futureBeforeShutdown =
                localOperationService.invokeOnTarget(null, new UnresponsiveTargetOperation(), remoteMember.getAddress());

        final CountDownLatch blockMonitorLatch = new CountDownLatch(1);
        final CountDownLatch resumeMonitorLatch = new CountDownLatch(1);

        localInvocationMonitor.execute(() -> {
            blockMonitorLatch.countDown();
            assertOpenEventually(resumeMonitorLatch);
        });

        assertOpenEventually(blockMonitorLatch);

        // Unresponsive operation should be executed before shutting down the node
        assertUnresponsiveTargetOperationStarted();

        remote.getLifecycleService().terminate();
        restartAction.run();

        assertTrue(remote.getLifecycleService().isRunning());

        Future<Object> futureAfterRestart =
                localOperationService.invokeOnTarget(null, new UnresponsiveTargetOperation(), remoteMember.getAddress());

        resumeMonitorLatch.countDown();

        try {
            futureBeforeShutdown.get();
            fail("Invocation should have failed with MemberLeftException!");
        } catch (MemberLeftException e) {
            ignore(e);
        }

        try {
            futureAfterRestart.get(1, TimeUnit.SECONDS);
            fail("future.get() should have failed with TimeoutException!");
        } catch (TimeoutException e) {
            ignore(e);
        }
    }

    private void assertUnresponsiveTargetOperationStarted() {
        assertTrueEventually(() -> assertNotNull(remote.getUserContext().get(UnresponsiveTargetOperation.COMPLETION_FLAG)));
    }

    private void assertUnresponsiveMasterOperationStartedOnMaster() {
        assertTrueEventually(() -> assertNotNull(master.getUserContext().get(UnresponsiveMasterOperation.COMPLETION_FLAG)));
    }

    private void assertUnresponsiveMasterOperationNotStartedOnNonMaster() {
        assertNull(nonMaster.getUserContext().get(UnresponsiveMasterOperation.COMPLETION_FLAG));
    }

    private void assertUnresponsiveMasterOperationStartedOnNonMaster() {
        assertTrueEventually(() -> assertNotNull(nonMaster.getUserContext().get(UnresponsiveMasterOperation.COMPLETION_FLAG)));
    }

    private static class UnresponsiveMasterOperation extends Operation {
        static final String COMPLETION_FLAG = UnresponsiveMasterOperation.class.getName();

        @Override
        public void run() throws Exception {
            getNodeEngine().getHazelcastInstance().getUserContext().put(COMPLETION_FLAG, new Object());
        }

        @Override
        public boolean returnsResponse() {
            return false;
        }
    }

    private static class FailingUnresponsiveMasterOperation extends UnresponsiveMasterOperation {
        @Override
        public ExceptionAction onMasterInvocationException(Throwable throwable) {
            MASTER_EXCEPTION_HANDLED.set(true);
            return ExceptionAction.THROW_EXCEPTION;
        }
    }

    private static class UnresponsiveTargetOperation extends Operation {
        static final String COMPLETION_FLAG = UnresponsiveTargetOperation.class.getName();

        @Override
        public void run() throws Exception {
            getNodeEngine().getHazelcastInstance().getUserContext().put(COMPLETION_FLAG, new Object());
        }

        @Override
        public boolean returnsResponse() {
            return false;
        }
    }
}
