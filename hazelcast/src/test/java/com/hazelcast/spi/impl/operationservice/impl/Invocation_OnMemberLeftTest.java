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

import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.StaticMemberNodeContext;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.AssertTask;
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

import static com.hazelcast.instance.impl.HazelcastInstanceFactory.newHazelcastInstance;
import static com.hazelcast.test.Accessors.getOperationService;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Invocation_OnMemberLeftTest extends HazelcastTestSupport {

    private OperationServiceImpl localOperationService;
    private InvocationMonitor localInvocationMonitor;
    private HazelcastInstance remote;
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
        remoteMember = (MemberImpl) remote.getCluster().getLocalMember();
    }

    @Test
    public void whenMemberLeaves() throws Exception {
        Future<Object> future =
                localOperationService.invokeOnTarget(null, new UnresponsiveTargetOperation(), remoteMember.getAddress());

        // Unresponsive operation should be executed before shutting down the node
        assertUnresponsiveOperationStarted();

        remote.getLifecycleService().terminate();

        try {
            future.get();
            fail("Invocation should have failed with MemberLeftException!");
        } catch (MemberLeftException e) {
            ignore(e);
        }
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
        assertUnresponsiveOperationStarted();

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

    private void assertUnresponsiveOperationStarted() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNotNull(remote.getUserContext().get(UnresponsiveTargetOperation.COMPLETION_FLAG));
            }
        });
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
