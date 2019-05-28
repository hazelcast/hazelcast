/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.cp.internal.datastructures.lock;

import com.hazelcast.client.cp.internal.session.ClientProxySessionManager;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.lock.FencedLockBasicTest;
import com.hazelcast.cp.internal.session.AbstractProxySessionManager;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FencedLockClientBasicTest extends FencedLockBasicTest {

    @Override
    protected TestHazelcastInstanceFactory createTestFactory() {
        return new TestHazelcastFactory();
    }

    @Override
    protected HazelcastInstance[] createInstances() {
        HazelcastInstance[] instances = super.createInstances();
        TestHazelcastFactory f = (TestHazelcastFactory) factory;
        lockInstance = f.newHazelcastClient();
        return instances;
    }

    @Test
    public void test_lockAutoRelease_onClientShutdown() {
        String proxyName = lock.getName();
        lock.lock();

        lockInstance.shutdown();

        assertFalse(instances[0].getCPSubsystem().getLock(proxyName).isLocked());
    }

    @Test
    public void test_sessionIsClosedOnCPSubsystemReset() throws Exception {
        lock.lock();

        instances[0].getCPSubsystem().getCPSubsystemManagementService().restart().get();

        assertTrueEventually(() -> {
            HazelcastClientProxy clientProxy = (HazelcastClientProxy) lockInstance;
            ClientProxySessionManager proxySessionManager = clientProxy.client.getProxySessionManager();
            assertEquals(NO_SESSION_ID, proxySessionManager.getSession((RaftGroupId) lock.getGroupId()));
        });
    }

    protected AbstractProxySessionManager getSessionManager(HazelcastInstance instance) {
        return (((HazelcastClientProxy) lockInstance).client).getProxySessionManager();
    }

}
