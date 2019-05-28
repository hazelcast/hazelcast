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

package com.hazelcast.client.cp.internal.session;

import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.session.AbstractProxySessionManager;
import com.hazelcast.cp.internal.session.AbstractProxySessionManagerTest;
import com.hazelcast.cp.internal.session.SessionAwareProxy;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientSessionManagerTest extends AbstractProxySessionManagerTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private HazelcastInstance client;

    @Override
    protected TestHazelcastInstanceFactory createTestFactory() {
        return new TestHazelcastFactory();
    }

    @Before
    public void setupClient() {
        TestHazelcastFactory f = (TestHazelcastFactory) factory;
        client = f.newHazelcastClient();
    }

    @Test
    public void testClientSessionManagerShutdown() throws ExecutionException, InterruptedException {
        AbstractProxySessionManager sessionManager = getSessionManager();
        SessionProxyImpl proxy = new SessionProxyImpl(sessionManager, groupId);
        proxy.createSession();

        Map<RaftGroupId, ICompletableFuture<Object>> futures = sessionManager.shutdown();
        assertEquals(1, futures.size());

        Entry<RaftGroupId, ICompletableFuture<Object>> e = futures.entrySet().iterator().next();
        assertEquals(groupId, e.getKey());
        e.getValue().get();

        exception.expect(IllegalStateException.class);
        proxy.createSession();
    }

    @After
    public void shutdown() {
        factory.terminateAll();
    }

    protected AbstractProxySessionManager getSessionManager() {
        return spy((((HazelcastClientProxy) client).client).getProxySessionManager());
    }

    private static class SessionProxyImpl extends SessionAwareProxy {

        SessionProxyImpl(AbstractProxySessionManager sessionManager, RaftGroupId groupId) {
            super(sessionManager, groupId);
        }

        long createSession() {
            return super.acquireSession();
        }
    }
}
