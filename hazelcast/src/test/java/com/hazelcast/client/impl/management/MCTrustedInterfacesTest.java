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

package com.hazelcast.client.impl.management;

import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MCAddWanBatchPublisherConfigCodec;
import com.hazelcast.client.impl.protocol.codec.MCApplyMCConfigCodec;
import com.hazelcast.client.impl.protocol.codec.MCChangeClusterStateCodec;
import com.hazelcast.client.impl.protocol.codec.MCChangeClusterVersionCodec;
import com.hazelcast.client.impl.protocol.codec.MCChangeWanReplicationStateCodec;
import com.hazelcast.client.impl.protocol.codec.MCCheckWanConsistencyCodec;
import com.hazelcast.client.impl.protocol.codec.MCClearWanQueuesCodec;
import com.hazelcast.client.impl.protocol.codec.MCGetCPMembersCodec;
import com.hazelcast.client.impl.protocol.codec.MCGetClusterMetadataCodec;
import com.hazelcast.client.impl.protocol.codec.MCGetMapConfigCodec;
import com.hazelcast.client.impl.protocol.codec.MCGetMemberConfigCodec;
import com.hazelcast.client.impl.protocol.codec.MCGetSystemPropertiesCodec;
import com.hazelcast.client.impl.protocol.codec.MCGetThreadDumpCodec;
import com.hazelcast.client.impl.protocol.codec.MCGetTimedMemberStateCodec;
import com.hazelcast.client.impl.protocol.codec.MCInterruptHotRestartBackupCodec;
import com.hazelcast.client.impl.protocol.codec.MCMatchMCConfigCodec;
import com.hazelcast.client.impl.protocol.codec.MCPollMCEventsCodec;
import com.hazelcast.client.impl.protocol.codec.MCPromoteLiteMemberCodec;
import com.hazelcast.client.impl.protocol.codec.MCPromoteToCPMemberCodec;
import com.hazelcast.client.impl.protocol.codec.MCReadMetricsCodec;
import com.hazelcast.client.impl.protocol.codec.MCRemoveCPMemberCodec;
import com.hazelcast.client.impl.protocol.codec.MCResetCPSubsystemCodec;
import com.hazelcast.client.impl.protocol.codec.MCRunConsoleCommandCodec;
import com.hazelcast.client.impl.protocol.codec.MCRunGcCodec;
import com.hazelcast.client.impl.protocol.codec.MCRunScriptCodec;
import com.hazelcast.client.impl.protocol.codec.MCShutdownClusterCodec;
import com.hazelcast.client.impl.protocol.codec.MCShutdownMemberCodec;
import com.hazelcast.client.impl.protocol.codec.MCTriggerForceStartCodec;
import com.hazelcast.client.impl.protocol.codec.MCTriggerHotRestartBackupCodec;
import com.hazelcast.client.impl.protocol.codec.MCTriggerPartialStartCodec;
import com.hazelcast.client.impl.protocol.codec.MCUpdateMapConfigCodec;
import com.hazelcast.client.impl.protocol.codec.MCWanSyncMapCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastParallelClassRunner.class)
@Category({ QuickTest.class, ParallelJVMTest.class })
public class MCTrustedInterfacesTest extends HazelcastTestSupport {

    HazelcastInstance client;
    HazelcastInstance member;
    private TestHazelcastFactory factory;

    @Before
    public void setUp() {
        factory = new TestHazelcastFactory();

        Config config = smallInstanceConfig();
        config.getManagementCenterConfig().addTrustedInterface("222.222.222.222");
        member = factory.newHazelcastInstance(config);
        client = factory.newHazelcastClient();
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void testGetSystemPropertiesMessageTask_passing() throws Exception {
        HazelcastInstance client = factory.newHazelcastClient(new ClientConfig(), "222.222.222.222");
        HazelcastClientInstanceImpl clientImpl = ((HazelcastClientProxy) client).client;
        ClientInvocation invocation = new ClientInvocation(
                clientImpl,
                MCGetSystemPropertiesCodec.encodeRequest(),
                null
        );

        ClientDelegatingFuture<List<Map.Entry<String, String>>> future = new ClientDelegatingFuture<>(
                invocation.invoke(),
                clientImpl.getSerializationService(),
                MCGetSystemPropertiesCodec::decodeResponse
        );

        assertFalse(future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS).isEmpty());
    }


    @Test
    public void testAddWanBatchPublisherConfigMessageTask() throws Exception {
        Random random = new Random();

        ClientMessage clientMessage = MCAddWanBatchPublisherConfigCodec.encodeRequest(randomString(), randomString(),
                randomString(), randomString(), random.nextInt(), random.nextInt(), random.nextInt(), random.nextInt(),
                random.nextInt(), random.nextInt());

        assertFailureOnUntrustedInterface(clientMessage);
    }

    @Test
    public void testApplyMCConfigMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCApplyMCConfigCodec.encodeRequest(randomString(), 999, new ArrayList<>()));
    }

    @Test
    public void testChangeClusterStateMessageTask() throws Exception {
        ClientMessage clientMessage = MCChangeClusterStateCodec.encodeRequest(888);
        assertFailureOnUntrustedInterface(clientMessage);
    }

    @Test
    public void testChangeClusterVersionMessageTask() throws Exception {
        ClientMessage clientMessage = MCChangeClusterVersionCodec.encodeRequest((byte) 8, (byte) 10);
        assertFailureOnUntrustedInterface(clientMessage);
    }

    @Test
    public void testChangeWanReplicationStateMessageTask() throws Exception {
        ClientMessage clientMessage = MCChangeWanReplicationStateCodec.encodeRequest(randomString(), randomString(),
                (byte) 127);
        assertFailureOnUntrustedInterface(clientMessage);
    }

    @Test
    public void testCheckWanConsistencyMessageTask() throws Exception {
        ClientMessage clientMessage = MCCheckWanConsistencyCodec.encodeRequest(randomString(), randomString(), randomString());
        assertFailureOnUntrustedInterface(clientMessage);
    }

    @Test
    public void testClearWanQueuesMessageTask() throws Exception {
        ClientMessage clientMessage = MCClearWanQueuesCodec.encodeRequest(randomString(), randomString());
        assertFailureOnUntrustedInterface(clientMessage);
    }

    @Test
    public void testGetClusterMetadataMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCGetClusterMetadataCodec.encodeRequest());
    }

    @Test
    public void testGetCPMembersMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCGetCPMembersCodec.encodeRequest());
    }

    @Test
    public void testGetMapConfigMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCGetMapConfigCodec.encodeRequest(randomString()));
    }

    @Test
    public void testGetMemberConfigMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCGetMemberConfigCodec.encodeRequest());
    }

    @Test
    public void testGetSystemPropertiesMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCGetSystemPropertiesCodec.encodeRequest());
    }

    @Test
    public void testGetThreadDumpMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCGetThreadDumpCodec.encodeRequest(false));
    }

    @Test
    public void testGetTimedMemberStateMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCGetTimedMemberStateCodec.encodeRequest());
    }

    @Test
    public void testHotRestartInterruptBackupMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCInterruptHotRestartBackupCodec.encodeRequest());
    }

    @Test
    public void testHotRestartTriggerBackupMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCTriggerHotRestartBackupCodec.encodeRequest());
    }

    @Test
    public void testHotRestartTriggerForceStartMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCTriggerForceStartCodec.encodeRequest());
    }

    @Test
    public void testHotRestartTriggerPartialStartMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCTriggerPartialStartCodec.encodeRequest());
    }

    @Test
    public void testMatchMCConfigMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCMatchMCConfigCodec.encodeRequest(randomString()));
    }

    @Test
    public void testPollMCEventsMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCPollMCEventsCodec.encodeRequest());
    }

    @Test
    public void testPromoteLiteMemberMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCPromoteLiteMemberCodec.encodeRequest());
    }

    @Test
    public void testPromoteToCPMemberMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCPromoteToCPMemberCodec.encodeRequest());
    }

    @Test
    public void testRemoveCPMemberMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCRemoveCPMemberCodec.encodeRequest(randomUUID()));
    }

    @Test
    public void testResetCPSubsystemMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCResetCPSubsystemCodec.encodeRequest());
    }

    @Test
    public void testRunConsoleCommandMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCRunConsoleCommandCodec.encodeRequest(randomString(), "help"));
    }

    @Test
    public void testRunGCMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCRunGcCodec.encodeRequest());
    }

    @Test
    public void testRunScriptMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCRunScriptCodec.encodeRequest(randomString(), randomString()));
    }

    @Test
    public void testShutdownClusterMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCShutdownClusterCodec.encodeRequest());
        assertTrue(member.getLifecycleService().isRunning());
    }

    @Test
    public void testShutdownMemberMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCShutdownMemberCodec.encodeRequest());
        assertTrue(member.getLifecycleService().isRunning());
    }

    @Test
    public void testUpdateMapConfigMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCUpdateMapConfigCodec.encodeRequest(randomString(), 100, 200, 0, false, 100, 0));
    }

    @Test
    public void testWanSyncMapMessageTask() throws Exception {
        ClientMessage clientMessage = MCWanSyncMapCodec.encodeRequest(randomString(), randomString(), 0, randomString());
        assertFailureOnUntrustedInterface(clientMessage);
    }

    @Test
    public void testReadMetrics() throws Exception {
        assertFailureOnUntrustedInterface(MCReadMetricsCodec.encodeRequest(randomUUID(), 0L));
    }

    private void assertFailureOnUntrustedInterface(ClientMessage clientMessage) throws Exception {
        ClientInvocation invocation = new ClientInvocation(((HazelcastClientProxy) client).client, clientMessage, null);
        ClientInvocationFuture future = invocation.invoke();
        try {
            future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
            fail("AccessControlException was expected.");
        } catch (ExecutionException e) {
            assertThat(e.getCause(), is(instanceOf(AccessControlException.class)));
        }
    }
}
