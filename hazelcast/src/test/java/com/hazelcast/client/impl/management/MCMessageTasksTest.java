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
import com.hazelcast.client.impl.protocol.codec.MCRemoveCPMemberCodec;
import com.hazelcast.client.impl.protocol.codec.MCResetCPSubsystemCodec;
import com.hazelcast.client.impl.protocol.codec.MCRunGcCodec;
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
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.internal.cluster.impl.VersionMismatchException;
import com.hazelcast.internal.management.dto.MCEventDTO;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.internal.util.StringUtil.isNullOrEmptyAfterTrim;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MCMessageTasksTest extends HazelcastTestSupport {

    HazelcastInstance client;
    HazelcastInstance member;
    private TestHazelcastFactory factory;

    @Before
    public void setUp() {
        factory = new TestHazelcastFactory();

        member = factory.newHazelcastInstance(smallInstanceConfig());
        client = factory.newHazelcastClient();
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void testAddWanBatchPublisherConfigMessageTask() throws Exception {
        Random random = new Random();

        ClientMessage clientMessage = MCAddWanBatchPublisherConfigCodec.encodeRequest(
                randomString(),
                randomString(),
                randomString(),
                randomString(),
                random.nextInt(),
                random.nextInt(),
                random.nextInt(),
                random.nextInt(),
                random.nextInt(),
                random.nextInt()
        );

        assertFailure(clientMessage, UnsupportedOperationException.class, "Adding new WAN config is not supported.");
    }

    @Test
    public void testApplyMCConfigMessageTask() throws Exception {
        ClientMessage clientMessage = MCApplyMCConfigCodec.encodeRequest(randomString(), 999, new ArrayList<>());
        assertFailure(clientMessage, IllegalArgumentException.class, "Unexpected client B/W list mode = [999]");
    }

    @Test
    public void testChangeClusterStateMessageTask() throws Exception {
        ClientMessage clientMessage = MCChangeClusterStateCodec.encodeRequest(888);
        assertFailure(clientMessage, IllegalArgumentException.class, "Unsupported ID value");
    }

    @Test
    public void testChangeClusterVersionMessageTask() throws Exception {
        ClientMessage clientMessage = MCChangeClusterVersionCodec.encodeRequest((byte) 8, (byte) 10);
        String expectedExceptionMsg = "Node's codebase version "
                + ((HazelcastInstanceProxy) member).getOriginal().node.getVersion()
                + " is incompatible with the requested cluster version 8.10";
        assertFailure(clientMessage, VersionMismatchException.class, expectedExceptionMsg);
    }

    @Test
    public void testChangeWanReplicationStateMessageTask() throws Exception {
        ClientMessage clientMessage = MCChangeWanReplicationStateCodec.encodeRequest(
                randomString(),
                randomString(),
                (byte) 127
        );
        assertFailure(clientMessage, IllegalArgumentException.class, "Unexpected WAN publisher state = [127]");
    }

    @Test
    public void testCheckWanConsistencyMessageTask() throws Exception {
        ClientMessage clientMessage = MCCheckWanConsistencyCodec.encodeRequest(
                randomString(),
                randomString(),
                randomString()
        );
        assertFailure(clientMessage, UnsupportedOperationException.class, "Consistency check is not supported.");
    }

    @Test
    public void testClearWanQueuesMessageTask() throws Exception {
        ClientMessage clientMessage = MCClearWanQueuesCodec.encodeRequest(randomString(), randomString());
        assertFailure(clientMessage, UnsupportedOperationException.class, "Clearing WAN replication queues is not supported.");
    }

    @Test
    public void testGetClusterMetadataMessageTask() throws Exception {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCGetClusterMetadataCodec.encodeRequest(),
                null
        );

        ClientDelegatingFuture<MCGetClusterMetadataCodec.ResponseParameters> future = new ClientDelegatingFuture<>(
                invocation.invoke(),
                getClientImpl().getSerializationService(),
                MCGetClusterMetadataCodec::decodeResponse
        );

        MCGetClusterMetadataCodec.ResponseParameters response = future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
        assertTrue(response.clusterTime > 0);
        assertEquals(0, response.currentState);
        assertEquals(BuildInfoProvider.getBuildInfo().getVersion(), response.memberVersion);
        assertNull(response.jetVersion);
    }

    @Test
    public void testGetCPMembersMessageTask() throws Exception {
        assertFailure(MCGetCPMembersCodec.encodeRequest(), HazelcastException.class, "CP Subsystem is not enabled!");
    }

    @Test
    public void testGetMapConfigMessageTask() throws Exception {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCGetMapConfigCodec.encodeRequest(randomString()),
                null
        );

        ClientDelegatingFuture<MCGetMapConfigCodec.ResponseParameters> future = new ClientDelegatingFuture<>(
                invocation.invoke(),
                getClientImpl().getSerializationService(),
                MCGetMapConfigCodec::decodeResponse
        );

        MCGetMapConfigCodec.ResponseParameters response = future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
        assertFalse(response.readBackupData);
    }

    @Test
    public void testGetMemberConfigMessageTask() throws Exception {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCGetMemberConfigCodec.encodeRequest(),
                null
        );

        ClientDelegatingFuture<String> future = new ClientDelegatingFuture<>(
                invocation.invoke(),
                getClientImpl().getSerializationService(),
                MCGetMemberConfigCodec::decodeResponse
        );

        assertFalse(isNullOrEmptyAfterTrim(future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS)));
    }

    @Test
    public void testGetSystemPropertiesMessageTask() throws Exception {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCGetSystemPropertiesCodec.encodeRequest(),
                null
        );

        ClientDelegatingFuture<List<Entry<String, String>>> future
                = new ClientDelegatingFuture<>(invocation.invoke(), getClientImpl().getSerializationService(),
                MCGetSystemPropertiesCodec::decodeResponse
        );

        assertFalse(future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS).isEmpty());
    }

    @Test
    public void testGetThreadDumpMessageTask() throws Exception {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCGetThreadDumpCodec.encodeRequest(false),
                null
        );

        ClientDelegatingFuture<String> future = new ClientDelegatingFuture<>(
                invocation.invoke(),
                getClientImpl().getSerializationService(),
                MCGetThreadDumpCodec::decodeResponse
        );

        assertFalse(isNullOrEmptyAfterTrim(future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS)));
    }

    @Test
    public void testGetTimedMemberStateMessageTask() throws Exception {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCGetTimedMemberStateCodec.encodeRequest(),
                null
        );

        ClientDelegatingFuture<String> future = new ClientDelegatingFuture<>(
                invocation.invoke(),
                getClientImpl().getSerializationService(),
                MCGetTimedMemberStateCodec::decodeResponse
        );

        assertFalse(isNullOrEmptyAfterTrim(future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS)));
    }

    @Test
    public void testHotRestartInterruptBackupMessageTask() throws Exception {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCInterruptHotRestartBackupCodec.encodeRequest(),
                null
        );

        ClientDelegatingFuture<Void> future = new ClientDelegatingFuture<>(
                invocation.invoke(),
                getClientImpl().getSerializationService(),
                clientMessage -> null
        );

        future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
    }

    @Test
    public void testHotRestartTriggerBackupMessageTask() throws Exception {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCTriggerHotRestartBackupCodec.encodeRequest(),
                null
        );

        ClientDelegatingFuture<Void> future = new ClientDelegatingFuture<>(
                invocation.invoke(),
                getClientImpl().getSerializationService(),
                clientMessage -> null
        );

        future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
    }

    @Test
    public void testHotRestartTriggerForceStartMessageTask() throws Exception {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCTriggerForceStartCodec.encodeRequest(),
                null
        );

        ClientDelegatingFuture<Boolean> future = new ClientDelegatingFuture<>(
                invocation.invoke(),
                getClientImpl().getSerializationService(),
                MCTriggerForceStartCodec::decodeResponse
        );

        assertFalse(future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS));
    }

    @Test
    public void testHotRestartTriggerPartialStartMessageTask() throws Exception {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCTriggerPartialStartCodec.encodeRequest(),
                null
        );

        ClientDelegatingFuture<Boolean> future = new ClientDelegatingFuture<>(
                invocation.invoke(),
                getClientImpl().getSerializationService(),
                MCTriggerPartialStartCodec::decodeResponse
        );

        assertFalse(future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS));
    }

    @Test
    public void testMatchMCConfigMessageTask() throws Exception {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCMatchMCConfigCodec.encodeRequest(randomString()),
                null
        );

        ClientDelegatingFuture<Boolean> future = new ClientDelegatingFuture<>(
                invocation.invoke(),
                getClientImpl().getSerializationService(),
                MCMatchMCConfigCodec::decodeResponse
        );

        assertFalse(future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS));
    }

    @Test
    public void testPollMCEventsMessageTask() throws Exception {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCPollMCEventsCodec.encodeRequest(),
                null
        );

        ClientDelegatingFuture<List<MCEventDTO>> future = new ClientDelegatingFuture<>(
                invocation.invoke(),
                getClientImpl().getSerializationService(),
                MCPollMCEventsCodec::decodeResponse
        );

        assertTrue(future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS).isEmpty());
    }

    @Test
    public void testPromoteLiteMemberMessageTask() throws Exception {
        assertFailure(MCPromoteLiteMemberCodec.encodeRequest(), IllegalStateException.class,
                member.getCluster().getLocalMember() + " is not a lite member!");
    }

    @Test
    public void testPromoteToCPMemberMessageTask() throws Exception {
        assertFailure(MCPromoteToCPMemberCodec.encodeRequest(), HazelcastException.class, "CP Subsystem is not enabled!");
    }

    @Test
    public void testRemoveCPMemberMessageTask() throws Exception {
        assertFailure(MCRemoveCPMemberCodec.encodeRequest(UUID.randomUUID()), HazelcastException.class, "CP Subsystem is not enabled!");
    }

    @Test
    public void testResetCPSubsystemMessageTask() throws Exception {
        assertFailure(MCResetCPSubsystemCodec.encodeRequest(), HazelcastException.class, "CP Subsystem is not enabled!");
    }

    @Test
    public void testRunGCMessageTask() throws Exception {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCRunGcCodec.encodeRequest(),
                null
        );

        ClientDelegatingFuture<Void> future = new ClientDelegatingFuture<>(
                invocation.invoke(),
                getClientImpl().getSerializationService(),
                clientMessage -> null
        );

        future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
    }

    @Test
    public void testShutdownClusterMessageTask() {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCShutdownClusterCodec.encodeRequest(),
                null
        );

        invocation.invoke();

        assertTrueEventually(() -> assertFalse(member.getLifecycleService().isRunning()));
    }

    @Test
    public void testShutdownMemberMessageTask() {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCShutdownMemberCodec.encodeRequest(),
                null
        );

        invocation.invoke();

        assertTrueEventually(() -> assertFalse(member.getLifecycleService().isRunning()));
    }

    @Test
    public void testUpdateMapConfigMessageTask() throws Exception {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCUpdateMapConfigCodec.encodeRequest(
                        randomString(),
                        100,
                        200,
                        0,
                        false,
                        100,
                        0
                ),
                null
        );

        ClientDelegatingFuture<Void> future = new ClientDelegatingFuture<>(
                invocation.invoke(),
                getClientImpl().getSerializationService(),
                clientMessage -> null
        );

        future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
    }

    @Test
    public void testWanSyncMapMessageTask() throws Exception {
        ClientMessage clientMessage = MCWanSyncMapCodec.encodeRequest(
                randomString(),
                randomString(),
                0,
                randomString()
        );
        assertFailure(clientMessage, UnsupportedOperationException.class, "WAN sync is not supported.");
    }

    private void assertFailure(ClientMessage clientMessage,
                               Class<? extends Exception> expectedExceptionType,
                               String expectedExceptionMsg) throws Exception {
        ClientInvocation invocation = new ClientInvocation(getClientImpl(), clientMessage, null);
        ClientInvocationFuture future = invocation.invoke();
        try {
            future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
            fail("Execution was successful whereas failure was expected.");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            assertTrue("Cause is of type " + cause.getClass().toString(),
                    cause.getClass().isAssignableFrom(expectedExceptionType));
            assertEquals(expectedExceptionMsg, cause.getMessage());
        }
    }

    private HazelcastClientInstanceImpl getClientImpl() {
        return ((HazelcastClientProxy) client).client;
    }
}
