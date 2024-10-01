/*
 * Copyright (c) 2008-, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.protocol.compatibility;

import com.hazelcast.client.HazelcastClientUtil;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.ClientMessageReader;
import com.hazelcast.client.impl.protocol.codec.*;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.hazelcast.client.impl.protocol.ClientMessage.IS_FINAL_FLAG;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@SuppressWarnings("unused")
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCompatibilityNullTest_2_9 {
    private final List<ClientMessage> clientMessages = new ArrayList<>();

    @Before
    public void setUp() throws IOException {
        try (InputStream inputStream = getClass().getResourceAsStream("/2.9.protocol.compatibility.null.binary")) {
            assert inputStream != null;
            byte[] data = inputStream.readAllBytes();
            ByteBuffer buffer = ByteBuffer.wrap(data);
            ClientMessageReader reader = new ClientMessageReader(0);
            while (reader.readFrom(buffer, true)) {
                clientMessages.add(reader.getClientMessage());
                reader.reset();
            }
        }
    }

    @Test
    public void test_ClientAuthenticationCodec_encodeRequest() {
        int fileClientMessageIndex = 0;
        ClientMessage encoded = ClientAuthenticationCodec.encodeRequest(aString, null, null, null, aString, aByte, aString, aString, aListOfStrings, aByte, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientAuthenticationCodec_decodeResponse() {
        int fileClientMessageIndex = 1;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientAuthenticationCodec.ResponseParameters parameters = ClientAuthenticationCodec.decodeResponse(fromFile);
        assertTrue(isEqual(aByte, parameters.status));
        assertTrue(isEqual(null, parameters.address));
        assertTrue(isEqual(null, parameters.memberUuid));
        assertTrue(isEqual(aByte, parameters.serializationVersion));
        assertTrue(isEqual(aString, parameters.serverHazelcastVersion));
        assertTrue(isEqual(anInt, parameters.partitionCount));
        assertTrue(isEqual(aUUID, parameters.clusterId));
        assertTrue(isEqual(aBoolean, parameters.failoverSupported));
        assertTrue(parameters.isTpcPortsExists);
        assertTrue(isEqual(null, parameters.tpcPorts));
        assertTrue(parameters.isTpcTokenExists);
        assertTrue(isEqual(null, parameters.tpcToken));
        assertTrue(parameters.isMemberListVersionExists);
        assertTrue(isEqual(anInt, parameters.memberListVersion));
        assertTrue(parameters.isMemberInfosExists);
        assertTrue(isEqual(aListOfMemberInfos, parameters.memberInfos));
        assertTrue(parameters.isPartitionListVersionExists);
        assertTrue(isEqual(anInt, parameters.partitionListVersion));
        assertTrue(parameters.isPartitionsExists);
        assertTrue(isEqual(aListOfUUIDToListOfIntegers, parameters.partitions));
        assertTrue(parameters.isKeyValuePairsExists);
        assertTrue(isEqual(aMapOfStringToString, parameters.keyValuePairs));
    }

    @Test
    public void test_ClientAuthenticationCustomCodec_encodeRequest() {
        int fileClientMessageIndex = 2;
        ClientMessage encoded = ClientAuthenticationCustomCodec.encodeRequest(aString, aByteArray, null, aString, aByte, aString, aString, aListOfStrings, aByte, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientAuthenticationCustomCodec_decodeResponse() {
        int fileClientMessageIndex = 3;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientAuthenticationCustomCodec.ResponseParameters parameters = ClientAuthenticationCustomCodec.decodeResponse(fromFile);
        assertTrue(isEqual(aByte, parameters.status));
        assertTrue(isEqual(null, parameters.address));
        assertTrue(isEqual(null, parameters.memberUuid));
        assertTrue(isEqual(aByte, parameters.serializationVersion));
        assertTrue(isEqual(aString, parameters.serverHazelcastVersion));
        assertTrue(isEqual(anInt, parameters.partitionCount));
        assertTrue(isEqual(aUUID, parameters.clusterId));
        assertTrue(isEqual(aBoolean, parameters.failoverSupported));
        assertTrue(parameters.isTpcPortsExists);
        assertTrue(isEqual(null, parameters.tpcPorts));
        assertTrue(parameters.isTpcTokenExists);
        assertTrue(isEqual(null, parameters.tpcToken));
        assertTrue(parameters.isMemberListVersionExists);
        assertTrue(isEqual(anInt, parameters.memberListVersion));
        assertTrue(parameters.isMemberInfosExists);
        assertTrue(isEqual(null, parameters.memberInfos));
        assertTrue(parameters.isPartitionListVersionExists);
        assertTrue(isEqual(anInt, parameters.partitionListVersion));
        assertTrue(parameters.isPartitionsExists);
        assertTrue(isEqual(null, parameters.partitions));
        assertTrue(parameters.isKeyValuePairsExists);
        assertTrue(isEqual(aMapOfStringToString, parameters.keyValuePairs));
    }

    @Test
    public void test_ClientAddClusterViewListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 4;
        ClientMessage encoded = ClientAddClusterViewListenerCodec.encodeRequest();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientAddClusterViewListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 5;
    }

    private static class ClientAddClusterViewListenerCodecHandler extends ClientAddClusterViewListenerCodec.AbstractEventHandler {
        @Override
        public void handleMembersViewEvent(int version, java.util.Collection<com.hazelcast.internal.cluster.MemberInfo> memberInfos) {
            assertTrue(isEqual(anInt, version));
            assertTrue(isEqual(aListOfMemberInfos, memberInfos));
        }
        @Override
        public void handlePartitionsViewEvent(int version, java.util.Collection<java.util.Map.Entry<java.util.UUID, java.util.List<java.lang.Integer>>> partitions) {
            assertTrue(isEqual(anInt, version));
            assertTrue(isEqual(aListOfUUIDToListOfIntegers, partitions));
        }
        @Override
        public void handleMemberGroupsViewEvent(int version, java.util.Collection<java.util.Collection<java.util.UUID>> memberGroups) {
            assertTrue(isEqual(anInt, version));
            assertTrue(isEqual(aListOfListOfUUIDs, memberGroups));
        }
        @Override
        public void handleClusterVersionEvent(com.hazelcast.version.Version version) {
            assertTrue(isEqual(aVersion, version));
        }
    }

    @Test
    public void test_ClientAddClusterViewListenerCodec_handleMembersViewEvent() {
        int fileClientMessageIndex = 6;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientAddClusterViewListenerCodecHandler handler = new ClientAddClusterViewListenerCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_ClientAddClusterViewListenerCodec_handlePartitionsViewEvent() {
        int fileClientMessageIndex = 7;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientAddClusterViewListenerCodecHandler handler = new ClientAddClusterViewListenerCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_ClientAddClusterViewListenerCodec_handleMemberGroupsViewEvent() {
        int fileClientMessageIndex = 8;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientAddClusterViewListenerCodecHandler handler = new ClientAddClusterViewListenerCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_ClientAddClusterViewListenerCodec_handleClusterVersionEvent() {
        int fileClientMessageIndex = 9;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientAddClusterViewListenerCodecHandler handler = new ClientAddClusterViewListenerCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_ClientCreateProxyCodec_encodeRequest() {
        int fileClientMessageIndex = 10;
        ClientMessage encoded = ClientCreateProxyCodec.encodeRequest(aString, aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientCreateProxyCodec_decodeResponse() {
        int fileClientMessageIndex = 11;
    }

    @Test
    public void test_ClientDestroyProxyCodec_encodeRequest() {
        int fileClientMessageIndex = 12;
        ClientMessage encoded = ClientDestroyProxyCodec.encodeRequest(aString, aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientDestroyProxyCodec_decodeResponse() {
        int fileClientMessageIndex = 13;
    }

    @Test
    public void test_ClientAddPartitionLostListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 14;
        ClientMessage encoded = ClientAddPartitionLostListenerCodec.encodeRequest(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientAddPartitionLostListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 15;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, ClientAddPartitionLostListenerCodec.decodeResponse(fromFile)));
    }

    private static class ClientAddPartitionLostListenerCodecHandler extends ClientAddPartitionLostListenerCodec.AbstractEventHandler {
        @Override
        public void handlePartitionLostEvent(int partitionId, int lostBackupCount, java.util.UUID source) {
            assertTrue(isEqual(anInt, partitionId));
            assertTrue(isEqual(anInt, lostBackupCount));
            assertTrue(isEqual(null, source));
        }
    }

    @Test
    public void test_ClientAddPartitionLostListenerCodec_handlePartitionLostEvent() {
        int fileClientMessageIndex = 16;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientAddPartitionLostListenerCodecHandler handler = new ClientAddPartitionLostListenerCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_ClientRemovePartitionLostListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 17;
        ClientMessage encoded = ClientRemovePartitionLostListenerCodec.encodeRequest(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientRemovePartitionLostListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 18;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ClientRemovePartitionLostListenerCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ClientGetDistributedObjectsCodec_encodeRequest() {
        int fileClientMessageIndex = 19;
        ClientMessage encoded = ClientGetDistributedObjectsCodec.encodeRequest();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientGetDistributedObjectsCodec_decodeResponse() {
        int fileClientMessageIndex = 20;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfDistributedObjectInfo, ClientGetDistributedObjectsCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ClientAddDistributedObjectListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 21;
        ClientMessage encoded = ClientAddDistributedObjectListenerCodec.encodeRequest(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientAddDistributedObjectListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 22;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, ClientAddDistributedObjectListenerCodec.decodeResponse(fromFile)));
    }

    private static class ClientAddDistributedObjectListenerCodecHandler extends ClientAddDistributedObjectListenerCodec.AbstractEventHandler {
        @Override
        public void handleDistributedObjectEvent(java.lang.String name, java.lang.String serviceName, java.lang.String eventType, java.util.UUID source) {
            assertTrue(isEqual(aString, name));
            assertTrue(isEqual(aString, serviceName));
            assertTrue(isEqual(aString, eventType));
            assertTrue(isEqual(aUUID, source));
        }
    }

    @Test
    public void test_ClientAddDistributedObjectListenerCodec_handleDistributedObjectEvent() {
        int fileClientMessageIndex = 23;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientAddDistributedObjectListenerCodecHandler handler = new ClientAddDistributedObjectListenerCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_ClientRemoveDistributedObjectListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 24;
        ClientMessage encoded = ClientRemoveDistributedObjectListenerCodec.encodeRequest(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientRemoveDistributedObjectListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 25;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ClientRemoveDistributedObjectListenerCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ClientPingCodec_encodeRequest() {
        int fileClientMessageIndex = 26;
        ClientMessage encoded = ClientPingCodec.encodeRequest();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientPingCodec_decodeResponse() {
        int fileClientMessageIndex = 27;
    }

    @Test
    public void test_ClientStatisticsCodec_encodeRequest() {
        int fileClientMessageIndex = 28;
        ClientMessage encoded = ClientStatisticsCodec.encodeRequest(aLong, aString, aByteArray);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientStatisticsCodec_decodeResponse() {
        int fileClientMessageIndex = 29;
    }

    @Test
    public void test_ClientDeployClassesCodec_encodeRequest() {
        int fileClientMessageIndex = 30;
        ClientMessage encoded = ClientDeployClassesCodec.encodeRequest(aListOfStringToByteArray);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientDeployClassesCodec_decodeResponse() {
        int fileClientMessageIndex = 31;
    }

    @Test
    public void test_ClientCreateProxiesCodec_encodeRequest() {
        int fileClientMessageIndex = 32;
        ClientMessage encoded = ClientCreateProxiesCodec.encodeRequest(aListOfStringToString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientCreateProxiesCodec_decodeResponse() {
        int fileClientMessageIndex = 33;
    }

    @Test
    public void test_ClientLocalBackupListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 34;
        ClientMessage encoded = ClientLocalBackupListenerCodec.encodeRequest();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientLocalBackupListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 35;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, ClientLocalBackupListenerCodec.decodeResponse(fromFile)));
    }

    private static class ClientLocalBackupListenerCodecHandler extends ClientLocalBackupListenerCodec.AbstractEventHandler {
        @Override
        public void handleBackupEvent(long sourceInvocationCorrelationId) {
            assertTrue(isEqual(aLong, sourceInvocationCorrelationId));
        }
    }

    @Test
    public void test_ClientLocalBackupListenerCodec_handleBackupEvent() {
        int fileClientMessageIndex = 36;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientLocalBackupListenerCodecHandler handler = new ClientLocalBackupListenerCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_ClientTriggerPartitionAssignmentCodec_encodeRequest() {
        int fileClientMessageIndex = 37;
        ClientMessage encoded = ClientTriggerPartitionAssignmentCodec.encodeRequest();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientTriggerPartitionAssignmentCodec_decodeResponse() {
        int fileClientMessageIndex = 38;
    }

    @Test
    public void test_ClientAddMigrationListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 39;
        ClientMessage encoded = ClientAddMigrationListenerCodec.encodeRequest(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientAddMigrationListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 40;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, ClientAddMigrationListenerCodec.decodeResponse(fromFile)));
    }

    private static class ClientAddMigrationListenerCodecHandler extends ClientAddMigrationListenerCodec.AbstractEventHandler {
        @Override
        public void handleMigrationEvent(com.hazelcast.partition.MigrationState migrationState, int type) {
            assertTrue(isEqual(aMigrationState, migrationState));
            assertTrue(isEqual(anInt, type));
        }
        @Override
        public void handleReplicaMigrationEvent(com.hazelcast.partition.MigrationState migrationState, int partitionId, int replicaIndex, java.util.UUID sourceUuid, java.util.UUID destUuid, boolean success, long elapsedTime) {
            assertTrue(isEqual(aMigrationState, migrationState));
            assertTrue(isEqual(anInt, partitionId));
            assertTrue(isEqual(anInt, replicaIndex));
            assertTrue(isEqual(null, sourceUuid));
            assertTrue(isEqual(null, destUuid));
            assertTrue(isEqual(aBoolean, success));
            assertTrue(isEqual(aLong, elapsedTime));
        }
    }

    @Test
    public void test_ClientAddMigrationListenerCodec_handleMigrationEvent() {
        int fileClientMessageIndex = 41;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientAddMigrationListenerCodecHandler handler = new ClientAddMigrationListenerCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_ClientAddMigrationListenerCodec_handleReplicaMigrationEvent() {
        int fileClientMessageIndex = 42;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientAddMigrationListenerCodecHandler handler = new ClientAddMigrationListenerCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_ClientRemoveMigrationListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 43;
        ClientMessage encoded = ClientRemoveMigrationListenerCodec.encodeRequest(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientRemoveMigrationListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 44;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ClientRemoveMigrationListenerCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ClientSendSchemaCodec_encodeRequest() {
        int fileClientMessageIndex = 45;
        ClientMessage encoded = ClientSendSchemaCodec.encodeRequest(aSchema);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientSendSchemaCodec_decodeResponse() {
        int fileClientMessageIndex = 46;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aSetOfUUIDs, ClientSendSchemaCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ClientFetchSchemaCodec_encodeRequest() {
        int fileClientMessageIndex = 47;
        ClientMessage encoded = ClientFetchSchemaCodec.encodeRequest(aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientFetchSchemaCodec_decodeResponse() {
        int fileClientMessageIndex = 48;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, ClientFetchSchemaCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ClientSendAllSchemasCodec_encodeRequest() {
        int fileClientMessageIndex = 49;
        ClientMessage encoded = ClientSendAllSchemasCodec.encodeRequest(aListOfSchemas);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientSendAllSchemasCodec_decodeResponse() {
        int fileClientMessageIndex = 50;
    }

    @Test
    public void test_ClientTpcAuthenticationCodec_encodeRequest() {
        int fileClientMessageIndex = 51;
        ClientMessage encoded = ClientTpcAuthenticationCodec.encodeRequest(aUUID, aByteArray);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientTpcAuthenticationCodec_decodeResponse() {
        int fileClientMessageIndex = 52;
    }

    @Test
    public void test_ClientAddCPGroupViewListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 53;
        ClientMessage encoded = ClientAddCPGroupViewListenerCodec.encodeRequest();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientAddCPGroupViewListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 54;
    }

    private static class ClientAddCPGroupViewListenerCodecHandler extends ClientAddCPGroupViewListenerCodec.AbstractEventHandler {
        @Override
        public void handleGroupsViewEvent(long version, java.util.Collection<com.hazelcast.cp.internal.RaftGroupInfo> groupsInfo, java.util.Collection<java.util.Map.Entry<java.util.UUID, java.util.UUID>> cpToApUuids) {
            assertTrue(isEqual(aLong, version));
            assertTrue(isEqual(aListOfRaftGroupInfo, groupsInfo));
            assertTrue(isEqual(aListOfUUIDToUUID, cpToApUuids));
        }
    }

    @Test
    public void test_ClientAddCPGroupViewListenerCodec_handleGroupsViewEvent() {
        int fileClientMessageIndex = 55;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientAddCPGroupViewListenerCodecHandler handler = new ClientAddCPGroupViewListenerCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_MapPutCodec_encodeRequest() {
        int fileClientMessageIndex = 56;
        ClientMessage encoded = MapPutCodec.encodeRequest(aString, aData, aData, aLong, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapPutCodec_decodeResponse() {
        int fileClientMessageIndex = 57;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, MapPutCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapGetCodec_encodeRequest() {
        int fileClientMessageIndex = 58;
        ClientMessage encoded = MapGetCodec.encodeRequest(aString, aData, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapGetCodec_decodeResponse() {
        int fileClientMessageIndex = 59;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, MapGetCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapRemoveCodec_encodeRequest() {
        int fileClientMessageIndex = 60;
        ClientMessage encoded = MapRemoveCodec.encodeRequest(aString, aData, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapRemoveCodec_decodeResponse() {
        int fileClientMessageIndex = 61;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, MapRemoveCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapReplaceCodec_encodeRequest() {
        int fileClientMessageIndex = 62;
        ClientMessage encoded = MapReplaceCodec.encodeRequest(aString, aData, aData, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapReplaceCodec_decodeResponse() {
        int fileClientMessageIndex = 63;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, MapReplaceCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapReplaceIfSameCodec_encodeRequest() {
        int fileClientMessageIndex = 64;
        ClientMessage encoded = MapReplaceIfSameCodec.encodeRequest(aString, aData, aData, aData, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapReplaceIfSameCodec_decodeResponse() {
        int fileClientMessageIndex = 65;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, MapReplaceIfSameCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapContainsKeyCodec_encodeRequest() {
        int fileClientMessageIndex = 66;
        ClientMessage encoded = MapContainsKeyCodec.encodeRequest(aString, aData, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapContainsKeyCodec_decodeResponse() {
        int fileClientMessageIndex = 67;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, MapContainsKeyCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapContainsValueCodec_encodeRequest() {
        int fileClientMessageIndex = 68;
        ClientMessage encoded = MapContainsValueCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapContainsValueCodec_decodeResponse() {
        int fileClientMessageIndex = 69;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, MapContainsValueCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapRemoveIfSameCodec_encodeRequest() {
        int fileClientMessageIndex = 70;
        ClientMessage encoded = MapRemoveIfSameCodec.encodeRequest(aString, aData, aData, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapRemoveIfSameCodec_decodeResponse() {
        int fileClientMessageIndex = 71;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, MapRemoveIfSameCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapDeleteCodec_encodeRequest() {
        int fileClientMessageIndex = 72;
        ClientMessage encoded = MapDeleteCodec.encodeRequest(aString, aData, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapDeleteCodec_decodeResponse() {
        int fileClientMessageIndex = 73;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapDeleteCodec.ResponseParameters parameters = MapDeleteCodec.decodeResponse(fromFile);
        assertTrue(parameters.isResponseExists);
        assertTrue(isEqual(aBoolean, parameters.response));
    }

    @Test
    public void test_MapFlushCodec_encodeRequest() {
        int fileClientMessageIndex = 74;
        ClientMessage encoded = MapFlushCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapFlushCodec_decodeResponse() {
        int fileClientMessageIndex = 75;
    }

    @Test
    public void test_MapTryRemoveCodec_encodeRequest() {
        int fileClientMessageIndex = 76;
        ClientMessage encoded = MapTryRemoveCodec.encodeRequest(aString, aData, aLong, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapTryRemoveCodec_decodeResponse() {
        int fileClientMessageIndex = 77;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, MapTryRemoveCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapTryPutCodec_encodeRequest() {
        int fileClientMessageIndex = 78;
        ClientMessage encoded = MapTryPutCodec.encodeRequest(aString, aData, aData, aLong, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapTryPutCodec_decodeResponse() {
        int fileClientMessageIndex = 79;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, MapTryPutCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapPutTransientCodec_encodeRequest() {
        int fileClientMessageIndex = 80;
        ClientMessage encoded = MapPutTransientCodec.encodeRequest(aString, aData, aData, aLong, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapPutTransientCodec_decodeResponse() {
        int fileClientMessageIndex = 81;
    }

    @Test
    public void test_MapPutIfAbsentCodec_encodeRequest() {
        int fileClientMessageIndex = 82;
        ClientMessage encoded = MapPutIfAbsentCodec.encodeRequest(aString, aData, aData, aLong, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapPutIfAbsentCodec_decodeResponse() {
        int fileClientMessageIndex = 83;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, MapPutIfAbsentCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapSetCodec_encodeRequest() {
        int fileClientMessageIndex = 84;
        ClientMessage encoded = MapSetCodec.encodeRequest(aString, aData, aData, aLong, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapSetCodec_decodeResponse() {
        int fileClientMessageIndex = 85;
    }

    @Test
    public void test_MapLockCodec_encodeRequest() {
        int fileClientMessageIndex = 86;
        ClientMessage encoded = MapLockCodec.encodeRequest(aString, aData, aLong, aLong, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapLockCodec_decodeResponse() {
        int fileClientMessageIndex = 87;
    }

    @Test
    public void test_MapTryLockCodec_encodeRequest() {
        int fileClientMessageIndex = 88;
        ClientMessage encoded = MapTryLockCodec.encodeRequest(aString, aData, aLong, aLong, aLong, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapTryLockCodec_decodeResponse() {
        int fileClientMessageIndex = 89;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, MapTryLockCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapIsLockedCodec_encodeRequest() {
        int fileClientMessageIndex = 90;
        ClientMessage encoded = MapIsLockedCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapIsLockedCodec_decodeResponse() {
        int fileClientMessageIndex = 91;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, MapIsLockedCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapUnlockCodec_encodeRequest() {
        int fileClientMessageIndex = 92;
        ClientMessage encoded = MapUnlockCodec.encodeRequest(aString, aData, aLong, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapUnlockCodec_decodeResponse() {
        int fileClientMessageIndex = 93;
    }

    @Test
    public void test_MapAddInterceptorCodec_encodeRequest() {
        int fileClientMessageIndex = 94;
        ClientMessage encoded = MapAddInterceptorCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapAddInterceptorCodec_decodeResponse() {
        int fileClientMessageIndex = 95;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, MapAddInterceptorCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapRemoveInterceptorCodec_encodeRequest() {
        int fileClientMessageIndex = 96;
        ClientMessage encoded = MapRemoveInterceptorCodec.encodeRequest(aString, aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapRemoveInterceptorCodec_decodeResponse() {
        int fileClientMessageIndex = 97;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, MapRemoveInterceptorCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapAddEntryListenerToKeyWithPredicateCodec_encodeRequest() {
        int fileClientMessageIndex = 98;
        ClientMessage encoded = MapAddEntryListenerToKeyWithPredicateCodec.encodeRequest(aString, aData, aData, aBoolean, anInt, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapAddEntryListenerToKeyWithPredicateCodec_decodeResponse() {
        int fileClientMessageIndex = 99;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, MapAddEntryListenerToKeyWithPredicateCodec.decodeResponse(fromFile)));
    }

    private static class MapAddEntryListenerToKeyWithPredicateCodecHandler extends MapAddEntryListenerToKeyWithPredicateCodec.AbstractEventHandler {
        @Override
        public void handleEntryEvent(com.hazelcast.internal.serialization.Data key, com.hazelcast.internal.serialization.Data value, com.hazelcast.internal.serialization.Data oldValue, com.hazelcast.internal.serialization.Data mergingValue, int eventType, java.util.UUID uuid, int numberOfAffectedEntries) {
            assertTrue(isEqual(null, key));
            assertTrue(isEqual(null, value));
            assertTrue(isEqual(null, oldValue));
            assertTrue(isEqual(null, mergingValue));
            assertTrue(isEqual(anInt, eventType));
            assertTrue(isEqual(aUUID, uuid));
            assertTrue(isEqual(anInt, numberOfAffectedEntries));
        }
    }

    @Test
    public void test_MapAddEntryListenerToKeyWithPredicateCodec_handleEntryEvent() {
        int fileClientMessageIndex = 100;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapAddEntryListenerToKeyWithPredicateCodecHandler handler = new MapAddEntryListenerToKeyWithPredicateCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_MapAddEntryListenerWithPredicateCodec_encodeRequest() {
        int fileClientMessageIndex = 101;
        ClientMessage encoded = MapAddEntryListenerWithPredicateCodec.encodeRequest(aString, aData, aBoolean, anInt, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapAddEntryListenerWithPredicateCodec_decodeResponse() {
        int fileClientMessageIndex = 102;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, MapAddEntryListenerWithPredicateCodec.decodeResponse(fromFile)));
    }

    private static class MapAddEntryListenerWithPredicateCodecHandler extends MapAddEntryListenerWithPredicateCodec.AbstractEventHandler {
        @Override
        public void handleEntryEvent(com.hazelcast.internal.serialization.Data key, com.hazelcast.internal.serialization.Data value, com.hazelcast.internal.serialization.Data oldValue, com.hazelcast.internal.serialization.Data mergingValue, int eventType, java.util.UUID uuid, int numberOfAffectedEntries) {
            assertTrue(isEqual(null, key));
            assertTrue(isEqual(null, value));
            assertTrue(isEqual(null, oldValue));
            assertTrue(isEqual(null, mergingValue));
            assertTrue(isEqual(anInt, eventType));
            assertTrue(isEqual(aUUID, uuid));
            assertTrue(isEqual(anInt, numberOfAffectedEntries));
        }
    }

    @Test
    public void test_MapAddEntryListenerWithPredicateCodec_handleEntryEvent() {
        int fileClientMessageIndex = 103;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapAddEntryListenerWithPredicateCodecHandler handler = new MapAddEntryListenerWithPredicateCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_MapAddEntryListenerToKeyCodec_encodeRequest() {
        int fileClientMessageIndex = 104;
        ClientMessage encoded = MapAddEntryListenerToKeyCodec.encodeRequest(aString, aData, aBoolean, anInt, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapAddEntryListenerToKeyCodec_decodeResponse() {
        int fileClientMessageIndex = 105;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, MapAddEntryListenerToKeyCodec.decodeResponse(fromFile)));
    }

    private static class MapAddEntryListenerToKeyCodecHandler extends MapAddEntryListenerToKeyCodec.AbstractEventHandler {
        @Override
        public void handleEntryEvent(com.hazelcast.internal.serialization.Data key, com.hazelcast.internal.serialization.Data value, com.hazelcast.internal.serialization.Data oldValue, com.hazelcast.internal.serialization.Data mergingValue, int eventType, java.util.UUID uuid, int numberOfAffectedEntries) {
            assertTrue(isEqual(null, key));
            assertTrue(isEqual(null, value));
            assertTrue(isEqual(null, oldValue));
            assertTrue(isEqual(null, mergingValue));
            assertTrue(isEqual(anInt, eventType));
            assertTrue(isEqual(aUUID, uuid));
            assertTrue(isEqual(anInt, numberOfAffectedEntries));
        }
    }

    @Test
    public void test_MapAddEntryListenerToKeyCodec_handleEntryEvent() {
        int fileClientMessageIndex = 106;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapAddEntryListenerToKeyCodecHandler handler = new MapAddEntryListenerToKeyCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_MapAddEntryListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 107;
        ClientMessage encoded = MapAddEntryListenerCodec.encodeRequest(aString, aBoolean, anInt, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapAddEntryListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 108;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, MapAddEntryListenerCodec.decodeResponse(fromFile)));
    }

    private static class MapAddEntryListenerCodecHandler extends MapAddEntryListenerCodec.AbstractEventHandler {
        @Override
        public void handleEntryEvent(com.hazelcast.internal.serialization.Data key, com.hazelcast.internal.serialization.Data value, com.hazelcast.internal.serialization.Data oldValue, com.hazelcast.internal.serialization.Data mergingValue, int eventType, java.util.UUID uuid, int numberOfAffectedEntries) {
            assertTrue(isEqual(null, key));
            assertTrue(isEqual(null, value));
            assertTrue(isEqual(null, oldValue));
            assertTrue(isEqual(null, mergingValue));
            assertTrue(isEqual(anInt, eventType));
            assertTrue(isEqual(aUUID, uuid));
            assertTrue(isEqual(anInt, numberOfAffectedEntries));
        }
    }

    @Test
    public void test_MapAddEntryListenerCodec_handleEntryEvent() {
        int fileClientMessageIndex = 109;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapAddEntryListenerCodecHandler handler = new MapAddEntryListenerCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_MapRemoveEntryListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 110;
        ClientMessage encoded = MapRemoveEntryListenerCodec.encodeRequest(aString, aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapRemoveEntryListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 111;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, MapRemoveEntryListenerCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapAddPartitionLostListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 112;
        ClientMessage encoded = MapAddPartitionLostListenerCodec.encodeRequest(aString, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapAddPartitionLostListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 113;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, MapAddPartitionLostListenerCodec.decodeResponse(fromFile)));
    }

    private static class MapAddPartitionLostListenerCodecHandler extends MapAddPartitionLostListenerCodec.AbstractEventHandler {
        @Override
        public void handleMapPartitionLostEvent(int partitionId, java.util.UUID uuid) {
            assertTrue(isEqual(anInt, partitionId));
            assertTrue(isEqual(aUUID, uuid));
        }
    }

    @Test
    public void test_MapAddPartitionLostListenerCodec_handleMapPartitionLostEvent() {
        int fileClientMessageIndex = 114;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapAddPartitionLostListenerCodecHandler handler = new MapAddPartitionLostListenerCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_MapRemovePartitionLostListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 115;
        ClientMessage encoded = MapRemovePartitionLostListenerCodec.encodeRequest(aString, aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapRemovePartitionLostListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 116;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, MapRemovePartitionLostListenerCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapGetEntryViewCodec_encodeRequest() {
        int fileClientMessageIndex = 117;
        ClientMessage encoded = MapGetEntryViewCodec.encodeRequest(aString, aData, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapGetEntryViewCodec_decodeResponse() {
        int fileClientMessageIndex = 118;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapGetEntryViewCodec.ResponseParameters parameters = MapGetEntryViewCodec.decodeResponse(fromFile);
        assertTrue(isEqual(null, parameters.response));
        assertTrue(isEqual(aLong, parameters.maxIdle));
    }

    @Test
    public void test_MapEvictCodec_encodeRequest() {
        int fileClientMessageIndex = 119;
        ClientMessage encoded = MapEvictCodec.encodeRequest(aString, aData, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapEvictCodec_decodeResponse() {
        int fileClientMessageIndex = 120;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, MapEvictCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapEvictAllCodec_encodeRequest() {
        int fileClientMessageIndex = 121;
        ClientMessage encoded = MapEvictAllCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapEvictAllCodec_decodeResponse() {
        int fileClientMessageIndex = 122;
    }

    @Test
    public void test_MapLoadAllCodec_encodeRequest() {
        int fileClientMessageIndex = 123;
        ClientMessage encoded = MapLoadAllCodec.encodeRequest(aString, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapLoadAllCodec_decodeResponse() {
        int fileClientMessageIndex = 124;
    }

    @Test
    public void test_MapLoadGivenKeysCodec_encodeRequest() {
        int fileClientMessageIndex = 125;
        ClientMessage encoded = MapLoadGivenKeysCodec.encodeRequest(aString, aListOfData, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapLoadGivenKeysCodec_decodeResponse() {
        int fileClientMessageIndex = 126;
    }

    @Test
    public void test_MapKeySetCodec_encodeRequest() {
        int fileClientMessageIndex = 127;
        ClientMessage encoded = MapKeySetCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapKeySetCodec_decodeResponse() {
        int fileClientMessageIndex = 128;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfData, MapKeySetCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapGetAllCodec_encodeRequest() {
        int fileClientMessageIndex = 129;
        ClientMessage encoded = MapGetAllCodec.encodeRequest(aString, aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapGetAllCodec_decodeResponse() {
        int fileClientMessageIndex = 130;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfDataToData, MapGetAllCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapValuesCodec_encodeRequest() {
        int fileClientMessageIndex = 131;
        ClientMessage encoded = MapValuesCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapValuesCodec_decodeResponse() {
        int fileClientMessageIndex = 132;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfData, MapValuesCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapEntrySetCodec_encodeRequest() {
        int fileClientMessageIndex = 133;
        ClientMessage encoded = MapEntrySetCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapEntrySetCodec_decodeResponse() {
        int fileClientMessageIndex = 134;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfDataToData, MapEntrySetCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapKeySetWithPredicateCodec_encodeRequest() {
        int fileClientMessageIndex = 135;
        ClientMessage encoded = MapKeySetWithPredicateCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapKeySetWithPredicateCodec_decodeResponse() {
        int fileClientMessageIndex = 136;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfData, MapKeySetWithPredicateCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapValuesWithPredicateCodec_encodeRequest() {
        int fileClientMessageIndex = 137;
        ClientMessage encoded = MapValuesWithPredicateCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapValuesWithPredicateCodec_decodeResponse() {
        int fileClientMessageIndex = 138;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfData, MapValuesWithPredicateCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapEntriesWithPredicateCodec_encodeRequest() {
        int fileClientMessageIndex = 139;
        ClientMessage encoded = MapEntriesWithPredicateCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapEntriesWithPredicateCodec_decodeResponse() {
        int fileClientMessageIndex = 140;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfDataToData, MapEntriesWithPredicateCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapAddIndexCodec_encodeRequest() {
        int fileClientMessageIndex = 141;
        ClientMessage encoded = MapAddIndexCodec.encodeRequest(aString, anIndexConfig);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapAddIndexCodec_decodeResponse() {
        int fileClientMessageIndex = 142;
    }

    @Test
    public void test_MapSizeCodec_encodeRequest() {
        int fileClientMessageIndex = 143;
        ClientMessage encoded = MapSizeCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapSizeCodec_decodeResponse() {
        int fileClientMessageIndex = 144;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(anInt, MapSizeCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapIsEmptyCodec_encodeRequest() {
        int fileClientMessageIndex = 145;
        ClientMessage encoded = MapIsEmptyCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapIsEmptyCodec_decodeResponse() {
        int fileClientMessageIndex = 146;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, MapIsEmptyCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapPutAllCodec_encodeRequest() {
        int fileClientMessageIndex = 147;
        ClientMessage encoded = MapPutAllCodec.encodeRequest(aString, aListOfDataToData, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapPutAllCodec_decodeResponse() {
        int fileClientMessageIndex = 148;
    }

    @Test
    public void test_MapClearCodec_encodeRequest() {
        int fileClientMessageIndex = 149;
        ClientMessage encoded = MapClearCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapClearCodec_decodeResponse() {
        int fileClientMessageIndex = 150;
    }

    @Test
    public void test_MapExecuteOnKeyCodec_encodeRequest() {
        int fileClientMessageIndex = 151;
        ClientMessage encoded = MapExecuteOnKeyCodec.encodeRequest(aString, aData, aData, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapExecuteOnKeyCodec_decodeResponse() {
        int fileClientMessageIndex = 152;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, MapExecuteOnKeyCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapSubmitToKeyCodec_encodeRequest() {
        int fileClientMessageIndex = 153;
        ClientMessage encoded = MapSubmitToKeyCodec.encodeRequest(aString, aData, aData, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapSubmitToKeyCodec_decodeResponse() {
        int fileClientMessageIndex = 154;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, MapSubmitToKeyCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapExecuteOnAllKeysCodec_encodeRequest() {
        int fileClientMessageIndex = 155;
        ClientMessage encoded = MapExecuteOnAllKeysCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapExecuteOnAllKeysCodec_decodeResponse() {
        int fileClientMessageIndex = 156;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfDataToData, MapExecuteOnAllKeysCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapExecuteWithPredicateCodec_encodeRequest() {
        int fileClientMessageIndex = 157;
        ClientMessage encoded = MapExecuteWithPredicateCodec.encodeRequest(aString, aData, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapExecuteWithPredicateCodec_decodeResponse() {
        int fileClientMessageIndex = 158;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfDataToData, MapExecuteWithPredicateCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapExecuteOnKeysCodec_encodeRequest() {
        int fileClientMessageIndex = 159;
        ClientMessage encoded = MapExecuteOnKeysCodec.encodeRequest(aString, aData, aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapExecuteOnKeysCodec_decodeResponse() {
        int fileClientMessageIndex = 160;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfDataToData, MapExecuteOnKeysCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapForceUnlockCodec_encodeRequest() {
        int fileClientMessageIndex = 161;
        ClientMessage encoded = MapForceUnlockCodec.encodeRequest(aString, aData, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapForceUnlockCodec_decodeResponse() {
        int fileClientMessageIndex = 162;
    }

    @Test
    public void test_MapKeySetWithPagingPredicateCodec_encodeRequest() {
        int fileClientMessageIndex = 163;
        ClientMessage encoded = MapKeySetWithPagingPredicateCodec.encodeRequest(aString, aPagingPredicateHolder);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapKeySetWithPagingPredicateCodec_decodeResponse() {
        int fileClientMessageIndex = 164;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapKeySetWithPagingPredicateCodec.ResponseParameters parameters = MapKeySetWithPagingPredicateCodec.decodeResponse(fromFile);
        assertTrue(isEqual(aListOfData, parameters.response));
        assertTrue(isEqual(anAnchorDataListHolder, parameters.anchorDataList));
    }

    @Test
    public void test_MapValuesWithPagingPredicateCodec_encodeRequest() {
        int fileClientMessageIndex = 165;
        ClientMessage encoded = MapValuesWithPagingPredicateCodec.encodeRequest(aString, aPagingPredicateHolder);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapValuesWithPagingPredicateCodec_decodeResponse() {
        int fileClientMessageIndex = 166;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapValuesWithPagingPredicateCodec.ResponseParameters parameters = MapValuesWithPagingPredicateCodec.decodeResponse(fromFile);
        assertTrue(isEqual(aListOfData, parameters.response));
        assertTrue(isEqual(anAnchorDataListHolder, parameters.anchorDataList));
    }

    @Test
    public void test_MapEntriesWithPagingPredicateCodec_encodeRequest() {
        int fileClientMessageIndex = 167;
        ClientMessage encoded = MapEntriesWithPagingPredicateCodec.encodeRequest(aString, aPagingPredicateHolder);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapEntriesWithPagingPredicateCodec_decodeResponse() {
        int fileClientMessageIndex = 168;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapEntriesWithPagingPredicateCodec.ResponseParameters parameters = MapEntriesWithPagingPredicateCodec.decodeResponse(fromFile);
        assertTrue(isEqual(aListOfDataToData, parameters.response));
        assertTrue(isEqual(anAnchorDataListHolder, parameters.anchorDataList));
    }

    @Test
    public void test_MapFetchKeysCodec_encodeRequest() {
        int fileClientMessageIndex = 169;
        ClientMessage encoded = MapFetchKeysCodec.encodeRequest(aString, aListOfIntegerToInteger, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapFetchKeysCodec_decodeResponse() {
        int fileClientMessageIndex = 170;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapFetchKeysCodec.ResponseParameters parameters = MapFetchKeysCodec.decodeResponse(fromFile);
        assertTrue(isEqual(aListOfIntegerToInteger, parameters.iterationPointers));
        assertTrue(isEqual(aListOfData, parameters.keys));
    }

    @Test
    public void test_MapFetchEntriesCodec_encodeRequest() {
        int fileClientMessageIndex = 171;
        ClientMessage encoded = MapFetchEntriesCodec.encodeRequest(aString, aListOfIntegerToInteger, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapFetchEntriesCodec_decodeResponse() {
        int fileClientMessageIndex = 172;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapFetchEntriesCodec.ResponseParameters parameters = MapFetchEntriesCodec.decodeResponse(fromFile);
        assertTrue(isEqual(aListOfIntegerToInteger, parameters.iterationPointers));
        assertTrue(isEqual(aListOfDataToData, parameters.entries));
    }

    @Test
    public void test_MapAggregateCodec_encodeRequest() {
        int fileClientMessageIndex = 173;
        ClientMessage encoded = MapAggregateCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapAggregateCodec_decodeResponse() {
        int fileClientMessageIndex = 174;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, MapAggregateCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapAggregateWithPredicateCodec_encodeRequest() {
        int fileClientMessageIndex = 175;
        ClientMessage encoded = MapAggregateWithPredicateCodec.encodeRequest(aString, aData, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapAggregateWithPredicateCodec_decodeResponse() {
        int fileClientMessageIndex = 176;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, MapAggregateWithPredicateCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapProjectCodec_encodeRequest() {
        int fileClientMessageIndex = 177;
        ClientMessage encoded = MapProjectCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapProjectCodec_decodeResponse() {
        int fileClientMessageIndex = 178;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfData, MapProjectCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapProjectWithPredicateCodec_encodeRequest() {
        int fileClientMessageIndex = 179;
        ClientMessage encoded = MapProjectWithPredicateCodec.encodeRequest(aString, aData, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapProjectWithPredicateCodec_decodeResponse() {
        int fileClientMessageIndex = 180;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfData, MapProjectWithPredicateCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapFetchNearCacheInvalidationMetadataCodec_encodeRequest() {
        int fileClientMessageIndex = 181;
        ClientMessage encoded = MapFetchNearCacheInvalidationMetadataCodec.encodeRequest(aListOfStrings, aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapFetchNearCacheInvalidationMetadataCodec_decodeResponse() {
        int fileClientMessageIndex = 182;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapFetchNearCacheInvalidationMetadataCodec.ResponseParameters parameters = MapFetchNearCacheInvalidationMetadataCodec.decodeResponse(fromFile);
        assertTrue(isEqual(aListOfStringToListOfIntegerToLong, parameters.namePartitionSequenceList));
        assertTrue(isEqual(aListOfIntegerToUUID, parameters.partitionUuidList));
    }

    @Test
    public void test_MapRemoveAllCodec_encodeRequest() {
        int fileClientMessageIndex = 183;
        ClientMessage encoded = MapRemoveAllCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapRemoveAllCodec_decodeResponse() {
        int fileClientMessageIndex = 184;
    }

    @Test
    public void test_MapAddNearCacheInvalidationListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 185;
        ClientMessage encoded = MapAddNearCacheInvalidationListenerCodec.encodeRequest(aString, anInt, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapAddNearCacheInvalidationListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 186;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, MapAddNearCacheInvalidationListenerCodec.decodeResponse(fromFile)));
    }

    private static class MapAddNearCacheInvalidationListenerCodecHandler extends MapAddNearCacheInvalidationListenerCodec.AbstractEventHandler {
        @Override
        public void handleIMapInvalidationEvent(com.hazelcast.internal.serialization.Data key, java.util.UUID sourceUuid, java.util.UUID partitionUuid, long sequence) {
            assertTrue(isEqual(null, key));
            assertTrue(isEqual(aUUID, sourceUuid));
            assertTrue(isEqual(aUUID, partitionUuid));
            assertTrue(isEqual(aLong, sequence));
        }
        @Override
        public void handleIMapBatchInvalidationEvent(java.util.Collection<com.hazelcast.internal.serialization.Data> keys, java.util.Collection<java.util.UUID> sourceUuids, java.util.Collection<java.util.UUID> partitionUuids, java.util.Collection<java.lang.Long> sequences) {
            assertTrue(isEqual(aListOfData, keys));
            assertTrue(isEqual(aListOfUUIDs, sourceUuids));
            assertTrue(isEqual(aListOfUUIDs, partitionUuids));
            assertTrue(isEqual(aListOfLongs, sequences));
        }
    }

    @Test
    public void test_MapAddNearCacheInvalidationListenerCodec_handleIMapInvalidationEvent() {
        int fileClientMessageIndex = 187;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapAddNearCacheInvalidationListenerCodecHandler handler = new MapAddNearCacheInvalidationListenerCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_MapAddNearCacheInvalidationListenerCodec_handleIMapBatchInvalidationEvent() {
        int fileClientMessageIndex = 188;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapAddNearCacheInvalidationListenerCodecHandler handler = new MapAddNearCacheInvalidationListenerCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_MapFetchWithQueryCodec_encodeRequest() {
        int fileClientMessageIndex = 189;
        ClientMessage encoded = MapFetchWithQueryCodec.encodeRequest(aString, aListOfIntegerToInteger, anInt, aData, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapFetchWithQueryCodec_decodeResponse() {
        int fileClientMessageIndex = 190;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapFetchWithQueryCodec.ResponseParameters parameters = MapFetchWithQueryCodec.decodeResponse(fromFile);
        assertTrue(isEqual(aListOfData, parameters.results));
        assertTrue(isEqual(aListOfIntegerToInteger, parameters.iterationPointers));
    }

    @Test
    public void test_MapEventJournalSubscribeCodec_encodeRequest() {
        int fileClientMessageIndex = 191;
        ClientMessage encoded = MapEventJournalSubscribeCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapEventJournalSubscribeCodec_decodeResponse() {
        int fileClientMessageIndex = 192;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapEventJournalSubscribeCodec.ResponseParameters parameters = MapEventJournalSubscribeCodec.decodeResponse(fromFile);
        assertTrue(isEqual(aLong, parameters.oldestSequence));
        assertTrue(isEqual(aLong, parameters.newestSequence));
    }

    @Test
    public void test_MapEventJournalReadCodec_encodeRequest() {
        int fileClientMessageIndex = 193;
        ClientMessage encoded = MapEventJournalReadCodec.encodeRequest(aString, aLong, anInt, anInt, null, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapEventJournalReadCodec_decodeResponse() {
        int fileClientMessageIndex = 194;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapEventJournalReadCodec.ResponseParameters parameters = MapEventJournalReadCodec.decodeResponse(fromFile);
        assertTrue(isEqual(anInt, parameters.readCount));
        assertTrue(isEqual(aListOfData, parameters.items));
        assertTrue(isEqual(null, parameters.itemSeqs));
        assertTrue(isEqual(aLong, parameters.nextSeq));
    }

    @Test
    public void test_MapSetTtlCodec_encodeRequest() {
        int fileClientMessageIndex = 195;
        ClientMessage encoded = MapSetTtlCodec.encodeRequest(aString, aData, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapSetTtlCodec_decodeResponse() {
        int fileClientMessageIndex = 196;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, MapSetTtlCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapPutWithMaxIdleCodec_encodeRequest() {
        int fileClientMessageIndex = 197;
        ClientMessage encoded = MapPutWithMaxIdleCodec.encodeRequest(aString, aData, aData, aLong, aLong, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapPutWithMaxIdleCodec_decodeResponse() {
        int fileClientMessageIndex = 198;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, MapPutWithMaxIdleCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapPutTransientWithMaxIdleCodec_encodeRequest() {
        int fileClientMessageIndex = 199;
        ClientMessage encoded = MapPutTransientWithMaxIdleCodec.encodeRequest(aString, aData, aData, aLong, aLong, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapPutTransientWithMaxIdleCodec_decodeResponse() {
        int fileClientMessageIndex = 200;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, MapPutTransientWithMaxIdleCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapPutIfAbsentWithMaxIdleCodec_encodeRequest() {
        int fileClientMessageIndex = 201;
        ClientMessage encoded = MapPutIfAbsentWithMaxIdleCodec.encodeRequest(aString, aData, aData, aLong, aLong, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapPutIfAbsentWithMaxIdleCodec_decodeResponse() {
        int fileClientMessageIndex = 202;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, MapPutIfAbsentWithMaxIdleCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapSetWithMaxIdleCodec_encodeRequest() {
        int fileClientMessageIndex = 203;
        ClientMessage encoded = MapSetWithMaxIdleCodec.encodeRequest(aString, aData, aData, aLong, aLong, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapSetWithMaxIdleCodec_decodeResponse() {
        int fileClientMessageIndex = 204;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, MapSetWithMaxIdleCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MapReplaceAllCodec_encodeRequest() {
        int fileClientMessageIndex = 205;
        ClientMessage encoded = MapReplaceAllCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapReplaceAllCodec_decodeResponse() {
        int fileClientMessageIndex = 206;
    }

    @Test
    public void test_MapPutAllWithMetadataCodec_encodeRequest() {
        int fileClientMessageIndex = 207;
        ClientMessage encoded = MapPutAllWithMetadataCodec.encodeRequest(aString, aListOfSimpleEntryViews);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapPutAllWithMetadataCodec_decodeResponse() {
        int fileClientMessageIndex = 208;
    }

    @Test
    public void test_MultiMapPutCodec_encodeRequest() {
        int fileClientMessageIndex = 209;
        ClientMessage encoded = MultiMapPutCodec.encodeRequest(aString, aData, aData, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapPutCodec_decodeResponse() {
        int fileClientMessageIndex = 210;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, MultiMapPutCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MultiMapGetCodec_encodeRequest() {
        int fileClientMessageIndex = 211;
        ClientMessage encoded = MultiMapGetCodec.encodeRequest(aString, aData, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapGetCodec_decodeResponse() {
        int fileClientMessageIndex = 212;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfData, MultiMapGetCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MultiMapRemoveCodec_encodeRequest() {
        int fileClientMessageIndex = 213;
        ClientMessage encoded = MultiMapRemoveCodec.encodeRequest(aString, aData, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapRemoveCodec_decodeResponse() {
        int fileClientMessageIndex = 214;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfData, MultiMapRemoveCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MultiMapKeySetCodec_encodeRequest() {
        int fileClientMessageIndex = 215;
        ClientMessage encoded = MultiMapKeySetCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapKeySetCodec_decodeResponse() {
        int fileClientMessageIndex = 216;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfData, MultiMapKeySetCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MultiMapValuesCodec_encodeRequest() {
        int fileClientMessageIndex = 217;
        ClientMessage encoded = MultiMapValuesCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapValuesCodec_decodeResponse() {
        int fileClientMessageIndex = 218;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfData, MultiMapValuesCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MultiMapEntrySetCodec_encodeRequest() {
        int fileClientMessageIndex = 219;
        ClientMessage encoded = MultiMapEntrySetCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapEntrySetCodec_decodeResponse() {
        int fileClientMessageIndex = 220;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfDataToData, MultiMapEntrySetCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MultiMapContainsKeyCodec_encodeRequest() {
        int fileClientMessageIndex = 221;
        ClientMessage encoded = MultiMapContainsKeyCodec.encodeRequest(aString, aData, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapContainsKeyCodec_decodeResponse() {
        int fileClientMessageIndex = 222;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, MultiMapContainsKeyCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MultiMapContainsValueCodec_encodeRequest() {
        int fileClientMessageIndex = 223;
        ClientMessage encoded = MultiMapContainsValueCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapContainsValueCodec_decodeResponse() {
        int fileClientMessageIndex = 224;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, MultiMapContainsValueCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MultiMapContainsEntryCodec_encodeRequest() {
        int fileClientMessageIndex = 225;
        ClientMessage encoded = MultiMapContainsEntryCodec.encodeRequest(aString, aData, aData, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapContainsEntryCodec_decodeResponse() {
        int fileClientMessageIndex = 226;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, MultiMapContainsEntryCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MultiMapSizeCodec_encodeRequest() {
        int fileClientMessageIndex = 227;
        ClientMessage encoded = MultiMapSizeCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapSizeCodec_decodeResponse() {
        int fileClientMessageIndex = 228;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(anInt, MultiMapSizeCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MultiMapClearCodec_encodeRequest() {
        int fileClientMessageIndex = 229;
        ClientMessage encoded = MultiMapClearCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapClearCodec_decodeResponse() {
        int fileClientMessageIndex = 230;
    }

    @Test
    public void test_MultiMapValueCountCodec_encodeRequest() {
        int fileClientMessageIndex = 231;
        ClientMessage encoded = MultiMapValueCountCodec.encodeRequest(aString, aData, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapValueCountCodec_decodeResponse() {
        int fileClientMessageIndex = 232;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(anInt, MultiMapValueCountCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MultiMapAddEntryListenerToKeyCodec_encodeRequest() {
        int fileClientMessageIndex = 233;
        ClientMessage encoded = MultiMapAddEntryListenerToKeyCodec.encodeRequest(aString, aData, aBoolean, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapAddEntryListenerToKeyCodec_decodeResponse() {
        int fileClientMessageIndex = 234;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, MultiMapAddEntryListenerToKeyCodec.decodeResponse(fromFile)));
    }

    private static class MultiMapAddEntryListenerToKeyCodecHandler extends MultiMapAddEntryListenerToKeyCodec.AbstractEventHandler {
        @Override
        public void handleEntryEvent(com.hazelcast.internal.serialization.Data key, com.hazelcast.internal.serialization.Data value, com.hazelcast.internal.serialization.Data oldValue, com.hazelcast.internal.serialization.Data mergingValue, int eventType, java.util.UUID uuid, int numberOfAffectedEntries) {
            assertTrue(isEqual(null, key));
            assertTrue(isEqual(null, value));
            assertTrue(isEqual(null, oldValue));
            assertTrue(isEqual(null, mergingValue));
            assertTrue(isEqual(anInt, eventType));
            assertTrue(isEqual(aUUID, uuid));
            assertTrue(isEqual(anInt, numberOfAffectedEntries));
        }
    }

    @Test
    public void test_MultiMapAddEntryListenerToKeyCodec_handleEntryEvent() {
        int fileClientMessageIndex = 235;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MultiMapAddEntryListenerToKeyCodecHandler handler = new MultiMapAddEntryListenerToKeyCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_MultiMapAddEntryListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 236;
        ClientMessage encoded = MultiMapAddEntryListenerCodec.encodeRequest(aString, aBoolean, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapAddEntryListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 237;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, MultiMapAddEntryListenerCodec.decodeResponse(fromFile)));
    }

    private static class MultiMapAddEntryListenerCodecHandler extends MultiMapAddEntryListenerCodec.AbstractEventHandler {
        @Override
        public void handleEntryEvent(com.hazelcast.internal.serialization.Data key, com.hazelcast.internal.serialization.Data value, com.hazelcast.internal.serialization.Data oldValue, com.hazelcast.internal.serialization.Data mergingValue, int eventType, java.util.UUID uuid, int numberOfAffectedEntries) {
            assertTrue(isEqual(null, key));
            assertTrue(isEqual(null, value));
            assertTrue(isEqual(null, oldValue));
            assertTrue(isEqual(null, mergingValue));
            assertTrue(isEqual(anInt, eventType));
            assertTrue(isEqual(aUUID, uuid));
            assertTrue(isEqual(anInt, numberOfAffectedEntries));
        }
    }

    @Test
    public void test_MultiMapAddEntryListenerCodec_handleEntryEvent() {
        int fileClientMessageIndex = 238;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MultiMapAddEntryListenerCodecHandler handler = new MultiMapAddEntryListenerCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_MultiMapRemoveEntryListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 239;
        ClientMessage encoded = MultiMapRemoveEntryListenerCodec.encodeRequest(aString, aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapRemoveEntryListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 240;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, MultiMapRemoveEntryListenerCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MultiMapLockCodec_encodeRequest() {
        int fileClientMessageIndex = 241;
        ClientMessage encoded = MultiMapLockCodec.encodeRequest(aString, aData, aLong, aLong, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapLockCodec_decodeResponse() {
        int fileClientMessageIndex = 242;
    }

    @Test
    public void test_MultiMapTryLockCodec_encodeRequest() {
        int fileClientMessageIndex = 243;
        ClientMessage encoded = MultiMapTryLockCodec.encodeRequest(aString, aData, aLong, aLong, aLong, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapTryLockCodec_decodeResponse() {
        int fileClientMessageIndex = 244;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, MultiMapTryLockCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MultiMapIsLockedCodec_encodeRequest() {
        int fileClientMessageIndex = 245;
        ClientMessage encoded = MultiMapIsLockedCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapIsLockedCodec_decodeResponse() {
        int fileClientMessageIndex = 246;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, MultiMapIsLockedCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MultiMapUnlockCodec_encodeRequest() {
        int fileClientMessageIndex = 247;
        ClientMessage encoded = MultiMapUnlockCodec.encodeRequest(aString, aData, aLong, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapUnlockCodec_decodeResponse() {
        int fileClientMessageIndex = 248;
    }

    @Test
    public void test_MultiMapForceUnlockCodec_encodeRequest() {
        int fileClientMessageIndex = 249;
        ClientMessage encoded = MultiMapForceUnlockCodec.encodeRequest(aString, aData, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapForceUnlockCodec_decodeResponse() {
        int fileClientMessageIndex = 250;
    }

    @Test
    public void test_MultiMapRemoveEntryCodec_encodeRequest() {
        int fileClientMessageIndex = 251;
        ClientMessage encoded = MultiMapRemoveEntryCodec.encodeRequest(aString, aData, aData, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapRemoveEntryCodec_decodeResponse() {
        int fileClientMessageIndex = 252;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, MultiMapRemoveEntryCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MultiMapDeleteCodec_encodeRequest() {
        int fileClientMessageIndex = 253;
        ClientMessage encoded = MultiMapDeleteCodec.encodeRequest(aString, aData, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapDeleteCodec_decodeResponse() {
        int fileClientMessageIndex = 254;
    }

    @Test
    public void test_MultiMapPutAllCodec_encodeRequest() {
        int fileClientMessageIndex = 255;
        ClientMessage encoded = MultiMapPutAllCodec.encodeRequest(aString, aListOfDataToListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapPutAllCodec_decodeResponse() {
        int fileClientMessageIndex = 256;
    }

    @Test
    public void test_QueueOfferCodec_encodeRequest() {
        int fileClientMessageIndex = 257;
        ClientMessage encoded = QueueOfferCodec.encodeRequest(aString, aData, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueOfferCodec_decodeResponse() {
        int fileClientMessageIndex = 258;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, QueueOfferCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_QueuePutCodec_encodeRequest() {
        int fileClientMessageIndex = 259;
        ClientMessage encoded = QueuePutCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueuePutCodec_decodeResponse() {
        int fileClientMessageIndex = 260;
    }

    @Test
    public void test_QueueSizeCodec_encodeRequest() {
        int fileClientMessageIndex = 261;
        ClientMessage encoded = QueueSizeCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueSizeCodec_decodeResponse() {
        int fileClientMessageIndex = 262;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(anInt, QueueSizeCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_QueueRemoveCodec_encodeRequest() {
        int fileClientMessageIndex = 263;
        ClientMessage encoded = QueueRemoveCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueRemoveCodec_decodeResponse() {
        int fileClientMessageIndex = 264;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, QueueRemoveCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_QueuePollCodec_encodeRequest() {
        int fileClientMessageIndex = 265;
        ClientMessage encoded = QueuePollCodec.encodeRequest(aString, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueuePollCodec_decodeResponse() {
        int fileClientMessageIndex = 266;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, QueuePollCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_QueueTakeCodec_encodeRequest() {
        int fileClientMessageIndex = 267;
        ClientMessage encoded = QueueTakeCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueTakeCodec_decodeResponse() {
        int fileClientMessageIndex = 268;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, QueueTakeCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_QueuePeekCodec_encodeRequest() {
        int fileClientMessageIndex = 269;
        ClientMessage encoded = QueuePeekCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueuePeekCodec_decodeResponse() {
        int fileClientMessageIndex = 270;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, QueuePeekCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_QueueIteratorCodec_encodeRequest() {
        int fileClientMessageIndex = 271;
        ClientMessage encoded = QueueIteratorCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueIteratorCodec_decodeResponse() {
        int fileClientMessageIndex = 272;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfData, QueueIteratorCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_QueueDrainToCodec_encodeRequest() {
        int fileClientMessageIndex = 273;
        ClientMessage encoded = QueueDrainToCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueDrainToCodec_decodeResponse() {
        int fileClientMessageIndex = 274;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfData, QueueDrainToCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_QueueDrainToMaxSizeCodec_encodeRequest() {
        int fileClientMessageIndex = 275;
        ClientMessage encoded = QueueDrainToMaxSizeCodec.encodeRequest(aString, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueDrainToMaxSizeCodec_decodeResponse() {
        int fileClientMessageIndex = 276;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfData, QueueDrainToMaxSizeCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_QueueContainsCodec_encodeRequest() {
        int fileClientMessageIndex = 277;
        ClientMessage encoded = QueueContainsCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueContainsCodec_decodeResponse() {
        int fileClientMessageIndex = 278;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, QueueContainsCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_QueueContainsAllCodec_encodeRequest() {
        int fileClientMessageIndex = 279;
        ClientMessage encoded = QueueContainsAllCodec.encodeRequest(aString, aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueContainsAllCodec_decodeResponse() {
        int fileClientMessageIndex = 280;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, QueueContainsAllCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_QueueCompareAndRemoveAllCodec_encodeRequest() {
        int fileClientMessageIndex = 281;
        ClientMessage encoded = QueueCompareAndRemoveAllCodec.encodeRequest(aString, aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueCompareAndRemoveAllCodec_decodeResponse() {
        int fileClientMessageIndex = 282;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, QueueCompareAndRemoveAllCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_QueueCompareAndRetainAllCodec_encodeRequest() {
        int fileClientMessageIndex = 283;
        ClientMessage encoded = QueueCompareAndRetainAllCodec.encodeRequest(aString, aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueCompareAndRetainAllCodec_decodeResponse() {
        int fileClientMessageIndex = 284;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, QueueCompareAndRetainAllCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_QueueClearCodec_encodeRequest() {
        int fileClientMessageIndex = 285;
        ClientMessage encoded = QueueClearCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueClearCodec_decodeResponse() {
        int fileClientMessageIndex = 286;
    }

    @Test
    public void test_QueueAddAllCodec_encodeRequest() {
        int fileClientMessageIndex = 287;
        ClientMessage encoded = QueueAddAllCodec.encodeRequest(aString, aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueAddAllCodec_decodeResponse() {
        int fileClientMessageIndex = 288;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, QueueAddAllCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_QueueAddListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 289;
        ClientMessage encoded = QueueAddListenerCodec.encodeRequest(aString, aBoolean, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueAddListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 290;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, QueueAddListenerCodec.decodeResponse(fromFile)));
    }

    private static class QueueAddListenerCodecHandler extends QueueAddListenerCodec.AbstractEventHandler {
        @Override
        public void handleItemEvent(com.hazelcast.internal.serialization.Data item, java.util.UUID uuid, int eventType) {
            assertTrue(isEqual(null, item));
            assertTrue(isEqual(aUUID, uuid));
            assertTrue(isEqual(anInt, eventType));
        }
    }

    @Test
    public void test_QueueAddListenerCodec_handleItemEvent() {
        int fileClientMessageIndex = 291;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        QueueAddListenerCodecHandler handler = new QueueAddListenerCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_QueueRemoveListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 292;
        ClientMessage encoded = QueueRemoveListenerCodec.encodeRequest(aString, aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueRemoveListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 293;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, QueueRemoveListenerCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_QueueRemainingCapacityCodec_encodeRequest() {
        int fileClientMessageIndex = 294;
        ClientMessage encoded = QueueRemainingCapacityCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueRemainingCapacityCodec_decodeResponse() {
        int fileClientMessageIndex = 295;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(anInt, QueueRemainingCapacityCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_QueueIsEmptyCodec_encodeRequest() {
        int fileClientMessageIndex = 296;
        ClientMessage encoded = QueueIsEmptyCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueIsEmptyCodec_decodeResponse() {
        int fileClientMessageIndex = 297;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, QueueIsEmptyCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TopicPublishCodec_encodeRequest() {
        int fileClientMessageIndex = 298;
        ClientMessage encoded = TopicPublishCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TopicPublishCodec_decodeResponse() {
        int fileClientMessageIndex = 299;
    }

    @Test
    public void test_TopicAddMessageListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 300;
        ClientMessage encoded = TopicAddMessageListenerCodec.encodeRequest(aString, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TopicAddMessageListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 301;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, TopicAddMessageListenerCodec.decodeResponse(fromFile)));
    }

    private static class TopicAddMessageListenerCodecHandler extends TopicAddMessageListenerCodec.AbstractEventHandler {
        @Override
        public void handleTopicEvent(com.hazelcast.internal.serialization.Data item, long publishTime, java.util.UUID uuid) {
            assertTrue(isEqual(aData, item));
            assertTrue(isEqual(aLong, publishTime));
            assertTrue(isEqual(aUUID, uuid));
        }
    }

    @Test
    public void test_TopicAddMessageListenerCodec_handleTopicEvent() {
        int fileClientMessageIndex = 302;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TopicAddMessageListenerCodecHandler handler = new TopicAddMessageListenerCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_TopicRemoveMessageListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 303;
        ClientMessage encoded = TopicRemoveMessageListenerCodec.encodeRequest(aString, aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TopicRemoveMessageListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 304;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, TopicRemoveMessageListenerCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TopicPublishAllCodec_encodeRequest() {
        int fileClientMessageIndex = 305;
        ClientMessage encoded = TopicPublishAllCodec.encodeRequest(aString, aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TopicPublishAllCodec_decodeResponse() {
        int fileClientMessageIndex = 306;
    }

    @Test
    public void test_ListSizeCodec_encodeRequest() {
        int fileClientMessageIndex = 307;
        ClientMessage encoded = ListSizeCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListSizeCodec_decodeResponse() {
        int fileClientMessageIndex = 308;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(anInt, ListSizeCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ListContainsCodec_encodeRequest() {
        int fileClientMessageIndex = 309;
        ClientMessage encoded = ListContainsCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListContainsCodec_decodeResponse() {
        int fileClientMessageIndex = 310;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ListContainsCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ListContainsAllCodec_encodeRequest() {
        int fileClientMessageIndex = 311;
        ClientMessage encoded = ListContainsAllCodec.encodeRequest(aString, aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListContainsAllCodec_decodeResponse() {
        int fileClientMessageIndex = 312;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ListContainsAllCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ListAddCodec_encodeRequest() {
        int fileClientMessageIndex = 313;
        ClientMessage encoded = ListAddCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListAddCodec_decodeResponse() {
        int fileClientMessageIndex = 314;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ListAddCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ListRemoveCodec_encodeRequest() {
        int fileClientMessageIndex = 315;
        ClientMessage encoded = ListRemoveCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListRemoveCodec_decodeResponse() {
        int fileClientMessageIndex = 316;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ListRemoveCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ListAddAllCodec_encodeRequest() {
        int fileClientMessageIndex = 317;
        ClientMessage encoded = ListAddAllCodec.encodeRequest(aString, aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListAddAllCodec_decodeResponse() {
        int fileClientMessageIndex = 318;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ListAddAllCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ListCompareAndRemoveAllCodec_encodeRequest() {
        int fileClientMessageIndex = 319;
        ClientMessage encoded = ListCompareAndRemoveAllCodec.encodeRequest(aString, aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListCompareAndRemoveAllCodec_decodeResponse() {
        int fileClientMessageIndex = 320;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ListCompareAndRemoveAllCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ListCompareAndRetainAllCodec_encodeRequest() {
        int fileClientMessageIndex = 321;
        ClientMessage encoded = ListCompareAndRetainAllCodec.encodeRequest(aString, aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListCompareAndRetainAllCodec_decodeResponse() {
        int fileClientMessageIndex = 322;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ListCompareAndRetainAllCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ListClearCodec_encodeRequest() {
        int fileClientMessageIndex = 323;
        ClientMessage encoded = ListClearCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListClearCodec_decodeResponse() {
        int fileClientMessageIndex = 324;
    }

    @Test
    public void test_ListGetAllCodec_encodeRequest() {
        int fileClientMessageIndex = 325;
        ClientMessage encoded = ListGetAllCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListGetAllCodec_decodeResponse() {
        int fileClientMessageIndex = 326;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfData, ListGetAllCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ListAddListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 327;
        ClientMessage encoded = ListAddListenerCodec.encodeRequest(aString, aBoolean, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListAddListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 328;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, ListAddListenerCodec.decodeResponse(fromFile)));
    }

    private static class ListAddListenerCodecHandler extends ListAddListenerCodec.AbstractEventHandler {
        @Override
        public void handleItemEvent(com.hazelcast.internal.serialization.Data item, java.util.UUID uuid, int eventType) {
            assertTrue(isEqual(null, item));
            assertTrue(isEqual(aUUID, uuid));
            assertTrue(isEqual(anInt, eventType));
        }
    }

    @Test
    public void test_ListAddListenerCodec_handleItemEvent() {
        int fileClientMessageIndex = 329;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ListAddListenerCodecHandler handler = new ListAddListenerCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_ListRemoveListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 330;
        ClientMessage encoded = ListRemoveListenerCodec.encodeRequest(aString, aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListRemoveListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 331;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ListRemoveListenerCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ListIsEmptyCodec_encodeRequest() {
        int fileClientMessageIndex = 332;
        ClientMessage encoded = ListIsEmptyCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListIsEmptyCodec_decodeResponse() {
        int fileClientMessageIndex = 333;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ListIsEmptyCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ListAddAllWithIndexCodec_encodeRequest() {
        int fileClientMessageIndex = 334;
        ClientMessage encoded = ListAddAllWithIndexCodec.encodeRequest(aString, anInt, aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListAddAllWithIndexCodec_decodeResponse() {
        int fileClientMessageIndex = 335;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ListAddAllWithIndexCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ListGetCodec_encodeRequest() {
        int fileClientMessageIndex = 336;
        ClientMessage encoded = ListGetCodec.encodeRequest(aString, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListGetCodec_decodeResponse() {
        int fileClientMessageIndex = 337;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, ListGetCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ListSetCodec_encodeRequest() {
        int fileClientMessageIndex = 338;
        ClientMessage encoded = ListSetCodec.encodeRequest(aString, anInt, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListSetCodec_decodeResponse() {
        int fileClientMessageIndex = 339;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, ListSetCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ListAddWithIndexCodec_encodeRequest() {
        int fileClientMessageIndex = 340;
        ClientMessage encoded = ListAddWithIndexCodec.encodeRequest(aString, anInt, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListAddWithIndexCodec_decodeResponse() {
        int fileClientMessageIndex = 341;
    }

    @Test
    public void test_ListRemoveWithIndexCodec_encodeRequest() {
        int fileClientMessageIndex = 342;
        ClientMessage encoded = ListRemoveWithIndexCodec.encodeRequest(aString, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListRemoveWithIndexCodec_decodeResponse() {
        int fileClientMessageIndex = 343;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, ListRemoveWithIndexCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ListLastIndexOfCodec_encodeRequest() {
        int fileClientMessageIndex = 344;
        ClientMessage encoded = ListLastIndexOfCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListLastIndexOfCodec_decodeResponse() {
        int fileClientMessageIndex = 345;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(anInt, ListLastIndexOfCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ListIndexOfCodec_encodeRequest() {
        int fileClientMessageIndex = 346;
        ClientMessage encoded = ListIndexOfCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListIndexOfCodec_decodeResponse() {
        int fileClientMessageIndex = 347;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(anInt, ListIndexOfCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ListSubCodec_encodeRequest() {
        int fileClientMessageIndex = 348;
        ClientMessage encoded = ListSubCodec.encodeRequest(aString, anInt, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListSubCodec_decodeResponse() {
        int fileClientMessageIndex = 349;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfData, ListSubCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ListIteratorCodec_encodeRequest() {
        int fileClientMessageIndex = 350;
        ClientMessage encoded = ListIteratorCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListIteratorCodec_decodeResponse() {
        int fileClientMessageIndex = 351;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfData, ListIteratorCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ListListIteratorCodec_encodeRequest() {
        int fileClientMessageIndex = 352;
        ClientMessage encoded = ListListIteratorCodec.encodeRequest(aString, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListListIteratorCodec_decodeResponse() {
        int fileClientMessageIndex = 353;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfData, ListListIteratorCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_SetSizeCodec_encodeRequest() {
        int fileClientMessageIndex = 354;
        ClientMessage encoded = SetSizeCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SetSizeCodec_decodeResponse() {
        int fileClientMessageIndex = 355;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(anInt, SetSizeCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_SetContainsCodec_encodeRequest() {
        int fileClientMessageIndex = 356;
        ClientMessage encoded = SetContainsCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SetContainsCodec_decodeResponse() {
        int fileClientMessageIndex = 357;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, SetContainsCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_SetContainsAllCodec_encodeRequest() {
        int fileClientMessageIndex = 358;
        ClientMessage encoded = SetContainsAllCodec.encodeRequest(aString, aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SetContainsAllCodec_decodeResponse() {
        int fileClientMessageIndex = 359;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, SetContainsAllCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_SetAddCodec_encodeRequest() {
        int fileClientMessageIndex = 360;
        ClientMessage encoded = SetAddCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SetAddCodec_decodeResponse() {
        int fileClientMessageIndex = 361;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, SetAddCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_SetRemoveCodec_encodeRequest() {
        int fileClientMessageIndex = 362;
        ClientMessage encoded = SetRemoveCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SetRemoveCodec_decodeResponse() {
        int fileClientMessageIndex = 363;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, SetRemoveCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_SetAddAllCodec_encodeRequest() {
        int fileClientMessageIndex = 364;
        ClientMessage encoded = SetAddAllCodec.encodeRequest(aString, aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SetAddAllCodec_decodeResponse() {
        int fileClientMessageIndex = 365;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, SetAddAllCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_SetCompareAndRemoveAllCodec_encodeRequest() {
        int fileClientMessageIndex = 366;
        ClientMessage encoded = SetCompareAndRemoveAllCodec.encodeRequest(aString, aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SetCompareAndRemoveAllCodec_decodeResponse() {
        int fileClientMessageIndex = 367;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, SetCompareAndRemoveAllCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_SetCompareAndRetainAllCodec_encodeRequest() {
        int fileClientMessageIndex = 368;
        ClientMessage encoded = SetCompareAndRetainAllCodec.encodeRequest(aString, aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SetCompareAndRetainAllCodec_decodeResponse() {
        int fileClientMessageIndex = 369;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, SetCompareAndRetainAllCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_SetClearCodec_encodeRequest() {
        int fileClientMessageIndex = 370;
        ClientMessage encoded = SetClearCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SetClearCodec_decodeResponse() {
        int fileClientMessageIndex = 371;
    }

    @Test
    public void test_SetGetAllCodec_encodeRequest() {
        int fileClientMessageIndex = 372;
        ClientMessage encoded = SetGetAllCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SetGetAllCodec_decodeResponse() {
        int fileClientMessageIndex = 373;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfData, SetGetAllCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_SetAddListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 374;
        ClientMessage encoded = SetAddListenerCodec.encodeRequest(aString, aBoolean, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SetAddListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 375;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, SetAddListenerCodec.decodeResponse(fromFile)));
    }

    private static class SetAddListenerCodecHandler extends SetAddListenerCodec.AbstractEventHandler {
        @Override
        public void handleItemEvent(com.hazelcast.internal.serialization.Data item, java.util.UUID uuid, int eventType) {
            assertTrue(isEqual(null, item));
            assertTrue(isEqual(aUUID, uuid));
            assertTrue(isEqual(anInt, eventType));
        }
    }

    @Test
    public void test_SetAddListenerCodec_handleItemEvent() {
        int fileClientMessageIndex = 376;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        SetAddListenerCodecHandler handler = new SetAddListenerCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_SetRemoveListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 377;
        ClientMessage encoded = SetRemoveListenerCodec.encodeRequest(aString, aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SetRemoveListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 378;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, SetRemoveListenerCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_SetIsEmptyCodec_encodeRequest() {
        int fileClientMessageIndex = 379;
        ClientMessage encoded = SetIsEmptyCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SetIsEmptyCodec_decodeResponse() {
        int fileClientMessageIndex = 380;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, SetIsEmptyCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_FencedLockLockCodec_encodeRequest() {
        int fileClientMessageIndex = 381;
        ClientMessage encoded = FencedLockLockCodec.encodeRequest(aRaftGroupId, aString, aLong, aLong, aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_FencedLockLockCodec_decodeResponse() {
        int fileClientMessageIndex = 382;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aLong, FencedLockLockCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_FencedLockTryLockCodec_encodeRequest() {
        int fileClientMessageIndex = 383;
        ClientMessage encoded = FencedLockTryLockCodec.encodeRequest(aRaftGroupId, aString, aLong, aLong, aUUID, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_FencedLockTryLockCodec_decodeResponse() {
        int fileClientMessageIndex = 384;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aLong, FencedLockTryLockCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_FencedLockUnlockCodec_encodeRequest() {
        int fileClientMessageIndex = 385;
        ClientMessage encoded = FencedLockUnlockCodec.encodeRequest(aRaftGroupId, aString, aLong, aLong, aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_FencedLockUnlockCodec_decodeResponse() {
        int fileClientMessageIndex = 386;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, FencedLockUnlockCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_FencedLockGetLockOwnershipCodec_encodeRequest() {
        int fileClientMessageIndex = 387;
        ClientMessage encoded = FencedLockGetLockOwnershipCodec.encodeRequest(aRaftGroupId, aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_FencedLockGetLockOwnershipCodec_decodeResponse() {
        int fileClientMessageIndex = 388;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        FencedLockGetLockOwnershipCodec.ResponseParameters parameters = FencedLockGetLockOwnershipCodec.decodeResponse(fromFile);
        assertTrue(isEqual(aLong, parameters.fence));
        assertTrue(isEqual(anInt, parameters.lockCount));
        assertTrue(isEqual(aLong, parameters.sessionId));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_ExecutorServiceShutdownCodec_encodeRequest() {
        int fileClientMessageIndex = 389;
        ClientMessage encoded = ExecutorServiceShutdownCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ExecutorServiceShutdownCodec_decodeResponse() {
        int fileClientMessageIndex = 390;
    }

    @Test
    public void test_ExecutorServiceIsShutdownCodec_encodeRequest() {
        int fileClientMessageIndex = 391;
        ClientMessage encoded = ExecutorServiceIsShutdownCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ExecutorServiceIsShutdownCodec_decodeResponse() {
        int fileClientMessageIndex = 392;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ExecutorServiceIsShutdownCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ExecutorServiceCancelOnPartitionCodec_encodeRequest() {
        int fileClientMessageIndex = 393;
        ClientMessage encoded = ExecutorServiceCancelOnPartitionCodec.encodeRequest(aUUID, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ExecutorServiceCancelOnPartitionCodec_decodeResponse() {
        int fileClientMessageIndex = 394;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ExecutorServiceCancelOnPartitionCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ExecutorServiceCancelOnMemberCodec_encodeRequest() {
        int fileClientMessageIndex = 395;
        ClientMessage encoded = ExecutorServiceCancelOnMemberCodec.encodeRequest(aUUID, aUUID, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ExecutorServiceCancelOnMemberCodec_decodeResponse() {
        int fileClientMessageIndex = 396;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ExecutorServiceCancelOnMemberCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ExecutorServiceSubmitToPartitionCodec_encodeRequest() {
        int fileClientMessageIndex = 397;
        ClientMessage encoded = ExecutorServiceSubmitToPartitionCodec.encodeRequest(aString, aUUID, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ExecutorServiceSubmitToPartitionCodec_decodeResponse() {
        int fileClientMessageIndex = 398;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, ExecutorServiceSubmitToPartitionCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ExecutorServiceSubmitToMemberCodec_encodeRequest() {
        int fileClientMessageIndex = 399;
        ClientMessage encoded = ExecutorServiceSubmitToMemberCodec.encodeRequest(aString, aUUID, aData, aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ExecutorServiceSubmitToMemberCodec_decodeResponse() {
        int fileClientMessageIndex = 400;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, ExecutorServiceSubmitToMemberCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_AtomicLongApplyCodec_encodeRequest() {
        int fileClientMessageIndex = 401;
        ClientMessage encoded = AtomicLongApplyCodec.encodeRequest(aRaftGroupId, aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_AtomicLongApplyCodec_decodeResponse() {
        int fileClientMessageIndex = 402;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, AtomicLongApplyCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_AtomicLongAlterCodec_encodeRequest() {
        int fileClientMessageIndex = 403;
        ClientMessage encoded = AtomicLongAlterCodec.encodeRequest(aRaftGroupId, aString, aData, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_AtomicLongAlterCodec_decodeResponse() {
        int fileClientMessageIndex = 404;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aLong, AtomicLongAlterCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_AtomicLongAddAndGetCodec_encodeRequest() {
        int fileClientMessageIndex = 405;
        ClientMessage encoded = AtomicLongAddAndGetCodec.encodeRequest(aRaftGroupId, aString, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_AtomicLongAddAndGetCodec_decodeResponse() {
        int fileClientMessageIndex = 406;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aLong, AtomicLongAddAndGetCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_AtomicLongCompareAndSetCodec_encodeRequest() {
        int fileClientMessageIndex = 407;
        ClientMessage encoded = AtomicLongCompareAndSetCodec.encodeRequest(aRaftGroupId, aString, aLong, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_AtomicLongCompareAndSetCodec_decodeResponse() {
        int fileClientMessageIndex = 408;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, AtomicLongCompareAndSetCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_AtomicLongGetCodec_encodeRequest() {
        int fileClientMessageIndex = 409;
        ClientMessage encoded = AtomicLongGetCodec.encodeRequest(aRaftGroupId, aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_AtomicLongGetCodec_decodeResponse() {
        int fileClientMessageIndex = 410;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aLong, AtomicLongGetCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_AtomicLongGetAndAddCodec_encodeRequest() {
        int fileClientMessageIndex = 411;
        ClientMessage encoded = AtomicLongGetAndAddCodec.encodeRequest(aRaftGroupId, aString, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_AtomicLongGetAndAddCodec_decodeResponse() {
        int fileClientMessageIndex = 412;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aLong, AtomicLongGetAndAddCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_AtomicLongGetAndSetCodec_encodeRequest() {
        int fileClientMessageIndex = 413;
        ClientMessage encoded = AtomicLongGetAndSetCodec.encodeRequest(aRaftGroupId, aString, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_AtomicLongGetAndSetCodec_decodeResponse() {
        int fileClientMessageIndex = 414;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aLong, AtomicLongGetAndSetCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_AtomicRefApplyCodec_encodeRequest() {
        int fileClientMessageIndex = 415;
        ClientMessage encoded = AtomicRefApplyCodec.encodeRequest(aRaftGroupId, aString, aData, anInt, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_AtomicRefApplyCodec_decodeResponse() {
        int fileClientMessageIndex = 416;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, AtomicRefApplyCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_AtomicRefCompareAndSetCodec_encodeRequest() {
        int fileClientMessageIndex = 417;
        ClientMessage encoded = AtomicRefCompareAndSetCodec.encodeRequest(aRaftGroupId, aString, null, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_AtomicRefCompareAndSetCodec_decodeResponse() {
        int fileClientMessageIndex = 418;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, AtomicRefCompareAndSetCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_AtomicRefContainsCodec_encodeRequest() {
        int fileClientMessageIndex = 419;
        ClientMessage encoded = AtomicRefContainsCodec.encodeRequest(aRaftGroupId, aString, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_AtomicRefContainsCodec_decodeResponse() {
        int fileClientMessageIndex = 420;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, AtomicRefContainsCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_AtomicRefGetCodec_encodeRequest() {
        int fileClientMessageIndex = 421;
        ClientMessage encoded = AtomicRefGetCodec.encodeRequest(aRaftGroupId, aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_AtomicRefGetCodec_decodeResponse() {
        int fileClientMessageIndex = 422;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, AtomicRefGetCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_AtomicRefSetCodec_encodeRequest() {
        int fileClientMessageIndex = 423;
        ClientMessage encoded = AtomicRefSetCodec.encodeRequest(aRaftGroupId, aString, null, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_AtomicRefSetCodec_decodeResponse() {
        int fileClientMessageIndex = 424;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, AtomicRefSetCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CountDownLatchTrySetCountCodec_encodeRequest() {
        int fileClientMessageIndex = 425;
        ClientMessage encoded = CountDownLatchTrySetCountCodec.encodeRequest(aRaftGroupId, aString, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CountDownLatchTrySetCountCodec_decodeResponse() {
        int fileClientMessageIndex = 426;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, CountDownLatchTrySetCountCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CountDownLatchAwaitCodec_encodeRequest() {
        int fileClientMessageIndex = 427;
        ClientMessage encoded = CountDownLatchAwaitCodec.encodeRequest(aRaftGroupId, aString, aUUID, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CountDownLatchAwaitCodec_decodeResponse() {
        int fileClientMessageIndex = 428;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, CountDownLatchAwaitCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CountDownLatchCountDownCodec_encodeRequest() {
        int fileClientMessageIndex = 429;
        ClientMessage encoded = CountDownLatchCountDownCodec.encodeRequest(aRaftGroupId, aString, aUUID, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CountDownLatchCountDownCodec_decodeResponse() {
        int fileClientMessageIndex = 430;
    }

    @Test
    public void test_CountDownLatchGetCountCodec_encodeRequest() {
        int fileClientMessageIndex = 431;
        ClientMessage encoded = CountDownLatchGetCountCodec.encodeRequest(aRaftGroupId, aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CountDownLatchGetCountCodec_decodeResponse() {
        int fileClientMessageIndex = 432;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(anInt, CountDownLatchGetCountCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CountDownLatchGetRoundCodec_encodeRequest() {
        int fileClientMessageIndex = 433;
        ClientMessage encoded = CountDownLatchGetRoundCodec.encodeRequest(aRaftGroupId, aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CountDownLatchGetRoundCodec_decodeResponse() {
        int fileClientMessageIndex = 434;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(anInt, CountDownLatchGetRoundCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_SemaphoreInitCodec_encodeRequest() {
        int fileClientMessageIndex = 435;
        ClientMessage encoded = SemaphoreInitCodec.encodeRequest(aRaftGroupId, aString, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SemaphoreInitCodec_decodeResponse() {
        int fileClientMessageIndex = 436;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, SemaphoreInitCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_SemaphoreAcquireCodec_encodeRequest() {
        int fileClientMessageIndex = 437;
        ClientMessage encoded = SemaphoreAcquireCodec.encodeRequest(aRaftGroupId, aString, aLong, aLong, aUUID, anInt, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SemaphoreAcquireCodec_decodeResponse() {
        int fileClientMessageIndex = 438;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, SemaphoreAcquireCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_SemaphoreReleaseCodec_encodeRequest() {
        int fileClientMessageIndex = 439;
        ClientMessage encoded = SemaphoreReleaseCodec.encodeRequest(aRaftGroupId, aString, aLong, aLong, aUUID, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SemaphoreReleaseCodec_decodeResponse() {
        int fileClientMessageIndex = 440;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, SemaphoreReleaseCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_SemaphoreDrainCodec_encodeRequest() {
        int fileClientMessageIndex = 441;
        ClientMessage encoded = SemaphoreDrainCodec.encodeRequest(aRaftGroupId, aString, aLong, aLong, aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SemaphoreDrainCodec_decodeResponse() {
        int fileClientMessageIndex = 442;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(anInt, SemaphoreDrainCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_SemaphoreChangeCodec_encodeRequest() {
        int fileClientMessageIndex = 443;
        ClientMessage encoded = SemaphoreChangeCodec.encodeRequest(aRaftGroupId, aString, aLong, aLong, aUUID, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SemaphoreChangeCodec_decodeResponse() {
        int fileClientMessageIndex = 444;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, SemaphoreChangeCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_SemaphoreAvailablePermitsCodec_encodeRequest() {
        int fileClientMessageIndex = 445;
        ClientMessage encoded = SemaphoreAvailablePermitsCodec.encodeRequest(aRaftGroupId, aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SemaphoreAvailablePermitsCodec_decodeResponse() {
        int fileClientMessageIndex = 446;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(anInt, SemaphoreAvailablePermitsCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_SemaphoreGetSemaphoreTypeCodec_encodeRequest() {
        int fileClientMessageIndex = 447;
        ClientMessage encoded = SemaphoreGetSemaphoreTypeCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SemaphoreGetSemaphoreTypeCodec_decodeResponse() {
        int fileClientMessageIndex = 448;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, SemaphoreGetSemaphoreTypeCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ReplicatedMapPutCodec_encodeRequest() {
        int fileClientMessageIndex = 449;
        ClientMessage encoded = ReplicatedMapPutCodec.encodeRequest(aString, aData, aData, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapPutCodec_decodeResponse() {
        int fileClientMessageIndex = 450;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, ReplicatedMapPutCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ReplicatedMapSizeCodec_encodeRequest() {
        int fileClientMessageIndex = 451;
        ClientMessage encoded = ReplicatedMapSizeCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapSizeCodec_decodeResponse() {
        int fileClientMessageIndex = 452;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(anInt, ReplicatedMapSizeCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ReplicatedMapIsEmptyCodec_encodeRequest() {
        int fileClientMessageIndex = 453;
        ClientMessage encoded = ReplicatedMapIsEmptyCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapIsEmptyCodec_decodeResponse() {
        int fileClientMessageIndex = 454;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ReplicatedMapIsEmptyCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ReplicatedMapContainsKeyCodec_encodeRequest() {
        int fileClientMessageIndex = 455;
        ClientMessage encoded = ReplicatedMapContainsKeyCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapContainsKeyCodec_decodeResponse() {
        int fileClientMessageIndex = 456;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ReplicatedMapContainsKeyCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ReplicatedMapContainsValueCodec_encodeRequest() {
        int fileClientMessageIndex = 457;
        ClientMessage encoded = ReplicatedMapContainsValueCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapContainsValueCodec_decodeResponse() {
        int fileClientMessageIndex = 458;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ReplicatedMapContainsValueCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ReplicatedMapGetCodec_encodeRequest() {
        int fileClientMessageIndex = 459;
        ClientMessage encoded = ReplicatedMapGetCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapGetCodec_decodeResponse() {
        int fileClientMessageIndex = 460;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, ReplicatedMapGetCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ReplicatedMapRemoveCodec_encodeRequest() {
        int fileClientMessageIndex = 461;
        ClientMessage encoded = ReplicatedMapRemoveCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapRemoveCodec_decodeResponse() {
        int fileClientMessageIndex = 462;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, ReplicatedMapRemoveCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ReplicatedMapPutAllCodec_encodeRequest() {
        int fileClientMessageIndex = 463;
        ClientMessage encoded = ReplicatedMapPutAllCodec.encodeRequest(aString, aListOfDataToData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapPutAllCodec_decodeResponse() {
        int fileClientMessageIndex = 464;
    }

    @Test
    public void test_ReplicatedMapClearCodec_encodeRequest() {
        int fileClientMessageIndex = 465;
        ClientMessage encoded = ReplicatedMapClearCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapClearCodec_decodeResponse() {
        int fileClientMessageIndex = 466;
    }

    @Test
    public void test_ReplicatedMapAddEntryListenerToKeyWithPredicateCodec_encodeRequest() {
        int fileClientMessageIndex = 467;
        ClientMessage encoded = ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.encodeRequest(aString, aData, aData, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapAddEntryListenerToKeyWithPredicateCodec_decodeResponse() {
        int fileClientMessageIndex = 468;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.decodeResponse(fromFile)));
    }

    private static class ReplicatedMapAddEntryListenerToKeyWithPredicateCodecHandler extends ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.AbstractEventHandler {
        @Override
        public void handleEntryEvent(com.hazelcast.internal.serialization.Data key, com.hazelcast.internal.serialization.Data value, com.hazelcast.internal.serialization.Data oldValue, com.hazelcast.internal.serialization.Data mergingValue, int eventType, java.util.UUID uuid, int numberOfAffectedEntries) {
            assertTrue(isEqual(null, key));
            assertTrue(isEqual(null, value));
            assertTrue(isEqual(null, oldValue));
            assertTrue(isEqual(null, mergingValue));
            assertTrue(isEqual(anInt, eventType));
            assertTrue(isEqual(aUUID, uuid));
            assertTrue(isEqual(anInt, numberOfAffectedEntries));
        }
    }

    @Test
    public void test_ReplicatedMapAddEntryListenerToKeyWithPredicateCodec_handleEntryEvent() {
        int fileClientMessageIndex = 469;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ReplicatedMapAddEntryListenerToKeyWithPredicateCodecHandler handler = new ReplicatedMapAddEntryListenerToKeyWithPredicateCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_ReplicatedMapAddEntryListenerWithPredicateCodec_encodeRequest() {
        int fileClientMessageIndex = 470;
        ClientMessage encoded = ReplicatedMapAddEntryListenerWithPredicateCodec.encodeRequest(aString, aData, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapAddEntryListenerWithPredicateCodec_decodeResponse() {
        int fileClientMessageIndex = 471;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, ReplicatedMapAddEntryListenerWithPredicateCodec.decodeResponse(fromFile)));
    }

    private static class ReplicatedMapAddEntryListenerWithPredicateCodecHandler extends ReplicatedMapAddEntryListenerWithPredicateCodec.AbstractEventHandler {
        @Override
        public void handleEntryEvent(com.hazelcast.internal.serialization.Data key, com.hazelcast.internal.serialization.Data value, com.hazelcast.internal.serialization.Data oldValue, com.hazelcast.internal.serialization.Data mergingValue, int eventType, java.util.UUID uuid, int numberOfAffectedEntries) {
            assertTrue(isEqual(null, key));
            assertTrue(isEqual(null, value));
            assertTrue(isEqual(null, oldValue));
            assertTrue(isEqual(null, mergingValue));
            assertTrue(isEqual(anInt, eventType));
            assertTrue(isEqual(aUUID, uuid));
            assertTrue(isEqual(anInt, numberOfAffectedEntries));
        }
    }

    @Test
    public void test_ReplicatedMapAddEntryListenerWithPredicateCodec_handleEntryEvent() {
        int fileClientMessageIndex = 472;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ReplicatedMapAddEntryListenerWithPredicateCodecHandler handler = new ReplicatedMapAddEntryListenerWithPredicateCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_ReplicatedMapAddEntryListenerToKeyCodec_encodeRequest() {
        int fileClientMessageIndex = 473;
        ClientMessage encoded = ReplicatedMapAddEntryListenerToKeyCodec.encodeRequest(aString, aData, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapAddEntryListenerToKeyCodec_decodeResponse() {
        int fileClientMessageIndex = 474;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, ReplicatedMapAddEntryListenerToKeyCodec.decodeResponse(fromFile)));
    }

    private static class ReplicatedMapAddEntryListenerToKeyCodecHandler extends ReplicatedMapAddEntryListenerToKeyCodec.AbstractEventHandler {
        @Override
        public void handleEntryEvent(com.hazelcast.internal.serialization.Data key, com.hazelcast.internal.serialization.Data value, com.hazelcast.internal.serialization.Data oldValue, com.hazelcast.internal.serialization.Data mergingValue, int eventType, java.util.UUID uuid, int numberOfAffectedEntries) {
            assertTrue(isEqual(null, key));
            assertTrue(isEqual(null, value));
            assertTrue(isEqual(null, oldValue));
            assertTrue(isEqual(null, mergingValue));
            assertTrue(isEqual(anInt, eventType));
            assertTrue(isEqual(aUUID, uuid));
            assertTrue(isEqual(anInt, numberOfAffectedEntries));
        }
    }

    @Test
    public void test_ReplicatedMapAddEntryListenerToKeyCodec_handleEntryEvent() {
        int fileClientMessageIndex = 475;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ReplicatedMapAddEntryListenerToKeyCodecHandler handler = new ReplicatedMapAddEntryListenerToKeyCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_ReplicatedMapAddEntryListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 476;
        ClientMessage encoded = ReplicatedMapAddEntryListenerCodec.encodeRequest(aString, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapAddEntryListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 477;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, ReplicatedMapAddEntryListenerCodec.decodeResponse(fromFile)));
    }

    private static class ReplicatedMapAddEntryListenerCodecHandler extends ReplicatedMapAddEntryListenerCodec.AbstractEventHandler {
        @Override
        public void handleEntryEvent(com.hazelcast.internal.serialization.Data key, com.hazelcast.internal.serialization.Data value, com.hazelcast.internal.serialization.Data oldValue, com.hazelcast.internal.serialization.Data mergingValue, int eventType, java.util.UUID uuid, int numberOfAffectedEntries) {
            assertTrue(isEqual(null, key));
            assertTrue(isEqual(null, value));
            assertTrue(isEqual(null, oldValue));
            assertTrue(isEqual(null, mergingValue));
            assertTrue(isEqual(anInt, eventType));
            assertTrue(isEqual(aUUID, uuid));
            assertTrue(isEqual(anInt, numberOfAffectedEntries));
        }
    }

    @Test
    public void test_ReplicatedMapAddEntryListenerCodec_handleEntryEvent() {
        int fileClientMessageIndex = 478;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ReplicatedMapAddEntryListenerCodecHandler handler = new ReplicatedMapAddEntryListenerCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_ReplicatedMapRemoveEntryListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 479;
        ClientMessage encoded = ReplicatedMapRemoveEntryListenerCodec.encodeRequest(aString, aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapRemoveEntryListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 480;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ReplicatedMapRemoveEntryListenerCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ReplicatedMapKeySetCodec_encodeRequest() {
        int fileClientMessageIndex = 481;
        ClientMessage encoded = ReplicatedMapKeySetCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapKeySetCodec_decodeResponse() {
        int fileClientMessageIndex = 482;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfData, ReplicatedMapKeySetCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ReplicatedMapValuesCodec_encodeRequest() {
        int fileClientMessageIndex = 483;
        ClientMessage encoded = ReplicatedMapValuesCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapValuesCodec_decodeResponse() {
        int fileClientMessageIndex = 484;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfData, ReplicatedMapValuesCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ReplicatedMapEntrySetCodec_encodeRequest() {
        int fileClientMessageIndex = 485;
        ClientMessage encoded = ReplicatedMapEntrySetCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapEntrySetCodec_decodeResponse() {
        int fileClientMessageIndex = 486;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfDataToData, ReplicatedMapEntrySetCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ReplicatedMapAddNearCacheEntryListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 487;
        ClientMessage encoded = ReplicatedMapAddNearCacheEntryListenerCodec.encodeRequest(aString, aBoolean, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapAddNearCacheEntryListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 488;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, ReplicatedMapAddNearCacheEntryListenerCodec.decodeResponse(fromFile)));
    }

    private static class ReplicatedMapAddNearCacheEntryListenerCodecHandler extends ReplicatedMapAddNearCacheEntryListenerCodec.AbstractEventHandler {
        @Override
        public void handleEntryEvent(com.hazelcast.internal.serialization.Data key, com.hazelcast.internal.serialization.Data value, com.hazelcast.internal.serialization.Data oldValue, com.hazelcast.internal.serialization.Data mergingValue, int eventType, java.util.UUID uuid, int numberOfAffectedEntries) {
            assertTrue(isEqual(null, key));
            assertTrue(isEqual(null, value));
            assertTrue(isEqual(null, oldValue));
            assertTrue(isEqual(null, mergingValue));
            assertTrue(isEqual(anInt, eventType));
            assertTrue(isEqual(aUUID, uuid));
            assertTrue(isEqual(anInt, numberOfAffectedEntries));
        }
    }

    @Test
    public void test_ReplicatedMapAddNearCacheEntryListenerCodec_handleEntryEvent() {
        int fileClientMessageIndex = 489;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ReplicatedMapAddNearCacheEntryListenerCodecHandler handler = new ReplicatedMapAddNearCacheEntryListenerCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_ReplicatedMapPutAllWithMetadataCodec_encodeRequest() {
        int fileClientMessageIndex = 490;
        ClientMessage encoded = ReplicatedMapPutAllWithMetadataCodec.encodeRequest(aString, aListOfReplicatedMapEntryViewHolders, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapPutAllWithMetadataCodec_decodeResponse() {
        int fileClientMessageIndex = 491;
    }

    @Test
    public void test_ReplicatedMapFetchEntryViewsCodec_encodeRequest() {
        int fileClientMessageIndex = 492;
        ClientMessage encoded = ReplicatedMapFetchEntryViewsCodec.encodeRequest(aString, aUUID, aBoolean, anInt, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapFetchEntryViewsCodec_decodeResponse() {
        int fileClientMessageIndex = 493;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ReplicatedMapFetchEntryViewsCodec.ResponseParameters parameters = ReplicatedMapFetchEntryViewsCodec.decodeResponse(fromFile);
        assertTrue(isEqual(aUUID, parameters.cursorId));
        assertTrue(isEqual(aListOfReplicatedMapEntryViewHolders, parameters.entryViews));
    }

    @Test
    public void test_ReplicatedMapEndEntryViewIterationCodec_encodeRequest() {
        int fileClientMessageIndex = 494;
        ClientMessage encoded = ReplicatedMapEndEntryViewIterationCodec.encodeRequest(aString, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapEndEntryViewIterationCodec_decodeResponse() {
        int fileClientMessageIndex = 495;
    }

    @Test
    public void test_TransactionalMapContainsKeyCodec_encodeRequest() {
        int fileClientMessageIndex = 496;
        ClientMessage encoded = TransactionalMapContainsKeyCodec.encodeRequest(aString, aUUID, aLong, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapContainsKeyCodec_decodeResponse() {
        int fileClientMessageIndex = 497;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, TransactionalMapContainsKeyCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalMapGetCodec_encodeRequest() {
        int fileClientMessageIndex = 498;
        ClientMessage encoded = TransactionalMapGetCodec.encodeRequest(aString, aUUID, aLong, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapGetCodec_decodeResponse() {
        int fileClientMessageIndex = 499;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, TransactionalMapGetCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalMapGetForUpdateCodec_encodeRequest() {
        int fileClientMessageIndex = 500;
        ClientMessage encoded = TransactionalMapGetForUpdateCodec.encodeRequest(aString, aUUID, aLong, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapGetForUpdateCodec_decodeResponse() {
        int fileClientMessageIndex = 501;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, TransactionalMapGetForUpdateCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalMapSizeCodec_encodeRequest() {
        int fileClientMessageIndex = 502;
        ClientMessage encoded = TransactionalMapSizeCodec.encodeRequest(aString, aUUID, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapSizeCodec_decodeResponse() {
        int fileClientMessageIndex = 503;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(anInt, TransactionalMapSizeCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalMapIsEmptyCodec_encodeRequest() {
        int fileClientMessageIndex = 504;
        ClientMessage encoded = TransactionalMapIsEmptyCodec.encodeRequest(aString, aUUID, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapIsEmptyCodec_decodeResponse() {
        int fileClientMessageIndex = 505;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, TransactionalMapIsEmptyCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalMapPutCodec_encodeRequest() {
        int fileClientMessageIndex = 506;
        ClientMessage encoded = TransactionalMapPutCodec.encodeRequest(aString, aUUID, aLong, aData, aData, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapPutCodec_decodeResponse() {
        int fileClientMessageIndex = 507;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, TransactionalMapPutCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalMapSetCodec_encodeRequest() {
        int fileClientMessageIndex = 508;
        ClientMessage encoded = TransactionalMapSetCodec.encodeRequest(aString, aUUID, aLong, aData, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapSetCodec_decodeResponse() {
        int fileClientMessageIndex = 509;
    }

    @Test
    public void test_TransactionalMapPutIfAbsentCodec_encodeRequest() {
        int fileClientMessageIndex = 510;
        ClientMessage encoded = TransactionalMapPutIfAbsentCodec.encodeRequest(aString, aUUID, aLong, aData, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapPutIfAbsentCodec_decodeResponse() {
        int fileClientMessageIndex = 511;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, TransactionalMapPutIfAbsentCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalMapReplaceCodec_encodeRequest() {
        int fileClientMessageIndex = 512;
        ClientMessage encoded = TransactionalMapReplaceCodec.encodeRequest(aString, aUUID, aLong, aData, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapReplaceCodec_decodeResponse() {
        int fileClientMessageIndex = 513;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, TransactionalMapReplaceCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalMapReplaceIfSameCodec_encodeRequest() {
        int fileClientMessageIndex = 514;
        ClientMessage encoded = TransactionalMapReplaceIfSameCodec.encodeRequest(aString, aUUID, aLong, aData, aData, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapReplaceIfSameCodec_decodeResponse() {
        int fileClientMessageIndex = 515;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, TransactionalMapReplaceIfSameCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalMapRemoveCodec_encodeRequest() {
        int fileClientMessageIndex = 516;
        ClientMessage encoded = TransactionalMapRemoveCodec.encodeRequest(aString, aUUID, aLong, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapRemoveCodec_decodeResponse() {
        int fileClientMessageIndex = 517;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, TransactionalMapRemoveCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalMapDeleteCodec_encodeRequest() {
        int fileClientMessageIndex = 518;
        ClientMessage encoded = TransactionalMapDeleteCodec.encodeRequest(aString, aUUID, aLong, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapDeleteCodec_decodeResponse() {
        int fileClientMessageIndex = 519;
    }

    @Test
    public void test_TransactionalMapRemoveIfSameCodec_encodeRequest() {
        int fileClientMessageIndex = 520;
        ClientMessage encoded = TransactionalMapRemoveIfSameCodec.encodeRequest(aString, aUUID, aLong, aData, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapRemoveIfSameCodec_decodeResponse() {
        int fileClientMessageIndex = 521;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, TransactionalMapRemoveIfSameCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalMapKeySetCodec_encodeRequest() {
        int fileClientMessageIndex = 522;
        ClientMessage encoded = TransactionalMapKeySetCodec.encodeRequest(aString, aUUID, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapKeySetCodec_decodeResponse() {
        int fileClientMessageIndex = 523;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfData, TransactionalMapKeySetCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalMapKeySetWithPredicateCodec_encodeRequest() {
        int fileClientMessageIndex = 524;
        ClientMessage encoded = TransactionalMapKeySetWithPredicateCodec.encodeRequest(aString, aUUID, aLong, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapKeySetWithPredicateCodec_decodeResponse() {
        int fileClientMessageIndex = 525;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfData, TransactionalMapKeySetWithPredicateCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalMapValuesCodec_encodeRequest() {
        int fileClientMessageIndex = 526;
        ClientMessage encoded = TransactionalMapValuesCodec.encodeRequest(aString, aUUID, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapValuesCodec_decodeResponse() {
        int fileClientMessageIndex = 527;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfData, TransactionalMapValuesCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalMapValuesWithPredicateCodec_encodeRequest() {
        int fileClientMessageIndex = 528;
        ClientMessage encoded = TransactionalMapValuesWithPredicateCodec.encodeRequest(aString, aUUID, aLong, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapValuesWithPredicateCodec_decodeResponse() {
        int fileClientMessageIndex = 529;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfData, TransactionalMapValuesWithPredicateCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalMapContainsValueCodec_encodeRequest() {
        int fileClientMessageIndex = 530;
        ClientMessage encoded = TransactionalMapContainsValueCodec.encodeRequest(aString, aUUID, aLong, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapContainsValueCodec_decodeResponse() {
        int fileClientMessageIndex = 531;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, TransactionalMapContainsValueCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalMultiMapPutCodec_encodeRequest() {
        int fileClientMessageIndex = 532;
        ClientMessage encoded = TransactionalMultiMapPutCodec.encodeRequest(aString, aUUID, aLong, aData, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMultiMapPutCodec_decodeResponse() {
        int fileClientMessageIndex = 533;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, TransactionalMultiMapPutCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalMultiMapGetCodec_encodeRequest() {
        int fileClientMessageIndex = 534;
        ClientMessage encoded = TransactionalMultiMapGetCodec.encodeRequest(aString, aUUID, aLong, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMultiMapGetCodec_decodeResponse() {
        int fileClientMessageIndex = 535;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfData, TransactionalMultiMapGetCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalMultiMapRemoveCodec_encodeRequest() {
        int fileClientMessageIndex = 536;
        ClientMessage encoded = TransactionalMultiMapRemoveCodec.encodeRequest(aString, aUUID, aLong, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMultiMapRemoveCodec_decodeResponse() {
        int fileClientMessageIndex = 537;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfData, TransactionalMultiMapRemoveCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalMultiMapRemoveEntryCodec_encodeRequest() {
        int fileClientMessageIndex = 538;
        ClientMessage encoded = TransactionalMultiMapRemoveEntryCodec.encodeRequest(aString, aUUID, aLong, aData, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMultiMapRemoveEntryCodec_decodeResponse() {
        int fileClientMessageIndex = 539;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, TransactionalMultiMapRemoveEntryCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalMultiMapValueCountCodec_encodeRequest() {
        int fileClientMessageIndex = 540;
        ClientMessage encoded = TransactionalMultiMapValueCountCodec.encodeRequest(aString, aUUID, aLong, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMultiMapValueCountCodec_decodeResponse() {
        int fileClientMessageIndex = 541;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(anInt, TransactionalMultiMapValueCountCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalMultiMapSizeCodec_encodeRequest() {
        int fileClientMessageIndex = 542;
        ClientMessage encoded = TransactionalMultiMapSizeCodec.encodeRequest(aString, aUUID, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMultiMapSizeCodec_decodeResponse() {
        int fileClientMessageIndex = 543;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(anInt, TransactionalMultiMapSizeCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalSetAddCodec_encodeRequest() {
        int fileClientMessageIndex = 544;
        ClientMessage encoded = TransactionalSetAddCodec.encodeRequest(aString, aUUID, aLong, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalSetAddCodec_decodeResponse() {
        int fileClientMessageIndex = 545;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, TransactionalSetAddCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalSetRemoveCodec_encodeRequest() {
        int fileClientMessageIndex = 546;
        ClientMessage encoded = TransactionalSetRemoveCodec.encodeRequest(aString, aUUID, aLong, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalSetRemoveCodec_decodeResponse() {
        int fileClientMessageIndex = 547;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, TransactionalSetRemoveCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalSetSizeCodec_encodeRequest() {
        int fileClientMessageIndex = 548;
        ClientMessage encoded = TransactionalSetSizeCodec.encodeRequest(aString, aUUID, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalSetSizeCodec_decodeResponse() {
        int fileClientMessageIndex = 549;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(anInt, TransactionalSetSizeCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalListAddCodec_encodeRequest() {
        int fileClientMessageIndex = 550;
        ClientMessage encoded = TransactionalListAddCodec.encodeRequest(aString, aUUID, aLong, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalListAddCodec_decodeResponse() {
        int fileClientMessageIndex = 551;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, TransactionalListAddCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalListRemoveCodec_encodeRequest() {
        int fileClientMessageIndex = 552;
        ClientMessage encoded = TransactionalListRemoveCodec.encodeRequest(aString, aUUID, aLong, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalListRemoveCodec_decodeResponse() {
        int fileClientMessageIndex = 553;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, TransactionalListRemoveCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalListSizeCodec_encodeRequest() {
        int fileClientMessageIndex = 554;
        ClientMessage encoded = TransactionalListSizeCodec.encodeRequest(aString, aUUID, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalListSizeCodec_decodeResponse() {
        int fileClientMessageIndex = 555;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(anInt, TransactionalListSizeCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalQueueOfferCodec_encodeRequest() {
        int fileClientMessageIndex = 556;
        ClientMessage encoded = TransactionalQueueOfferCodec.encodeRequest(aString, aUUID, aLong, aData, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalQueueOfferCodec_decodeResponse() {
        int fileClientMessageIndex = 557;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, TransactionalQueueOfferCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalQueueTakeCodec_encodeRequest() {
        int fileClientMessageIndex = 558;
        ClientMessage encoded = TransactionalQueueTakeCodec.encodeRequest(aString, aUUID, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalQueueTakeCodec_decodeResponse() {
        int fileClientMessageIndex = 559;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, TransactionalQueueTakeCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalQueuePollCodec_encodeRequest() {
        int fileClientMessageIndex = 560;
        ClientMessage encoded = TransactionalQueuePollCodec.encodeRequest(aString, aUUID, aLong, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalQueuePollCodec_decodeResponse() {
        int fileClientMessageIndex = 561;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, TransactionalQueuePollCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalQueuePeekCodec_encodeRequest() {
        int fileClientMessageIndex = 562;
        ClientMessage encoded = TransactionalQueuePeekCodec.encodeRequest(aString, aUUID, aLong, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalQueuePeekCodec_decodeResponse() {
        int fileClientMessageIndex = 563;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, TransactionalQueuePeekCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionalQueueSizeCodec_encodeRequest() {
        int fileClientMessageIndex = 564;
        ClientMessage encoded = TransactionalQueueSizeCodec.encodeRequest(aString, aUUID, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalQueueSizeCodec_decodeResponse() {
        int fileClientMessageIndex = 565;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(anInt, TransactionalQueueSizeCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CacheAddEntryListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 566;
        ClientMessage encoded = CacheAddEntryListenerCodec.encodeRequest(aString, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheAddEntryListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 567;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, CacheAddEntryListenerCodec.decodeResponse(fromFile)));
    }

    private static class CacheAddEntryListenerCodecHandler extends CacheAddEntryListenerCodec.AbstractEventHandler {
        @Override
        public void handleCacheEvent(int type, java.util.Collection<com.hazelcast.cache.impl.CacheEventData> keys, int completionId) {
            assertTrue(isEqual(anInt, type));
            assertTrue(isEqual(aListOfCacheEventData, keys));
            assertTrue(isEqual(anInt, completionId));
        }
    }

    @Test
    public void test_CacheAddEntryListenerCodec_handleCacheEvent() {
        int fileClientMessageIndex = 568;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheAddEntryListenerCodecHandler handler = new CacheAddEntryListenerCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_CacheClearCodec_encodeRequest() {
        int fileClientMessageIndex = 569;
        ClientMessage encoded = CacheClearCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheClearCodec_decodeResponse() {
        int fileClientMessageIndex = 570;
    }

    @Test
    public void test_CacheRemoveAllKeysCodec_encodeRequest() {
        int fileClientMessageIndex = 571;
        ClientMessage encoded = CacheRemoveAllKeysCodec.encodeRequest(aString, aListOfData, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheRemoveAllKeysCodec_decodeResponse() {
        int fileClientMessageIndex = 572;
    }

    @Test
    public void test_CacheRemoveAllCodec_encodeRequest() {
        int fileClientMessageIndex = 573;
        ClientMessage encoded = CacheRemoveAllCodec.encodeRequest(aString, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheRemoveAllCodec_decodeResponse() {
        int fileClientMessageIndex = 574;
    }

    @Test
    public void test_CacheContainsKeyCodec_encodeRequest() {
        int fileClientMessageIndex = 575;
        ClientMessage encoded = CacheContainsKeyCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheContainsKeyCodec_decodeResponse() {
        int fileClientMessageIndex = 576;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, CacheContainsKeyCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CacheCreateConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 577;
        ClientMessage encoded = CacheCreateConfigCodec.encodeRequest(aCacheConfigHolder, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheCreateConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 578;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, CacheCreateConfigCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CacheDestroyCodec_encodeRequest() {
        int fileClientMessageIndex = 579;
        ClientMessage encoded = CacheDestroyCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheDestroyCodec_decodeResponse() {
        int fileClientMessageIndex = 580;
    }

    @Test
    public void test_CacheEntryProcessorCodec_encodeRequest() {
        int fileClientMessageIndex = 581;
        ClientMessage encoded = CacheEntryProcessorCodec.encodeRequest(aString, aData, aData, aListOfData, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheEntryProcessorCodec_decodeResponse() {
        int fileClientMessageIndex = 582;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, CacheEntryProcessorCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CacheGetAllCodec_encodeRequest() {
        int fileClientMessageIndex = 583;
        ClientMessage encoded = CacheGetAllCodec.encodeRequest(aString, aListOfData, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheGetAllCodec_decodeResponse() {
        int fileClientMessageIndex = 584;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfDataToData, CacheGetAllCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CacheGetAndRemoveCodec_encodeRequest() {
        int fileClientMessageIndex = 585;
        ClientMessage encoded = CacheGetAndRemoveCodec.encodeRequest(aString, aData, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheGetAndRemoveCodec_decodeResponse() {
        int fileClientMessageIndex = 586;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, CacheGetAndRemoveCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CacheGetAndReplaceCodec_encodeRequest() {
        int fileClientMessageIndex = 587;
        ClientMessage encoded = CacheGetAndReplaceCodec.encodeRequest(aString, aData, aData, null, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheGetAndReplaceCodec_decodeResponse() {
        int fileClientMessageIndex = 588;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, CacheGetAndReplaceCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CacheGetConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 589;
        ClientMessage encoded = CacheGetConfigCodec.encodeRequest(aString, aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheGetConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 590;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, CacheGetConfigCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CacheGetCodec_encodeRequest() {
        int fileClientMessageIndex = 591;
        ClientMessage encoded = CacheGetCodec.encodeRequest(aString, aData, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheGetCodec_decodeResponse() {
        int fileClientMessageIndex = 592;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, CacheGetCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CacheIterateCodec_encodeRequest() {
        int fileClientMessageIndex = 593;
        ClientMessage encoded = CacheIterateCodec.encodeRequest(aString, aListOfIntegerToInteger, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheIterateCodec_decodeResponse() {
        int fileClientMessageIndex = 594;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheIterateCodec.ResponseParameters parameters = CacheIterateCodec.decodeResponse(fromFile);
        assertTrue(isEqual(aListOfIntegerToInteger, parameters.iterationPointers));
        assertTrue(isEqual(aListOfData, parameters.keys));
    }

    @Test
    public void test_CacheListenerRegistrationCodec_encodeRequest() {
        int fileClientMessageIndex = 595;
        ClientMessage encoded = CacheListenerRegistrationCodec.encodeRequest(aString, aData, aBoolean, aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheListenerRegistrationCodec_decodeResponse() {
        int fileClientMessageIndex = 596;
    }

    @Test
    public void test_CacheLoadAllCodec_encodeRequest() {
        int fileClientMessageIndex = 597;
        ClientMessage encoded = CacheLoadAllCodec.encodeRequest(aString, aListOfData, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheLoadAllCodec_decodeResponse() {
        int fileClientMessageIndex = 598;
    }

    @Test
    public void test_CacheManagementConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 599;
        ClientMessage encoded = CacheManagementConfigCodec.encodeRequest(aString, aBoolean, aBoolean, aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheManagementConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 600;
    }

    @Test
    public void test_CachePutIfAbsentCodec_encodeRequest() {
        int fileClientMessageIndex = 601;
        ClientMessage encoded = CachePutIfAbsentCodec.encodeRequest(aString, aData, aData, null, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CachePutIfAbsentCodec_decodeResponse() {
        int fileClientMessageIndex = 602;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, CachePutIfAbsentCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CachePutCodec_encodeRequest() {
        int fileClientMessageIndex = 603;
        ClientMessage encoded = CachePutCodec.encodeRequest(aString, aData, aData, null, aBoolean, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CachePutCodec_decodeResponse() {
        int fileClientMessageIndex = 604;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, CachePutCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CacheRemoveEntryListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 605;
        ClientMessage encoded = CacheRemoveEntryListenerCodec.encodeRequest(aString, aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheRemoveEntryListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 606;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, CacheRemoveEntryListenerCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CacheRemoveInvalidationListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 607;
        ClientMessage encoded = CacheRemoveInvalidationListenerCodec.encodeRequest(aString, aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheRemoveInvalidationListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 608;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, CacheRemoveInvalidationListenerCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CacheRemoveCodec_encodeRequest() {
        int fileClientMessageIndex = 609;
        ClientMessage encoded = CacheRemoveCodec.encodeRequest(aString, aData, null, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheRemoveCodec_decodeResponse() {
        int fileClientMessageIndex = 610;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, CacheRemoveCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CacheReplaceCodec_encodeRequest() {
        int fileClientMessageIndex = 611;
        ClientMessage encoded = CacheReplaceCodec.encodeRequest(aString, aData, null, aData, null, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheReplaceCodec_decodeResponse() {
        int fileClientMessageIndex = 612;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, CacheReplaceCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CacheSizeCodec_encodeRequest() {
        int fileClientMessageIndex = 613;
        ClientMessage encoded = CacheSizeCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheSizeCodec_decodeResponse() {
        int fileClientMessageIndex = 614;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(anInt, CacheSizeCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CacheAddPartitionLostListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 615;
        ClientMessage encoded = CacheAddPartitionLostListenerCodec.encodeRequest(aString, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheAddPartitionLostListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 616;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, CacheAddPartitionLostListenerCodec.decodeResponse(fromFile)));
    }

    private static class CacheAddPartitionLostListenerCodecHandler extends CacheAddPartitionLostListenerCodec.AbstractEventHandler {
        @Override
        public void handleCachePartitionLostEvent(int partitionId, java.util.UUID uuid) {
            assertTrue(isEqual(anInt, partitionId));
            assertTrue(isEqual(aUUID, uuid));
        }
    }

    @Test
    public void test_CacheAddPartitionLostListenerCodec_handleCachePartitionLostEvent() {
        int fileClientMessageIndex = 617;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheAddPartitionLostListenerCodecHandler handler = new CacheAddPartitionLostListenerCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_CacheRemovePartitionLostListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 618;
        ClientMessage encoded = CacheRemovePartitionLostListenerCodec.encodeRequest(aString, aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheRemovePartitionLostListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 619;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, CacheRemovePartitionLostListenerCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CachePutAllCodec_encodeRequest() {
        int fileClientMessageIndex = 620;
        ClientMessage encoded = CachePutAllCodec.encodeRequest(aString, aListOfDataToData, null, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CachePutAllCodec_decodeResponse() {
        int fileClientMessageIndex = 621;
    }

    @Test
    public void test_CacheIterateEntriesCodec_encodeRequest() {
        int fileClientMessageIndex = 622;
        ClientMessage encoded = CacheIterateEntriesCodec.encodeRequest(aString, aListOfIntegerToInteger, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheIterateEntriesCodec_decodeResponse() {
        int fileClientMessageIndex = 623;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheIterateEntriesCodec.ResponseParameters parameters = CacheIterateEntriesCodec.decodeResponse(fromFile);
        assertTrue(isEqual(aListOfIntegerToInteger, parameters.iterationPointers));
        assertTrue(isEqual(aListOfDataToData, parameters.entries));
    }

    @Test
    public void test_CacheAddNearCacheInvalidationListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 624;
        ClientMessage encoded = CacheAddNearCacheInvalidationListenerCodec.encodeRequest(aString, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheAddNearCacheInvalidationListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 625;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, CacheAddNearCacheInvalidationListenerCodec.decodeResponse(fromFile)));
    }

    private static class CacheAddNearCacheInvalidationListenerCodecHandler extends CacheAddNearCacheInvalidationListenerCodec.AbstractEventHandler {
        @Override
        public void handleCacheInvalidationEvent(java.lang.String name, com.hazelcast.internal.serialization.Data key, java.util.UUID sourceUuid, java.util.UUID partitionUuid, long sequence) {
            assertTrue(isEqual(aString, name));
            assertTrue(isEqual(null, key));
            assertTrue(isEqual(null, sourceUuid));
            assertTrue(isEqual(aUUID, partitionUuid));
            assertTrue(isEqual(aLong, sequence));
        }
        @Override
        public void handleCacheBatchInvalidationEvent(java.lang.String name, java.util.Collection<com.hazelcast.internal.serialization.Data> keys, java.util.Collection<java.util.UUID> sourceUuids, java.util.Collection<java.util.UUID> partitionUuids, java.util.Collection<java.lang.Long> sequences) {
            assertTrue(isEqual(aString, name));
            assertTrue(isEqual(aListOfData, keys));
            assertTrue(isEqual(aListOfUUIDs, sourceUuids));
            assertTrue(isEqual(aListOfUUIDs, partitionUuids));
            assertTrue(isEqual(aListOfLongs, sequences));
        }
    }

    @Test
    public void test_CacheAddNearCacheInvalidationListenerCodec_handleCacheInvalidationEvent() {
        int fileClientMessageIndex = 626;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheAddNearCacheInvalidationListenerCodecHandler handler = new CacheAddNearCacheInvalidationListenerCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_CacheAddNearCacheInvalidationListenerCodec_handleCacheBatchInvalidationEvent() {
        int fileClientMessageIndex = 627;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheAddNearCacheInvalidationListenerCodecHandler handler = new CacheAddNearCacheInvalidationListenerCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_CacheFetchNearCacheInvalidationMetadataCodec_encodeRequest() {
        int fileClientMessageIndex = 628;
        ClientMessage encoded = CacheFetchNearCacheInvalidationMetadataCodec.encodeRequest(aListOfStrings, aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheFetchNearCacheInvalidationMetadataCodec_decodeResponse() {
        int fileClientMessageIndex = 629;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheFetchNearCacheInvalidationMetadataCodec.ResponseParameters parameters = CacheFetchNearCacheInvalidationMetadataCodec.decodeResponse(fromFile);
        assertTrue(isEqual(aListOfStringToListOfIntegerToLong, parameters.namePartitionSequenceList));
        assertTrue(isEqual(aListOfIntegerToUUID, parameters.partitionUuidList));
    }

    @Test
    public void test_CacheEventJournalSubscribeCodec_encodeRequest() {
        int fileClientMessageIndex = 630;
        ClientMessage encoded = CacheEventJournalSubscribeCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheEventJournalSubscribeCodec_decodeResponse() {
        int fileClientMessageIndex = 631;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheEventJournalSubscribeCodec.ResponseParameters parameters = CacheEventJournalSubscribeCodec.decodeResponse(fromFile);
        assertTrue(isEqual(aLong, parameters.oldestSequence));
        assertTrue(isEqual(aLong, parameters.newestSequence));
    }

    @Test
    public void test_CacheEventJournalReadCodec_encodeRequest() {
        int fileClientMessageIndex = 632;
        ClientMessage encoded = CacheEventJournalReadCodec.encodeRequest(aString, aLong, anInt, anInt, null, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheEventJournalReadCodec_decodeResponse() {
        int fileClientMessageIndex = 633;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheEventJournalReadCodec.ResponseParameters parameters = CacheEventJournalReadCodec.decodeResponse(fromFile);
        assertTrue(isEqual(anInt, parameters.readCount));
        assertTrue(isEqual(aListOfData, parameters.items));
        assertTrue(isEqual(null, parameters.itemSeqs));
        assertTrue(isEqual(aLong, parameters.nextSeq));
    }

    @Test
    public void test_CacheSetExpiryPolicyCodec_encodeRequest() {
        int fileClientMessageIndex = 634;
        ClientMessage encoded = CacheSetExpiryPolicyCodec.encodeRequest(aString, aListOfData, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheSetExpiryPolicyCodec_decodeResponse() {
        int fileClientMessageIndex = 635;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, CacheSetExpiryPolicyCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_XATransactionClearRemoteCodec_encodeRequest() {
        int fileClientMessageIndex = 636;
        ClientMessage encoded = XATransactionClearRemoteCodec.encodeRequest(anXid);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_XATransactionClearRemoteCodec_decodeResponse() {
        int fileClientMessageIndex = 637;
    }

    @Test
    public void test_XATransactionCollectTransactionsCodec_encodeRequest() {
        int fileClientMessageIndex = 638;
        ClientMessage encoded = XATransactionCollectTransactionsCodec.encodeRequest();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_XATransactionCollectTransactionsCodec_decodeResponse() {
        int fileClientMessageIndex = 639;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfXids, XATransactionCollectTransactionsCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_XATransactionFinalizeCodec_encodeRequest() {
        int fileClientMessageIndex = 640;
        ClientMessage encoded = XATransactionFinalizeCodec.encodeRequest(anXid, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_XATransactionFinalizeCodec_decodeResponse() {
        int fileClientMessageIndex = 641;
    }

    @Test
    public void test_XATransactionCommitCodec_encodeRequest() {
        int fileClientMessageIndex = 642;
        ClientMessage encoded = XATransactionCommitCodec.encodeRequest(aUUID, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_XATransactionCommitCodec_decodeResponse() {
        int fileClientMessageIndex = 643;
    }

    @Test
    public void test_XATransactionCreateCodec_encodeRequest() {
        int fileClientMessageIndex = 644;
        ClientMessage encoded = XATransactionCreateCodec.encodeRequest(anXid, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_XATransactionCreateCodec_decodeResponse() {
        int fileClientMessageIndex = 645;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, XATransactionCreateCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_XATransactionPrepareCodec_encodeRequest() {
        int fileClientMessageIndex = 646;
        ClientMessage encoded = XATransactionPrepareCodec.encodeRequest(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_XATransactionPrepareCodec_decodeResponse() {
        int fileClientMessageIndex = 647;
    }

    @Test
    public void test_XATransactionRollbackCodec_encodeRequest() {
        int fileClientMessageIndex = 648;
        ClientMessage encoded = XATransactionRollbackCodec.encodeRequest(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_XATransactionRollbackCodec_decodeResponse() {
        int fileClientMessageIndex = 649;
    }

    @Test
    public void test_TransactionCommitCodec_encodeRequest() {
        int fileClientMessageIndex = 650;
        ClientMessage encoded = TransactionCommitCodec.encodeRequest(aUUID, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionCommitCodec_decodeResponse() {
        int fileClientMessageIndex = 651;
    }

    @Test
    public void test_TransactionCreateCodec_encodeRequest() {
        int fileClientMessageIndex = 652;
        ClientMessage encoded = TransactionCreateCodec.encodeRequest(aLong, anInt, anInt, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionCreateCodec_decodeResponse() {
        int fileClientMessageIndex = 653;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, TransactionCreateCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_TransactionRollbackCodec_encodeRequest() {
        int fileClientMessageIndex = 654;
        ClientMessage encoded = TransactionRollbackCodec.encodeRequest(aUUID, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionRollbackCodec_decodeResponse() {
        int fileClientMessageIndex = 655;
    }

    @Test
    public void test_ContinuousQueryPublisherCreateWithValueCodec_encodeRequest() {
        int fileClientMessageIndex = 656;
        ClientMessage encoded = ContinuousQueryPublisherCreateWithValueCodec.encodeRequest(aString, aString, aData, anInt, anInt, aLong, aBoolean, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ContinuousQueryPublisherCreateWithValueCodec_decodeResponse() {
        int fileClientMessageIndex = 657;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfDataToData, ContinuousQueryPublisherCreateWithValueCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ContinuousQueryPublisherCreateCodec_encodeRequest() {
        int fileClientMessageIndex = 658;
        ClientMessage encoded = ContinuousQueryPublisherCreateCodec.encodeRequest(aString, aString, aData, anInt, anInt, aLong, aBoolean, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ContinuousQueryPublisherCreateCodec_decodeResponse() {
        int fileClientMessageIndex = 659;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfData, ContinuousQueryPublisherCreateCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ContinuousQueryMadePublishableCodec_encodeRequest() {
        int fileClientMessageIndex = 660;
        ClientMessage encoded = ContinuousQueryMadePublishableCodec.encodeRequest(aString, aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ContinuousQueryMadePublishableCodec_decodeResponse() {
        int fileClientMessageIndex = 661;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ContinuousQueryMadePublishableCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ContinuousQueryAddListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 662;
        ClientMessage encoded = ContinuousQueryAddListenerCodec.encodeRequest(aString, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ContinuousQueryAddListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 663;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, ContinuousQueryAddListenerCodec.decodeResponse(fromFile)));
    }

    private static class ContinuousQueryAddListenerCodecHandler extends ContinuousQueryAddListenerCodec.AbstractEventHandler {
        @Override
        public void handleQueryCacheSingleEvent(com.hazelcast.map.impl.querycache.event.QueryCacheEventData data) {
            assertTrue(isEqual(aQueryCacheEventData, data));
        }
        @Override
        public void handleQueryCacheBatchEvent(java.util.Collection<com.hazelcast.map.impl.querycache.event.QueryCacheEventData> events, java.lang.String source, int partitionId) {
            assertTrue(isEqual(aListOfQueryCacheEventData, events));
            assertTrue(isEqual(aString, source));
            assertTrue(isEqual(anInt, partitionId));
        }
    }

    @Test
    public void test_ContinuousQueryAddListenerCodec_handleQueryCacheSingleEvent() {
        int fileClientMessageIndex = 664;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ContinuousQueryAddListenerCodecHandler handler = new ContinuousQueryAddListenerCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_ContinuousQueryAddListenerCodec_handleQueryCacheBatchEvent() {
        int fileClientMessageIndex = 665;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ContinuousQueryAddListenerCodecHandler handler = new ContinuousQueryAddListenerCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_ContinuousQuerySetReadCursorCodec_encodeRequest() {
        int fileClientMessageIndex = 666;
        ClientMessage encoded = ContinuousQuerySetReadCursorCodec.encodeRequest(aString, aString, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ContinuousQuerySetReadCursorCodec_decodeResponse() {
        int fileClientMessageIndex = 667;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ContinuousQuerySetReadCursorCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ContinuousQueryDestroyCacheCodec_encodeRequest() {
        int fileClientMessageIndex = 668;
        ClientMessage encoded = ContinuousQueryDestroyCacheCodec.encodeRequest(aString, aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ContinuousQueryDestroyCacheCodec_decodeResponse() {
        int fileClientMessageIndex = 669;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ContinuousQueryDestroyCacheCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_RingbufferSizeCodec_encodeRequest() {
        int fileClientMessageIndex = 670;
        ClientMessage encoded = RingbufferSizeCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_RingbufferSizeCodec_decodeResponse() {
        int fileClientMessageIndex = 671;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aLong, RingbufferSizeCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_RingbufferTailSequenceCodec_encodeRequest() {
        int fileClientMessageIndex = 672;
        ClientMessage encoded = RingbufferTailSequenceCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_RingbufferTailSequenceCodec_decodeResponse() {
        int fileClientMessageIndex = 673;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aLong, RingbufferTailSequenceCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_RingbufferHeadSequenceCodec_encodeRequest() {
        int fileClientMessageIndex = 674;
        ClientMessage encoded = RingbufferHeadSequenceCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_RingbufferHeadSequenceCodec_decodeResponse() {
        int fileClientMessageIndex = 675;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aLong, RingbufferHeadSequenceCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_RingbufferCapacityCodec_encodeRequest() {
        int fileClientMessageIndex = 676;
        ClientMessage encoded = RingbufferCapacityCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_RingbufferCapacityCodec_decodeResponse() {
        int fileClientMessageIndex = 677;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aLong, RingbufferCapacityCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_RingbufferRemainingCapacityCodec_encodeRequest() {
        int fileClientMessageIndex = 678;
        ClientMessage encoded = RingbufferRemainingCapacityCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_RingbufferRemainingCapacityCodec_decodeResponse() {
        int fileClientMessageIndex = 679;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aLong, RingbufferRemainingCapacityCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_RingbufferAddCodec_encodeRequest() {
        int fileClientMessageIndex = 680;
        ClientMessage encoded = RingbufferAddCodec.encodeRequest(aString, anInt, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_RingbufferAddCodec_decodeResponse() {
        int fileClientMessageIndex = 681;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aLong, RingbufferAddCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_RingbufferReadOneCodec_encodeRequest() {
        int fileClientMessageIndex = 682;
        ClientMessage encoded = RingbufferReadOneCodec.encodeRequest(aString, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_RingbufferReadOneCodec_decodeResponse() {
        int fileClientMessageIndex = 683;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, RingbufferReadOneCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_RingbufferAddAllCodec_encodeRequest() {
        int fileClientMessageIndex = 684;
        ClientMessage encoded = RingbufferAddAllCodec.encodeRequest(aString, aListOfData, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_RingbufferAddAllCodec_decodeResponse() {
        int fileClientMessageIndex = 685;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aLong, RingbufferAddAllCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_RingbufferReadManyCodec_encodeRequest() {
        int fileClientMessageIndex = 686;
        ClientMessage encoded = RingbufferReadManyCodec.encodeRequest(aString, aLong, anInt, anInt, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_RingbufferReadManyCodec_decodeResponse() {
        int fileClientMessageIndex = 687;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        RingbufferReadManyCodec.ResponseParameters parameters = RingbufferReadManyCodec.decodeResponse(fromFile);
        assertTrue(isEqual(anInt, parameters.readCount));
        assertTrue(isEqual(aListOfData, parameters.items));
        assertTrue(isEqual(null, parameters.itemSeqs));
        assertTrue(isEqual(aLong, parameters.nextSeq));
    }

    @Test
    public void test_DurableExecutorShutdownCodec_encodeRequest() {
        int fileClientMessageIndex = 688;
        ClientMessage encoded = DurableExecutorShutdownCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DurableExecutorShutdownCodec_decodeResponse() {
        int fileClientMessageIndex = 689;
    }

    @Test
    public void test_DurableExecutorIsShutdownCodec_encodeRequest() {
        int fileClientMessageIndex = 690;
        ClientMessage encoded = DurableExecutorIsShutdownCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DurableExecutorIsShutdownCodec_decodeResponse() {
        int fileClientMessageIndex = 691;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, DurableExecutorIsShutdownCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_DurableExecutorSubmitToPartitionCodec_encodeRequest() {
        int fileClientMessageIndex = 692;
        ClientMessage encoded = DurableExecutorSubmitToPartitionCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DurableExecutorSubmitToPartitionCodec_decodeResponse() {
        int fileClientMessageIndex = 693;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(anInt, DurableExecutorSubmitToPartitionCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_DurableExecutorRetrieveResultCodec_encodeRequest() {
        int fileClientMessageIndex = 694;
        ClientMessage encoded = DurableExecutorRetrieveResultCodec.encodeRequest(aString, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DurableExecutorRetrieveResultCodec_decodeResponse() {
        int fileClientMessageIndex = 695;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, DurableExecutorRetrieveResultCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_DurableExecutorDisposeResultCodec_encodeRequest() {
        int fileClientMessageIndex = 696;
        ClientMessage encoded = DurableExecutorDisposeResultCodec.encodeRequest(aString, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DurableExecutorDisposeResultCodec_decodeResponse() {
        int fileClientMessageIndex = 697;
    }

    @Test
    public void test_DurableExecutorRetrieveAndDisposeResultCodec_encodeRequest() {
        int fileClientMessageIndex = 698;
        ClientMessage encoded = DurableExecutorRetrieveAndDisposeResultCodec.encodeRequest(aString, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DurableExecutorRetrieveAndDisposeResultCodec_decodeResponse() {
        int fileClientMessageIndex = 699;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, DurableExecutorRetrieveAndDisposeResultCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CardinalityEstimatorAddCodec_encodeRequest() {
        int fileClientMessageIndex = 700;
        ClientMessage encoded = CardinalityEstimatorAddCodec.encodeRequest(aString, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CardinalityEstimatorAddCodec_decodeResponse() {
        int fileClientMessageIndex = 701;
    }

    @Test
    public void test_CardinalityEstimatorEstimateCodec_encodeRequest() {
        int fileClientMessageIndex = 702;
        ClientMessage encoded = CardinalityEstimatorEstimateCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CardinalityEstimatorEstimateCodec_decodeResponse() {
        int fileClientMessageIndex = 703;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aLong, CardinalityEstimatorEstimateCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ScheduledExecutorShutdownCodec_encodeRequest() {
        int fileClientMessageIndex = 704;
        ClientMessage encoded = ScheduledExecutorShutdownCodec.encodeRequest(aString, aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorShutdownCodec_decodeResponse() {
        int fileClientMessageIndex = 705;
    }

    @Test
    public void test_ScheduledExecutorSubmitToPartitionCodec_encodeRequest() {
        int fileClientMessageIndex = 706;
        ClientMessage encoded = ScheduledExecutorSubmitToPartitionCodec.encodeRequest(aString, aByte, aString, aData, aLong, aLong, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorSubmitToPartitionCodec_decodeResponse() {
        int fileClientMessageIndex = 707;
    }

    @Test
    public void test_ScheduledExecutorSubmitToMemberCodec_encodeRequest() {
        int fileClientMessageIndex = 708;
        ClientMessage encoded = ScheduledExecutorSubmitToMemberCodec.encodeRequest(aString, aUUID, aByte, aString, aData, aLong, aLong, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorSubmitToMemberCodec_decodeResponse() {
        int fileClientMessageIndex = 709;
    }

    @Test
    public void test_ScheduledExecutorGetAllScheduledFuturesCodec_encodeRequest() {
        int fileClientMessageIndex = 710;
        ClientMessage encoded = ScheduledExecutorGetAllScheduledFuturesCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorGetAllScheduledFuturesCodec_decodeResponse() {
        int fileClientMessageIndex = 711;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfScheduledTaskHandler, ScheduledExecutorGetAllScheduledFuturesCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ScheduledExecutorGetStatsFromPartitionCodec_encodeRequest() {
        int fileClientMessageIndex = 712;
        ClientMessage encoded = ScheduledExecutorGetStatsFromPartitionCodec.encodeRequest(aString, aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorGetStatsFromPartitionCodec_decodeResponse() {
        int fileClientMessageIndex = 713;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ScheduledExecutorGetStatsFromPartitionCodec.ResponseParameters parameters = ScheduledExecutorGetStatsFromPartitionCodec.decodeResponse(fromFile);
        assertTrue(isEqual(aLong, parameters.lastIdleTimeNanos));
        assertTrue(isEqual(aLong, parameters.totalIdleTimeNanos));
        assertTrue(isEqual(aLong, parameters.totalRuns));
        assertTrue(isEqual(aLong, parameters.totalRunTimeNanos));
        assertTrue(isEqual(aLong, parameters.lastRunDurationNanos));
    }

    @Test
    public void test_ScheduledExecutorGetStatsFromMemberCodec_encodeRequest() {
        int fileClientMessageIndex = 714;
        ClientMessage encoded = ScheduledExecutorGetStatsFromMemberCodec.encodeRequest(aString, aString, aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorGetStatsFromMemberCodec_decodeResponse() {
        int fileClientMessageIndex = 715;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ScheduledExecutorGetStatsFromMemberCodec.ResponseParameters parameters = ScheduledExecutorGetStatsFromMemberCodec.decodeResponse(fromFile);
        assertTrue(isEqual(aLong, parameters.lastIdleTimeNanos));
        assertTrue(isEqual(aLong, parameters.totalIdleTimeNanos));
        assertTrue(isEqual(aLong, parameters.totalRuns));
        assertTrue(isEqual(aLong, parameters.totalRunTimeNanos));
        assertTrue(isEqual(aLong, parameters.lastRunDurationNanos));
    }

    @Test
    public void test_ScheduledExecutorGetDelayFromPartitionCodec_encodeRequest() {
        int fileClientMessageIndex = 716;
        ClientMessage encoded = ScheduledExecutorGetDelayFromPartitionCodec.encodeRequest(aString, aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorGetDelayFromPartitionCodec_decodeResponse() {
        int fileClientMessageIndex = 717;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aLong, ScheduledExecutorGetDelayFromPartitionCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ScheduledExecutorGetDelayFromMemberCodec_encodeRequest() {
        int fileClientMessageIndex = 718;
        ClientMessage encoded = ScheduledExecutorGetDelayFromMemberCodec.encodeRequest(aString, aString, aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorGetDelayFromMemberCodec_decodeResponse() {
        int fileClientMessageIndex = 719;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aLong, ScheduledExecutorGetDelayFromMemberCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ScheduledExecutorCancelFromPartitionCodec_encodeRequest() {
        int fileClientMessageIndex = 720;
        ClientMessage encoded = ScheduledExecutorCancelFromPartitionCodec.encodeRequest(aString, aString, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorCancelFromPartitionCodec_decodeResponse() {
        int fileClientMessageIndex = 721;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ScheduledExecutorCancelFromPartitionCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ScheduledExecutorCancelFromMemberCodec_encodeRequest() {
        int fileClientMessageIndex = 722;
        ClientMessage encoded = ScheduledExecutorCancelFromMemberCodec.encodeRequest(aString, aString, aUUID, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorCancelFromMemberCodec_decodeResponse() {
        int fileClientMessageIndex = 723;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ScheduledExecutorCancelFromMemberCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ScheduledExecutorIsCancelledFromPartitionCodec_encodeRequest() {
        int fileClientMessageIndex = 724;
        ClientMessage encoded = ScheduledExecutorIsCancelledFromPartitionCodec.encodeRequest(aString, aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorIsCancelledFromPartitionCodec_decodeResponse() {
        int fileClientMessageIndex = 725;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ScheduledExecutorIsCancelledFromPartitionCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ScheduledExecutorIsCancelledFromMemberCodec_encodeRequest() {
        int fileClientMessageIndex = 726;
        ClientMessage encoded = ScheduledExecutorIsCancelledFromMemberCodec.encodeRequest(aString, aString, aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorIsCancelledFromMemberCodec_decodeResponse() {
        int fileClientMessageIndex = 727;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ScheduledExecutorIsCancelledFromMemberCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ScheduledExecutorIsDoneFromPartitionCodec_encodeRequest() {
        int fileClientMessageIndex = 728;
        ClientMessage encoded = ScheduledExecutorIsDoneFromPartitionCodec.encodeRequest(aString, aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorIsDoneFromPartitionCodec_decodeResponse() {
        int fileClientMessageIndex = 729;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ScheduledExecutorIsDoneFromPartitionCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ScheduledExecutorIsDoneFromMemberCodec_encodeRequest() {
        int fileClientMessageIndex = 730;
        ClientMessage encoded = ScheduledExecutorIsDoneFromMemberCodec.encodeRequest(aString, aString, aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorIsDoneFromMemberCodec_decodeResponse() {
        int fileClientMessageIndex = 731;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ScheduledExecutorIsDoneFromMemberCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ScheduledExecutorGetResultFromPartitionCodec_encodeRequest() {
        int fileClientMessageIndex = 732;
        ClientMessage encoded = ScheduledExecutorGetResultFromPartitionCodec.encodeRequest(aString, aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorGetResultFromPartitionCodec_decodeResponse() {
        int fileClientMessageIndex = 733;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, ScheduledExecutorGetResultFromPartitionCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ScheduledExecutorGetResultFromMemberCodec_encodeRequest() {
        int fileClientMessageIndex = 734;
        ClientMessage encoded = ScheduledExecutorGetResultFromMemberCodec.encodeRequest(aString, aString, aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorGetResultFromMemberCodec_decodeResponse() {
        int fileClientMessageIndex = 735;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, ScheduledExecutorGetResultFromMemberCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ScheduledExecutorDisposeFromPartitionCodec_encodeRequest() {
        int fileClientMessageIndex = 736;
        ClientMessage encoded = ScheduledExecutorDisposeFromPartitionCodec.encodeRequest(aString, aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorDisposeFromPartitionCodec_decodeResponse() {
        int fileClientMessageIndex = 737;
    }

    @Test
    public void test_ScheduledExecutorDisposeFromMemberCodec_encodeRequest() {
        int fileClientMessageIndex = 738;
        ClientMessage encoded = ScheduledExecutorDisposeFromMemberCodec.encodeRequest(aString, aString, aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorDisposeFromMemberCodec_decodeResponse() {
        int fileClientMessageIndex = 739;
    }

    @Test
    public void test_DynamicConfigAddMultiMapConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 740;
        ClientMessage encoded = DynamicConfigAddMultiMapConfigCodec.encodeRequest(aString, aString, null, aBoolean, anInt, anInt, aBoolean, null, aString, anInt, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddMultiMapConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 741;
    }

    @Test
    public void test_DynamicConfigAddRingbufferConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 742;
        ClientMessage encoded = DynamicConfigAddRingbufferConfigCodec.encodeRequest(aString, anInt, anInt, anInt, anInt, aString, null, null, aString, anInt, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddRingbufferConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 743;
    }

    @Test
    public void test_DynamicConfigAddCardinalityEstimatorConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 744;
        ClientMessage encoded = DynamicConfigAddCardinalityEstimatorConfigCodec.encodeRequest(aString, anInt, anInt, null, aString, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddCardinalityEstimatorConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 745;
    }

    @Test
    public void test_DynamicConfigAddListConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 746;
        ClientMessage encoded = DynamicConfigAddListConfigCodec.encodeRequest(aString, null, anInt, anInt, anInt, aBoolean, null, aString, anInt, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddListConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 747;
    }

    @Test
    public void test_DynamicConfigAddSetConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 748;
        ClientMessage encoded = DynamicConfigAddSetConfigCodec.encodeRequest(aString, null, anInt, anInt, anInt, aBoolean, null, aString, anInt, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddSetConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 749;
    }

    @Test
    public void test_DynamicConfigAddReplicatedMapConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 750;
        ClientMessage encoded = DynamicConfigAddReplicatedMapConfigCodec.encodeRequest(aString, aString, aBoolean, aBoolean, aString, null, null, anInt, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddReplicatedMapConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 751;
    }

    @Test
    public void test_DynamicConfigAddTopicConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 752;
        ClientMessage encoded = DynamicConfigAddTopicConfigCodec.encodeRequest(aString, aBoolean, aBoolean, aBoolean, null, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddTopicConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 753;
    }

    @Test
    public void test_DynamicConfigAddExecutorConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 754;
        ClientMessage encoded = DynamicConfigAddExecutorConfigCodec.encodeRequest(aString, anInt, anInt, aBoolean, null, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddExecutorConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 755;
    }

    @Test
    public void test_DynamicConfigAddDurableExecutorConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 756;
        ClientMessage encoded = DynamicConfigAddDurableExecutorConfigCodec.encodeRequest(aString, anInt, anInt, anInt, null, aBoolean, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddDurableExecutorConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 757;
    }

    @Test
    public void test_DynamicConfigAddScheduledExecutorConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 758;
        ClientMessage encoded = DynamicConfigAddScheduledExecutorConfigCodec.encodeRequest(aString, anInt, anInt, anInt, null, aString, anInt, aBoolean, aByte, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddScheduledExecutorConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 759;
    }

    @Test
    public void test_DynamicConfigAddQueueConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 760;
        ClientMessage encoded = DynamicConfigAddQueueConfigCodec.encodeRequest(aString, null, anInt, anInt, anInt, anInt, aBoolean, null, null, aString, anInt, null, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddQueueConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 761;
    }

    @Test
    public void test_DynamicConfigAddMapConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 762;
        ClientMessage encoded = DynamicConfigAddMapConfigCodec.encodeRequest(aString, anInt, anInt, anInt, anInt, null, aBoolean, aString, aString, anInt, aString, null, null, aBoolean, null, null, null, null, null, null, null, null, null, null, null, null, anInt, aBoolean, aDataPersistenceConfig, aTieredStoreConfig, null, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddMapConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 763;
    }

    @Test
    public void test_DynamicConfigAddReliableTopicConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 764;
        ClientMessage encoded = DynamicConfigAddReliableTopicConfigCodec.encodeRequest(aString, null, anInt, aBoolean, aString, null, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddReliableTopicConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 765;
    }

    @Test
    public void test_DynamicConfigAddCacheConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 766;
        ClientMessage encoded = DynamicConfigAddCacheConfigCodec.encodeRequest(aString, null, null, aBoolean, aBoolean, aBoolean, aBoolean, null, null, null, null, anInt, anInt, aString, null, null, anInt, aBoolean, null, null, null, null, null, null, null, null, null, aDataPersistenceConfig, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddCacheConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 767;
    }

    @Test
    public void test_DynamicConfigAddFlakeIdGeneratorConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 768;
        ClientMessage encoded = DynamicConfigAddFlakeIdGeneratorConfigCodec.encodeRequest(aString, anInt, aLong, aBoolean, aLong, aLong, anInt, anInt, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddFlakeIdGeneratorConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 769;
    }

    @Test
    public void test_DynamicConfigAddPNCounterConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 770;
        ClientMessage encoded = DynamicConfigAddPNCounterConfigCodec.encodeRequest(aString, anInt, aBoolean, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddPNCounterConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 771;
    }

    @Test
    public void test_DynamicConfigAddDataConnectionConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 772;
        ClientMessage encoded = DynamicConfigAddDataConnectionConfigCodec.encodeRequest(aString, aString, aBoolean, aMapOfStringToString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddDataConnectionConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 773;
    }

    @Test
    public void test_DynamicConfigAddWanReplicationConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 774;
        ClientMessage encoded = DynamicConfigAddWanReplicationConfigCodec.encodeRequest(aString, null, aListOfWanCustomPublisherConfigsHolders, aListOfWanBatchPublisherConfigHolders);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddWanReplicationConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 775;
    }

    @Test
    public void test_DynamicConfigAddUserCodeNamespaceConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 776;
        ClientMessage encoded = DynamicConfigAddUserCodeNamespaceConfigCodec.encodeRequest(aString, aListOfResourceDefinitionHolders);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddUserCodeNamespaceConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 777;
    }

    @Test
    public void test_DynamicConfigAddVectorCollectionConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 778;
        ClientMessage encoded = DynamicConfigAddVectorCollectionConfigCodec.encodeRequest(aString, aList_VectorIndexConfig, anInt, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddVectorCollectionConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 779;
    }

    @Test
    public void test_FlakeIdGeneratorNewIdBatchCodec_encodeRequest() {
        int fileClientMessageIndex = 780;
        ClientMessage encoded = FlakeIdGeneratorNewIdBatchCodec.encodeRequest(aString, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_FlakeIdGeneratorNewIdBatchCodec_decodeResponse() {
        int fileClientMessageIndex = 781;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        FlakeIdGeneratorNewIdBatchCodec.ResponseParameters parameters = FlakeIdGeneratorNewIdBatchCodec.decodeResponse(fromFile);
        assertTrue(isEqual(aLong, parameters.base));
        assertTrue(isEqual(aLong, parameters.increment));
        assertTrue(isEqual(anInt, parameters.batchSize));
    }

    @Test
    public void test_PNCounterGetCodec_encodeRequest() {
        int fileClientMessageIndex = 782;
        ClientMessage encoded = PNCounterGetCodec.encodeRequest(aString, aListOfUuidToLong, aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_PNCounterGetCodec_decodeResponse() {
        int fileClientMessageIndex = 783;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        PNCounterGetCodec.ResponseParameters parameters = PNCounterGetCodec.decodeResponse(fromFile);
        assertTrue(isEqual(aLong, parameters.value));
        assertTrue(isEqual(aListOfUuidToLong, parameters.replicaTimestamps));
        assertTrue(isEqual(anInt, parameters.replicaCount));
    }

    @Test
    public void test_PNCounterAddCodec_encodeRequest() {
        int fileClientMessageIndex = 784;
        ClientMessage encoded = PNCounterAddCodec.encodeRequest(aString, aLong, aBoolean, aListOfUuidToLong, aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_PNCounterAddCodec_decodeResponse() {
        int fileClientMessageIndex = 785;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        PNCounterAddCodec.ResponseParameters parameters = PNCounterAddCodec.decodeResponse(fromFile);
        assertTrue(isEqual(aLong, parameters.value));
        assertTrue(isEqual(aListOfUuidToLong, parameters.replicaTimestamps));
        assertTrue(isEqual(anInt, parameters.replicaCount));
    }

    @Test
    public void test_PNCounterGetConfiguredReplicaCountCodec_encodeRequest() {
        int fileClientMessageIndex = 786;
        ClientMessage encoded = PNCounterGetConfiguredReplicaCountCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_PNCounterGetConfiguredReplicaCountCodec_decodeResponse() {
        int fileClientMessageIndex = 787;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(anInt, PNCounterGetConfiguredReplicaCountCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CPGroupCreateCPGroupCodec_encodeRequest() {
        int fileClientMessageIndex = 788;
        ClientMessage encoded = CPGroupCreateCPGroupCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CPGroupCreateCPGroupCodec_decodeResponse() {
        int fileClientMessageIndex = 789;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aRaftGroupId, CPGroupCreateCPGroupCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CPGroupDestroyCPObjectCodec_encodeRequest() {
        int fileClientMessageIndex = 790;
        ClientMessage encoded = CPGroupDestroyCPObjectCodec.encodeRequest(aRaftGroupId, aString, aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CPGroupDestroyCPObjectCodec_decodeResponse() {
        int fileClientMessageIndex = 791;
    }

    @Test
    public void test_CPSessionCreateSessionCodec_encodeRequest() {
        int fileClientMessageIndex = 792;
        ClientMessage encoded = CPSessionCreateSessionCodec.encodeRequest(aRaftGroupId, aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CPSessionCreateSessionCodec_decodeResponse() {
        int fileClientMessageIndex = 793;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CPSessionCreateSessionCodec.ResponseParameters parameters = CPSessionCreateSessionCodec.decodeResponse(fromFile);
        assertTrue(isEqual(aLong, parameters.sessionId));
        assertTrue(isEqual(aLong, parameters.ttlMillis));
        assertTrue(isEqual(aLong, parameters.heartbeatMillis));
    }

    @Test
    public void test_CPSessionCloseSessionCodec_encodeRequest() {
        int fileClientMessageIndex = 794;
        ClientMessage encoded = CPSessionCloseSessionCodec.encodeRequest(aRaftGroupId, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CPSessionCloseSessionCodec_decodeResponse() {
        int fileClientMessageIndex = 795;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, CPSessionCloseSessionCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CPSessionHeartbeatSessionCodec_encodeRequest() {
        int fileClientMessageIndex = 796;
        ClientMessage encoded = CPSessionHeartbeatSessionCodec.encodeRequest(aRaftGroupId, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CPSessionHeartbeatSessionCodec_decodeResponse() {
        int fileClientMessageIndex = 797;
    }

    @Test
    public void test_CPSessionGenerateThreadIdCodec_encodeRequest() {
        int fileClientMessageIndex = 798;
        ClientMessage encoded = CPSessionGenerateThreadIdCodec.encodeRequest(aRaftGroupId);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CPSessionGenerateThreadIdCodec_decodeResponse() {
        int fileClientMessageIndex = 799;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aLong, CPSessionGenerateThreadIdCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MCReadMetricsCodec_encodeRequest() {
        int fileClientMessageIndex = 800;
        ClientMessage encoded = MCReadMetricsCodec.encodeRequest(aUUID, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCReadMetricsCodec_decodeResponse() {
        int fileClientMessageIndex = 801;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MCReadMetricsCodec.ResponseParameters parameters = MCReadMetricsCodec.decodeResponse(fromFile);
        assertTrue(isEqual(aListOfLongToByteArray, parameters.elements));
        assertTrue(isEqual(aLong, parameters.nextSequence));
    }

    @Test
    public void test_MCChangeClusterStateCodec_encodeRequest() {
        int fileClientMessageIndex = 802;
        ClientMessage encoded = MCChangeClusterStateCodec.encodeRequest(anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCChangeClusterStateCodec_decodeResponse() {
        int fileClientMessageIndex = 803;
    }

    @Test
    public void test_MCGetMapConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 804;
        ClientMessage encoded = MCGetMapConfigCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCGetMapConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 805;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MCGetMapConfigCodec.ResponseParameters parameters = MCGetMapConfigCodec.decodeResponse(fromFile);
        assertTrue(isEqual(anInt, parameters.inMemoryFormat));
        assertTrue(isEqual(anInt, parameters.backupCount));
        assertTrue(isEqual(anInt, parameters.asyncBackupCount));
        assertTrue(isEqual(anInt, parameters.timeToLiveSeconds));
        assertTrue(isEqual(anInt, parameters.maxIdleSeconds));
        assertTrue(isEqual(anInt, parameters.maxSize));
        assertTrue(isEqual(anInt, parameters.maxSizePolicy));
        assertTrue(isEqual(aBoolean, parameters.readBackupData));
        assertTrue(isEqual(anInt, parameters.evictionPolicy));
        assertTrue(isEqual(aString, parameters.mergePolicy));
        assertTrue(parameters.isGlobalIndexesExists);
        assertTrue(isEqual(aListOfIndexConfigs, parameters.globalIndexes));
        assertTrue(parameters.isWanReplicationRefExists);
        assertTrue(isEqual(null, parameters.wanReplicationRef));
    }

    @Test
    public void test_MCUpdateMapConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 806;
        ClientMessage encoded = MCUpdateMapConfigCodec.encodeRequest(aString, anInt, anInt, anInt, aBoolean, anInt, anInt, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCUpdateMapConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 807;
    }

    @Test
    public void test_MCGetMemberConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 808;
        ClientMessage encoded = MCGetMemberConfigCodec.encodeRequest();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCGetMemberConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 809;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, MCGetMemberConfigCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MCRunGcCodec_encodeRequest() {
        int fileClientMessageIndex = 810;
        ClientMessage encoded = MCRunGcCodec.encodeRequest();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCRunGcCodec_decodeResponse() {
        int fileClientMessageIndex = 811;
    }

    @Test
    public void test_MCGetThreadDumpCodec_encodeRequest() {
        int fileClientMessageIndex = 812;
        ClientMessage encoded = MCGetThreadDumpCodec.encodeRequest(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCGetThreadDumpCodec_decodeResponse() {
        int fileClientMessageIndex = 813;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, MCGetThreadDumpCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MCShutdownMemberCodec_encodeRequest() {
        int fileClientMessageIndex = 814;
        ClientMessage encoded = MCShutdownMemberCodec.encodeRequest();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCShutdownMemberCodec_decodeResponse() {
        int fileClientMessageIndex = 815;
    }

    @Test
    public void test_MCPromoteLiteMemberCodec_encodeRequest() {
        int fileClientMessageIndex = 816;
        ClientMessage encoded = MCPromoteLiteMemberCodec.encodeRequest();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCPromoteLiteMemberCodec_decodeResponse() {
        int fileClientMessageIndex = 817;
    }

    @Test
    public void test_MCGetSystemPropertiesCodec_encodeRequest() {
        int fileClientMessageIndex = 818;
        ClientMessage encoded = MCGetSystemPropertiesCodec.encodeRequest();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCGetSystemPropertiesCodec_decodeResponse() {
        int fileClientMessageIndex = 819;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfStringToString, MCGetSystemPropertiesCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MCGetTimedMemberStateCodec_encodeRequest() {
        int fileClientMessageIndex = 820;
        ClientMessage encoded = MCGetTimedMemberStateCodec.encodeRequest();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCGetTimedMemberStateCodec_decodeResponse() {
        int fileClientMessageIndex = 821;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, MCGetTimedMemberStateCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MCMatchMCConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 822;
        ClientMessage encoded = MCMatchMCConfigCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCMatchMCConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 823;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, MCMatchMCConfigCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MCApplyMCConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 824;
        ClientMessage encoded = MCApplyMCConfigCodec.encodeRequest(aString, anInt, aListOfClientBwListEntries);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCApplyMCConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 825;
    }

    @Test
    public void test_MCGetClusterMetadataCodec_encodeRequest() {
        int fileClientMessageIndex = 826;
        ClientMessage encoded = MCGetClusterMetadataCodec.encodeRequest();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCGetClusterMetadataCodec_decodeResponse() {
        int fileClientMessageIndex = 827;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MCGetClusterMetadataCodec.ResponseParameters parameters = MCGetClusterMetadataCodec.decodeResponse(fromFile);
        assertTrue(isEqual(aByte, parameters.currentState));
        assertTrue(isEqual(aString, parameters.memberVersion));
        assertTrue(isEqual(null, parameters.jetVersion));
        assertTrue(isEqual(aLong, parameters.clusterTime));
        assertTrue(parameters.isClusterIdExists);
        assertTrue(isEqual(aUUID, parameters.clusterId));
    }

    @Test
    public void test_MCShutdownClusterCodec_encodeRequest() {
        int fileClientMessageIndex = 828;
        ClientMessage encoded = MCShutdownClusterCodec.encodeRequest();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCShutdownClusterCodec_decodeResponse() {
        int fileClientMessageIndex = 829;
    }

    @Test
    public void test_MCChangeClusterVersionCodec_encodeRequest() {
        int fileClientMessageIndex = 830;
        ClientMessage encoded = MCChangeClusterVersionCodec.encodeRequest(aByte, aByte);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCChangeClusterVersionCodec_decodeResponse() {
        int fileClientMessageIndex = 831;
    }

    @Test
    public void test_MCRunScriptCodec_encodeRequest() {
        int fileClientMessageIndex = 832;
        ClientMessage encoded = MCRunScriptCodec.encodeRequest(aString, aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCRunScriptCodec_decodeResponse() {
        int fileClientMessageIndex = 833;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, MCRunScriptCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MCRunConsoleCommandCodec_encodeRequest() {
        int fileClientMessageIndex = 834;
        ClientMessage encoded = MCRunConsoleCommandCodec.encodeRequest(null, aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCRunConsoleCommandCodec_decodeResponse() {
        int fileClientMessageIndex = 835;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, MCRunConsoleCommandCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MCChangeWanReplicationStateCodec_encodeRequest() {
        int fileClientMessageIndex = 836;
        ClientMessage encoded = MCChangeWanReplicationStateCodec.encodeRequest(aString, aString, aByte);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCChangeWanReplicationStateCodec_decodeResponse() {
        int fileClientMessageIndex = 837;
    }

    @Test
    public void test_MCClearWanQueuesCodec_encodeRequest() {
        int fileClientMessageIndex = 838;
        ClientMessage encoded = MCClearWanQueuesCodec.encodeRequest(aString, aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCClearWanQueuesCodec_decodeResponse() {
        int fileClientMessageIndex = 839;
    }

    @Test
    public void test_MCAddWanBatchPublisherConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 840;
        ClientMessage encoded = MCAddWanBatchPublisherConfigCodec.encodeRequest(aString, aString, null, aString, anInt, anInt, anInt, anInt, anInt, anInt, aByte);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCAddWanBatchPublisherConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 841;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MCAddWanBatchPublisherConfigCodec.ResponseParameters parameters = MCAddWanBatchPublisherConfigCodec.decodeResponse(fromFile);
        assertTrue(isEqual(aListOfStrings, parameters.addedPublisherIds));
        assertTrue(isEqual(aListOfStrings, parameters.ignoredPublisherIds));
    }

    @Test
    public void test_MCWanSyncMapCodec_encodeRequest() {
        int fileClientMessageIndex = 842;
        ClientMessage encoded = MCWanSyncMapCodec.encodeRequest(aString, aString, anInt, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCWanSyncMapCodec_decodeResponse() {
        int fileClientMessageIndex = 843;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, MCWanSyncMapCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MCCheckWanConsistencyCodec_encodeRequest() {
        int fileClientMessageIndex = 844;
        ClientMessage encoded = MCCheckWanConsistencyCodec.encodeRequest(aString, aString, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCCheckWanConsistencyCodec_decodeResponse() {
        int fileClientMessageIndex = 845;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, MCCheckWanConsistencyCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MCPollMCEventsCodec_encodeRequest() {
        int fileClientMessageIndex = 846;
        ClientMessage encoded = MCPollMCEventsCodec.encodeRequest();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCPollMCEventsCodec_decodeResponse() {
        int fileClientMessageIndex = 847;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfMCEvents, MCPollMCEventsCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MCGetCPMembersCodec_encodeRequest() {
        int fileClientMessageIndex = 848;
        ClientMessage encoded = MCGetCPMembersCodec.encodeRequest();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCGetCPMembersCodec_decodeResponse() {
        int fileClientMessageIndex = 849;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfUUIDToUUID, MCGetCPMembersCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MCPromoteToCPMemberCodec_encodeRequest() {
        int fileClientMessageIndex = 850;
        ClientMessage encoded = MCPromoteToCPMemberCodec.encodeRequest();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCPromoteToCPMemberCodec_decodeResponse() {
        int fileClientMessageIndex = 851;
    }

    @Test
    public void test_MCRemoveCPMemberCodec_encodeRequest() {
        int fileClientMessageIndex = 852;
        ClientMessage encoded = MCRemoveCPMemberCodec.encodeRequest(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCRemoveCPMemberCodec_decodeResponse() {
        int fileClientMessageIndex = 853;
    }

    @Test
    public void test_MCResetCPSubsystemCodec_encodeRequest() {
        int fileClientMessageIndex = 854;
        ClientMessage encoded = MCResetCPSubsystemCodec.encodeRequest();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCResetCPSubsystemCodec_decodeResponse() {
        int fileClientMessageIndex = 855;
    }

    @Test
    public void test_MCTriggerPartialStartCodec_encodeRequest() {
        int fileClientMessageIndex = 856;
        ClientMessage encoded = MCTriggerPartialStartCodec.encodeRequest();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCTriggerPartialStartCodec_decodeResponse() {
        int fileClientMessageIndex = 857;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, MCTriggerPartialStartCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MCTriggerForceStartCodec_encodeRequest() {
        int fileClientMessageIndex = 858;
        ClientMessage encoded = MCTriggerForceStartCodec.encodeRequest();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCTriggerForceStartCodec_decodeResponse() {
        int fileClientMessageIndex = 859;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, MCTriggerForceStartCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MCTriggerHotRestartBackupCodec_encodeRequest() {
        int fileClientMessageIndex = 860;
        ClientMessage encoded = MCTriggerHotRestartBackupCodec.encodeRequest();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCTriggerHotRestartBackupCodec_decodeResponse() {
        int fileClientMessageIndex = 861;
    }

    @Test
    public void test_MCInterruptHotRestartBackupCodec_encodeRequest() {
        int fileClientMessageIndex = 862;
        ClientMessage encoded = MCInterruptHotRestartBackupCodec.encodeRequest();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCInterruptHotRestartBackupCodec_decodeResponse() {
        int fileClientMessageIndex = 863;
    }

    @Test
    public void test_MCResetQueueAgeStatisticsCodec_encodeRequest() {
        int fileClientMessageIndex = 864;
        ClientMessage encoded = MCResetQueueAgeStatisticsCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCResetQueueAgeStatisticsCodec_decodeResponse() {
        int fileClientMessageIndex = 865;
    }

    @Test
    public void test_MCReloadConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 866;
        ClientMessage encoded = MCReloadConfigCodec.encodeRequest();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCReloadConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 867;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, MCReloadConfigCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MCUpdateConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 868;
        ClientMessage encoded = MCUpdateConfigCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCUpdateConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 869;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, MCUpdateConfigCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MCForceCloseCPSessionCodec_encodeRequest() {
        int fileClientMessageIndex = 870;
        ClientMessage encoded = MCForceCloseCPSessionCodec.encodeRequest(aString, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCForceCloseCPSessionCodec_decodeResponse() {
        int fileClientMessageIndex = 871;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, MCForceCloseCPSessionCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_MCDemoteDataMemberCodec_encodeRequest() {
        int fileClientMessageIndex = 872;
        ClientMessage encoded = MCDemoteDataMemberCodec.encodeRequest();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCDemoteDataMemberCodec_decodeResponse() {
        int fileClientMessageIndex = 873;
    }

    @Test
    public void test_SqlExecute_reservedCodec_encodeRequest() {
        int fileClientMessageIndex = 874;
        ClientMessage encoded = SqlExecute_reservedCodec.encodeRequest(aString, aListOfData, aLong, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SqlExecute_reservedCodec_decodeResponse() {
        int fileClientMessageIndex = 875;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        SqlExecute_reservedCodec.ResponseParameters parameters = SqlExecute_reservedCodec.decodeResponse(fromFile);
        assertTrue(isEqual(null, parameters.queryId));
        assertTrue(isEqual(null, parameters.rowMetadata));
        assertTrue(isEqual(null, parameters.rowPage));
        assertTrue(isEqual(aBoolean, parameters.rowPageLast));
        assertTrue(isEqual(aLong, parameters.updateCount));
        assertTrue(isEqual(null, parameters.error));
    }

    @Test
    public void test_SqlFetch_reservedCodec_encodeRequest() {
        int fileClientMessageIndex = 876;
        ClientMessage encoded = SqlFetch_reservedCodec.encodeRequest(anSqlQueryId, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SqlFetch_reservedCodec_decodeResponse() {
        int fileClientMessageIndex = 877;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        SqlFetch_reservedCodec.ResponseParameters parameters = SqlFetch_reservedCodec.decodeResponse(fromFile);
        assertTrue(isEqual(null, parameters.rowPage));
        assertTrue(isEqual(aBoolean, parameters.rowPageLast));
        assertTrue(isEqual(null, parameters.error));
    }

    @Test
    public void test_SqlCloseCodec_encodeRequest() {
        int fileClientMessageIndex = 878;
        ClientMessage encoded = SqlCloseCodec.encodeRequest(anSqlQueryId);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SqlCloseCodec_decodeResponse() {
        int fileClientMessageIndex = 879;
    }

    @Test
    public void test_SqlExecuteCodec_encodeRequest() {
        int fileClientMessageIndex = 880;
        ClientMessage encoded = SqlExecuteCodec.encodeRequest(aString, aListOfData, aLong, anInt, null, aByte, anSqlQueryId, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SqlExecuteCodec_decodeResponse() {
        int fileClientMessageIndex = 881;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        SqlExecuteCodec.ResponseParameters parameters = SqlExecuteCodec.decodeResponse(fromFile);
        assertTrue(isEqual(null, parameters.rowMetadata));
        assertTrue(isEqual(null, parameters.rowPage));
        assertTrue(isEqual(aLong, parameters.updateCount));
        assertTrue(isEqual(null, parameters.error));
        assertTrue(parameters.isIsInfiniteRowsExists);
        assertTrue(isEqual(aBoolean, parameters.isInfiniteRows));
        assertTrue(parameters.isPartitionArgumentIndexExists);
        assertTrue(isEqual(anInt, parameters.partitionArgumentIndex));
    }

    @Test
    public void test_SqlFetchCodec_encodeRequest() {
        int fileClientMessageIndex = 882;
        ClientMessage encoded = SqlFetchCodec.encodeRequest(anSqlQueryId, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SqlFetchCodec_decodeResponse() {
        int fileClientMessageIndex = 883;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        SqlFetchCodec.ResponseParameters parameters = SqlFetchCodec.decodeResponse(fromFile);
        assertTrue(isEqual(null, parameters.rowPage));
        assertTrue(isEqual(null, parameters.error));
    }

    @Test
    public void test_SqlMappingDdlCodec_encodeRequest() {
        int fileClientMessageIndex = 884;
        ClientMessage encoded = SqlMappingDdlCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SqlMappingDdlCodec_decodeResponse() {
        int fileClientMessageIndex = 885;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, SqlMappingDdlCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CPSubsystemAddMembershipListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 886;
        ClientMessage encoded = CPSubsystemAddMembershipListenerCodec.encodeRequest(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CPSubsystemAddMembershipListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 887;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, CPSubsystemAddMembershipListenerCodec.decodeResponse(fromFile)));
    }

    private static class CPSubsystemAddMembershipListenerCodecHandler extends CPSubsystemAddMembershipListenerCodec.AbstractEventHandler {
        @Override
        public void handleMembershipEventEvent(com.hazelcast.cp.CPMember member, byte type) {
            assertTrue(isEqual(aCpMember, member));
            assertTrue(isEqual(aByte, type));
        }
    }

    @Test
    public void test_CPSubsystemAddMembershipListenerCodec_handleMembershipEventEvent() {
        int fileClientMessageIndex = 888;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CPSubsystemAddMembershipListenerCodecHandler handler = new CPSubsystemAddMembershipListenerCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_CPSubsystemRemoveMembershipListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 889;
        ClientMessage encoded = CPSubsystemRemoveMembershipListenerCodec.encodeRequest(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CPSubsystemRemoveMembershipListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 890;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, CPSubsystemRemoveMembershipListenerCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CPSubsystemAddGroupAvailabilityListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 891;
        ClientMessage encoded = CPSubsystemAddGroupAvailabilityListenerCodec.encodeRequest(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CPSubsystemAddGroupAvailabilityListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 892;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, CPSubsystemAddGroupAvailabilityListenerCodec.decodeResponse(fromFile)));
    }

    private static class CPSubsystemAddGroupAvailabilityListenerCodecHandler extends CPSubsystemAddGroupAvailabilityListenerCodec.AbstractEventHandler {
        @Override
        public void handleGroupAvailabilityEventEvent(com.hazelcast.cp.internal.RaftGroupId groupId, java.util.Collection<com.hazelcast.cp.CPMember> members, java.util.Collection<com.hazelcast.cp.CPMember> unavailableMembers, boolean isIsShutdownExists, boolean isShutdown) {
            assertTrue(isEqual(aRaftGroupId, groupId));
            assertTrue(isEqual(aListOfCpMembers, members));
            assertTrue(isEqual(aListOfCpMembers, unavailableMembers));
            assertTrue(isIsShutdownExists);
            assertTrue(isEqual(aBoolean, isShutdown));
        }
    }

    @Test
    public void test_CPSubsystemAddGroupAvailabilityListenerCodec_handleGroupAvailabilityEventEvent() {
        int fileClientMessageIndex = 893;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CPSubsystemAddGroupAvailabilityListenerCodecHandler handler = new CPSubsystemAddGroupAvailabilityListenerCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_CPSubsystemRemoveGroupAvailabilityListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 894;
        ClientMessage encoded = CPSubsystemRemoveGroupAvailabilityListenerCodec.encodeRequest(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CPSubsystemRemoveGroupAvailabilityListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 895;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, CPSubsystemRemoveGroupAvailabilityListenerCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CPSubsystemGetCPGroupIdsCodec_encodeRequest() {
        int fileClientMessageIndex = 896;
        ClientMessage encoded = CPSubsystemGetCPGroupIdsCodec.encodeRequest();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CPSubsystemGetCPGroupIdsCodec_decodeResponse() {
        int fileClientMessageIndex = 897;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfRaftGroupIds, CPSubsystemGetCPGroupIdsCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CPSubsystemGetCPObjectInfosCodec_encodeRequest() {
        int fileClientMessageIndex = 898;
        ClientMessage encoded = CPSubsystemGetCPObjectInfosCodec.encodeRequest(aRaftGroupId, aString, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CPSubsystemGetCPObjectInfosCodec_decodeResponse() {
        int fileClientMessageIndex = 899;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfStrings, CPSubsystemGetCPObjectInfosCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CPMapGetCodec_encodeRequest() {
        int fileClientMessageIndex = 900;
        ClientMessage encoded = CPMapGetCodec.encodeRequest(aRaftGroupId, aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CPMapGetCodec_decodeResponse() {
        int fileClientMessageIndex = 901;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, CPMapGetCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CPMapPutCodec_encodeRequest() {
        int fileClientMessageIndex = 902;
        ClientMessage encoded = CPMapPutCodec.encodeRequest(aRaftGroupId, aString, aData, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CPMapPutCodec_decodeResponse() {
        int fileClientMessageIndex = 903;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, CPMapPutCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CPMapSetCodec_encodeRequest() {
        int fileClientMessageIndex = 904;
        ClientMessage encoded = CPMapSetCodec.encodeRequest(aRaftGroupId, aString, aData, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CPMapSetCodec_decodeResponse() {
        int fileClientMessageIndex = 905;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, CPMapSetCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CPMapRemoveCodec_encodeRequest() {
        int fileClientMessageIndex = 906;
        ClientMessage encoded = CPMapRemoveCodec.encodeRequest(aRaftGroupId, aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CPMapRemoveCodec_decodeResponse() {
        int fileClientMessageIndex = 907;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, CPMapRemoveCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CPMapDeleteCodec_encodeRequest() {
        int fileClientMessageIndex = 908;
        ClientMessage encoded = CPMapDeleteCodec.encodeRequest(aRaftGroupId, aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CPMapDeleteCodec_decodeResponse() {
        int fileClientMessageIndex = 909;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, CPMapDeleteCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CPMapCompareAndSetCodec_encodeRequest() {
        int fileClientMessageIndex = 910;
        ClientMessage encoded = CPMapCompareAndSetCodec.encodeRequest(aRaftGroupId, aString, aData, aData, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CPMapCompareAndSetCodec_decodeResponse() {
        int fileClientMessageIndex = 911;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, CPMapCompareAndSetCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_CPMapPutIfAbsentCodec_encodeRequest() {
        int fileClientMessageIndex = 912;
        ClientMessage encoded = CPMapPutIfAbsentCodec.encodeRequest(aRaftGroupId, aString, aData, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CPMapPutIfAbsentCodec_decodeResponse() {
        int fileClientMessageIndex = 913;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, CPMapPutIfAbsentCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_VectorCollectionPutCodec_encodeRequest() {
        int fileClientMessageIndex = 914;
        ClientMessage encoded = VectorCollectionPutCodec.encodeRequest(aString, aData, aVectorDocument);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_VectorCollectionPutCodec_decodeResponse() {
        int fileClientMessageIndex = 915;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, VectorCollectionPutCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_VectorCollectionPutIfAbsentCodec_encodeRequest() {
        int fileClientMessageIndex = 916;
        ClientMessage encoded = VectorCollectionPutIfAbsentCodec.encodeRequest(aString, aData, aVectorDocument);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_VectorCollectionPutIfAbsentCodec_decodeResponse() {
        int fileClientMessageIndex = 917;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, VectorCollectionPutIfAbsentCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_VectorCollectionPutAllCodec_encodeRequest() {
        int fileClientMessageIndex = 918;
        ClientMessage encoded = VectorCollectionPutAllCodec.encodeRequest(aString, aEntryList_Data_VectorDocument);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_VectorCollectionPutAllCodec_decodeResponse() {
        int fileClientMessageIndex = 919;
    }

    @Test
    public void test_VectorCollectionGetCodec_encodeRequest() {
        int fileClientMessageIndex = 920;
        ClientMessage encoded = VectorCollectionGetCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_VectorCollectionGetCodec_decodeResponse() {
        int fileClientMessageIndex = 921;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, VectorCollectionGetCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_VectorCollectionRemoveCodec_encodeRequest() {
        int fileClientMessageIndex = 922;
        ClientMessage encoded = VectorCollectionRemoveCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_VectorCollectionRemoveCodec_decodeResponse() {
        int fileClientMessageIndex = 923;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, VectorCollectionRemoveCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_VectorCollectionSetCodec_encodeRequest() {
        int fileClientMessageIndex = 924;
        ClientMessage encoded = VectorCollectionSetCodec.encodeRequest(aString, aData, aVectorDocument);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_VectorCollectionSetCodec_decodeResponse() {
        int fileClientMessageIndex = 925;
    }

    @Test
    public void test_VectorCollectionDeleteCodec_encodeRequest() {
        int fileClientMessageIndex = 926;
        ClientMessage encoded = VectorCollectionDeleteCodec.encodeRequest(aString, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_VectorCollectionDeleteCodec_decodeResponse() {
        int fileClientMessageIndex = 927;
    }

    @Test
    public void test_VectorCollectionSearchNearVectorCodec_encodeRequest() {
        int fileClientMessageIndex = 928;
        ClientMessage encoded = VectorCollectionSearchNearVectorCodec.encodeRequest(aString, aList_VectorPair, aVectorSearchOptions);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_VectorCollectionSearchNearVectorCodec_decodeResponse() {
        int fileClientMessageIndex = 929;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aList_VectorSearchResult, VectorCollectionSearchNearVectorCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_VectorCollectionOptimizeCodec_encodeRequest() {
        int fileClientMessageIndex = 930;
        ClientMessage encoded = VectorCollectionOptimizeCodec.encodeRequest(aString, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_VectorCollectionOptimizeCodec_decodeResponse() {
        int fileClientMessageIndex = 931;
    }

    @Test
    public void test_VectorCollectionClearCodec_encodeRequest() {
        int fileClientMessageIndex = 932;
        ClientMessage encoded = VectorCollectionClearCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_VectorCollectionClearCodec_decodeResponse() {
        int fileClientMessageIndex = 933;
    }

    @Test
    public void test_VectorCollectionSizeCodec_encodeRequest() {
        int fileClientMessageIndex = 934;
        ClientMessage encoded = VectorCollectionSizeCodec.encodeRequest(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_VectorCollectionSizeCodec_decodeResponse() {
        int fileClientMessageIndex = 935;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aLong, VectorCollectionSizeCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_ExperimentalPipelineSubmitCodec_encodeRequest() {
        int fileClientMessageIndex = 936;
        ClientMessage encoded = ExperimentalPipelineSubmitCodec.encodeRequest(null, aString, null, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ExperimentalPipelineSubmitCodec_decodeResponse() {
        int fileClientMessageIndex = 937;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aLong, ExperimentalPipelineSubmitCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_JetSubmitJobCodec_encodeRequest() {
        int fileClientMessageIndex = 938;
        ClientMessage encoded = JetSubmitJobCodec.encodeRequest(aLong, aData, null, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetSubmitJobCodec_decodeResponse() {
        int fileClientMessageIndex = 939;
    }

    @Test
    public void test_JetTerminateJobCodec_encodeRequest() {
        int fileClientMessageIndex = 940;
        ClientMessage encoded = JetTerminateJobCodec.encodeRequest(aLong, anInt, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetTerminateJobCodec_decodeResponse() {
        int fileClientMessageIndex = 941;
    }

    @Test
    public void test_JetGetJobStatusCodec_encodeRequest() {
        int fileClientMessageIndex = 942;
        ClientMessage encoded = JetGetJobStatusCodec.encodeRequest(aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetGetJobStatusCodec_decodeResponse() {
        int fileClientMessageIndex = 943;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(anInt, JetGetJobStatusCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_JetGetJobIdsCodec_encodeRequest() {
        int fileClientMessageIndex = 944;
        ClientMessage encoded = JetGetJobIdsCodec.encodeRequest(null, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetGetJobIdsCodec_decodeResponse() {
        int fileClientMessageIndex = 945;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        JetGetJobIdsCodec.ResponseParameters parameters = JetGetJobIdsCodec.decodeResponse(fromFile);
        assertTrue(parameters.isResponseExists);
        assertTrue(isEqual(aData, parameters.response));
    }

    @Test
    public void test_JetJoinSubmittedJobCodec_encodeRequest() {
        int fileClientMessageIndex = 946;
        ClientMessage encoded = JetJoinSubmittedJobCodec.encodeRequest(aLong, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetJoinSubmittedJobCodec_decodeResponse() {
        int fileClientMessageIndex = 947;
    }

    @Test
    public void test_JetGetJobSubmissionTimeCodec_encodeRequest() {
        int fileClientMessageIndex = 948;
        ClientMessage encoded = JetGetJobSubmissionTimeCodec.encodeRequest(aLong, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetGetJobSubmissionTimeCodec_decodeResponse() {
        int fileClientMessageIndex = 949;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aLong, JetGetJobSubmissionTimeCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_JetGetJobConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 950;
        ClientMessage encoded = JetGetJobConfigCodec.encodeRequest(aLong, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetGetJobConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 951;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aData, JetGetJobConfigCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_JetResumeJobCodec_encodeRequest() {
        int fileClientMessageIndex = 952;
        ClientMessage encoded = JetResumeJobCodec.encodeRequest(aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetResumeJobCodec_decodeResponse() {
        int fileClientMessageIndex = 953;
    }

    @Test
    public void test_JetExportSnapshotCodec_encodeRequest() {
        int fileClientMessageIndex = 954;
        ClientMessage encoded = JetExportSnapshotCodec.encodeRequest(aLong, aString, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetExportSnapshotCodec_decodeResponse() {
        int fileClientMessageIndex = 955;
    }

    @Test
    public void test_JetGetJobSummaryListCodec_encodeRequest() {
        int fileClientMessageIndex = 956;
        ClientMessage encoded = JetGetJobSummaryListCodec.encodeRequest();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetGetJobSummaryListCodec_decodeResponse() {
        int fileClientMessageIndex = 957;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aData, JetGetJobSummaryListCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_JetExistsDistributedObjectCodec_encodeRequest() {
        int fileClientMessageIndex = 958;
        ClientMessage encoded = JetExistsDistributedObjectCodec.encodeRequest(aString, aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetExistsDistributedObjectCodec_decodeResponse() {
        int fileClientMessageIndex = 959;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, JetExistsDistributedObjectCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_JetGetJobMetricsCodec_encodeRequest() {
        int fileClientMessageIndex = 960;
        ClientMessage encoded = JetGetJobMetricsCodec.encodeRequest(aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetGetJobMetricsCodec_decodeResponse() {
        int fileClientMessageIndex = 961;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aData, JetGetJobMetricsCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_JetGetJobSuspensionCauseCodec_encodeRequest() {
        int fileClientMessageIndex = 962;
        ClientMessage encoded = JetGetJobSuspensionCauseCodec.encodeRequest(aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetGetJobSuspensionCauseCodec_decodeResponse() {
        int fileClientMessageIndex = 963;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aData, JetGetJobSuspensionCauseCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_JetGetJobAndSqlSummaryListCodec_encodeRequest() {
        int fileClientMessageIndex = 964;
        ClientMessage encoded = JetGetJobAndSqlSummaryListCodec.encodeRequest();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetGetJobAndSqlSummaryListCodec_decodeResponse() {
        int fileClientMessageIndex = 965;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListJobAndSqlSummary, JetGetJobAndSqlSummaryListCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_JetIsJobUserCancelledCodec_encodeRequest() {
        int fileClientMessageIndex = 966;
        ClientMessage encoded = JetIsJobUserCancelledCodec.encodeRequest(aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetIsJobUserCancelledCodec_decodeResponse() {
        int fileClientMessageIndex = 967;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, JetIsJobUserCancelledCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_JetUploadJobMetaDataCodec_encodeRequest() {
        int fileClientMessageIndex = 968;
        ClientMessage encoded = JetUploadJobMetaDataCodec.encodeRequest(aUUID, aBoolean, aString, aString, null, null, null, aListOfStrings);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetUploadJobMetaDataCodec_decodeResponse() {
        int fileClientMessageIndex = 969;
    }

    @Test
    public void test_JetUploadJobMultipartCodec_encodeRequest() {
        int fileClientMessageIndex = 970;
        ClientMessage encoded = JetUploadJobMultipartCodec.encodeRequest(aUUID, anInt, anInt, aByteArray, anInt, aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetUploadJobMultipartCodec_decodeResponse() {
        int fileClientMessageIndex = 971;
    }

    @Test
    public void test_JetAddJobStatusListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 972;
        ClientMessage encoded = JetAddJobStatusListenerCodec.encodeRequest(aLong, null, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetAddJobStatusListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 973;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(null, JetAddJobStatusListenerCodec.decodeResponse(fromFile)));
    }

    private static class JetAddJobStatusListenerCodecHandler extends JetAddJobStatusListenerCodec.AbstractEventHandler {
        @Override
        public void handleJobStatusEvent(long jobId, int previousStatus, int newStatus, java.lang.String description, boolean userRequested) {
            assertTrue(isEqual(aLong, jobId));
            assertTrue(isEqual(anInt, previousStatus));
            assertTrue(isEqual(anInt, newStatus));
            assertTrue(isEqual(null, description));
            assertTrue(isEqual(aBoolean, userRequested));
        }
    }

    @Test
    public void test_JetAddJobStatusListenerCodec_handleJobStatusEvent() {
        int fileClientMessageIndex = 974;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        JetAddJobStatusListenerCodecHandler handler = new JetAddJobStatusListenerCodecHandler();
        handler.handle(fromFile);
    }

    @Test
    public void test_JetRemoveJobStatusListenerCodec_encodeRequest() {
        int fileClientMessageIndex = 975;
        ClientMessage encoded = JetRemoveJobStatusListenerCodec.encodeRequest(aLong, aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetRemoveJobStatusListenerCodec_decodeResponse() {
        int fileClientMessageIndex = 976;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, JetRemoveJobStatusListenerCodec.decodeResponse(fromFile)));
    }

    @Test
    public void test_JetUpdateJobConfigCodec_encodeRequest() {
        int fileClientMessageIndex = 977;
        ClientMessage encoded = JetUpdateJobConfigCodec.encodeRequest(aLong, aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetUpdateJobConfigCodec_decodeResponse() {
        int fileClientMessageIndex = 978;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aData, JetUpdateJobConfigCodec.decodeResponse(fromFile)));
    }

    private void compareClientMessages(ClientMessage binaryMessage, ClientMessage encodedMessage) {
        ClientMessage.Frame binaryFrame, encodedFrame;

        ClientMessage.ForwardFrameIterator binaryFrameIterator = binaryMessage.frameIterator();
        ClientMessage.ForwardFrameIterator encodedFrameIterator = encodedMessage.frameIterator();
        assertTrue("Client message that is read from the binary file does not have any frames", binaryFrameIterator.hasNext());

        while (binaryFrameIterator.hasNext()) {
            binaryFrame = binaryFrameIterator.next();
            encodedFrame = encodedFrameIterator.next();
            assertNotNull("Encoded client message has less frames.", encodedFrame);

            if (binaryFrame.isEndFrame() && !encodedFrame.isEndFrame()) {
                if (encodedFrame.isBeginFrame()) {
                    HazelcastClientUtil.fastForwardToEndFrame(encodedFrameIterator);
                }
                encodedFrame = HazelcastClientUtil.fastForwardToEndFrame(encodedFrameIterator);
            }

            boolean isFinal = binaryFrameIterator.peekNext() == null;
            int flags = isFinal ? encodedFrame.flags | IS_FINAL_FLAG : encodedFrame.flags;
            assertArrayEquals("Frames have different contents", binaryFrame.content, Arrays.copyOf(encodedFrame.content, binaryFrame.content.length));
            assertEquals("Frames have different flags", binaryFrame.flags, flags);
        }
    }
}
