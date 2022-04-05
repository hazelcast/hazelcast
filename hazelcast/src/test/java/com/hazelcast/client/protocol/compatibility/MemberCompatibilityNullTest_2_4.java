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

import java.io.File;
import java.io.FileInputStream;
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

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MemberCompatibilityNullTest_2_4 {
    private final List<ClientMessage> clientMessages = new ArrayList<>();

    @Before
    public void setUp() throws IOException {
        File file = new File(getClass().getResource("/2.4.protocol.compatibility.null.binary").getFile());
        InputStream inputStream = new FileInputStream(file);
        byte[] data = new byte[(int) file.length()];
        inputStream.read(data);
        ByteBuffer buffer = ByteBuffer.wrap(data);
        ClientMessageReader reader = new ClientMessageReader(0);
        while (reader.readFrom(buffer, true)) {
            clientMessages.add(reader.getClientMessage());
            reader.reset();
        }
    }

    @Test
    public void test_ClientAuthenticationCodec_decodeRequest() {
        int fileClientMessageIndex = 0;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientAuthenticationCodec.RequestParameters parameters = ClientAuthenticationCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.clusterName));
        assertTrue(isEqual(null, parameters.username));
        assertTrue(isEqual(null, parameters.password));
        assertTrue(isEqual(null, parameters.uuid));
        assertTrue(isEqual(aString, parameters.clientType));
        assertTrue(isEqual(aByte, parameters.serializationVersion));
        assertTrue(isEqual(aString, parameters.clientHazelcastVersion));
        assertTrue(isEqual(aString, parameters.clientName));
        assertTrue(isEqual(aListOfStrings, parameters.labels));
    }

    @Test
    public void test_ClientAuthenticationCodec_encodeResponse() {
        int fileClientMessageIndex = 1;
        ClientMessage encoded = ClientAuthenticationCodec.encodeResponse(aByte, null, null, aByte, aString, anInt, aUUID, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientAuthenticationCustomCodec_decodeRequest() {
        int fileClientMessageIndex = 2;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientAuthenticationCustomCodec.RequestParameters parameters = ClientAuthenticationCustomCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.clusterName));
        assertTrue(isEqual(aByteArray, parameters.credentials));
        assertTrue(isEqual(null, parameters.uuid));
        assertTrue(isEqual(aString, parameters.clientType));
        assertTrue(isEqual(aByte, parameters.serializationVersion));
        assertTrue(isEqual(aString, parameters.clientHazelcastVersion));
        assertTrue(isEqual(aString, parameters.clientName));
        assertTrue(isEqual(aListOfStrings, parameters.labels));
    }

    @Test
    public void test_ClientAuthenticationCustomCodec_encodeResponse() {
        int fileClientMessageIndex = 3;
        ClientMessage encoded = ClientAuthenticationCustomCodec.encodeResponse(aByte, null, null, aByte, aString, anInt, aUUID, aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientAddClusterViewListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 4;
    }

    @Test
    public void test_ClientAddClusterViewListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 5;
        ClientMessage encoded = ClientAddClusterViewListenerCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientAddClusterViewListenerCodec_encodeMembersViewEvent() {
        int fileClientMessageIndex = 6;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = ClientAddClusterViewListenerCodec.encodeMembersViewEvent(anInt, aListOfMemberInfos);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientAddClusterViewListenerCodec_encodePartitionsViewEvent() {
        int fileClientMessageIndex = 7;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = ClientAddClusterViewListenerCodec.encodePartitionsViewEvent(anInt, aListOfUUIDToListOfIntegers);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientCreateProxyCodec_decodeRequest() {
        int fileClientMessageIndex = 8;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientCreateProxyCodec.RequestParameters parameters = ClientCreateProxyCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aString, parameters.serviceName));
    }

    @Test
    public void test_ClientCreateProxyCodec_encodeResponse() {
        int fileClientMessageIndex = 9;
        ClientMessage encoded = ClientCreateProxyCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientDestroyProxyCodec_decodeRequest() {
        int fileClientMessageIndex = 10;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientDestroyProxyCodec.RequestParameters parameters = ClientDestroyProxyCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aString, parameters.serviceName));
    }

    @Test
    public void test_ClientDestroyProxyCodec_encodeResponse() {
        int fileClientMessageIndex = 11;
        ClientMessage encoded = ClientDestroyProxyCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientAddPartitionLostListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 12;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ClientAddPartitionLostListenerCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_ClientAddPartitionLostListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 13;
        ClientMessage encoded = ClientAddPartitionLostListenerCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientAddPartitionLostListenerCodec_encodePartitionLostEvent() {
        int fileClientMessageIndex = 14;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = ClientAddPartitionLostListenerCodec.encodePartitionLostEvent(anInt, anInt, null);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientRemovePartitionLostListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 15;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, ClientRemovePartitionLostListenerCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_ClientRemovePartitionLostListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 16;
        ClientMessage encoded = ClientRemovePartitionLostListenerCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientGetDistributedObjectsCodec_decodeRequest() {
        int fileClientMessageIndex = 17;
    }

    @Test
    public void test_ClientGetDistributedObjectsCodec_encodeResponse() {
        int fileClientMessageIndex = 18;
        ClientMessage encoded = ClientGetDistributedObjectsCodec.encodeResponse(aListOfDistributedObjectInfo);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientAddDistributedObjectListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 19;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ClientAddDistributedObjectListenerCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_ClientAddDistributedObjectListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 20;
        ClientMessage encoded = ClientAddDistributedObjectListenerCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientAddDistributedObjectListenerCodec_encodeDistributedObjectEvent() {
        int fileClientMessageIndex = 21;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = ClientAddDistributedObjectListenerCodec.encodeDistributedObjectEvent(aString, aString, aString, aUUID);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientRemoveDistributedObjectListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 22;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, ClientRemoveDistributedObjectListenerCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_ClientRemoveDistributedObjectListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 23;
        ClientMessage encoded = ClientRemoveDistributedObjectListenerCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientPingCodec_decodeRequest() {
        int fileClientMessageIndex = 24;
    }

    @Test
    public void test_ClientPingCodec_encodeResponse() {
        int fileClientMessageIndex = 25;
        ClientMessage encoded = ClientPingCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientStatisticsCodec_decodeRequest() {
        int fileClientMessageIndex = 26;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientStatisticsCodec.RequestParameters parameters = ClientStatisticsCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aLong, parameters.timestamp));
        assertTrue(isEqual(aString, parameters.clientAttributes));
        assertTrue(isEqual(aByteArray, parameters.metricsBlob));
    }

    @Test
    public void test_ClientStatisticsCodec_encodeResponse() {
        int fileClientMessageIndex = 27;
        ClientMessage encoded = ClientStatisticsCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientDeployClassesCodec_decodeRequest() {
        int fileClientMessageIndex = 28;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfStringToByteArray, ClientDeployClassesCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_ClientDeployClassesCodec_encodeResponse() {
        int fileClientMessageIndex = 29;
        ClientMessage encoded = ClientDeployClassesCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientCreateProxiesCodec_decodeRequest() {
        int fileClientMessageIndex = 30;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfStringToString, ClientCreateProxiesCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_ClientCreateProxiesCodec_encodeResponse() {
        int fileClientMessageIndex = 31;
        ClientMessage encoded = ClientCreateProxiesCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientLocalBackupListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 32;
    }

    @Test
    public void test_ClientLocalBackupListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 33;
        ClientMessage encoded = ClientLocalBackupListenerCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientLocalBackupListenerCodec_encodeBackupEvent() {
        int fileClientMessageIndex = 34;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = ClientLocalBackupListenerCodec.encodeBackupEvent(aLong);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientTriggerPartitionAssignmentCodec_decodeRequest() {
        int fileClientMessageIndex = 35;
    }

    @Test
    public void test_ClientTriggerPartitionAssignmentCodec_encodeResponse() {
        int fileClientMessageIndex = 36;
        ClientMessage encoded = ClientTriggerPartitionAssignmentCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientAddMigrationListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 37;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, ClientAddMigrationListenerCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_ClientAddMigrationListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 38;
        ClientMessage encoded = ClientAddMigrationListenerCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientAddMigrationListenerCodec_encodeMigrationEvent() {
        int fileClientMessageIndex = 39;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = ClientAddMigrationListenerCodec.encodeMigrationEvent(aMigrationState, anInt);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientAddMigrationListenerCodec_encodeReplicaMigrationEvent() {
        int fileClientMessageIndex = 40;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = ClientAddMigrationListenerCodec.encodeReplicaMigrationEvent(aMigrationState, anInt, anInt, null, null, aBoolean, aLong);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientRemoveMigrationListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 41;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, ClientRemoveMigrationListenerCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_ClientRemoveMigrationListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 42;
        ClientMessage encoded = ClientRemoveMigrationListenerCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientSendSchemaCodec_decodeRequest() {
        int fileClientMessageIndex = 43;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aSchema, ClientSendSchemaCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_ClientSendSchemaCodec_encodeResponse() {
        int fileClientMessageIndex = 44;
        ClientMessage encoded = ClientSendSchemaCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientFetchSchemaCodec_decodeRequest() {
        int fileClientMessageIndex = 45;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aLong, ClientFetchSchemaCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_ClientFetchSchemaCodec_encodeResponse() {
        int fileClientMessageIndex = 46;
        ClientMessage encoded = ClientFetchSchemaCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ClientSendAllSchemasCodec_decodeRequest() {
        int fileClientMessageIndex = 47;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aListOfSchemas, ClientSendAllSchemasCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_ClientSendAllSchemasCodec_encodeResponse() {
        int fileClientMessageIndex = 48;
        ClientMessage encoded = ClientSendAllSchemasCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapPutCodec_decodeRequest() {
        int fileClientMessageIndex = 49;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapPutCodec.RequestParameters parameters = MapPutCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aData, parameters.value));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aLong, parameters.ttl));
    }

    @Test
    public void test_MapPutCodec_encodeResponse() {
        int fileClientMessageIndex = 50;
        ClientMessage encoded = MapPutCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapGetCodec_decodeRequest() {
        int fileClientMessageIndex = 51;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapGetCodec.RequestParameters parameters = MapGetCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_MapGetCodec_encodeResponse() {
        int fileClientMessageIndex = 52;
        ClientMessage encoded = MapGetCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapRemoveCodec_decodeRequest() {
        int fileClientMessageIndex = 53;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapRemoveCodec.RequestParameters parameters = MapRemoveCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_MapRemoveCodec_encodeResponse() {
        int fileClientMessageIndex = 54;
        ClientMessage encoded = MapRemoveCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapReplaceCodec_decodeRequest() {
        int fileClientMessageIndex = 55;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapReplaceCodec.RequestParameters parameters = MapReplaceCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aData, parameters.value));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_MapReplaceCodec_encodeResponse() {
        int fileClientMessageIndex = 56;
        ClientMessage encoded = MapReplaceCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapReplaceIfSameCodec_decodeRequest() {
        int fileClientMessageIndex = 57;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapReplaceIfSameCodec.RequestParameters parameters = MapReplaceIfSameCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aData, parameters.testValue));
        assertTrue(isEqual(aData, parameters.value));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_MapReplaceIfSameCodec_encodeResponse() {
        int fileClientMessageIndex = 58;
        ClientMessage encoded = MapReplaceIfSameCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapContainsKeyCodec_decodeRequest() {
        int fileClientMessageIndex = 59;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapContainsKeyCodec.RequestParameters parameters = MapContainsKeyCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_MapContainsKeyCodec_encodeResponse() {
        int fileClientMessageIndex = 60;
        ClientMessage encoded = MapContainsKeyCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapContainsValueCodec_decodeRequest() {
        int fileClientMessageIndex = 61;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapContainsValueCodec.RequestParameters parameters = MapContainsValueCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.value));
    }

    @Test
    public void test_MapContainsValueCodec_encodeResponse() {
        int fileClientMessageIndex = 62;
        ClientMessage encoded = MapContainsValueCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapRemoveIfSameCodec_decodeRequest() {
        int fileClientMessageIndex = 63;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapRemoveIfSameCodec.RequestParameters parameters = MapRemoveIfSameCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aData, parameters.value));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_MapRemoveIfSameCodec_encodeResponse() {
        int fileClientMessageIndex = 64;
        ClientMessage encoded = MapRemoveIfSameCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapDeleteCodec_decodeRequest() {
        int fileClientMessageIndex = 65;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapDeleteCodec.RequestParameters parameters = MapDeleteCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_MapDeleteCodec_encodeResponse() {
        int fileClientMessageIndex = 66;
        ClientMessage encoded = MapDeleteCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapFlushCodec_decodeRequest() {
        int fileClientMessageIndex = 67;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, MapFlushCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_MapFlushCodec_encodeResponse() {
        int fileClientMessageIndex = 68;
        ClientMessage encoded = MapFlushCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapTryRemoveCodec_decodeRequest() {
        int fileClientMessageIndex = 69;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapTryRemoveCodec.RequestParameters parameters = MapTryRemoveCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aLong, parameters.timeout));
    }

    @Test
    public void test_MapTryRemoveCodec_encodeResponse() {
        int fileClientMessageIndex = 70;
        ClientMessage encoded = MapTryRemoveCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapTryPutCodec_decodeRequest() {
        int fileClientMessageIndex = 71;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapTryPutCodec.RequestParameters parameters = MapTryPutCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aData, parameters.value));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aLong, parameters.timeout));
    }

    @Test
    public void test_MapTryPutCodec_encodeResponse() {
        int fileClientMessageIndex = 72;
        ClientMessage encoded = MapTryPutCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapPutTransientCodec_decodeRequest() {
        int fileClientMessageIndex = 73;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapPutTransientCodec.RequestParameters parameters = MapPutTransientCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aData, parameters.value));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aLong, parameters.ttl));
    }

    @Test
    public void test_MapPutTransientCodec_encodeResponse() {
        int fileClientMessageIndex = 74;
        ClientMessage encoded = MapPutTransientCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapPutIfAbsentCodec_decodeRequest() {
        int fileClientMessageIndex = 75;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapPutIfAbsentCodec.RequestParameters parameters = MapPutIfAbsentCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aData, parameters.value));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aLong, parameters.ttl));
    }

    @Test
    public void test_MapPutIfAbsentCodec_encodeResponse() {
        int fileClientMessageIndex = 76;
        ClientMessage encoded = MapPutIfAbsentCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapSetCodec_decodeRequest() {
        int fileClientMessageIndex = 77;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapSetCodec.RequestParameters parameters = MapSetCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aData, parameters.value));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aLong, parameters.ttl));
    }

    @Test
    public void test_MapSetCodec_encodeResponse() {
        int fileClientMessageIndex = 78;
        ClientMessage encoded = MapSetCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapLockCodec_decodeRequest() {
        int fileClientMessageIndex = 79;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapLockCodec.RequestParameters parameters = MapLockCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aLong, parameters.ttl));
        assertTrue(isEqual(aLong, parameters.referenceId));
    }

    @Test
    public void test_MapLockCodec_encodeResponse() {
        int fileClientMessageIndex = 80;
        ClientMessage encoded = MapLockCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapTryLockCodec_decodeRequest() {
        int fileClientMessageIndex = 81;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapTryLockCodec.RequestParameters parameters = MapTryLockCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aLong, parameters.lease));
        assertTrue(isEqual(aLong, parameters.timeout));
        assertTrue(isEqual(aLong, parameters.referenceId));
    }

    @Test
    public void test_MapTryLockCodec_encodeResponse() {
        int fileClientMessageIndex = 82;
        ClientMessage encoded = MapTryLockCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapIsLockedCodec_decodeRequest() {
        int fileClientMessageIndex = 83;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapIsLockedCodec.RequestParameters parameters = MapIsLockedCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
    }

    @Test
    public void test_MapIsLockedCodec_encodeResponse() {
        int fileClientMessageIndex = 84;
        ClientMessage encoded = MapIsLockedCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapUnlockCodec_decodeRequest() {
        int fileClientMessageIndex = 85;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapUnlockCodec.RequestParameters parameters = MapUnlockCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aLong, parameters.referenceId));
    }

    @Test
    public void test_MapUnlockCodec_encodeResponse() {
        int fileClientMessageIndex = 86;
        ClientMessage encoded = MapUnlockCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapAddInterceptorCodec_decodeRequest() {
        int fileClientMessageIndex = 87;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapAddInterceptorCodec.RequestParameters parameters = MapAddInterceptorCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.interceptor));
    }

    @Test
    public void test_MapAddInterceptorCodec_encodeResponse() {
        int fileClientMessageIndex = 88;
        ClientMessage encoded = MapAddInterceptorCodec.encodeResponse(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapRemoveInterceptorCodec_decodeRequest() {
        int fileClientMessageIndex = 89;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapRemoveInterceptorCodec.RequestParameters parameters = MapRemoveInterceptorCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aString, parameters.id));
    }

    @Test
    public void test_MapRemoveInterceptorCodec_encodeResponse() {
        int fileClientMessageIndex = 90;
        ClientMessage encoded = MapRemoveInterceptorCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapAddEntryListenerToKeyWithPredicateCodec_decodeRequest() {
        int fileClientMessageIndex = 91;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapAddEntryListenerToKeyWithPredicateCodec.RequestParameters parameters = MapAddEntryListenerToKeyWithPredicateCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aData, parameters.predicate));
        assertTrue(isEqual(aBoolean, parameters.includeValue));
        assertTrue(isEqual(anInt, parameters.listenerFlags));
        assertTrue(isEqual(aBoolean, parameters.localOnly));
    }

    @Test
    public void test_MapAddEntryListenerToKeyWithPredicateCodec_encodeResponse() {
        int fileClientMessageIndex = 92;
        ClientMessage encoded = MapAddEntryListenerToKeyWithPredicateCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapAddEntryListenerToKeyWithPredicateCodec_encodeEntryEvent() {
        int fileClientMessageIndex = 93;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = MapAddEntryListenerToKeyWithPredicateCodec.encodeEntryEvent(null, null, null, null, anInt, aUUID, anInt);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapAddEntryListenerWithPredicateCodec_decodeRequest() {
        int fileClientMessageIndex = 94;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapAddEntryListenerWithPredicateCodec.RequestParameters parameters = MapAddEntryListenerWithPredicateCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.predicate));
        assertTrue(isEqual(aBoolean, parameters.includeValue));
        assertTrue(isEqual(anInt, parameters.listenerFlags));
        assertTrue(isEqual(aBoolean, parameters.localOnly));
    }

    @Test
    public void test_MapAddEntryListenerWithPredicateCodec_encodeResponse() {
        int fileClientMessageIndex = 95;
        ClientMessage encoded = MapAddEntryListenerWithPredicateCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapAddEntryListenerWithPredicateCodec_encodeEntryEvent() {
        int fileClientMessageIndex = 96;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = MapAddEntryListenerWithPredicateCodec.encodeEntryEvent(null, null, null, null, anInt, aUUID, anInt);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapAddEntryListenerToKeyCodec_decodeRequest() {
        int fileClientMessageIndex = 97;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapAddEntryListenerToKeyCodec.RequestParameters parameters = MapAddEntryListenerToKeyCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aBoolean, parameters.includeValue));
        assertTrue(isEqual(anInt, parameters.listenerFlags));
        assertTrue(isEqual(aBoolean, parameters.localOnly));
    }

    @Test
    public void test_MapAddEntryListenerToKeyCodec_encodeResponse() {
        int fileClientMessageIndex = 98;
        ClientMessage encoded = MapAddEntryListenerToKeyCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapAddEntryListenerToKeyCodec_encodeEntryEvent() {
        int fileClientMessageIndex = 99;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = MapAddEntryListenerToKeyCodec.encodeEntryEvent(null, null, null, null, anInt, aUUID, anInt);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapAddEntryListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 100;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapAddEntryListenerCodec.RequestParameters parameters = MapAddEntryListenerCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aBoolean, parameters.includeValue));
        assertTrue(isEqual(anInt, parameters.listenerFlags));
        assertTrue(isEqual(aBoolean, parameters.localOnly));
    }

    @Test
    public void test_MapAddEntryListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 101;
        ClientMessage encoded = MapAddEntryListenerCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapAddEntryListenerCodec_encodeEntryEvent() {
        int fileClientMessageIndex = 102;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = MapAddEntryListenerCodec.encodeEntryEvent(null, null, null, null, anInt, aUUID, anInt);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapRemoveEntryListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 103;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapRemoveEntryListenerCodec.RequestParameters parameters = MapRemoveEntryListenerCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.registrationId));
    }

    @Test
    public void test_MapRemoveEntryListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 104;
        ClientMessage encoded = MapRemoveEntryListenerCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapAddPartitionLostListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 105;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapAddPartitionLostListenerCodec.RequestParameters parameters = MapAddPartitionLostListenerCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aBoolean, parameters.localOnly));
    }

    @Test
    public void test_MapAddPartitionLostListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 106;
        ClientMessage encoded = MapAddPartitionLostListenerCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapAddPartitionLostListenerCodec_encodeMapPartitionLostEvent() {
        int fileClientMessageIndex = 107;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = MapAddPartitionLostListenerCodec.encodeMapPartitionLostEvent(anInt, aUUID);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapRemovePartitionLostListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 108;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapRemovePartitionLostListenerCodec.RequestParameters parameters = MapRemovePartitionLostListenerCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.registrationId));
    }

    @Test
    public void test_MapRemovePartitionLostListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 109;
        ClientMessage encoded = MapRemovePartitionLostListenerCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapGetEntryViewCodec_decodeRequest() {
        int fileClientMessageIndex = 110;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapGetEntryViewCodec.RequestParameters parameters = MapGetEntryViewCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_MapGetEntryViewCodec_encodeResponse() {
        int fileClientMessageIndex = 111;
        ClientMessage encoded = MapGetEntryViewCodec.encodeResponse(null, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapEvictCodec_decodeRequest() {
        int fileClientMessageIndex = 112;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapEvictCodec.RequestParameters parameters = MapEvictCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_MapEvictCodec_encodeResponse() {
        int fileClientMessageIndex = 113;
        ClientMessage encoded = MapEvictCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapEvictAllCodec_decodeRequest() {
        int fileClientMessageIndex = 114;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, MapEvictAllCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_MapEvictAllCodec_encodeResponse() {
        int fileClientMessageIndex = 115;
        ClientMessage encoded = MapEvictAllCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapLoadAllCodec_decodeRequest() {
        int fileClientMessageIndex = 116;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapLoadAllCodec.RequestParameters parameters = MapLoadAllCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aBoolean, parameters.replaceExistingValues));
    }

    @Test
    public void test_MapLoadAllCodec_encodeResponse() {
        int fileClientMessageIndex = 117;
        ClientMessage encoded = MapLoadAllCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapLoadGivenKeysCodec_decodeRequest() {
        int fileClientMessageIndex = 118;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapLoadGivenKeysCodec.RequestParameters parameters = MapLoadGivenKeysCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aListOfData, parameters.keys));
        assertTrue(isEqual(aBoolean, parameters.replaceExistingValues));
    }

    @Test
    public void test_MapLoadGivenKeysCodec_encodeResponse() {
        int fileClientMessageIndex = 119;
        ClientMessage encoded = MapLoadGivenKeysCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapKeySetCodec_decodeRequest() {
        int fileClientMessageIndex = 120;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, MapKeySetCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_MapKeySetCodec_encodeResponse() {
        int fileClientMessageIndex = 121;
        ClientMessage encoded = MapKeySetCodec.encodeResponse(aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapGetAllCodec_decodeRequest() {
        int fileClientMessageIndex = 122;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapGetAllCodec.RequestParameters parameters = MapGetAllCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aListOfData, parameters.keys));
    }

    @Test
    public void test_MapGetAllCodec_encodeResponse() {
        int fileClientMessageIndex = 123;
        ClientMessage encoded = MapGetAllCodec.encodeResponse(aListOfDataToData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapValuesCodec_decodeRequest() {
        int fileClientMessageIndex = 124;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, MapValuesCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_MapValuesCodec_encodeResponse() {
        int fileClientMessageIndex = 125;
        ClientMessage encoded = MapValuesCodec.encodeResponse(aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapEntrySetCodec_decodeRequest() {
        int fileClientMessageIndex = 126;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, MapEntrySetCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_MapEntrySetCodec_encodeResponse() {
        int fileClientMessageIndex = 127;
        ClientMessage encoded = MapEntrySetCodec.encodeResponse(aListOfDataToData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapKeySetWithPredicateCodec_decodeRequest() {
        int fileClientMessageIndex = 128;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapKeySetWithPredicateCodec.RequestParameters parameters = MapKeySetWithPredicateCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.predicate));
    }

    @Test
    public void test_MapKeySetWithPredicateCodec_encodeResponse() {
        int fileClientMessageIndex = 129;
        ClientMessage encoded = MapKeySetWithPredicateCodec.encodeResponse(aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapValuesWithPredicateCodec_decodeRequest() {
        int fileClientMessageIndex = 130;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapValuesWithPredicateCodec.RequestParameters parameters = MapValuesWithPredicateCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.predicate));
    }

    @Test
    public void test_MapValuesWithPredicateCodec_encodeResponse() {
        int fileClientMessageIndex = 131;
        ClientMessage encoded = MapValuesWithPredicateCodec.encodeResponse(aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapEntriesWithPredicateCodec_decodeRequest() {
        int fileClientMessageIndex = 132;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapEntriesWithPredicateCodec.RequestParameters parameters = MapEntriesWithPredicateCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.predicate));
    }

    @Test
    public void test_MapEntriesWithPredicateCodec_encodeResponse() {
        int fileClientMessageIndex = 133;
        ClientMessage encoded = MapEntriesWithPredicateCodec.encodeResponse(aListOfDataToData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapAddIndexCodec_decodeRequest() {
        int fileClientMessageIndex = 134;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapAddIndexCodec.RequestParameters parameters = MapAddIndexCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(anIndexConfig, parameters.indexConfig));
    }

    @Test
    public void test_MapAddIndexCodec_encodeResponse() {
        int fileClientMessageIndex = 135;
        ClientMessage encoded = MapAddIndexCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapSizeCodec_decodeRequest() {
        int fileClientMessageIndex = 136;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, MapSizeCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_MapSizeCodec_encodeResponse() {
        int fileClientMessageIndex = 137;
        ClientMessage encoded = MapSizeCodec.encodeResponse(anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapIsEmptyCodec_decodeRequest() {
        int fileClientMessageIndex = 138;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, MapIsEmptyCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_MapIsEmptyCodec_encodeResponse() {
        int fileClientMessageIndex = 139;
        ClientMessage encoded = MapIsEmptyCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapPutAllCodec_decodeRequest() {
        int fileClientMessageIndex = 140;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapPutAllCodec.RequestParameters parameters = MapPutAllCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aListOfDataToData, parameters.entries));
        assertTrue(parameters.isTriggerMapLoaderExists);
        assertTrue(isEqual(aBoolean, parameters.triggerMapLoader));
    }

    @Test
    public void test_MapPutAllCodec_encodeResponse() {
        int fileClientMessageIndex = 141;
        ClientMessage encoded = MapPutAllCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapClearCodec_decodeRequest() {
        int fileClientMessageIndex = 142;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, MapClearCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_MapClearCodec_encodeResponse() {
        int fileClientMessageIndex = 143;
        ClientMessage encoded = MapClearCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapExecuteOnKeyCodec_decodeRequest() {
        int fileClientMessageIndex = 144;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapExecuteOnKeyCodec.RequestParameters parameters = MapExecuteOnKeyCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.entryProcessor));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_MapExecuteOnKeyCodec_encodeResponse() {
        int fileClientMessageIndex = 145;
        ClientMessage encoded = MapExecuteOnKeyCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapSubmitToKeyCodec_decodeRequest() {
        int fileClientMessageIndex = 146;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapSubmitToKeyCodec.RequestParameters parameters = MapSubmitToKeyCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.entryProcessor));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_MapSubmitToKeyCodec_encodeResponse() {
        int fileClientMessageIndex = 147;
        ClientMessage encoded = MapSubmitToKeyCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapExecuteOnAllKeysCodec_decodeRequest() {
        int fileClientMessageIndex = 148;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapExecuteOnAllKeysCodec.RequestParameters parameters = MapExecuteOnAllKeysCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.entryProcessor));
    }

    @Test
    public void test_MapExecuteOnAllKeysCodec_encodeResponse() {
        int fileClientMessageIndex = 149;
        ClientMessage encoded = MapExecuteOnAllKeysCodec.encodeResponse(aListOfDataToData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapExecuteWithPredicateCodec_decodeRequest() {
        int fileClientMessageIndex = 150;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapExecuteWithPredicateCodec.RequestParameters parameters = MapExecuteWithPredicateCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.entryProcessor));
        assertTrue(isEqual(aData, parameters.predicate));
    }

    @Test
    public void test_MapExecuteWithPredicateCodec_encodeResponse() {
        int fileClientMessageIndex = 151;
        ClientMessage encoded = MapExecuteWithPredicateCodec.encodeResponse(aListOfDataToData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapExecuteOnKeysCodec_decodeRequest() {
        int fileClientMessageIndex = 152;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapExecuteOnKeysCodec.RequestParameters parameters = MapExecuteOnKeysCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.entryProcessor));
        assertTrue(isEqual(aListOfData, parameters.keys));
    }

    @Test
    public void test_MapExecuteOnKeysCodec_encodeResponse() {
        int fileClientMessageIndex = 153;
        ClientMessage encoded = MapExecuteOnKeysCodec.encodeResponse(aListOfDataToData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapForceUnlockCodec_decodeRequest() {
        int fileClientMessageIndex = 154;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapForceUnlockCodec.RequestParameters parameters = MapForceUnlockCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aLong, parameters.referenceId));
    }

    @Test
    public void test_MapForceUnlockCodec_encodeResponse() {
        int fileClientMessageIndex = 155;
        ClientMessage encoded = MapForceUnlockCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapKeySetWithPagingPredicateCodec_decodeRequest() {
        int fileClientMessageIndex = 156;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapKeySetWithPagingPredicateCodec.RequestParameters parameters = MapKeySetWithPagingPredicateCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aPagingPredicateHolder, parameters.predicate));
    }

    @Test
    public void test_MapKeySetWithPagingPredicateCodec_encodeResponse() {
        int fileClientMessageIndex = 157;
        ClientMessage encoded = MapKeySetWithPagingPredicateCodec.encodeResponse(aListOfData, anAnchorDataListHolder);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapValuesWithPagingPredicateCodec_decodeRequest() {
        int fileClientMessageIndex = 158;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapValuesWithPagingPredicateCodec.RequestParameters parameters = MapValuesWithPagingPredicateCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aPagingPredicateHolder, parameters.predicate));
    }

    @Test
    public void test_MapValuesWithPagingPredicateCodec_encodeResponse() {
        int fileClientMessageIndex = 159;
        ClientMessage encoded = MapValuesWithPagingPredicateCodec.encodeResponse(aListOfData, anAnchorDataListHolder);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapEntriesWithPagingPredicateCodec_decodeRequest() {
        int fileClientMessageIndex = 160;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapEntriesWithPagingPredicateCodec.RequestParameters parameters = MapEntriesWithPagingPredicateCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aPagingPredicateHolder, parameters.predicate));
    }

    @Test
    public void test_MapEntriesWithPagingPredicateCodec_encodeResponse() {
        int fileClientMessageIndex = 161;
        ClientMessage encoded = MapEntriesWithPagingPredicateCodec.encodeResponse(aListOfDataToData, anAnchorDataListHolder);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapFetchKeysCodec_decodeRequest() {
        int fileClientMessageIndex = 162;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapFetchKeysCodec.RequestParameters parameters = MapFetchKeysCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aListOfIntegerToInteger, parameters.iterationPointers));
        assertTrue(isEqual(anInt, parameters.batch));
    }

    @Test
    public void test_MapFetchKeysCodec_encodeResponse() {
        int fileClientMessageIndex = 163;
        ClientMessage encoded = MapFetchKeysCodec.encodeResponse(aListOfIntegerToInteger, aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapFetchEntriesCodec_decodeRequest() {
        int fileClientMessageIndex = 164;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapFetchEntriesCodec.RequestParameters parameters = MapFetchEntriesCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aListOfIntegerToInteger, parameters.iterationPointers));
        assertTrue(isEqual(anInt, parameters.batch));
    }

    @Test
    public void test_MapFetchEntriesCodec_encodeResponse() {
        int fileClientMessageIndex = 165;
        ClientMessage encoded = MapFetchEntriesCodec.encodeResponse(aListOfIntegerToInteger, aListOfDataToData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapAggregateCodec_decodeRequest() {
        int fileClientMessageIndex = 166;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapAggregateCodec.RequestParameters parameters = MapAggregateCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.aggregator));
    }

    @Test
    public void test_MapAggregateCodec_encodeResponse() {
        int fileClientMessageIndex = 167;
        ClientMessage encoded = MapAggregateCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapAggregateWithPredicateCodec_decodeRequest() {
        int fileClientMessageIndex = 168;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapAggregateWithPredicateCodec.RequestParameters parameters = MapAggregateWithPredicateCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.aggregator));
        assertTrue(isEqual(aData, parameters.predicate));
    }

    @Test
    public void test_MapAggregateWithPredicateCodec_encodeResponse() {
        int fileClientMessageIndex = 169;
        ClientMessage encoded = MapAggregateWithPredicateCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapProjectCodec_decodeRequest() {
        int fileClientMessageIndex = 170;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapProjectCodec.RequestParameters parameters = MapProjectCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.projection));
    }

    @Test
    public void test_MapProjectCodec_encodeResponse() {
        int fileClientMessageIndex = 171;
        ClientMessage encoded = MapProjectCodec.encodeResponse(aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapProjectWithPredicateCodec_decodeRequest() {
        int fileClientMessageIndex = 172;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapProjectWithPredicateCodec.RequestParameters parameters = MapProjectWithPredicateCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.projection));
        assertTrue(isEqual(aData, parameters.predicate));
    }

    @Test
    public void test_MapProjectWithPredicateCodec_encodeResponse() {
        int fileClientMessageIndex = 173;
        ClientMessage encoded = MapProjectWithPredicateCodec.encodeResponse(aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapFetchNearCacheInvalidationMetadataCodec_decodeRequest() {
        int fileClientMessageIndex = 174;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapFetchNearCacheInvalidationMetadataCodec.RequestParameters parameters = MapFetchNearCacheInvalidationMetadataCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aListOfStrings, parameters.names));
        assertTrue(isEqual(aUUID, parameters.uuid));
    }

    @Test
    public void test_MapFetchNearCacheInvalidationMetadataCodec_encodeResponse() {
        int fileClientMessageIndex = 175;
        ClientMessage encoded = MapFetchNearCacheInvalidationMetadataCodec.encodeResponse(aListOfStringToListOfIntegerToLong, aListOfIntegerToUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapRemoveAllCodec_decodeRequest() {
        int fileClientMessageIndex = 176;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapRemoveAllCodec.RequestParameters parameters = MapRemoveAllCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.predicate));
    }

    @Test
    public void test_MapRemoveAllCodec_encodeResponse() {
        int fileClientMessageIndex = 177;
        ClientMessage encoded = MapRemoveAllCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapAddNearCacheInvalidationListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 178;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapAddNearCacheInvalidationListenerCodec.RequestParameters parameters = MapAddNearCacheInvalidationListenerCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(anInt, parameters.listenerFlags));
        assertTrue(isEqual(aBoolean, parameters.localOnly));
    }

    @Test
    public void test_MapAddNearCacheInvalidationListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 179;
        ClientMessage encoded = MapAddNearCacheInvalidationListenerCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapAddNearCacheInvalidationListenerCodec_encodeIMapInvalidationEvent() {
        int fileClientMessageIndex = 180;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = MapAddNearCacheInvalidationListenerCodec.encodeIMapInvalidationEvent(null, aUUID, aUUID, aLong);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapAddNearCacheInvalidationListenerCodec_encodeIMapBatchInvalidationEvent() {
        int fileClientMessageIndex = 181;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = MapAddNearCacheInvalidationListenerCodec.encodeIMapBatchInvalidationEvent(aListOfData, aListOfUUIDs, aListOfUUIDs, aListOfLongs);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapFetchWithQueryCodec_decodeRequest() {
        int fileClientMessageIndex = 182;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapFetchWithQueryCodec.RequestParameters parameters = MapFetchWithQueryCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aListOfIntegerToInteger, parameters.iterationPointers));
        assertTrue(isEqual(anInt, parameters.batch));
        assertTrue(isEqual(aData, parameters.projection));
        assertTrue(isEqual(aData, parameters.predicate));
    }

    @Test
    public void test_MapFetchWithQueryCodec_encodeResponse() {
        int fileClientMessageIndex = 183;
        ClientMessage encoded = MapFetchWithQueryCodec.encodeResponse(aListOfData, aListOfIntegerToInteger);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapEventJournalSubscribeCodec_decodeRequest() {
        int fileClientMessageIndex = 184;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, MapEventJournalSubscribeCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_MapEventJournalSubscribeCodec_encodeResponse() {
        int fileClientMessageIndex = 185;
        ClientMessage encoded = MapEventJournalSubscribeCodec.encodeResponse(aLong, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapEventJournalReadCodec_decodeRequest() {
        int fileClientMessageIndex = 186;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapEventJournalReadCodec.RequestParameters parameters = MapEventJournalReadCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aLong, parameters.startSequence));
        assertTrue(isEqual(anInt, parameters.minSize));
        assertTrue(isEqual(anInt, parameters.maxSize));
        assertTrue(isEqual(null, parameters.predicate));
        assertTrue(isEqual(null, parameters.projection));
    }

    @Test
    public void test_MapEventJournalReadCodec_encodeResponse() {
        int fileClientMessageIndex = 187;
        ClientMessage encoded = MapEventJournalReadCodec.encodeResponse(anInt, aListOfData, null, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapSetTtlCodec_decodeRequest() {
        int fileClientMessageIndex = 188;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapSetTtlCodec.RequestParameters parameters = MapSetTtlCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aLong, parameters.ttl));
    }

    @Test
    public void test_MapSetTtlCodec_encodeResponse() {
        int fileClientMessageIndex = 189;
        ClientMessage encoded = MapSetTtlCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapPutWithMaxIdleCodec_decodeRequest() {
        int fileClientMessageIndex = 190;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapPutWithMaxIdleCodec.RequestParameters parameters = MapPutWithMaxIdleCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aData, parameters.value));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aLong, parameters.ttl));
        assertTrue(isEqual(aLong, parameters.maxIdle));
    }

    @Test
    public void test_MapPutWithMaxIdleCodec_encodeResponse() {
        int fileClientMessageIndex = 191;
        ClientMessage encoded = MapPutWithMaxIdleCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapPutTransientWithMaxIdleCodec_decodeRequest() {
        int fileClientMessageIndex = 192;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapPutTransientWithMaxIdleCodec.RequestParameters parameters = MapPutTransientWithMaxIdleCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aData, parameters.value));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aLong, parameters.ttl));
        assertTrue(isEqual(aLong, parameters.maxIdle));
    }

    @Test
    public void test_MapPutTransientWithMaxIdleCodec_encodeResponse() {
        int fileClientMessageIndex = 193;
        ClientMessage encoded = MapPutTransientWithMaxIdleCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapPutIfAbsentWithMaxIdleCodec_decodeRequest() {
        int fileClientMessageIndex = 194;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapPutIfAbsentWithMaxIdleCodec.RequestParameters parameters = MapPutIfAbsentWithMaxIdleCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aData, parameters.value));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aLong, parameters.ttl));
        assertTrue(isEqual(aLong, parameters.maxIdle));
    }

    @Test
    public void test_MapPutIfAbsentWithMaxIdleCodec_encodeResponse() {
        int fileClientMessageIndex = 195;
        ClientMessage encoded = MapPutIfAbsentWithMaxIdleCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapSetWithMaxIdleCodec_decodeRequest() {
        int fileClientMessageIndex = 196;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapSetWithMaxIdleCodec.RequestParameters parameters = MapSetWithMaxIdleCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aData, parameters.value));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aLong, parameters.ttl));
        assertTrue(isEqual(aLong, parameters.maxIdle));
    }

    @Test
    public void test_MapSetWithMaxIdleCodec_encodeResponse() {
        int fileClientMessageIndex = 197;
        ClientMessage encoded = MapSetWithMaxIdleCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MapReplaceAllCodec_decodeRequest() {
        int fileClientMessageIndex = 198;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MapReplaceAllCodec.RequestParameters parameters = MapReplaceAllCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.function));
    }

    @Test
    public void test_MapReplaceAllCodec_encodeResponse() {
        int fileClientMessageIndex = 199;
        ClientMessage encoded = MapReplaceAllCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapPutCodec_decodeRequest() {
        int fileClientMessageIndex = 200;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MultiMapPutCodec.RequestParameters parameters = MultiMapPutCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aData, parameters.value));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_MultiMapPutCodec_encodeResponse() {
        int fileClientMessageIndex = 201;
        ClientMessage encoded = MultiMapPutCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapGetCodec_decodeRequest() {
        int fileClientMessageIndex = 202;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MultiMapGetCodec.RequestParameters parameters = MultiMapGetCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_MultiMapGetCodec_encodeResponse() {
        int fileClientMessageIndex = 203;
        ClientMessage encoded = MultiMapGetCodec.encodeResponse(aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapRemoveCodec_decodeRequest() {
        int fileClientMessageIndex = 204;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MultiMapRemoveCodec.RequestParameters parameters = MultiMapRemoveCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_MultiMapRemoveCodec_encodeResponse() {
        int fileClientMessageIndex = 205;
        ClientMessage encoded = MultiMapRemoveCodec.encodeResponse(aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapKeySetCodec_decodeRequest() {
        int fileClientMessageIndex = 206;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, MultiMapKeySetCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_MultiMapKeySetCodec_encodeResponse() {
        int fileClientMessageIndex = 207;
        ClientMessage encoded = MultiMapKeySetCodec.encodeResponse(aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapValuesCodec_decodeRequest() {
        int fileClientMessageIndex = 208;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, MultiMapValuesCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_MultiMapValuesCodec_encodeResponse() {
        int fileClientMessageIndex = 209;
        ClientMessage encoded = MultiMapValuesCodec.encodeResponse(aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapEntrySetCodec_decodeRequest() {
        int fileClientMessageIndex = 210;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, MultiMapEntrySetCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_MultiMapEntrySetCodec_encodeResponse() {
        int fileClientMessageIndex = 211;
        ClientMessage encoded = MultiMapEntrySetCodec.encodeResponse(aListOfDataToData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapContainsKeyCodec_decodeRequest() {
        int fileClientMessageIndex = 212;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MultiMapContainsKeyCodec.RequestParameters parameters = MultiMapContainsKeyCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_MultiMapContainsKeyCodec_encodeResponse() {
        int fileClientMessageIndex = 213;
        ClientMessage encoded = MultiMapContainsKeyCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapContainsValueCodec_decodeRequest() {
        int fileClientMessageIndex = 214;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MultiMapContainsValueCodec.RequestParameters parameters = MultiMapContainsValueCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.value));
    }

    @Test
    public void test_MultiMapContainsValueCodec_encodeResponse() {
        int fileClientMessageIndex = 215;
        ClientMessage encoded = MultiMapContainsValueCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapContainsEntryCodec_decodeRequest() {
        int fileClientMessageIndex = 216;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MultiMapContainsEntryCodec.RequestParameters parameters = MultiMapContainsEntryCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aData, parameters.value));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_MultiMapContainsEntryCodec_encodeResponse() {
        int fileClientMessageIndex = 217;
        ClientMessage encoded = MultiMapContainsEntryCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapSizeCodec_decodeRequest() {
        int fileClientMessageIndex = 218;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, MultiMapSizeCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_MultiMapSizeCodec_encodeResponse() {
        int fileClientMessageIndex = 219;
        ClientMessage encoded = MultiMapSizeCodec.encodeResponse(anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapClearCodec_decodeRequest() {
        int fileClientMessageIndex = 220;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, MultiMapClearCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_MultiMapClearCodec_encodeResponse() {
        int fileClientMessageIndex = 221;
        ClientMessage encoded = MultiMapClearCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapValueCountCodec_decodeRequest() {
        int fileClientMessageIndex = 222;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MultiMapValueCountCodec.RequestParameters parameters = MultiMapValueCountCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_MultiMapValueCountCodec_encodeResponse() {
        int fileClientMessageIndex = 223;
        ClientMessage encoded = MultiMapValueCountCodec.encodeResponse(anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapAddEntryListenerToKeyCodec_decodeRequest() {
        int fileClientMessageIndex = 224;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MultiMapAddEntryListenerToKeyCodec.RequestParameters parameters = MultiMapAddEntryListenerToKeyCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aBoolean, parameters.includeValue));
        assertTrue(isEqual(aBoolean, parameters.localOnly));
    }

    @Test
    public void test_MultiMapAddEntryListenerToKeyCodec_encodeResponse() {
        int fileClientMessageIndex = 225;
        ClientMessage encoded = MultiMapAddEntryListenerToKeyCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapAddEntryListenerToKeyCodec_encodeEntryEvent() {
        int fileClientMessageIndex = 226;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = MultiMapAddEntryListenerToKeyCodec.encodeEntryEvent(null, null, null, null, anInt, aUUID, anInt);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapAddEntryListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 227;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MultiMapAddEntryListenerCodec.RequestParameters parameters = MultiMapAddEntryListenerCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aBoolean, parameters.includeValue));
        assertTrue(isEqual(aBoolean, parameters.localOnly));
    }

    @Test
    public void test_MultiMapAddEntryListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 228;
        ClientMessage encoded = MultiMapAddEntryListenerCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapAddEntryListenerCodec_encodeEntryEvent() {
        int fileClientMessageIndex = 229;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = MultiMapAddEntryListenerCodec.encodeEntryEvent(null, null, null, null, anInt, aUUID, anInt);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapRemoveEntryListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 230;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MultiMapRemoveEntryListenerCodec.RequestParameters parameters = MultiMapRemoveEntryListenerCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.registrationId));
    }

    @Test
    public void test_MultiMapRemoveEntryListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 231;
        ClientMessage encoded = MultiMapRemoveEntryListenerCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapLockCodec_decodeRequest() {
        int fileClientMessageIndex = 232;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MultiMapLockCodec.RequestParameters parameters = MultiMapLockCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aLong, parameters.ttl));
        assertTrue(isEqual(aLong, parameters.referenceId));
    }

    @Test
    public void test_MultiMapLockCodec_encodeResponse() {
        int fileClientMessageIndex = 233;
        ClientMessage encoded = MultiMapLockCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapTryLockCodec_decodeRequest() {
        int fileClientMessageIndex = 234;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MultiMapTryLockCodec.RequestParameters parameters = MultiMapTryLockCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aLong, parameters.lease));
        assertTrue(isEqual(aLong, parameters.timeout));
        assertTrue(isEqual(aLong, parameters.referenceId));
    }

    @Test
    public void test_MultiMapTryLockCodec_encodeResponse() {
        int fileClientMessageIndex = 235;
        ClientMessage encoded = MultiMapTryLockCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapIsLockedCodec_decodeRequest() {
        int fileClientMessageIndex = 236;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MultiMapIsLockedCodec.RequestParameters parameters = MultiMapIsLockedCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
    }

    @Test
    public void test_MultiMapIsLockedCodec_encodeResponse() {
        int fileClientMessageIndex = 237;
        ClientMessage encoded = MultiMapIsLockedCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapUnlockCodec_decodeRequest() {
        int fileClientMessageIndex = 238;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MultiMapUnlockCodec.RequestParameters parameters = MultiMapUnlockCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aLong, parameters.referenceId));
    }

    @Test
    public void test_MultiMapUnlockCodec_encodeResponse() {
        int fileClientMessageIndex = 239;
        ClientMessage encoded = MultiMapUnlockCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapForceUnlockCodec_decodeRequest() {
        int fileClientMessageIndex = 240;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MultiMapForceUnlockCodec.RequestParameters parameters = MultiMapForceUnlockCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aLong, parameters.referenceId));
    }

    @Test
    public void test_MultiMapForceUnlockCodec_encodeResponse() {
        int fileClientMessageIndex = 241;
        ClientMessage encoded = MultiMapForceUnlockCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapRemoveEntryCodec_decodeRequest() {
        int fileClientMessageIndex = 242;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MultiMapRemoveEntryCodec.RequestParameters parameters = MultiMapRemoveEntryCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aData, parameters.value));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_MultiMapRemoveEntryCodec_encodeResponse() {
        int fileClientMessageIndex = 243;
        ClientMessage encoded = MultiMapRemoveEntryCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapDeleteCodec_decodeRequest() {
        int fileClientMessageIndex = 244;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MultiMapDeleteCodec.RequestParameters parameters = MultiMapDeleteCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_MultiMapDeleteCodec_encodeResponse() {
        int fileClientMessageIndex = 245;
        ClientMessage encoded = MultiMapDeleteCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MultiMapPutAllCodec_decodeRequest() {
        int fileClientMessageIndex = 246;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MultiMapPutAllCodec.RequestParameters parameters = MultiMapPutAllCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aListOfDataToListOfData, parameters.entries));
    }

    @Test
    public void test_MultiMapPutAllCodec_encodeResponse() {
        int fileClientMessageIndex = 247;
        ClientMessage encoded = MultiMapPutAllCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueOfferCodec_decodeRequest() {
        int fileClientMessageIndex = 248;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        QueueOfferCodec.RequestParameters parameters = QueueOfferCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.value));
        assertTrue(isEqual(aLong, parameters.timeoutMillis));
    }

    @Test
    public void test_QueueOfferCodec_encodeResponse() {
        int fileClientMessageIndex = 249;
        ClientMessage encoded = QueueOfferCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueuePutCodec_decodeRequest() {
        int fileClientMessageIndex = 250;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        QueuePutCodec.RequestParameters parameters = QueuePutCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.value));
    }

    @Test
    public void test_QueuePutCodec_encodeResponse() {
        int fileClientMessageIndex = 251;
        ClientMessage encoded = QueuePutCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueSizeCodec_decodeRequest() {
        int fileClientMessageIndex = 252;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, QueueSizeCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_QueueSizeCodec_encodeResponse() {
        int fileClientMessageIndex = 253;
        ClientMessage encoded = QueueSizeCodec.encodeResponse(anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueRemoveCodec_decodeRequest() {
        int fileClientMessageIndex = 254;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        QueueRemoveCodec.RequestParameters parameters = QueueRemoveCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.value));
    }

    @Test
    public void test_QueueRemoveCodec_encodeResponse() {
        int fileClientMessageIndex = 255;
        ClientMessage encoded = QueueRemoveCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueuePollCodec_decodeRequest() {
        int fileClientMessageIndex = 256;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        QueuePollCodec.RequestParameters parameters = QueuePollCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aLong, parameters.timeoutMillis));
    }

    @Test
    public void test_QueuePollCodec_encodeResponse() {
        int fileClientMessageIndex = 257;
        ClientMessage encoded = QueuePollCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueTakeCodec_decodeRequest() {
        int fileClientMessageIndex = 258;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, QueueTakeCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_QueueTakeCodec_encodeResponse() {
        int fileClientMessageIndex = 259;
        ClientMessage encoded = QueueTakeCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueuePeekCodec_decodeRequest() {
        int fileClientMessageIndex = 260;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, QueuePeekCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_QueuePeekCodec_encodeResponse() {
        int fileClientMessageIndex = 261;
        ClientMessage encoded = QueuePeekCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueIteratorCodec_decodeRequest() {
        int fileClientMessageIndex = 262;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, QueueIteratorCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_QueueIteratorCodec_encodeResponse() {
        int fileClientMessageIndex = 263;
        ClientMessage encoded = QueueIteratorCodec.encodeResponse(aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueDrainToCodec_decodeRequest() {
        int fileClientMessageIndex = 264;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, QueueDrainToCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_QueueDrainToCodec_encodeResponse() {
        int fileClientMessageIndex = 265;
        ClientMessage encoded = QueueDrainToCodec.encodeResponse(aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueDrainToMaxSizeCodec_decodeRequest() {
        int fileClientMessageIndex = 266;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        QueueDrainToMaxSizeCodec.RequestParameters parameters = QueueDrainToMaxSizeCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(anInt, parameters.maxSize));
    }

    @Test
    public void test_QueueDrainToMaxSizeCodec_encodeResponse() {
        int fileClientMessageIndex = 267;
        ClientMessage encoded = QueueDrainToMaxSizeCodec.encodeResponse(aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueContainsCodec_decodeRequest() {
        int fileClientMessageIndex = 268;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        QueueContainsCodec.RequestParameters parameters = QueueContainsCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.value));
    }

    @Test
    public void test_QueueContainsCodec_encodeResponse() {
        int fileClientMessageIndex = 269;
        ClientMessage encoded = QueueContainsCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueContainsAllCodec_decodeRequest() {
        int fileClientMessageIndex = 270;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        QueueContainsAllCodec.RequestParameters parameters = QueueContainsAllCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aListOfData, parameters.dataList));
    }

    @Test
    public void test_QueueContainsAllCodec_encodeResponse() {
        int fileClientMessageIndex = 271;
        ClientMessage encoded = QueueContainsAllCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueCompareAndRemoveAllCodec_decodeRequest() {
        int fileClientMessageIndex = 272;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        QueueCompareAndRemoveAllCodec.RequestParameters parameters = QueueCompareAndRemoveAllCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aListOfData, parameters.dataList));
    }

    @Test
    public void test_QueueCompareAndRemoveAllCodec_encodeResponse() {
        int fileClientMessageIndex = 273;
        ClientMessage encoded = QueueCompareAndRemoveAllCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueCompareAndRetainAllCodec_decodeRequest() {
        int fileClientMessageIndex = 274;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        QueueCompareAndRetainAllCodec.RequestParameters parameters = QueueCompareAndRetainAllCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aListOfData, parameters.dataList));
    }

    @Test
    public void test_QueueCompareAndRetainAllCodec_encodeResponse() {
        int fileClientMessageIndex = 275;
        ClientMessage encoded = QueueCompareAndRetainAllCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueClearCodec_decodeRequest() {
        int fileClientMessageIndex = 276;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, QueueClearCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_QueueClearCodec_encodeResponse() {
        int fileClientMessageIndex = 277;
        ClientMessage encoded = QueueClearCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueAddAllCodec_decodeRequest() {
        int fileClientMessageIndex = 278;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        QueueAddAllCodec.RequestParameters parameters = QueueAddAllCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aListOfData, parameters.dataList));
    }

    @Test
    public void test_QueueAddAllCodec_encodeResponse() {
        int fileClientMessageIndex = 279;
        ClientMessage encoded = QueueAddAllCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueAddListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 280;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        QueueAddListenerCodec.RequestParameters parameters = QueueAddListenerCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aBoolean, parameters.includeValue));
        assertTrue(isEqual(aBoolean, parameters.localOnly));
    }

    @Test
    public void test_QueueAddListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 281;
        ClientMessage encoded = QueueAddListenerCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueAddListenerCodec_encodeItemEvent() {
        int fileClientMessageIndex = 282;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = QueueAddListenerCodec.encodeItemEvent(null, aUUID, anInt);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueRemoveListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 283;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        QueueRemoveListenerCodec.RequestParameters parameters = QueueRemoveListenerCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.registrationId));
    }

    @Test
    public void test_QueueRemoveListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 284;
        ClientMessage encoded = QueueRemoveListenerCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueRemainingCapacityCodec_decodeRequest() {
        int fileClientMessageIndex = 285;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, QueueRemainingCapacityCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_QueueRemainingCapacityCodec_encodeResponse() {
        int fileClientMessageIndex = 286;
        ClientMessage encoded = QueueRemainingCapacityCodec.encodeResponse(anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_QueueIsEmptyCodec_decodeRequest() {
        int fileClientMessageIndex = 287;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, QueueIsEmptyCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_QueueIsEmptyCodec_encodeResponse() {
        int fileClientMessageIndex = 288;
        ClientMessage encoded = QueueIsEmptyCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TopicPublishCodec_decodeRequest() {
        int fileClientMessageIndex = 289;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TopicPublishCodec.RequestParameters parameters = TopicPublishCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.message));
    }

    @Test
    public void test_TopicPublishCodec_encodeResponse() {
        int fileClientMessageIndex = 290;
        ClientMessage encoded = TopicPublishCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TopicAddMessageListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 291;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TopicAddMessageListenerCodec.RequestParameters parameters = TopicAddMessageListenerCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aBoolean, parameters.localOnly));
    }

    @Test
    public void test_TopicAddMessageListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 292;
        ClientMessage encoded = TopicAddMessageListenerCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TopicAddMessageListenerCodec_encodeTopicEvent() {
        int fileClientMessageIndex = 293;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = TopicAddMessageListenerCodec.encodeTopicEvent(aData, aLong, aUUID);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TopicRemoveMessageListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 294;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TopicRemoveMessageListenerCodec.RequestParameters parameters = TopicRemoveMessageListenerCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.registrationId));
    }

    @Test
    public void test_TopicRemoveMessageListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 295;
        ClientMessage encoded = TopicRemoveMessageListenerCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TopicPublishAllCodec_decodeRequest() {
        int fileClientMessageIndex = 296;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TopicPublishAllCodec.RequestParameters parameters = TopicPublishAllCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aListOfData, parameters.messages));
    }

    @Test
    public void test_TopicPublishAllCodec_encodeResponse() {
        int fileClientMessageIndex = 297;
        ClientMessage encoded = TopicPublishAllCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListSizeCodec_decodeRequest() {
        int fileClientMessageIndex = 298;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, ListSizeCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_ListSizeCodec_encodeResponse() {
        int fileClientMessageIndex = 299;
        ClientMessage encoded = ListSizeCodec.encodeResponse(anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListContainsCodec_decodeRequest() {
        int fileClientMessageIndex = 300;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ListContainsCodec.RequestParameters parameters = ListContainsCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.value));
    }

    @Test
    public void test_ListContainsCodec_encodeResponse() {
        int fileClientMessageIndex = 301;
        ClientMessage encoded = ListContainsCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListContainsAllCodec_decodeRequest() {
        int fileClientMessageIndex = 302;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ListContainsAllCodec.RequestParameters parameters = ListContainsAllCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aListOfData, parameters.values));
    }

    @Test
    public void test_ListContainsAllCodec_encodeResponse() {
        int fileClientMessageIndex = 303;
        ClientMessage encoded = ListContainsAllCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListAddCodec_decodeRequest() {
        int fileClientMessageIndex = 304;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ListAddCodec.RequestParameters parameters = ListAddCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.value));
    }

    @Test
    public void test_ListAddCodec_encodeResponse() {
        int fileClientMessageIndex = 305;
        ClientMessage encoded = ListAddCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListRemoveCodec_decodeRequest() {
        int fileClientMessageIndex = 306;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ListRemoveCodec.RequestParameters parameters = ListRemoveCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.value));
    }

    @Test
    public void test_ListRemoveCodec_encodeResponse() {
        int fileClientMessageIndex = 307;
        ClientMessage encoded = ListRemoveCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListAddAllCodec_decodeRequest() {
        int fileClientMessageIndex = 308;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ListAddAllCodec.RequestParameters parameters = ListAddAllCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aListOfData, parameters.valueList));
    }

    @Test
    public void test_ListAddAllCodec_encodeResponse() {
        int fileClientMessageIndex = 309;
        ClientMessage encoded = ListAddAllCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListCompareAndRemoveAllCodec_decodeRequest() {
        int fileClientMessageIndex = 310;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ListCompareAndRemoveAllCodec.RequestParameters parameters = ListCompareAndRemoveAllCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aListOfData, parameters.values));
    }

    @Test
    public void test_ListCompareAndRemoveAllCodec_encodeResponse() {
        int fileClientMessageIndex = 311;
        ClientMessage encoded = ListCompareAndRemoveAllCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListCompareAndRetainAllCodec_decodeRequest() {
        int fileClientMessageIndex = 312;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ListCompareAndRetainAllCodec.RequestParameters parameters = ListCompareAndRetainAllCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aListOfData, parameters.values));
    }

    @Test
    public void test_ListCompareAndRetainAllCodec_encodeResponse() {
        int fileClientMessageIndex = 313;
        ClientMessage encoded = ListCompareAndRetainAllCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListClearCodec_decodeRequest() {
        int fileClientMessageIndex = 314;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, ListClearCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_ListClearCodec_encodeResponse() {
        int fileClientMessageIndex = 315;
        ClientMessage encoded = ListClearCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListGetAllCodec_decodeRequest() {
        int fileClientMessageIndex = 316;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, ListGetAllCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_ListGetAllCodec_encodeResponse() {
        int fileClientMessageIndex = 317;
        ClientMessage encoded = ListGetAllCodec.encodeResponse(aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListAddListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 318;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ListAddListenerCodec.RequestParameters parameters = ListAddListenerCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aBoolean, parameters.includeValue));
        assertTrue(isEqual(aBoolean, parameters.localOnly));
    }

    @Test
    public void test_ListAddListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 319;
        ClientMessage encoded = ListAddListenerCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListAddListenerCodec_encodeItemEvent() {
        int fileClientMessageIndex = 320;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = ListAddListenerCodec.encodeItemEvent(null, aUUID, anInt);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListRemoveListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 321;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ListRemoveListenerCodec.RequestParameters parameters = ListRemoveListenerCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.registrationId));
    }

    @Test
    public void test_ListRemoveListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 322;
        ClientMessage encoded = ListRemoveListenerCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListIsEmptyCodec_decodeRequest() {
        int fileClientMessageIndex = 323;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, ListIsEmptyCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_ListIsEmptyCodec_encodeResponse() {
        int fileClientMessageIndex = 324;
        ClientMessage encoded = ListIsEmptyCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListAddAllWithIndexCodec_decodeRequest() {
        int fileClientMessageIndex = 325;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ListAddAllWithIndexCodec.RequestParameters parameters = ListAddAllWithIndexCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(anInt, parameters.index));
        assertTrue(isEqual(aListOfData, parameters.valueList));
    }

    @Test
    public void test_ListAddAllWithIndexCodec_encodeResponse() {
        int fileClientMessageIndex = 326;
        ClientMessage encoded = ListAddAllWithIndexCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListGetCodec_decodeRequest() {
        int fileClientMessageIndex = 327;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ListGetCodec.RequestParameters parameters = ListGetCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(anInt, parameters.index));
    }

    @Test
    public void test_ListGetCodec_encodeResponse() {
        int fileClientMessageIndex = 328;
        ClientMessage encoded = ListGetCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListSetCodec_decodeRequest() {
        int fileClientMessageIndex = 329;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ListSetCodec.RequestParameters parameters = ListSetCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(anInt, parameters.index));
        assertTrue(isEqual(aData, parameters.value));
    }

    @Test
    public void test_ListSetCodec_encodeResponse() {
        int fileClientMessageIndex = 330;
        ClientMessage encoded = ListSetCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListAddWithIndexCodec_decodeRequest() {
        int fileClientMessageIndex = 331;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ListAddWithIndexCodec.RequestParameters parameters = ListAddWithIndexCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(anInt, parameters.index));
        assertTrue(isEqual(aData, parameters.value));
    }

    @Test
    public void test_ListAddWithIndexCodec_encodeResponse() {
        int fileClientMessageIndex = 332;
        ClientMessage encoded = ListAddWithIndexCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListRemoveWithIndexCodec_decodeRequest() {
        int fileClientMessageIndex = 333;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ListRemoveWithIndexCodec.RequestParameters parameters = ListRemoveWithIndexCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(anInt, parameters.index));
    }

    @Test
    public void test_ListRemoveWithIndexCodec_encodeResponse() {
        int fileClientMessageIndex = 334;
        ClientMessage encoded = ListRemoveWithIndexCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListLastIndexOfCodec_decodeRequest() {
        int fileClientMessageIndex = 335;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ListLastIndexOfCodec.RequestParameters parameters = ListLastIndexOfCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.value));
    }

    @Test
    public void test_ListLastIndexOfCodec_encodeResponse() {
        int fileClientMessageIndex = 336;
        ClientMessage encoded = ListLastIndexOfCodec.encodeResponse(anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListIndexOfCodec_decodeRequest() {
        int fileClientMessageIndex = 337;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ListIndexOfCodec.RequestParameters parameters = ListIndexOfCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.value));
    }

    @Test
    public void test_ListIndexOfCodec_encodeResponse() {
        int fileClientMessageIndex = 338;
        ClientMessage encoded = ListIndexOfCodec.encodeResponse(anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListSubCodec_decodeRequest() {
        int fileClientMessageIndex = 339;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ListSubCodec.RequestParameters parameters = ListSubCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(anInt, parameters.from));
        assertTrue(isEqual(anInt, parameters.to));
    }

    @Test
    public void test_ListSubCodec_encodeResponse() {
        int fileClientMessageIndex = 340;
        ClientMessage encoded = ListSubCodec.encodeResponse(aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListIteratorCodec_decodeRequest() {
        int fileClientMessageIndex = 341;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, ListIteratorCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_ListIteratorCodec_encodeResponse() {
        int fileClientMessageIndex = 342;
        ClientMessage encoded = ListIteratorCodec.encodeResponse(aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ListListIteratorCodec_decodeRequest() {
        int fileClientMessageIndex = 343;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ListListIteratorCodec.RequestParameters parameters = ListListIteratorCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(anInt, parameters.index));
    }

    @Test
    public void test_ListListIteratorCodec_encodeResponse() {
        int fileClientMessageIndex = 344;
        ClientMessage encoded = ListListIteratorCodec.encodeResponse(aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SetSizeCodec_decodeRequest() {
        int fileClientMessageIndex = 345;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, SetSizeCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_SetSizeCodec_encodeResponse() {
        int fileClientMessageIndex = 346;
        ClientMessage encoded = SetSizeCodec.encodeResponse(anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SetContainsCodec_decodeRequest() {
        int fileClientMessageIndex = 347;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        SetContainsCodec.RequestParameters parameters = SetContainsCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.value));
    }

    @Test
    public void test_SetContainsCodec_encodeResponse() {
        int fileClientMessageIndex = 348;
        ClientMessage encoded = SetContainsCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SetContainsAllCodec_decodeRequest() {
        int fileClientMessageIndex = 349;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        SetContainsAllCodec.RequestParameters parameters = SetContainsAllCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aListOfData, parameters.items));
    }

    @Test
    public void test_SetContainsAllCodec_encodeResponse() {
        int fileClientMessageIndex = 350;
        ClientMessage encoded = SetContainsAllCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SetAddCodec_decodeRequest() {
        int fileClientMessageIndex = 351;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        SetAddCodec.RequestParameters parameters = SetAddCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.value));
    }

    @Test
    public void test_SetAddCodec_encodeResponse() {
        int fileClientMessageIndex = 352;
        ClientMessage encoded = SetAddCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SetRemoveCodec_decodeRequest() {
        int fileClientMessageIndex = 353;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        SetRemoveCodec.RequestParameters parameters = SetRemoveCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.value));
    }

    @Test
    public void test_SetRemoveCodec_encodeResponse() {
        int fileClientMessageIndex = 354;
        ClientMessage encoded = SetRemoveCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SetAddAllCodec_decodeRequest() {
        int fileClientMessageIndex = 355;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        SetAddAllCodec.RequestParameters parameters = SetAddAllCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aListOfData, parameters.valueList));
    }

    @Test
    public void test_SetAddAllCodec_encodeResponse() {
        int fileClientMessageIndex = 356;
        ClientMessage encoded = SetAddAllCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SetCompareAndRemoveAllCodec_decodeRequest() {
        int fileClientMessageIndex = 357;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        SetCompareAndRemoveAllCodec.RequestParameters parameters = SetCompareAndRemoveAllCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aListOfData, parameters.values));
    }

    @Test
    public void test_SetCompareAndRemoveAllCodec_encodeResponse() {
        int fileClientMessageIndex = 358;
        ClientMessage encoded = SetCompareAndRemoveAllCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SetCompareAndRetainAllCodec_decodeRequest() {
        int fileClientMessageIndex = 359;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        SetCompareAndRetainAllCodec.RequestParameters parameters = SetCompareAndRetainAllCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aListOfData, parameters.values));
    }

    @Test
    public void test_SetCompareAndRetainAllCodec_encodeResponse() {
        int fileClientMessageIndex = 360;
        ClientMessage encoded = SetCompareAndRetainAllCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SetClearCodec_decodeRequest() {
        int fileClientMessageIndex = 361;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, SetClearCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_SetClearCodec_encodeResponse() {
        int fileClientMessageIndex = 362;
        ClientMessage encoded = SetClearCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SetGetAllCodec_decodeRequest() {
        int fileClientMessageIndex = 363;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, SetGetAllCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_SetGetAllCodec_encodeResponse() {
        int fileClientMessageIndex = 364;
        ClientMessage encoded = SetGetAllCodec.encodeResponse(aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SetAddListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 365;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        SetAddListenerCodec.RequestParameters parameters = SetAddListenerCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aBoolean, parameters.includeValue));
        assertTrue(isEqual(aBoolean, parameters.localOnly));
    }

    @Test
    public void test_SetAddListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 366;
        ClientMessage encoded = SetAddListenerCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SetAddListenerCodec_encodeItemEvent() {
        int fileClientMessageIndex = 367;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = SetAddListenerCodec.encodeItemEvent(null, aUUID, anInt);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SetRemoveListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 368;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        SetRemoveListenerCodec.RequestParameters parameters = SetRemoveListenerCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.registrationId));
    }

    @Test
    public void test_SetRemoveListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 369;
        ClientMessage encoded = SetRemoveListenerCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SetIsEmptyCodec_decodeRequest() {
        int fileClientMessageIndex = 370;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, SetIsEmptyCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_SetIsEmptyCodec_encodeResponse() {
        int fileClientMessageIndex = 371;
        ClientMessage encoded = SetIsEmptyCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_FencedLockLockCodec_decodeRequest() {
        int fileClientMessageIndex = 372;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        FencedLockLockCodec.RequestParameters parameters = FencedLockLockCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aRaftGroupId, parameters.groupId));
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aLong, parameters.sessionId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aUUID, parameters.invocationUid));
    }

    @Test
    public void test_FencedLockLockCodec_encodeResponse() {
        int fileClientMessageIndex = 373;
        ClientMessage encoded = FencedLockLockCodec.encodeResponse(aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_FencedLockTryLockCodec_decodeRequest() {
        int fileClientMessageIndex = 374;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        FencedLockTryLockCodec.RequestParameters parameters = FencedLockTryLockCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aRaftGroupId, parameters.groupId));
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aLong, parameters.sessionId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aUUID, parameters.invocationUid));
        assertTrue(isEqual(aLong, parameters.timeoutMs));
    }

    @Test
    public void test_FencedLockTryLockCodec_encodeResponse() {
        int fileClientMessageIndex = 375;
        ClientMessage encoded = FencedLockTryLockCodec.encodeResponse(aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_FencedLockUnlockCodec_decodeRequest() {
        int fileClientMessageIndex = 376;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        FencedLockUnlockCodec.RequestParameters parameters = FencedLockUnlockCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aRaftGroupId, parameters.groupId));
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aLong, parameters.sessionId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aUUID, parameters.invocationUid));
    }

    @Test
    public void test_FencedLockUnlockCodec_encodeResponse() {
        int fileClientMessageIndex = 377;
        ClientMessage encoded = FencedLockUnlockCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_FencedLockGetLockOwnershipCodec_decodeRequest() {
        int fileClientMessageIndex = 378;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        FencedLockGetLockOwnershipCodec.RequestParameters parameters = FencedLockGetLockOwnershipCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aRaftGroupId, parameters.groupId));
        assertTrue(isEqual(aString, parameters.name));
    }

    @Test
    public void test_FencedLockGetLockOwnershipCodec_encodeResponse() {
        int fileClientMessageIndex = 379;
        ClientMessage encoded = FencedLockGetLockOwnershipCodec.encodeResponse(aLong, anInt, aLong, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ExecutorServiceShutdownCodec_decodeRequest() {
        int fileClientMessageIndex = 380;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, ExecutorServiceShutdownCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_ExecutorServiceShutdownCodec_encodeResponse() {
        int fileClientMessageIndex = 381;
        ClientMessage encoded = ExecutorServiceShutdownCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ExecutorServiceIsShutdownCodec_decodeRequest() {
        int fileClientMessageIndex = 382;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, ExecutorServiceIsShutdownCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_ExecutorServiceIsShutdownCodec_encodeResponse() {
        int fileClientMessageIndex = 383;
        ClientMessage encoded = ExecutorServiceIsShutdownCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ExecutorServiceCancelOnPartitionCodec_decodeRequest() {
        int fileClientMessageIndex = 384;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ExecutorServiceCancelOnPartitionCodec.RequestParameters parameters = ExecutorServiceCancelOnPartitionCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aUUID, parameters.uuid));
        assertTrue(isEqual(aBoolean, parameters.interrupt));
    }

    @Test
    public void test_ExecutorServiceCancelOnPartitionCodec_encodeResponse() {
        int fileClientMessageIndex = 385;
        ClientMessage encoded = ExecutorServiceCancelOnPartitionCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ExecutorServiceCancelOnMemberCodec_decodeRequest() {
        int fileClientMessageIndex = 386;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ExecutorServiceCancelOnMemberCodec.RequestParameters parameters = ExecutorServiceCancelOnMemberCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aUUID, parameters.uuid));
        assertTrue(isEqual(aUUID, parameters.memberUUID));
        assertTrue(isEqual(aBoolean, parameters.interrupt));
    }

    @Test
    public void test_ExecutorServiceCancelOnMemberCodec_encodeResponse() {
        int fileClientMessageIndex = 387;
        ClientMessage encoded = ExecutorServiceCancelOnMemberCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ExecutorServiceSubmitToPartitionCodec_decodeRequest() {
        int fileClientMessageIndex = 388;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ExecutorServiceSubmitToPartitionCodec.RequestParameters parameters = ExecutorServiceSubmitToPartitionCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.uuid));
        assertTrue(isEqual(aData, parameters.callable));
    }

    @Test
    public void test_ExecutorServiceSubmitToPartitionCodec_encodeResponse() {
        int fileClientMessageIndex = 389;
        ClientMessage encoded = ExecutorServiceSubmitToPartitionCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ExecutorServiceSubmitToMemberCodec_decodeRequest() {
        int fileClientMessageIndex = 390;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ExecutorServiceSubmitToMemberCodec.RequestParameters parameters = ExecutorServiceSubmitToMemberCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.uuid));
        assertTrue(isEqual(aData, parameters.callable));
        assertTrue(isEqual(aUUID, parameters.memberUUID));
    }

    @Test
    public void test_ExecutorServiceSubmitToMemberCodec_encodeResponse() {
        int fileClientMessageIndex = 391;
        ClientMessage encoded = ExecutorServiceSubmitToMemberCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_AtomicLongApplyCodec_decodeRequest() {
        int fileClientMessageIndex = 392;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        AtomicLongApplyCodec.RequestParameters parameters = AtomicLongApplyCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aRaftGroupId, parameters.groupId));
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.function));
    }

    @Test
    public void test_AtomicLongApplyCodec_encodeResponse() {
        int fileClientMessageIndex = 393;
        ClientMessage encoded = AtomicLongApplyCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_AtomicLongAlterCodec_decodeRequest() {
        int fileClientMessageIndex = 394;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        AtomicLongAlterCodec.RequestParameters parameters = AtomicLongAlterCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aRaftGroupId, parameters.groupId));
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.function));
        assertTrue(isEqual(anInt, parameters.returnValueType));
    }

    @Test
    public void test_AtomicLongAlterCodec_encodeResponse() {
        int fileClientMessageIndex = 395;
        ClientMessage encoded = AtomicLongAlterCodec.encodeResponse(aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_AtomicLongAddAndGetCodec_decodeRequest() {
        int fileClientMessageIndex = 396;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        AtomicLongAddAndGetCodec.RequestParameters parameters = AtomicLongAddAndGetCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aRaftGroupId, parameters.groupId));
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aLong, parameters.delta));
    }

    @Test
    public void test_AtomicLongAddAndGetCodec_encodeResponse() {
        int fileClientMessageIndex = 397;
        ClientMessage encoded = AtomicLongAddAndGetCodec.encodeResponse(aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_AtomicLongCompareAndSetCodec_decodeRequest() {
        int fileClientMessageIndex = 398;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        AtomicLongCompareAndSetCodec.RequestParameters parameters = AtomicLongCompareAndSetCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aRaftGroupId, parameters.groupId));
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aLong, parameters.expected));
        assertTrue(isEqual(aLong, parameters.updated));
    }

    @Test
    public void test_AtomicLongCompareAndSetCodec_encodeResponse() {
        int fileClientMessageIndex = 399;
        ClientMessage encoded = AtomicLongCompareAndSetCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_AtomicLongGetCodec_decodeRequest() {
        int fileClientMessageIndex = 400;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        AtomicLongGetCodec.RequestParameters parameters = AtomicLongGetCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aRaftGroupId, parameters.groupId));
        assertTrue(isEqual(aString, parameters.name));
    }

    @Test
    public void test_AtomicLongGetCodec_encodeResponse() {
        int fileClientMessageIndex = 401;
        ClientMessage encoded = AtomicLongGetCodec.encodeResponse(aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_AtomicLongGetAndAddCodec_decodeRequest() {
        int fileClientMessageIndex = 402;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        AtomicLongGetAndAddCodec.RequestParameters parameters = AtomicLongGetAndAddCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aRaftGroupId, parameters.groupId));
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aLong, parameters.delta));
    }

    @Test
    public void test_AtomicLongGetAndAddCodec_encodeResponse() {
        int fileClientMessageIndex = 403;
        ClientMessage encoded = AtomicLongGetAndAddCodec.encodeResponse(aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_AtomicLongGetAndSetCodec_decodeRequest() {
        int fileClientMessageIndex = 404;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        AtomicLongGetAndSetCodec.RequestParameters parameters = AtomicLongGetAndSetCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aRaftGroupId, parameters.groupId));
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aLong, parameters.newValue));
    }

    @Test
    public void test_AtomicLongGetAndSetCodec_encodeResponse() {
        int fileClientMessageIndex = 405;
        ClientMessage encoded = AtomicLongGetAndSetCodec.encodeResponse(aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_AtomicRefApplyCodec_decodeRequest() {
        int fileClientMessageIndex = 406;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        AtomicRefApplyCodec.RequestParameters parameters = AtomicRefApplyCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aRaftGroupId, parameters.groupId));
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.function));
        assertTrue(isEqual(anInt, parameters.returnValueType));
        assertTrue(isEqual(aBoolean, parameters.alter));
    }

    @Test
    public void test_AtomicRefApplyCodec_encodeResponse() {
        int fileClientMessageIndex = 407;
        ClientMessage encoded = AtomicRefApplyCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_AtomicRefCompareAndSetCodec_decodeRequest() {
        int fileClientMessageIndex = 408;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        AtomicRefCompareAndSetCodec.RequestParameters parameters = AtomicRefCompareAndSetCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aRaftGroupId, parameters.groupId));
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(null, parameters.oldValue));
        assertTrue(isEqual(null, parameters.newValue));
    }

    @Test
    public void test_AtomicRefCompareAndSetCodec_encodeResponse() {
        int fileClientMessageIndex = 409;
        ClientMessage encoded = AtomicRefCompareAndSetCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_AtomicRefContainsCodec_decodeRequest() {
        int fileClientMessageIndex = 410;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        AtomicRefContainsCodec.RequestParameters parameters = AtomicRefContainsCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aRaftGroupId, parameters.groupId));
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(null, parameters.value));
    }

    @Test
    public void test_AtomicRefContainsCodec_encodeResponse() {
        int fileClientMessageIndex = 411;
        ClientMessage encoded = AtomicRefContainsCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_AtomicRefGetCodec_decodeRequest() {
        int fileClientMessageIndex = 412;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        AtomicRefGetCodec.RequestParameters parameters = AtomicRefGetCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aRaftGroupId, parameters.groupId));
        assertTrue(isEqual(aString, parameters.name));
    }

    @Test
    public void test_AtomicRefGetCodec_encodeResponse() {
        int fileClientMessageIndex = 413;
        ClientMessage encoded = AtomicRefGetCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_AtomicRefSetCodec_decodeRequest() {
        int fileClientMessageIndex = 414;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        AtomicRefSetCodec.RequestParameters parameters = AtomicRefSetCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aRaftGroupId, parameters.groupId));
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(null, parameters.newValue));
        assertTrue(isEqual(aBoolean, parameters.returnOldValue));
    }

    @Test
    public void test_AtomicRefSetCodec_encodeResponse() {
        int fileClientMessageIndex = 415;
        ClientMessage encoded = AtomicRefSetCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CountDownLatchTrySetCountCodec_decodeRequest() {
        int fileClientMessageIndex = 416;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CountDownLatchTrySetCountCodec.RequestParameters parameters = CountDownLatchTrySetCountCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aRaftGroupId, parameters.groupId));
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(anInt, parameters.count));
    }

    @Test
    public void test_CountDownLatchTrySetCountCodec_encodeResponse() {
        int fileClientMessageIndex = 417;
        ClientMessage encoded = CountDownLatchTrySetCountCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CountDownLatchAwaitCodec_decodeRequest() {
        int fileClientMessageIndex = 418;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CountDownLatchAwaitCodec.RequestParameters parameters = CountDownLatchAwaitCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aRaftGroupId, parameters.groupId));
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.invocationUid));
        assertTrue(isEqual(aLong, parameters.timeoutMs));
    }

    @Test
    public void test_CountDownLatchAwaitCodec_encodeResponse() {
        int fileClientMessageIndex = 419;
        ClientMessage encoded = CountDownLatchAwaitCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CountDownLatchCountDownCodec_decodeRequest() {
        int fileClientMessageIndex = 420;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CountDownLatchCountDownCodec.RequestParameters parameters = CountDownLatchCountDownCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aRaftGroupId, parameters.groupId));
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.invocationUid));
        assertTrue(isEqual(anInt, parameters.expectedRound));
    }

    @Test
    public void test_CountDownLatchCountDownCodec_encodeResponse() {
        int fileClientMessageIndex = 421;
        ClientMessage encoded = CountDownLatchCountDownCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CountDownLatchGetCountCodec_decodeRequest() {
        int fileClientMessageIndex = 422;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CountDownLatchGetCountCodec.RequestParameters parameters = CountDownLatchGetCountCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aRaftGroupId, parameters.groupId));
        assertTrue(isEqual(aString, parameters.name));
    }

    @Test
    public void test_CountDownLatchGetCountCodec_encodeResponse() {
        int fileClientMessageIndex = 423;
        ClientMessage encoded = CountDownLatchGetCountCodec.encodeResponse(anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CountDownLatchGetRoundCodec_decodeRequest() {
        int fileClientMessageIndex = 424;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CountDownLatchGetRoundCodec.RequestParameters parameters = CountDownLatchGetRoundCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aRaftGroupId, parameters.groupId));
        assertTrue(isEqual(aString, parameters.name));
    }

    @Test
    public void test_CountDownLatchGetRoundCodec_encodeResponse() {
        int fileClientMessageIndex = 425;
        ClientMessage encoded = CountDownLatchGetRoundCodec.encodeResponse(anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SemaphoreInitCodec_decodeRequest() {
        int fileClientMessageIndex = 426;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        SemaphoreInitCodec.RequestParameters parameters = SemaphoreInitCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aRaftGroupId, parameters.groupId));
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(anInt, parameters.permits));
    }

    @Test
    public void test_SemaphoreInitCodec_encodeResponse() {
        int fileClientMessageIndex = 427;
        ClientMessage encoded = SemaphoreInitCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SemaphoreAcquireCodec_decodeRequest() {
        int fileClientMessageIndex = 428;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        SemaphoreAcquireCodec.RequestParameters parameters = SemaphoreAcquireCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aRaftGroupId, parameters.groupId));
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aLong, parameters.sessionId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aUUID, parameters.invocationUid));
        assertTrue(isEqual(anInt, parameters.permits));
        assertTrue(isEqual(aLong, parameters.timeoutMs));
    }

    @Test
    public void test_SemaphoreAcquireCodec_encodeResponse() {
        int fileClientMessageIndex = 429;
        ClientMessage encoded = SemaphoreAcquireCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SemaphoreReleaseCodec_decodeRequest() {
        int fileClientMessageIndex = 430;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        SemaphoreReleaseCodec.RequestParameters parameters = SemaphoreReleaseCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aRaftGroupId, parameters.groupId));
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aLong, parameters.sessionId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aUUID, parameters.invocationUid));
        assertTrue(isEqual(anInt, parameters.permits));
    }

    @Test
    public void test_SemaphoreReleaseCodec_encodeResponse() {
        int fileClientMessageIndex = 431;
        ClientMessage encoded = SemaphoreReleaseCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SemaphoreDrainCodec_decodeRequest() {
        int fileClientMessageIndex = 432;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        SemaphoreDrainCodec.RequestParameters parameters = SemaphoreDrainCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aRaftGroupId, parameters.groupId));
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aLong, parameters.sessionId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aUUID, parameters.invocationUid));
    }

    @Test
    public void test_SemaphoreDrainCodec_encodeResponse() {
        int fileClientMessageIndex = 433;
        ClientMessage encoded = SemaphoreDrainCodec.encodeResponse(anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SemaphoreChangeCodec_decodeRequest() {
        int fileClientMessageIndex = 434;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        SemaphoreChangeCodec.RequestParameters parameters = SemaphoreChangeCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aRaftGroupId, parameters.groupId));
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aLong, parameters.sessionId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aUUID, parameters.invocationUid));
        assertTrue(isEqual(anInt, parameters.permits));
    }

    @Test
    public void test_SemaphoreChangeCodec_encodeResponse() {
        int fileClientMessageIndex = 435;
        ClientMessage encoded = SemaphoreChangeCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SemaphoreAvailablePermitsCodec_decodeRequest() {
        int fileClientMessageIndex = 436;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        SemaphoreAvailablePermitsCodec.RequestParameters parameters = SemaphoreAvailablePermitsCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aRaftGroupId, parameters.groupId));
        assertTrue(isEqual(aString, parameters.name));
    }

    @Test
    public void test_SemaphoreAvailablePermitsCodec_encodeResponse() {
        int fileClientMessageIndex = 437;
        ClientMessage encoded = SemaphoreAvailablePermitsCodec.encodeResponse(anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SemaphoreGetSemaphoreTypeCodec_decodeRequest() {
        int fileClientMessageIndex = 438;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, SemaphoreGetSemaphoreTypeCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_SemaphoreGetSemaphoreTypeCodec_encodeResponse() {
        int fileClientMessageIndex = 439;
        ClientMessage encoded = SemaphoreGetSemaphoreTypeCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapPutCodec_decodeRequest() {
        int fileClientMessageIndex = 440;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ReplicatedMapPutCodec.RequestParameters parameters = ReplicatedMapPutCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aData, parameters.value));
        assertTrue(isEqual(aLong, parameters.ttl));
    }

    @Test
    public void test_ReplicatedMapPutCodec_encodeResponse() {
        int fileClientMessageIndex = 441;
        ClientMessage encoded = ReplicatedMapPutCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapSizeCodec_decodeRequest() {
        int fileClientMessageIndex = 442;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, ReplicatedMapSizeCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_ReplicatedMapSizeCodec_encodeResponse() {
        int fileClientMessageIndex = 443;
        ClientMessage encoded = ReplicatedMapSizeCodec.encodeResponse(anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapIsEmptyCodec_decodeRequest() {
        int fileClientMessageIndex = 444;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, ReplicatedMapIsEmptyCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_ReplicatedMapIsEmptyCodec_encodeResponse() {
        int fileClientMessageIndex = 445;
        ClientMessage encoded = ReplicatedMapIsEmptyCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapContainsKeyCodec_decodeRequest() {
        int fileClientMessageIndex = 446;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ReplicatedMapContainsKeyCodec.RequestParameters parameters = ReplicatedMapContainsKeyCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
    }

    @Test
    public void test_ReplicatedMapContainsKeyCodec_encodeResponse() {
        int fileClientMessageIndex = 447;
        ClientMessage encoded = ReplicatedMapContainsKeyCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapContainsValueCodec_decodeRequest() {
        int fileClientMessageIndex = 448;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ReplicatedMapContainsValueCodec.RequestParameters parameters = ReplicatedMapContainsValueCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.value));
    }

    @Test
    public void test_ReplicatedMapContainsValueCodec_encodeResponse() {
        int fileClientMessageIndex = 449;
        ClientMessage encoded = ReplicatedMapContainsValueCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapGetCodec_decodeRequest() {
        int fileClientMessageIndex = 450;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ReplicatedMapGetCodec.RequestParameters parameters = ReplicatedMapGetCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
    }

    @Test
    public void test_ReplicatedMapGetCodec_encodeResponse() {
        int fileClientMessageIndex = 451;
        ClientMessage encoded = ReplicatedMapGetCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapRemoveCodec_decodeRequest() {
        int fileClientMessageIndex = 452;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ReplicatedMapRemoveCodec.RequestParameters parameters = ReplicatedMapRemoveCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
    }

    @Test
    public void test_ReplicatedMapRemoveCodec_encodeResponse() {
        int fileClientMessageIndex = 453;
        ClientMessage encoded = ReplicatedMapRemoveCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapPutAllCodec_decodeRequest() {
        int fileClientMessageIndex = 454;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ReplicatedMapPutAllCodec.RequestParameters parameters = ReplicatedMapPutAllCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aListOfDataToData, parameters.entries));
    }

    @Test
    public void test_ReplicatedMapPutAllCodec_encodeResponse() {
        int fileClientMessageIndex = 455;
        ClientMessage encoded = ReplicatedMapPutAllCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapClearCodec_decodeRequest() {
        int fileClientMessageIndex = 456;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, ReplicatedMapClearCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_ReplicatedMapClearCodec_encodeResponse() {
        int fileClientMessageIndex = 457;
        ClientMessage encoded = ReplicatedMapClearCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapAddEntryListenerToKeyWithPredicateCodec_decodeRequest() {
        int fileClientMessageIndex = 458;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.RequestParameters parameters = ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aData, parameters.predicate));
        assertTrue(isEqual(aBoolean, parameters.localOnly));
    }

    @Test
    public void test_ReplicatedMapAddEntryListenerToKeyWithPredicateCodec_encodeResponse() {
        int fileClientMessageIndex = 459;
        ClientMessage encoded = ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapAddEntryListenerToKeyWithPredicateCodec_encodeEntryEvent() {
        int fileClientMessageIndex = 460;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.encodeEntryEvent(null, null, null, null, anInt, aUUID, anInt);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapAddEntryListenerWithPredicateCodec_decodeRequest() {
        int fileClientMessageIndex = 461;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ReplicatedMapAddEntryListenerWithPredicateCodec.RequestParameters parameters = ReplicatedMapAddEntryListenerWithPredicateCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.predicate));
        assertTrue(isEqual(aBoolean, parameters.localOnly));
    }

    @Test
    public void test_ReplicatedMapAddEntryListenerWithPredicateCodec_encodeResponse() {
        int fileClientMessageIndex = 462;
        ClientMessage encoded = ReplicatedMapAddEntryListenerWithPredicateCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapAddEntryListenerWithPredicateCodec_encodeEntryEvent() {
        int fileClientMessageIndex = 463;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = ReplicatedMapAddEntryListenerWithPredicateCodec.encodeEntryEvent(null, null, null, null, anInt, aUUID, anInt);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapAddEntryListenerToKeyCodec_decodeRequest() {
        int fileClientMessageIndex = 464;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ReplicatedMapAddEntryListenerToKeyCodec.RequestParameters parameters = ReplicatedMapAddEntryListenerToKeyCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aBoolean, parameters.localOnly));
    }

    @Test
    public void test_ReplicatedMapAddEntryListenerToKeyCodec_encodeResponse() {
        int fileClientMessageIndex = 465;
        ClientMessage encoded = ReplicatedMapAddEntryListenerToKeyCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapAddEntryListenerToKeyCodec_encodeEntryEvent() {
        int fileClientMessageIndex = 466;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = ReplicatedMapAddEntryListenerToKeyCodec.encodeEntryEvent(null, null, null, null, anInt, aUUID, anInt);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapAddEntryListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 467;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ReplicatedMapAddEntryListenerCodec.RequestParameters parameters = ReplicatedMapAddEntryListenerCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aBoolean, parameters.localOnly));
    }

    @Test
    public void test_ReplicatedMapAddEntryListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 468;
        ClientMessage encoded = ReplicatedMapAddEntryListenerCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapAddEntryListenerCodec_encodeEntryEvent() {
        int fileClientMessageIndex = 469;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = ReplicatedMapAddEntryListenerCodec.encodeEntryEvent(null, null, null, null, anInt, aUUID, anInt);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapRemoveEntryListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 470;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ReplicatedMapRemoveEntryListenerCodec.RequestParameters parameters = ReplicatedMapRemoveEntryListenerCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.registrationId));
    }

    @Test
    public void test_ReplicatedMapRemoveEntryListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 471;
        ClientMessage encoded = ReplicatedMapRemoveEntryListenerCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapKeySetCodec_decodeRequest() {
        int fileClientMessageIndex = 472;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, ReplicatedMapKeySetCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_ReplicatedMapKeySetCodec_encodeResponse() {
        int fileClientMessageIndex = 473;
        ClientMessage encoded = ReplicatedMapKeySetCodec.encodeResponse(aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapValuesCodec_decodeRequest() {
        int fileClientMessageIndex = 474;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, ReplicatedMapValuesCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_ReplicatedMapValuesCodec_encodeResponse() {
        int fileClientMessageIndex = 475;
        ClientMessage encoded = ReplicatedMapValuesCodec.encodeResponse(aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapEntrySetCodec_decodeRequest() {
        int fileClientMessageIndex = 476;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, ReplicatedMapEntrySetCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_ReplicatedMapEntrySetCodec_encodeResponse() {
        int fileClientMessageIndex = 477;
        ClientMessage encoded = ReplicatedMapEntrySetCodec.encodeResponse(aListOfDataToData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapAddNearCacheEntryListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 478;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ReplicatedMapAddNearCacheEntryListenerCodec.RequestParameters parameters = ReplicatedMapAddNearCacheEntryListenerCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aBoolean, parameters.includeValue));
        assertTrue(isEqual(aBoolean, parameters.localOnly));
    }

    @Test
    public void test_ReplicatedMapAddNearCacheEntryListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 479;
        ClientMessage encoded = ReplicatedMapAddNearCacheEntryListenerCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ReplicatedMapAddNearCacheEntryListenerCodec_encodeEntryEvent() {
        int fileClientMessageIndex = 480;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = ReplicatedMapAddNearCacheEntryListenerCodec.encodeEntryEvent(null, null, null, null, anInt, aUUID, anInt);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapContainsKeyCodec_decodeRequest() {
        int fileClientMessageIndex = 481;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalMapContainsKeyCodec.RequestParameters parameters = TransactionalMapContainsKeyCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aData, parameters.key));
    }

    @Test
    public void test_TransactionalMapContainsKeyCodec_encodeResponse() {
        int fileClientMessageIndex = 482;
        ClientMessage encoded = TransactionalMapContainsKeyCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapGetCodec_decodeRequest() {
        int fileClientMessageIndex = 483;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalMapGetCodec.RequestParameters parameters = TransactionalMapGetCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aData, parameters.key));
    }

    @Test
    public void test_TransactionalMapGetCodec_encodeResponse() {
        int fileClientMessageIndex = 484;
        ClientMessage encoded = TransactionalMapGetCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapGetForUpdateCodec_decodeRequest() {
        int fileClientMessageIndex = 485;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalMapGetForUpdateCodec.RequestParameters parameters = TransactionalMapGetForUpdateCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aData, parameters.key));
    }

    @Test
    public void test_TransactionalMapGetForUpdateCodec_encodeResponse() {
        int fileClientMessageIndex = 486;
        ClientMessage encoded = TransactionalMapGetForUpdateCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapSizeCodec_decodeRequest() {
        int fileClientMessageIndex = 487;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalMapSizeCodec.RequestParameters parameters = TransactionalMapSizeCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_TransactionalMapSizeCodec_encodeResponse() {
        int fileClientMessageIndex = 488;
        ClientMessage encoded = TransactionalMapSizeCodec.encodeResponse(anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapIsEmptyCodec_decodeRequest() {
        int fileClientMessageIndex = 489;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalMapIsEmptyCodec.RequestParameters parameters = TransactionalMapIsEmptyCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_TransactionalMapIsEmptyCodec_encodeResponse() {
        int fileClientMessageIndex = 490;
        ClientMessage encoded = TransactionalMapIsEmptyCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapPutCodec_decodeRequest() {
        int fileClientMessageIndex = 491;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalMapPutCodec.RequestParameters parameters = TransactionalMapPutCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aData, parameters.value));
        assertTrue(isEqual(aLong, parameters.ttl));
    }

    @Test
    public void test_TransactionalMapPutCodec_encodeResponse() {
        int fileClientMessageIndex = 492;
        ClientMessage encoded = TransactionalMapPutCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapSetCodec_decodeRequest() {
        int fileClientMessageIndex = 493;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalMapSetCodec.RequestParameters parameters = TransactionalMapSetCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aData, parameters.value));
    }

    @Test
    public void test_TransactionalMapSetCodec_encodeResponse() {
        int fileClientMessageIndex = 494;
        ClientMessage encoded = TransactionalMapSetCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapPutIfAbsentCodec_decodeRequest() {
        int fileClientMessageIndex = 495;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalMapPutIfAbsentCodec.RequestParameters parameters = TransactionalMapPutIfAbsentCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aData, parameters.value));
    }

    @Test
    public void test_TransactionalMapPutIfAbsentCodec_encodeResponse() {
        int fileClientMessageIndex = 496;
        ClientMessage encoded = TransactionalMapPutIfAbsentCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapReplaceCodec_decodeRequest() {
        int fileClientMessageIndex = 497;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalMapReplaceCodec.RequestParameters parameters = TransactionalMapReplaceCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aData, parameters.value));
    }

    @Test
    public void test_TransactionalMapReplaceCodec_encodeResponse() {
        int fileClientMessageIndex = 498;
        ClientMessage encoded = TransactionalMapReplaceCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapReplaceIfSameCodec_decodeRequest() {
        int fileClientMessageIndex = 499;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalMapReplaceIfSameCodec.RequestParameters parameters = TransactionalMapReplaceIfSameCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aData, parameters.oldValue));
        assertTrue(isEqual(aData, parameters.newValue));
    }

    @Test
    public void test_TransactionalMapReplaceIfSameCodec_encodeResponse() {
        int fileClientMessageIndex = 500;
        ClientMessage encoded = TransactionalMapReplaceIfSameCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapRemoveCodec_decodeRequest() {
        int fileClientMessageIndex = 501;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalMapRemoveCodec.RequestParameters parameters = TransactionalMapRemoveCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aData, parameters.key));
    }

    @Test
    public void test_TransactionalMapRemoveCodec_encodeResponse() {
        int fileClientMessageIndex = 502;
        ClientMessage encoded = TransactionalMapRemoveCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapDeleteCodec_decodeRequest() {
        int fileClientMessageIndex = 503;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalMapDeleteCodec.RequestParameters parameters = TransactionalMapDeleteCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aData, parameters.key));
    }

    @Test
    public void test_TransactionalMapDeleteCodec_encodeResponse() {
        int fileClientMessageIndex = 504;
        ClientMessage encoded = TransactionalMapDeleteCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapRemoveIfSameCodec_decodeRequest() {
        int fileClientMessageIndex = 505;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalMapRemoveIfSameCodec.RequestParameters parameters = TransactionalMapRemoveIfSameCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aData, parameters.value));
    }

    @Test
    public void test_TransactionalMapRemoveIfSameCodec_encodeResponse() {
        int fileClientMessageIndex = 506;
        ClientMessage encoded = TransactionalMapRemoveIfSameCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapKeySetCodec_decodeRequest() {
        int fileClientMessageIndex = 507;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalMapKeySetCodec.RequestParameters parameters = TransactionalMapKeySetCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_TransactionalMapKeySetCodec_encodeResponse() {
        int fileClientMessageIndex = 508;
        ClientMessage encoded = TransactionalMapKeySetCodec.encodeResponse(aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapKeySetWithPredicateCodec_decodeRequest() {
        int fileClientMessageIndex = 509;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalMapKeySetWithPredicateCodec.RequestParameters parameters = TransactionalMapKeySetWithPredicateCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aData, parameters.predicate));
    }

    @Test
    public void test_TransactionalMapKeySetWithPredicateCodec_encodeResponse() {
        int fileClientMessageIndex = 510;
        ClientMessage encoded = TransactionalMapKeySetWithPredicateCodec.encodeResponse(aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapValuesCodec_decodeRequest() {
        int fileClientMessageIndex = 511;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalMapValuesCodec.RequestParameters parameters = TransactionalMapValuesCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_TransactionalMapValuesCodec_encodeResponse() {
        int fileClientMessageIndex = 512;
        ClientMessage encoded = TransactionalMapValuesCodec.encodeResponse(aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapValuesWithPredicateCodec_decodeRequest() {
        int fileClientMessageIndex = 513;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalMapValuesWithPredicateCodec.RequestParameters parameters = TransactionalMapValuesWithPredicateCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aData, parameters.predicate));
    }

    @Test
    public void test_TransactionalMapValuesWithPredicateCodec_encodeResponse() {
        int fileClientMessageIndex = 514;
        ClientMessage encoded = TransactionalMapValuesWithPredicateCodec.encodeResponse(aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMapContainsValueCodec_decodeRequest() {
        int fileClientMessageIndex = 515;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalMapContainsValueCodec.RequestParameters parameters = TransactionalMapContainsValueCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aData, parameters.value));
    }

    @Test
    public void test_TransactionalMapContainsValueCodec_encodeResponse() {
        int fileClientMessageIndex = 516;
        ClientMessage encoded = TransactionalMapContainsValueCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMultiMapPutCodec_decodeRequest() {
        int fileClientMessageIndex = 517;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalMultiMapPutCodec.RequestParameters parameters = TransactionalMultiMapPutCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aData, parameters.value));
    }

    @Test
    public void test_TransactionalMultiMapPutCodec_encodeResponse() {
        int fileClientMessageIndex = 518;
        ClientMessage encoded = TransactionalMultiMapPutCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMultiMapGetCodec_decodeRequest() {
        int fileClientMessageIndex = 519;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalMultiMapGetCodec.RequestParameters parameters = TransactionalMultiMapGetCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aData, parameters.key));
    }

    @Test
    public void test_TransactionalMultiMapGetCodec_encodeResponse() {
        int fileClientMessageIndex = 520;
        ClientMessage encoded = TransactionalMultiMapGetCodec.encodeResponse(aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMultiMapRemoveCodec_decodeRequest() {
        int fileClientMessageIndex = 521;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalMultiMapRemoveCodec.RequestParameters parameters = TransactionalMultiMapRemoveCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aData, parameters.key));
    }

    @Test
    public void test_TransactionalMultiMapRemoveCodec_encodeResponse() {
        int fileClientMessageIndex = 522;
        ClientMessage encoded = TransactionalMultiMapRemoveCodec.encodeResponse(aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMultiMapRemoveEntryCodec_decodeRequest() {
        int fileClientMessageIndex = 523;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalMultiMapRemoveEntryCodec.RequestParameters parameters = TransactionalMultiMapRemoveEntryCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aData, parameters.value));
    }

    @Test
    public void test_TransactionalMultiMapRemoveEntryCodec_encodeResponse() {
        int fileClientMessageIndex = 524;
        ClientMessage encoded = TransactionalMultiMapRemoveEntryCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMultiMapValueCountCodec_decodeRequest() {
        int fileClientMessageIndex = 525;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalMultiMapValueCountCodec.RequestParameters parameters = TransactionalMultiMapValueCountCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aData, parameters.key));
    }

    @Test
    public void test_TransactionalMultiMapValueCountCodec_encodeResponse() {
        int fileClientMessageIndex = 526;
        ClientMessage encoded = TransactionalMultiMapValueCountCodec.encodeResponse(anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalMultiMapSizeCodec_decodeRequest() {
        int fileClientMessageIndex = 527;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalMultiMapSizeCodec.RequestParameters parameters = TransactionalMultiMapSizeCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_TransactionalMultiMapSizeCodec_encodeResponse() {
        int fileClientMessageIndex = 528;
        ClientMessage encoded = TransactionalMultiMapSizeCodec.encodeResponse(anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalSetAddCodec_decodeRequest() {
        int fileClientMessageIndex = 529;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalSetAddCodec.RequestParameters parameters = TransactionalSetAddCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aData, parameters.item));
    }

    @Test
    public void test_TransactionalSetAddCodec_encodeResponse() {
        int fileClientMessageIndex = 530;
        ClientMessage encoded = TransactionalSetAddCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalSetRemoveCodec_decodeRequest() {
        int fileClientMessageIndex = 531;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalSetRemoveCodec.RequestParameters parameters = TransactionalSetRemoveCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aData, parameters.item));
    }

    @Test
    public void test_TransactionalSetRemoveCodec_encodeResponse() {
        int fileClientMessageIndex = 532;
        ClientMessage encoded = TransactionalSetRemoveCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalSetSizeCodec_decodeRequest() {
        int fileClientMessageIndex = 533;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalSetSizeCodec.RequestParameters parameters = TransactionalSetSizeCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_TransactionalSetSizeCodec_encodeResponse() {
        int fileClientMessageIndex = 534;
        ClientMessage encoded = TransactionalSetSizeCodec.encodeResponse(anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalListAddCodec_decodeRequest() {
        int fileClientMessageIndex = 535;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalListAddCodec.RequestParameters parameters = TransactionalListAddCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aData, parameters.item));
    }

    @Test
    public void test_TransactionalListAddCodec_encodeResponse() {
        int fileClientMessageIndex = 536;
        ClientMessage encoded = TransactionalListAddCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalListRemoveCodec_decodeRequest() {
        int fileClientMessageIndex = 537;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalListRemoveCodec.RequestParameters parameters = TransactionalListRemoveCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aData, parameters.item));
    }

    @Test
    public void test_TransactionalListRemoveCodec_encodeResponse() {
        int fileClientMessageIndex = 538;
        ClientMessage encoded = TransactionalListRemoveCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalListSizeCodec_decodeRequest() {
        int fileClientMessageIndex = 539;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalListSizeCodec.RequestParameters parameters = TransactionalListSizeCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_TransactionalListSizeCodec_encodeResponse() {
        int fileClientMessageIndex = 540;
        ClientMessage encoded = TransactionalListSizeCodec.encodeResponse(anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalQueueOfferCodec_decodeRequest() {
        int fileClientMessageIndex = 541;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalQueueOfferCodec.RequestParameters parameters = TransactionalQueueOfferCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aData, parameters.item));
        assertTrue(isEqual(aLong, parameters.timeout));
    }

    @Test
    public void test_TransactionalQueueOfferCodec_encodeResponse() {
        int fileClientMessageIndex = 542;
        ClientMessage encoded = TransactionalQueueOfferCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalQueueTakeCodec_decodeRequest() {
        int fileClientMessageIndex = 543;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalQueueTakeCodec.RequestParameters parameters = TransactionalQueueTakeCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_TransactionalQueueTakeCodec_encodeResponse() {
        int fileClientMessageIndex = 544;
        ClientMessage encoded = TransactionalQueueTakeCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalQueuePollCodec_decodeRequest() {
        int fileClientMessageIndex = 545;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalQueuePollCodec.RequestParameters parameters = TransactionalQueuePollCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aLong, parameters.timeout));
    }

    @Test
    public void test_TransactionalQueuePollCodec_encodeResponse() {
        int fileClientMessageIndex = 546;
        ClientMessage encoded = TransactionalQueuePollCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalQueuePeekCodec_decodeRequest() {
        int fileClientMessageIndex = 547;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalQueuePeekCodec.RequestParameters parameters = TransactionalQueuePeekCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
        assertTrue(isEqual(aLong, parameters.timeout));
    }

    @Test
    public void test_TransactionalQueuePeekCodec_encodeResponse() {
        int fileClientMessageIndex = 548;
        ClientMessage encoded = TransactionalQueuePeekCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionalQueueSizeCodec_decodeRequest() {
        int fileClientMessageIndex = 549;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionalQueueSizeCodec.RequestParameters parameters = TransactionalQueueSizeCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.txnId));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_TransactionalQueueSizeCodec_encodeResponse() {
        int fileClientMessageIndex = 550;
        ClientMessage encoded = TransactionalQueueSizeCodec.encodeResponse(anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheAddEntryListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 551;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheAddEntryListenerCodec.RequestParameters parameters = CacheAddEntryListenerCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aBoolean, parameters.localOnly));
    }

    @Test
    public void test_CacheAddEntryListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 552;
        ClientMessage encoded = CacheAddEntryListenerCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheAddEntryListenerCodec_encodeCacheEvent() {
        int fileClientMessageIndex = 553;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = CacheAddEntryListenerCodec.encodeCacheEvent(anInt, aListOfCacheEventData, anInt);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheClearCodec_decodeRequest() {
        int fileClientMessageIndex = 554;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, CacheClearCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_CacheClearCodec_encodeResponse() {
        int fileClientMessageIndex = 555;
        ClientMessage encoded = CacheClearCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheRemoveAllKeysCodec_decodeRequest() {
        int fileClientMessageIndex = 556;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheRemoveAllKeysCodec.RequestParameters parameters = CacheRemoveAllKeysCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aListOfData, parameters.keys));
        assertTrue(isEqual(anInt, parameters.completionId));
    }

    @Test
    public void test_CacheRemoveAllKeysCodec_encodeResponse() {
        int fileClientMessageIndex = 557;
        ClientMessage encoded = CacheRemoveAllKeysCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheRemoveAllCodec_decodeRequest() {
        int fileClientMessageIndex = 558;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheRemoveAllCodec.RequestParameters parameters = CacheRemoveAllCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(anInt, parameters.completionId));
    }

    @Test
    public void test_CacheRemoveAllCodec_encodeResponse() {
        int fileClientMessageIndex = 559;
        ClientMessage encoded = CacheRemoveAllCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheContainsKeyCodec_decodeRequest() {
        int fileClientMessageIndex = 560;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheContainsKeyCodec.RequestParameters parameters = CacheContainsKeyCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
    }

    @Test
    public void test_CacheContainsKeyCodec_encodeResponse() {
        int fileClientMessageIndex = 561;
        ClientMessage encoded = CacheContainsKeyCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheCreateConfigCodec_decodeRequest() {
        int fileClientMessageIndex = 562;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheCreateConfigCodec.RequestParameters parameters = CacheCreateConfigCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aCacheConfigHolder, parameters.cacheConfig));
        assertTrue(isEqual(aBoolean, parameters.createAlsoOnOthers));
    }

    @Test
    public void test_CacheCreateConfigCodec_encodeResponse() {
        int fileClientMessageIndex = 563;
        ClientMessage encoded = CacheCreateConfigCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheDestroyCodec_decodeRequest() {
        int fileClientMessageIndex = 564;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, CacheDestroyCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_CacheDestroyCodec_encodeResponse() {
        int fileClientMessageIndex = 565;
        ClientMessage encoded = CacheDestroyCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheEntryProcessorCodec_decodeRequest() {
        int fileClientMessageIndex = 566;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheEntryProcessorCodec.RequestParameters parameters = CacheEntryProcessorCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aData, parameters.entryProcessor));
        assertTrue(isEqual(aListOfData, parameters.arguments));
        assertTrue(isEqual(anInt, parameters.completionId));
    }

    @Test
    public void test_CacheEntryProcessorCodec_encodeResponse() {
        int fileClientMessageIndex = 567;
        ClientMessage encoded = CacheEntryProcessorCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheGetAllCodec_decodeRequest() {
        int fileClientMessageIndex = 568;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheGetAllCodec.RequestParameters parameters = CacheGetAllCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aListOfData, parameters.keys));
        assertTrue(isEqual(null, parameters.expiryPolicy));
    }

    @Test
    public void test_CacheGetAllCodec_encodeResponse() {
        int fileClientMessageIndex = 569;
        ClientMessage encoded = CacheGetAllCodec.encodeResponse(aListOfDataToData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheGetAndRemoveCodec_decodeRequest() {
        int fileClientMessageIndex = 570;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheGetAndRemoveCodec.RequestParameters parameters = CacheGetAndRemoveCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(anInt, parameters.completionId));
    }

    @Test
    public void test_CacheGetAndRemoveCodec_encodeResponse() {
        int fileClientMessageIndex = 571;
        ClientMessage encoded = CacheGetAndRemoveCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheGetAndReplaceCodec_decodeRequest() {
        int fileClientMessageIndex = 572;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheGetAndReplaceCodec.RequestParameters parameters = CacheGetAndReplaceCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aData, parameters.value));
        assertTrue(isEqual(null, parameters.expiryPolicy));
        assertTrue(isEqual(anInt, parameters.completionId));
    }

    @Test
    public void test_CacheGetAndReplaceCodec_encodeResponse() {
        int fileClientMessageIndex = 573;
        ClientMessage encoded = CacheGetAndReplaceCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheGetConfigCodec_decodeRequest() {
        int fileClientMessageIndex = 574;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheGetConfigCodec.RequestParameters parameters = CacheGetConfigCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aString, parameters.simpleName));
    }

    @Test
    public void test_CacheGetConfigCodec_encodeResponse() {
        int fileClientMessageIndex = 575;
        ClientMessage encoded = CacheGetConfigCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheGetCodec_decodeRequest() {
        int fileClientMessageIndex = 576;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheGetCodec.RequestParameters parameters = CacheGetCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(null, parameters.expiryPolicy));
    }

    @Test
    public void test_CacheGetCodec_encodeResponse() {
        int fileClientMessageIndex = 577;
        ClientMessage encoded = CacheGetCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheIterateCodec_decodeRequest() {
        int fileClientMessageIndex = 578;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheIterateCodec.RequestParameters parameters = CacheIterateCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aListOfIntegerToInteger, parameters.iterationPointers));
        assertTrue(isEqual(anInt, parameters.batch));
    }

    @Test
    public void test_CacheIterateCodec_encodeResponse() {
        int fileClientMessageIndex = 579;
        ClientMessage encoded = CacheIterateCodec.encodeResponse(aListOfIntegerToInteger, aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheListenerRegistrationCodec_decodeRequest() {
        int fileClientMessageIndex = 580;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheListenerRegistrationCodec.RequestParameters parameters = CacheListenerRegistrationCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.listenerConfig));
        assertTrue(isEqual(aBoolean, parameters.shouldRegister));
        assertTrue(isEqual(aUUID, parameters.uuid));
    }

    @Test
    public void test_CacheListenerRegistrationCodec_encodeResponse() {
        int fileClientMessageIndex = 581;
        ClientMessage encoded = CacheListenerRegistrationCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheLoadAllCodec_decodeRequest() {
        int fileClientMessageIndex = 582;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheLoadAllCodec.RequestParameters parameters = CacheLoadAllCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aListOfData, parameters.keys));
        assertTrue(isEqual(aBoolean, parameters.replaceExistingValues));
    }

    @Test
    public void test_CacheLoadAllCodec_encodeResponse() {
        int fileClientMessageIndex = 583;
        ClientMessage encoded = CacheLoadAllCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheManagementConfigCodec_decodeRequest() {
        int fileClientMessageIndex = 584;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheManagementConfigCodec.RequestParameters parameters = CacheManagementConfigCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aBoolean, parameters.isStat));
        assertTrue(isEqual(aBoolean, parameters.enabled));
        assertTrue(isEqual(aUUID, parameters.uuid));
    }

    @Test
    public void test_CacheManagementConfigCodec_encodeResponse() {
        int fileClientMessageIndex = 585;
        ClientMessage encoded = CacheManagementConfigCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CachePutIfAbsentCodec_decodeRequest() {
        int fileClientMessageIndex = 586;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CachePutIfAbsentCodec.RequestParameters parameters = CachePutIfAbsentCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aData, parameters.value));
        assertTrue(isEqual(null, parameters.expiryPolicy));
        assertTrue(isEqual(anInt, parameters.completionId));
    }

    @Test
    public void test_CachePutIfAbsentCodec_encodeResponse() {
        int fileClientMessageIndex = 587;
        ClientMessage encoded = CachePutIfAbsentCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CachePutCodec_decodeRequest() {
        int fileClientMessageIndex = 588;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CachePutCodec.RequestParameters parameters = CachePutCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(aData, parameters.value));
        assertTrue(isEqual(null, parameters.expiryPolicy));
        assertTrue(isEqual(aBoolean, parameters.get));
        assertTrue(isEqual(anInt, parameters.completionId));
    }

    @Test
    public void test_CachePutCodec_encodeResponse() {
        int fileClientMessageIndex = 589;
        ClientMessage encoded = CachePutCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheRemoveEntryListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 590;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheRemoveEntryListenerCodec.RequestParameters parameters = CacheRemoveEntryListenerCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.registrationId));
    }

    @Test
    public void test_CacheRemoveEntryListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 591;
        ClientMessage encoded = CacheRemoveEntryListenerCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheRemoveInvalidationListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 592;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheRemoveInvalidationListenerCodec.RequestParameters parameters = CacheRemoveInvalidationListenerCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.registrationId));
    }

    @Test
    public void test_CacheRemoveInvalidationListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 593;
        ClientMessage encoded = CacheRemoveInvalidationListenerCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheRemoveCodec_decodeRequest() {
        int fileClientMessageIndex = 594;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheRemoveCodec.RequestParameters parameters = CacheRemoveCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(null, parameters.currentValue));
        assertTrue(isEqual(anInt, parameters.completionId));
    }

    @Test
    public void test_CacheRemoveCodec_encodeResponse() {
        int fileClientMessageIndex = 595;
        ClientMessage encoded = CacheRemoveCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheReplaceCodec_decodeRequest() {
        int fileClientMessageIndex = 596;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheReplaceCodec.RequestParameters parameters = CacheReplaceCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.key));
        assertTrue(isEqual(null, parameters.oldValue));
        assertTrue(isEqual(aData, parameters.newValue));
        assertTrue(isEqual(null, parameters.expiryPolicy));
        assertTrue(isEqual(anInt, parameters.completionId));
    }

    @Test
    public void test_CacheReplaceCodec_encodeResponse() {
        int fileClientMessageIndex = 597;
        ClientMessage encoded = CacheReplaceCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheSizeCodec_decodeRequest() {
        int fileClientMessageIndex = 598;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, CacheSizeCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_CacheSizeCodec_encodeResponse() {
        int fileClientMessageIndex = 599;
        ClientMessage encoded = CacheSizeCodec.encodeResponse(anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheAddPartitionLostListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 600;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheAddPartitionLostListenerCodec.RequestParameters parameters = CacheAddPartitionLostListenerCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aBoolean, parameters.localOnly));
    }

    @Test
    public void test_CacheAddPartitionLostListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 601;
        ClientMessage encoded = CacheAddPartitionLostListenerCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheAddPartitionLostListenerCodec_encodeCachePartitionLostEvent() {
        int fileClientMessageIndex = 602;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = CacheAddPartitionLostListenerCodec.encodeCachePartitionLostEvent(anInt, aUUID);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheRemovePartitionLostListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 603;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheRemovePartitionLostListenerCodec.RequestParameters parameters = CacheRemovePartitionLostListenerCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aUUID, parameters.registrationId));
    }

    @Test
    public void test_CacheRemovePartitionLostListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 604;
        ClientMessage encoded = CacheRemovePartitionLostListenerCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CachePutAllCodec_decodeRequest() {
        int fileClientMessageIndex = 605;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CachePutAllCodec.RequestParameters parameters = CachePutAllCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aListOfDataToData, parameters.entries));
        assertTrue(isEqual(null, parameters.expiryPolicy));
        assertTrue(isEqual(anInt, parameters.completionId));
    }

    @Test
    public void test_CachePutAllCodec_encodeResponse() {
        int fileClientMessageIndex = 606;
        ClientMessage encoded = CachePutAllCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheIterateEntriesCodec_decodeRequest() {
        int fileClientMessageIndex = 607;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheIterateEntriesCodec.RequestParameters parameters = CacheIterateEntriesCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aListOfIntegerToInteger, parameters.iterationPointers));
        assertTrue(isEqual(anInt, parameters.batch));
    }

    @Test
    public void test_CacheIterateEntriesCodec_encodeResponse() {
        int fileClientMessageIndex = 608;
        ClientMessage encoded = CacheIterateEntriesCodec.encodeResponse(aListOfIntegerToInteger, aListOfDataToData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheAddNearCacheInvalidationListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 609;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheAddNearCacheInvalidationListenerCodec.RequestParameters parameters = CacheAddNearCacheInvalidationListenerCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aBoolean, parameters.localOnly));
    }

    @Test
    public void test_CacheAddNearCacheInvalidationListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 610;
        ClientMessage encoded = CacheAddNearCacheInvalidationListenerCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheAddNearCacheInvalidationListenerCodec_encodeCacheInvalidationEvent() {
        int fileClientMessageIndex = 611;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = CacheAddNearCacheInvalidationListenerCodec.encodeCacheInvalidationEvent(aString, null, null, aUUID, aLong);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheAddNearCacheInvalidationListenerCodec_encodeCacheBatchInvalidationEvent() {
        int fileClientMessageIndex = 612;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = CacheAddNearCacheInvalidationListenerCodec.encodeCacheBatchInvalidationEvent(aString, aListOfData, aListOfUUIDs, aListOfUUIDs, aListOfLongs);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheFetchNearCacheInvalidationMetadataCodec_decodeRequest() {
        int fileClientMessageIndex = 613;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheFetchNearCacheInvalidationMetadataCodec.RequestParameters parameters = CacheFetchNearCacheInvalidationMetadataCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aListOfStrings, parameters.names));
        assertTrue(isEqual(aUUID, parameters.uuid));
    }

    @Test
    public void test_CacheFetchNearCacheInvalidationMetadataCodec_encodeResponse() {
        int fileClientMessageIndex = 614;
        ClientMessage encoded = CacheFetchNearCacheInvalidationMetadataCodec.encodeResponse(aListOfStringToListOfIntegerToLong, aListOfIntegerToUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheEventJournalSubscribeCodec_decodeRequest() {
        int fileClientMessageIndex = 615;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, CacheEventJournalSubscribeCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_CacheEventJournalSubscribeCodec_encodeResponse() {
        int fileClientMessageIndex = 616;
        ClientMessage encoded = CacheEventJournalSubscribeCodec.encodeResponse(aLong, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheEventJournalReadCodec_decodeRequest() {
        int fileClientMessageIndex = 617;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheEventJournalReadCodec.RequestParameters parameters = CacheEventJournalReadCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aLong, parameters.startSequence));
        assertTrue(isEqual(anInt, parameters.minSize));
        assertTrue(isEqual(anInt, parameters.maxSize));
        assertTrue(isEqual(null, parameters.predicate));
        assertTrue(isEqual(null, parameters.projection));
    }

    @Test
    public void test_CacheEventJournalReadCodec_encodeResponse() {
        int fileClientMessageIndex = 618;
        ClientMessage encoded = CacheEventJournalReadCodec.encodeResponse(anInt, aListOfData, null, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CacheSetExpiryPolicyCodec_decodeRequest() {
        int fileClientMessageIndex = 619;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CacheSetExpiryPolicyCodec.RequestParameters parameters = CacheSetExpiryPolicyCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aListOfData, parameters.keys));
        assertTrue(isEqual(aData, parameters.expiryPolicy));
    }

    @Test
    public void test_CacheSetExpiryPolicyCodec_encodeResponse() {
        int fileClientMessageIndex = 620;
        ClientMessage encoded = CacheSetExpiryPolicyCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_XATransactionClearRemoteCodec_decodeRequest() {
        int fileClientMessageIndex = 621;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(anXid, XATransactionClearRemoteCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_XATransactionClearRemoteCodec_encodeResponse() {
        int fileClientMessageIndex = 622;
        ClientMessage encoded = XATransactionClearRemoteCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_XATransactionCollectTransactionsCodec_decodeRequest() {
        int fileClientMessageIndex = 623;
    }

    @Test
    public void test_XATransactionCollectTransactionsCodec_encodeResponse() {
        int fileClientMessageIndex = 624;
        ClientMessage encoded = XATransactionCollectTransactionsCodec.encodeResponse(aListOfXids);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_XATransactionFinalizeCodec_decodeRequest() {
        int fileClientMessageIndex = 625;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        XATransactionFinalizeCodec.RequestParameters parameters = XATransactionFinalizeCodec.decodeRequest(fromFile);
        assertTrue(isEqual(anXid, parameters.xid));
        assertTrue(isEqual(aBoolean, parameters.isCommit));
    }

    @Test
    public void test_XATransactionFinalizeCodec_encodeResponse() {
        int fileClientMessageIndex = 626;
        ClientMessage encoded = XATransactionFinalizeCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_XATransactionCommitCodec_decodeRequest() {
        int fileClientMessageIndex = 627;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        XATransactionCommitCodec.RequestParameters parameters = XATransactionCommitCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aUUID, parameters.transactionId));
        assertTrue(isEqual(aBoolean, parameters.onePhase));
    }

    @Test
    public void test_XATransactionCommitCodec_encodeResponse() {
        int fileClientMessageIndex = 628;
        ClientMessage encoded = XATransactionCommitCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_XATransactionCreateCodec_decodeRequest() {
        int fileClientMessageIndex = 629;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        XATransactionCreateCodec.RequestParameters parameters = XATransactionCreateCodec.decodeRequest(fromFile);
        assertTrue(isEqual(anXid, parameters.xid));
        assertTrue(isEqual(aLong, parameters.timeout));
    }

    @Test
    public void test_XATransactionCreateCodec_encodeResponse() {
        int fileClientMessageIndex = 630;
        ClientMessage encoded = XATransactionCreateCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_XATransactionPrepareCodec_decodeRequest() {
        int fileClientMessageIndex = 631;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, XATransactionPrepareCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_XATransactionPrepareCodec_encodeResponse() {
        int fileClientMessageIndex = 632;
        ClientMessage encoded = XATransactionPrepareCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_XATransactionRollbackCodec_decodeRequest() {
        int fileClientMessageIndex = 633;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, XATransactionRollbackCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_XATransactionRollbackCodec_encodeResponse() {
        int fileClientMessageIndex = 634;
        ClientMessage encoded = XATransactionRollbackCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionCommitCodec_decodeRequest() {
        int fileClientMessageIndex = 635;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionCommitCodec.RequestParameters parameters = TransactionCommitCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aUUID, parameters.transactionId));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_TransactionCommitCodec_encodeResponse() {
        int fileClientMessageIndex = 636;
        ClientMessage encoded = TransactionCommitCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionCreateCodec_decodeRequest() {
        int fileClientMessageIndex = 637;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionCreateCodec.RequestParameters parameters = TransactionCreateCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aLong, parameters.timeout));
        assertTrue(isEqual(anInt, parameters.durability));
        assertTrue(isEqual(anInt, parameters.transactionType));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_TransactionCreateCodec_encodeResponse() {
        int fileClientMessageIndex = 638;
        ClientMessage encoded = TransactionCreateCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_TransactionRollbackCodec_decodeRequest() {
        int fileClientMessageIndex = 639;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        TransactionRollbackCodec.RequestParameters parameters = TransactionRollbackCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aUUID, parameters.transactionId));
        assertTrue(isEqual(aLong, parameters.threadId));
    }

    @Test
    public void test_TransactionRollbackCodec_encodeResponse() {
        int fileClientMessageIndex = 640;
        ClientMessage encoded = TransactionRollbackCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ContinuousQueryPublisherCreateWithValueCodec_decodeRequest() {
        int fileClientMessageIndex = 641;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ContinuousQueryPublisherCreateWithValueCodec.RequestParameters parameters = ContinuousQueryPublisherCreateWithValueCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.mapName));
        assertTrue(isEqual(aString, parameters.cacheName));
        assertTrue(isEqual(aData, parameters.predicate));
        assertTrue(isEqual(anInt, parameters.batchSize));
        assertTrue(isEqual(anInt, parameters.bufferSize));
        assertTrue(isEqual(aLong, parameters.delaySeconds));
        assertTrue(isEqual(aBoolean, parameters.populate));
        assertTrue(isEqual(aBoolean, parameters.coalesce));
    }

    @Test
    public void test_ContinuousQueryPublisherCreateWithValueCodec_encodeResponse() {
        int fileClientMessageIndex = 642;
        ClientMessage encoded = ContinuousQueryPublisherCreateWithValueCodec.encodeResponse(aListOfDataToData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ContinuousQueryPublisherCreateCodec_decodeRequest() {
        int fileClientMessageIndex = 643;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ContinuousQueryPublisherCreateCodec.RequestParameters parameters = ContinuousQueryPublisherCreateCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.mapName));
        assertTrue(isEqual(aString, parameters.cacheName));
        assertTrue(isEqual(aData, parameters.predicate));
        assertTrue(isEqual(anInt, parameters.batchSize));
        assertTrue(isEqual(anInt, parameters.bufferSize));
        assertTrue(isEqual(aLong, parameters.delaySeconds));
        assertTrue(isEqual(aBoolean, parameters.populate));
        assertTrue(isEqual(aBoolean, parameters.coalesce));
    }

    @Test
    public void test_ContinuousQueryPublisherCreateCodec_encodeResponse() {
        int fileClientMessageIndex = 644;
        ClientMessage encoded = ContinuousQueryPublisherCreateCodec.encodeResponse(aListOfData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ContinuousQueryMadePublishableCodec_decodeRequest() {
        int fileClientMessageIndex = 645;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ContinuousQueryMadePublishableCodec.RequestParameters parameters = ContinuousQueryMadePublishableCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.mapName));
        assertTrue(isEqual(aString, parameters.cacheName));
    }

    @Test
    public void test_ContinuousQueryMadePublishableCodec_encodeResponse() {
        int fileClientMessageIndex = 646;
        ClientMessage encoded = ContinuousQueryMadePublishableCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ContinuousQueryAddListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 647;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ContinuousQueryAddListenerCodec.RequestParameters parameters = ContinuousQueryAddListenerCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.listenerName));
        assertTrue(isEqual(aBoolean, parameters.localOnly));
    }

    @Test
    public void test_ContinuousQueryAddListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 648;
        ClientMessage encoded = ContinuousQueryAddListenerCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ContinuousQueryAddListenerCodec_encodeQueryCacheSingleEvent() {
        int fileClientMessageIndex = 649;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = ContinuousQueryAddListenerCodec.encodeQueryCacheSingleEvent(aQueryCacheEventData);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ContinuousQueryAddListenerCodec_encodeQueryCacheBatchEvent() {
        int fileClientMessageIndex = 650;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = ContinuousQueryAddListenerCodec.encodeQueryCacheBatchEvent(aListOfQueryCacheEventData, aString, anInt);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ContinuousQuerySetReadCursorCodec_decodeRequest() {
        int fileClientMessageIndex = 651;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ContinuousQuerySetReadCursorCodec.RequestParameters parameters = ContinuousQuerySetReadCursorCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.mapName));
        assertTrue(isEqual(aString, parameters.cacheName));
        assertTrue(isEqual(aLong, parameters.sequence));
    }

    @Test
    public void test_ContinuousQuerySetReadCursorCodec_encodeResponse() {
        int fileClientMessageIndex = 652;
        ClientMessage encoded = ContinuousQuerySetReadCursorCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ContinuousQueryDestroyCacheCodec_decodeRequest() {
        int fileClientMessageIndex = 653;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ContinuousQueryDestroyCacheCodec.RequestParameters parameters = ContinuousQueryDestroyCacheCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.mapName));
        assertTrue(isEqual(aString, parameters.cacheName));
    }

    @Test
    public void test_ContinuousQueryDestroyCacheCodec_encodeResponse() {
        int fileClientMessageIndex = 654;
        ClientMessage encoded = ContinuousQueryDestroyCacheCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_RingbufferSizeCodec_decodeRequest() {
        int fileClientMessageIndex = 655;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, RingbufferSizeCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_RingbufferSizeCodec_encodeResponse() {
        int fileClientMessageIndex = 656;
        ClientMessage encoded = RingbufferSizeCodec.encodeResponse(aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_RingbufferTailSequenceCodec_decodeRequest() {
        int fileClientMessageIndex = 657;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, RingbufferTailSequenceCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_RingbufferTailSequenceCodec_encodeResponse() {
        int fileClientMessageIndex = 658;
        ClientMessage encoded = RingbufferTailSequenceCodec.encodeResponse(aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_RingbufferHeadSequenceCodec_decodeRequest() {
        int fileClientMessageIndex = 659;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, RingbufferHeadSequenceCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_RingbufferHeadSequenceCodec_encodeResponse() {
        int fileClientMessageIndex = 660;
        ClientMessage encoded = RingbufferHeadSequenceCodec.encodeResponse(aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_RingbufferCapacityCodec_decodeRequest() {
        int fileClientMessageIndex = 661;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, RingbufferCapacityCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_RingbufferCapacityCodec_encodeResponse() {
        int fileClientMessageIndex = 662;
        ClientMessage encoded = RingbufferCapacityCodec.encodeResponse(aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_RingbufferRemainingCapacityCodec_decodeRequest() {
        int fileClientMessageIndex = 663;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, RingbufferRemainingCapacityCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_RingbufferRemainingCapacityCodec_encodeResponse() {
        int fileClientMessageIndex = 664;
        ClientMessage encoded = RingbufferRemainingCapacityCodec.encodeResponse(aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_RingbufferAddCodec_decodeRequest() {
        int fileClientMessageIndex = 665;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        RingbufferAddCodec.RequestParameters parameters = RingbufferAddCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(anInt, parameters.overflowPolicy));
        assertTrue(isEqual(aData, parameters.value));
    }

    @Test
    public void test_RingbufferAddCodec_encodeResponse() {
        int fileClientMessageIndex = 666;
        ClientMessage encoded = RingbufferAddCodec.encodeResponse(aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_RingbufferReadOneCodec_decodeRequest() {
        int fileClientMessageIndex = 667;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        RingbufferReadOneCodec.RequestParameters parameters = RingbufferReadOneCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aLong, parameters.sequence));
    }

    @Test
    public void test_RingbufferReadOneCodec_encodeResponse() {
        int fileClientMessageIndex = 668;
        ClientMessage encoded = RingbufferReadOneCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_RingbufferAddAllCodec_decodeRequest() {
        int fileClientMessageIndex = 669;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        RingbufferAddAllCodec.RequestParameters parameters = RingbufferAddAllCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aListOfData, parameters.valueList));
        assertTrue(isEqual(anInt, parameters.overflowPolicy));
    }

    @Test
    public void test_RingbufferAddAllCodec_encodeResponse() {
        int fileClientMessageIndex = 670;
        ClientMessage encoded = RingbufferAddAllCodec.encodeResponse(aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_RingbufferReadManyCodec_decodeRequest() {
        int fileClientMessageIndex = 671;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        RingbufferReadManyCodec.RequestParameters parameters = RingbufferReadManyCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aLong, parameters.startSequence));
        assertTrue(isEqual(anInt, parameters.minCount));
        assertTrue(isEqual(anInt, parameters.maxCount));
        assertTrue(isEqual(null, parameters.filter));
    }

    @Test
    public void test_RingbufferReadManyCodec_encodeResponse() {
        int fileClientMessageIndex = 672;
        ClientMessage encoded = RingbufferReadManyCodec.encodeResponse(anInt, aListOfData, null, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DurableExecutorShutdownCodec_decodeRequest() {
        int fileClientMessageIndex = 673;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, DurableExecutorShutdownCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_DurableExecutorShutdownCodec_encodeResponse() {
        int fileClientMessageIndex = 674;
        ClientMessage encoded = DurableExecutorShutdownCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DurableExecutorIsShutdownCodec_decodeRequest() {
        int fileClientMessageIndex = 675;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, DurableExecutorIsShutdownCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_DurableExecutorIsShutdownCodec_encodeResponse() {
        int fileClientMessageIndex = 676;
        ClientMessage encoded = DurableExecutorIsShutdownCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DurableExecutorSubmitToPartitionCodec_decodeRequest() {
        int fileClientMessageIndex = 677;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        DurableExecutorSubmitToPartitionCodec.RequestParameters parameters = DurableExecutorSubmitToPartitionCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aData, parameters.callable));
    }

    @Test
    public void test_DurableExecutorSubmitToPartitionCodec_encodeResponse() {
        int fileClientMessageIndex = 678;
        ClientMessage encoded = DurableExecutorSubmitToPartitionCodec.encodeResponse(anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DurableExecutorRetrieveResultCodec_decodeRequest() {
        int fileClientMessageIndex = 679;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        DurableExecutorRetrieveResultCodec.RequestParameters parameters = DurableExecutorRetrieveResultCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(anInt, parameters.sequence));
    }

    @Test
    public void test_DurableExecutorRetrieveResultCodec_encodeResponse() {
        int fileClientMessageIndex = 680;
        ClientMessage encoded = DurableExecutorRetrieveResultCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DurableExecutorDisposeResultCodec_decodeRequest() {
        int fileClientMessageIndex = 681;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        DurableExecutorDisposeResultCodec.RequestParameters parameters = DurableExecutorDisposeResultCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(anInt, parameters.sequence));
    }

    @Test
    public void test_DurableExecutorDisposeResultCodec_encodeResponse() {
        int fileClientMessageIndex = 682;
        ClientMessage encoded = DurableExecutorDisposeResultCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DurableExecutorRetrieveAndDisposeResultCodec_decodeRequest() {
        int fileClientMessageIndex = 683;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        DurableExecutorRetrieveAndDisposeResultCodec.RequestParameters parameters = DurableExecutorRetrieveAndDisposeResultCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(anInt, parameters.sequence));
    }

    @Test
    public void test_DurableExecutorRetrieveAndDisposeResultCodec_encodeResponse() {
        int fileClientMessageIndex = 684;
        ClientMessage encoded = DurableExecutorRetrieveAndDisposeResultCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CardinalityEstimatorAddCodec_decodeRequest() {
        int fileClientMessageIndex = 685;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CardinalityEstimatorAddCodec.RequestParameters parameters = CardinalityEstimatorAddCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aLong, parameters.hash));
    }

    @Test
    public void test_CardinalityEstimatorAddCodec_encodeResponse() {
        int fileClientMessageIndex = 686;
        ClientMessage encoded = CardinalityEstimatorAddCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CardinalityEstimatorEstimateCodec_decodeRequest() {
        int fileClientMessageIndex = 687;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, CardinalityEstimatorEstimateCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_CardinalityEstimatorEstimateCodec_encodeResponse() {
        int fileClientMessageIndex = 688;
        ClientMessage encoded = CardinalityEstimatorEstimateCodec.encodeResponse(aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorShutdownCodec_decodeRequest() {
        int fileClientMessageIndex = 689;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ScheduledExecutorShutdownCodec.RequestParameters parameters = ScheduledExecutorShutdownCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.schedulerName));
        assertTrue(isEqual(aUUID, parameters.memberUuid));
    }

    @Test
    public void test_ScheduledExecutorShutdownCodec_encodeResponse() {
        int fileClientMessageIndex = 690;
        ClientMessage encoded = ScheduledExecutorShutdownCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorSubmitToPartitionCodec_decodeRequest() {
        int fileClientMessageIndex = 691;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ScheduledExecutorSubmitToPartitionCodec.RequestParameters parameters = ScheduledExecutorSubmitToPartitionCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.schedulerName));
        assertTrue(isEqual(aByte, parameters.type));
        assertTrue(isEqual(aString, parameters.taskName));
        assertTrue(isEqual(aData, parameters.task));
        assertTrue(isEqual(aLong, parameters.initialDelayInMillis));
        assertTrue(isEqual(aLong, parameters.periodInMillis));
        assertTrue(parameters.isAutoDisposableExists);
        assertTrue(isEqual(aBoolean, parameters.autoDisposable));
    }

    @Test
    public void test_ScheduledExecutorSubmitToPartitionCodec_encodeResponse() {
        int fileClientMessageIndex = 692;
        ClientMessage encoded = ScheduledExecutorSubmitToPartitionCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorSubmitToMemberCodec_decodeRequest() {
        int fileClientMessageIndex = 693;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ScheduledExecutorSubmitToMemberCodec.RequestParameters parameters = ScheduledExecutorSubmitToMemberCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.schedulerName));
        assertTrue(isEqual(aUUID, parameters.memberUuid));
        assertTrue(isEqual(aByte, parameters.type));
        assertTrue(isEqual(aString, parameters.taskName));
        assertTrue(isEqual(aData, parameters.task));
        assertTrue(isEqual(aLong, parameters.initialDelayInMillis));
        assertTrue(isEqual(aLong, parameters.periodInMillis));
        assertTrue(parameters.isAutoDisposableExists);
        assertTrue(isEqual(aBoolean, parameters.autoDisposable));
    }

    @Test
    public void test_ScheduledExecutorSubmitToMemberCodec_encodeResponse() {
        int fileClientMessageIndex = 694;
        ClientMessage encoded = ScheduledExecutorSubmitToMemberCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorGetAllScheduledFuturesCodec_decodeRequest() {
        int fileClientMessageIndex = 695;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, ScheduledExecutorGetAllScheduledFuturesCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_ScheduledExecutorGetAllScheduledFuturesCodec_encodeResponse() {
        int fileClientMessageIndex = 696;
        ClientMessage encoded = ScheduledExecutorGetAllScheduledFuturesCodec.encodeResponse(aListOfScheduledTaskHandler);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorGetStatsFromPartitionCodec_decodeRequest() {
        int fileClientMessageIndex = 697;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ScheduledExecutorGetStatsFromPartitionCodec.RequestParameters parameters = ScheduledExecutorGetStatsFromPartitionCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.schedulerName));
        assertTrue(isEqual(aString, parameters.taskName));
    }

    @Test
    public void test_ScheduledExecutorGetStatsFromPartitionCodec_encodeResponse() {
        int fileClientMessageIndex = 698;
        ClientMessage encoded = ScheduledExecutorGetStatsFromPartitionCodec.encodeResponse(aLong, aLong, aLong, aLong, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorGetStatsFromMemberCodec_decodeRequest() {
        int fileClientMessageIndex = 699;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ScheduledExecutorGetStatsFromMemberCodec.RequestParameters parameters = ScheduledExecutorGetStatsFromMemberCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.schedulerName));
        assertTrue(isEqual(aString, parameters.taskName));
        assertTrue(isEqual(aUUID, parameters.memberUuid));
    }

    @Test
    public void test_ScheduledExecutorGetStatsFromMemberCodec_encodeResponse() {
        int fileClientMessageIndex = 700;
        ClientMessage encoded = ScheduledExecutorGetStatsFromMemberCodec.encodeResponse(aLong, aLong, aLong, aLong, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorGetDelayFromPartitionCodec_decodeRequest() {
        int fileClientMessageIndex = 701;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ScheduledExecutorGetDelayFromPartitionCodec.RequestParameters parameters = ScheduledExecutorGetDelayFromPartitionCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.schedulerName));
        assertTrue(isEqual(aString, parameters.taskName));
    }

    @Test
    public void test_ScheduledExecutorGetDelayFromPartitionCodec_encodeResponse() {
        int fileClientMessageIndex = 702;
        ClientMessage encoded = ScheduledExecutorGetDelayFromPartitionCodec.encodeResponse(aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorGetDelayFromMemberCodec_decodeRequest() {
        int fileClientMessageIndex = 703;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ScheduledExecutorGetDelayFromMemberCodec.RequestParameters parameters = ScheduledExecutorGetDelayFromMemberCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.schedulerName));
        assertTrue(isEqual(aString, parameters.taskName));
        assertTrue(isEqual(aUUID, parameters.memberUuid));
    }

    @Test
    public void test_ScheduledExecutorGetDelayFromMemberCodec_encodeResponse() {
        int fileClientMessageIndex = 704;
        ClientMessage encoded = ScheduledExecutorGetDelayFromMemberCodec.encodeResponse(aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorCancelFromPartitionCodec_decodeRequest() {
        int fileClientMessageIndex = 705;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ScheduledExecutorCancelFromPartitionCodec.RequestParameters parameters = ScheduledExecutorCancelFromPartitionCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.schedulerName));
        assertTrue(isEqual(aString, parameters.taskName));
        assertTrue(isEqual(aBoolean, parameters.mayInterruptIfRunning));
    }

    @Test
    public void test_ScheduledExecutorCancelFromPartitionCodec_encodeResponse() {
        int fileClientMessageIndex = 706;
        ClientMessage encoded = ScheduledExecutorCancelFromPartitionCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorCancelFromMemberCodec_decodeRequest() {
        int fileClientMessageIndex = 707;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ScheduledExecutorCancelFromMemberCodec.RequestParameters parameters = ScheduledExecutorCancelFromMemberCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.schedulerName));
        assertTrue(isEqual(aString, parameters.taskName));
        assertTrue(isEqual(aUUID, parameters.memberUuid));
        assertTrue(isEqual(aBoolean, parameters.mayInterruptIfRunning));
    }

    @Test
    public void test_ScheduledExecutorCancelFromMemberCodec_encodeResponse() {
        int fileClientMessageIndex = 708;
        ClientMessage encoded = ScheduledExecutorCancelFromMemberCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorIsCancelledFromPartitionCodec_decodeRequest() {
        int fileClientMessageIndex = 709;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ScheduledExecutorIsCancelledFromPartitionCodec.RequestParameters parameters = ScheduledExecutorIsCancelledFromPartitionCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.schedulerName));
        assertTrue(isEqual(aString, parameters.taskName));
    }

    @Test
    public void test_ScheduledExecutorIsCancelledFromPartitionCodec_encodeResponse() {
        int fileClientMessageIndex = 710;
        ClientMessage encoded = ScheduledExecutorIsCancelledFromPartitionCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorIsCancelledFromMemberCodec_decodeRequest() {
        int fileClientMessageIndex = 711;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ScheduledExecutorIsCancelledFromMemberCodec.RequestParameters parameters = ScheduledExecutorIsCancelledFromMemberCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.schedulerName));
        assertTrue(isEqual(aString, parameters.taskName));
        assertTrue(isEqual(aUUID, parameters.memberUuid));
    }

    @Test
    public void test_ScheduledExecutorIsCancelledFromMemberCodec_encodeResponse() {
        int fileClientMessageIndex = 712;
        ClientMessage encoded = ScheduledExecutorIsCancelledFromMemberCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorIsDoneFromPartitionCodec_decodeRequest() {
        int fileClientMessageIndex = 713;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ScheduledExecutorIsDoneFromPartitionCodec.RequestParameters parameters = ScheduledExecutorIsDoneFromPartitionCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.schedulerName));
        assertTrue(isEqual(aString, parameters.taskName));
    }

    @Test
    public void test_ScheduledExecutorIsDoneFromPartitionCodec_encodeResponse() {
        int fileClientMessageIndex = 714;
        ClientMessage encoded = ScheduledExecutorIsDoneFromPartitionCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorIsDoneFromMemberCodec_decodeRequest() {
        int fileClientMessageIndex = 715;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ScheduledExecutorIsDoneFromMemberCodec.RequestParameters parameters = ScheduledExecutorIsDoneFromMemberCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.schedulerName));
        assertTrue(isEqual(aString, parameters.taskName));
        assertTrue(isEqual(aUUID, parameters.memberUuid));
    }

    @Test
    public void test_ScheduledExecutorIsDoneFromMemberCodec_encodeResponse() {
        int fileClientMessageIndex = 716;
        ClientMessage encoded = ScheduledExecutorIsDoneFromMemberCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorGetResultFromPartitionCodec_decodeRequest() {
        int fileClientMessageIndex = 717;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ScheduledExecutorGetResultFromPartitionCodec.RequestParameters parameters = ScheduledExecutorGetResultFromPartitionCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.schedulerName));
        assertTrue(isEqual(aString, parameters.taskName));
    }

    @Test
    public void test_ScheduledExecutorGetResultFromPartitionCodec_encodeResponse() {
        int fileClientMessageIndex = 718;
        ClientMessage encoded = ScheduledExecutorGetResultFromPartitionCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorGetResultFromMemberCodec_decodeRequest() {
        int fileClientMessageIndex = 719;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ScheduledExecutorGetResultFromMemberCodec.RequestParameters parameters = ScheduledExecutorGetResultFromMemberCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.schedulerName));
        assertTrue(isEqual(aString, parameters.taskName));
        assertTrue(isEqual(aUUID, parameters.memberUuid));
    }

    @Test
    public void test_ScheduledExecutorGetResultFromMemberCodec_encodeResponse() {
        int fileClientMessageIndex = 720;
        ClientMessage encoded = ScheduledExecutorGetResultFromMemberCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorDisposeFromPartitionCodec_decodeRequest() {
        int fileClientMessageIndex = 721;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ScheduledExecutorDisposeFromPartitionCodec.RequestParameters parameters = ScheduledExecutorDisposeFromPartitionCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.schedulerName));
        assertTrue(isEqual(aString, parameters.taskName));
    }

    @Test
    public void test_ScheduledExecutorDisposeFromPartitionCodec_encodeResponse() {
        int fileClientMessageIndex = 722;
        ClientMessage encoded = ScheduledExecutorDisposeFromPartitionCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_ScheduledExecutorDisposeFromMemberCodec_decodeRequest() {
        int fileClientMessageIndex = 723;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ScheduledExecutorDisposeFromMemberCodec.RequestParameters parameters = ScheduledExecutorDisposeFromMemberCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.schedulerName));
        assertTrue(isEqual(aString, parameters.taskName));
        assertTrue(isEqual(aUUID, parameters.memberUuid));
    }

    @Test
    public void test_ScheduledExecutorDisposeFromMemberCodec_encodeResponse() {
        int fileClientMessageIndex = 724;
        ClientMessage encoded = ScheduledExecutorDisposeFromMemberCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddMultiMapConfigCodec_decodeRequest() {
        int fileClientMessageIndex = 725;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        DynamicConfigAddMultiMapConfigCodec.RequestParameters parameters = DynamicConfigAddMultiMapConfigCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aString, parameters.collectionType));
        assertTrue(isEqual(null, parameters.listenerConfigs));
        assertTrue(isEqual(aBoolean, parameters.binary));
        assertTrue(isEqual(anInt, parameters.backupCount));
        assertTrue(isEqual(anInt, parameters.asyncBackupCount));
        assertTrue(isEqual(aBoolean, parameters.statisticsEnabled));
        assertTrue(isEqual(null, parameters.splitBrainProtectionName));
        assertTrue(isEqual(aString, parameters.mergePolicy));
        assertTrue(isEqual(anInt, parameters.mergeBatchSize));
    }

    @Test
    public void test_DynamicConfigAddMultiMapConfigCodec_encodeResponse() {
        int fileClientMessageIndex = 726;
        ClientMessage encoded = DynamicConfigAddMultiMapConfigCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddRingbufferConfigCodec_decodeRequest() {
        int fileClientMessageIndex = 727;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        DynamicConfigAddRingbufferConfigCodec.RequestParameters parameters = DynamicConfigAddRingbufferConfigCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(anInt, parameters.capacity));
        assertTrue(isEqual(anInt, parameters.backupCount));
        assertTrue(isEqual(anInt, parameters.asyncBackupCount));
        assertTrue(isEqual(anInt, parameters.timeToLiveSeconds));
        assertTrue(isEqual(aString, parameters.inMemoryFormat));
        assertTrue(isEqual(null, parameters.ringbufferStoreConfig));
        assertTrue(isEqual(null, parameters.splitBrainProtectionName));
        assertTrue(isEqual(aString, parameters.mergePolicy));
        assertTrue(isEqual(anInt, parameters.mergeBatchSize));
    }

    @Test
    public void test_DynamicConfigAddRingbufferConfigCodec_encodeResponse() {
        int fileClientMessageIndex = 728;
        ClientMessage encoded = DynamicConfigAddRingbufferConfigCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddCardinalityEstimatorConfigCodec_decodeRequest() {
        int fileClientMessageIndex = 729;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        DynamicConfigAddCardinalityEstimatorConfigCodec.RequestParameters parameters = DynamicConfigAddCardinalityEstimatorConfigCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(anInt, parameters.backupCount));
        assertTrue(isEqual(anInt, parameters.asyncBackupCount));
        assertTrue(isEqual(null, parameters.splitBrainProtectionName));
        assertTrue(isEqual(aString, parameters.mergePolicy));
        assertTrue(isEqual(anInt, parameters.mergeBatchSize));
    }

    @Test
    public void test_DynamicConfigAddCardinalityEstimatorConfigCodec_encodeResponse() {
        int fileClientMessageIndex = 730;
        ClientMessage encoded = DynamicConfigAddCardinalityEstimatorConfigCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddListConfigCodec_decodeRequest() {
        int fileClientMessageIndex = 731;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        DynamicConfigAddListConfigCodec.RequestParameters parameters = DynamicConfigAddListConfigCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(null, parameters.listenerConfigs));
        assertTrue(isEqual(anInt, parameters.backupCount));
        assertTrue(isEqual(anInt, parameters.asyncBackupCount));
        assertTrue(isEqual(anInt, parameters.maxSize));
        assertTrue(isEqual(aBoolean, parameters.statisticsEnabled));
        assertTrue(isEqual(null, parameters.splitBrainProtectionName));
        assertTrue(isEqual(aString, parameters.mergePolicy));
        assertTrue(isEqual(anInt, parameters.mergeBatchSize));
    }

    @Test
    public void test_DynamicConfigAddListConfigCodec_encodeResponse() {
        int fileClientMessageIndex = 732;
        ClientMessage encoded = DynamicConfigAddListConfigCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddSetConfigCodec_decodeRequest() {
        int fileClientMessageIndex = 733;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        DynamicConfigAddSetConfigCodec.RequestParameters parameters = DynamicConfigAddSetConfigCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(null, parameters.listenerConfigs));
        assertTrue(isEqual(anInt, parameters.backupCount));
        assertTrue(isEqual(anInt, parameters.asyncBackupCount));
        assertTrue(isEqual(anInt, parameters.maxSize));
        assertTrue(isEqual(aBoolean, parameters.statisticsEnabled));
        assertTrue(isEqual(null, parameters.splitBrainProtectionName));
        assertTrue(isEqual(aString, parameters.mergePolicy));
        assertTrue(isEqual(anInt, parameters.mergeBatchSize));
    }

    @Test
    public void test_DynamicConfigAddSetConfigCodec_encodeResponse() {
        int fileClientMessageIndex = 734;
        ClientMessage encoded = DynamicConfigAddSetConfigCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddReplicatedMapConfigCodec_decodeRequest() {
        int fileClientMessageIndex = 735;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        DynamicConfigAddReplicatedMapConfigCodec.RequestParameters parameters = DynamicConfigAddReplicatedMapConfigCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aString, parameters.inMemoryFormat));
        assertTrue(isEqual(aBoolean, parameters.asyncFillup));
        assertTrue(isEqual(aBoolean, parameters.statisticsEnabled));
        assertTrue(isEqual(aString, parameters.mergePolicy));
        assertTrue(isEqual(null, parameters.listenerConfigs));
        assertTrue(isEqual(null, parameters.splitBrainProtectionName));
        assertTrue(isEqual(anInt, parameters.mergeBatchSize));
    }

    @Test
    public void test_DynamicConfigAddReplicatedMapConfigCodec_encodeResponse() {
        int fileClientMessageIndex = 736;
        ClientMessage encoded = DynamicConfigAddReplicatedMapConfigCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddTopicConfigCodec_decodeRequest() {
        int fileClientMessageIndex = 737;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        DynamicConfigAddTopicConfigCodec.RequestParameters parameters = DynamicConfigAddTopicConfigCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aBoolean, parameters.globalOrderingEnabled));
        assertTrue(isEqual(aBoolean, parameters.statisticsEnabled));
        assertTrue(isEqual(aBoolean, parameters.multiThreadingEnabled));
        assertTrue(isEqual(null, parameters.listenerConfigs));
    }

    @Test
    public void test_DynamicConfigAddTopicConfigCodec_encodeResponse() {
        int fileClientMessageIndex = 738;
        ClientMessage encoded = DynamicConfigAddTopicConfigCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddExecutorConfigCodec_decodeRequest() {
        int fileClientMessageIndex = 739;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        DynamicConfigAddExecutorConfigCodec.RequestParameters parameters = DynamicConfigAddExecutorConfigCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(anInt, parameters.poolSize));
        assertTrue(isEqual(anInt, parameters.queueCapacity));
        assertTrue(isEqual(aBoolean, parameters.statisticsEnabled));
        assertTrue(isEqual(null, parameters.splitBrainProtectionName));
    }

    @Test
    public void test_DynamicConfigAddExecutorConfigCodec_encodeResponse() {
        int fileClientMessageIndex = 740;
        ClientMessage encoded = DynamicConfigAddExecutorConfigCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddDurableExecutorConfigCodec_decodeRequest() {
        int fileClientMessageIndex = 741;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        DynamicConfigAddDurableExecutorConfigCodec.RequestParameters parameters = DynamicConfigAddDurableExecutorConfigCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(anInt, parameters.poolSize));
        assertTrue(isEqual(anInt, parameters.durability));
        assertTrue(isEqual(anInt, parameters.capacity));
        assertTrue(isEqual(null, parameters.splitBrainProtectionName));
        assertTrue(parameters.isStatisticsEnabledExists);
        assertTrue(isEqual(aBoolean, parameters.statisticsEnabled));
    }

    @Test
    public void test_DynamicConfigAddDurableExecutorConfigCodec_encodeResponse() {
        int fileClientMessageIndex = 742;
        ClientMessage encoded = DynamicConfigAddDurableExecutorConfigCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddScheduledExecutorConfigCodec_decodeRequest() {
        int fileClientMessageIndex = 743;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        DynamicConfigAddScheduledExecutorConfigCodec.RequestParameters parameters = DynamicConfigAddScheduledExecutorConfigCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(anInt, parameters.poolSize));
        assertTrue(isEqual(anInt, parameters.durability));
        assertTrue(isEqual(anInt, parameters.capacity));
        assertTrue(isEqual(null, parameters.splitBrainProtectionName));
        assertTrue(isEqual(aString, parameters.mergePolicy));
        assertTrue(isEqual(anInt, parameters.mergeBatchSize));
        assertTrue(parameters.isStatisticsEnabledExists);
        assertTrue(isEqual(aBoolean, parameters.statisticsEnabled));
    }

    @Test
    public void test_DynamicConfigAddScheduledExecutorConfigCodec_encodeResponse() {
        int fileClientMessageIndex = 744;
        ClientMessage encoded = DynamicConfigAddScheduledExecutorConfigCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddQueueConfigCodec_decodeRequest() {
        int fileClientMessageIndex = 745;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        DynamicConfigAddQueueConfigCodec.RequestParameters parameters = DynamicConfigAddQueueConfigCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(null, parameters.listenerConfigs));
        assertTrue(isEqual(anInt, parameters.backupCount));
        assertTrue(isEqual(anInt, parameters.asyncBackupCount));
        assertTrue(isEqual(anInt, parameters.maxSize));
        assertTrue(isEqual(anInt, parameters.emptyQueueTtl));
        assertTrue(isEqual(aBoolean, parameters.statisticsEnabled));
        assertTrue(isEqual(null, parameters.splitBrainProtectionName));
        assertTrue(isEqual(null, parameters.queueStoreConfig));
        assertTrue(isEqual(aString, parameters.mergePolicy));
        assertTrue(isEqual(anInt, parameters.mergeBatchSize));
        assertTrue(parameters.isPriorityComparatorClassNameExists);
        assertTrue(isEqual(null, parameters.priorityComparatorClassName));
    }

    @Test
    public void test_DynamicConfigAddQueueConfigCodec_encodeResponse() {
        int fileClientMessageIndex = 746;
        ClientMessage encoded = DynamicConfigAddQueueConfigCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddMapConfigCodec_decodeRequest() {
        int fileClientMessageIndex = 747;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        DynamicConfigAddMapConfigCodec.RequestParameters parameters = DynamicConfigAddMapConfigCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(anInt, parameters.backupCount));
        assertTrue(isEqual(anInt, parameters.asyncBackupCount));
        assertTrue(isEqual(anInt, parameters.timeToLiveSeconds));
        assertTrue(isEqual(anInt, parameters.maxIdleSeconds));
        assertTrue(isEqual(null, parameters.evictionConfig));
        assertTrue(isEqual(aBoolean, parameters.readBackupData));
        assertTrue(isEqual(aString, parameters.cacheDeserializedValues));
        assertTrue(isEqual(aString, parameters.mergePolicy));
        assertTrue(isEqual(anInt, parameters.mergeBatchSize));
        assertTrue(isEqual(aString, parameters.inMemoryFormat));
        assertTrue(isEqual(null, parameters.listenerConfigs));
        assertTrue(isEqual(null, parameters.partitionLostListenerConfigs));
        assertTrue(isEqual(aBoolean, parameters.statisticsEnabled));
        assertTrue(isEqual(null, parameters.splitBrainProtectionName));
        assertTrue(isEqual(null, parameters.mapStoreConfig));
        assertTrue(isEqual(null, parameters.nearCacheConfig));
        assertTrue(isEqual(null, parameters.wanReplicationRef));
        assertTrue(isEqual(null, parameters.indexConfigs));
        assertTrue(isEqual(null, parameters.attributeConfigs));
        assertTrue(isEqual(null, parameters.queryCacheConfigs));
        assertTrue(isEqual(null, parameters.partitioningStrategyClassName));
        assertTrue(isEqual(null, parameters.partitioningStrategyImplementation));
        assertTrue(isEqual(null, parameters.hotRestartConfig));
        assertTrue(isEqual(null, parameters.eventJournalConfig));
        assertTrue(isEqual(null, parameters.merkleTreeConfig));
        assertTrue(isEqual(anInt, parameters.metadataPolicy));
        assertTrue(parameters.isPerEntryStatsEnabledExists);
        assertTrue(isEqual(aBoolean, parameters.perEntryStatsEnabled));
    }

    @Test
    public void test_DynamicConfigAddMapConfigCodec_encodeResponse() {
        int fileClientMessageIndex = 748;
        ClientMessage encoded = DynamicConfigAddMapConfigCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddReliableTopicConfigCodec_decodeRequest() {
        int fileClientMessageIndex = 749;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        DynamicConfigAddReliableTopicConfigCodec.RequestParameters parameters = DynamicConfigAddReliableTopicConfigCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(null, parameters.listenerConfigs));
        assertTrue(isEqual(anInt, parameters.readBatchSize));
        assertTrue(isEqual(aBoolean, parameters.statisticsEnabled));
        assertTrue(isEqual(aString, parameters.topicOverloadPolicy));
        assertTrue(isEqual(null, parameters.executor));
    }

    @Test
    public void test_DynamicConfigAddReliableTopicConfigCodec_encodeResponse() {
        int fileClientMessageIndex = 750;
        ClientMessage encoded = DynamicConfigAddReliableTopicConfigCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddCacheConfigCodec_decodeRequest() {
        int fileClientMessageIndex = 751;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        DynamicConfigAddCacheConfigCodec.RequestParameters parameters = DynamicConfigAddCacheConfigCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(null, parameters.keyType));
        assertTrue(isEqual(null, parameters.valueType));
        assertTrue(isEqual(aBoolean, parameters.statisticsEnabled));
        assertTrue(isEqual(aBoolean, parameters.managementEnabled));
        assertTrue(isEqual(aBoolean, parameters.readThrough));
        assertTrue(isEqual(aBoolean, parameters.writeThrough));
        assertTrue(isEqual(null, parameters.cacheLoaderFactory));
        assertTrue(isEqual(null, parameters.cacheWriterFactory));
        assertTrue(isEqual(null, parameters.cacheLoader));
        assertTrue(isEqual(null, parameters.cacheWriter));
        assertTrue(isEqual(anInt, parameters.backupCount));
        assertTrue(isEqual(anInt, parameters.asyncBackupCount));
        assertTrue(isEqual(aString, parameters.inMemoryFormat));
        assertTrue(isEqual(null, parameters.splitBrainProtectionName));
        assertTrue(isEqual(null, parameters.mergePolicy));
        assertTrue(isEqual(anInt, parameters.mergeBatchSize));
        assertTrue(isEqual(aBoolean, parameters.disablePerEntryInvalidationEvents));
        assertTrue(isEqual(null, parameters.partitionLostListenerConfigs));
        assertTrue(isEqual(null, parameters.expiryPolicyFactoryClassName));
        assertTrue(isEqual(null, parameters.timedExpiryPolicyFactoryConfig));
        assertTrue(isEqual(null, parameters.cacheEntryListeners));
        assertTrue(isEqual(null, parameters.evictionConfig));
        assertTrue(isEqual(null, parameters.wanReplicationRef));
        assertTrue(isEqual(null, parameters.eventJournalConfig));
        assertTrue(isEqual(null, parameters.hotRestartConfig));
        assertTrue(parameters.isMerkleTreeConfigExists);
        assertTrue(isEqual(null, parameters.merkleTreeConfig));
    }

    @Test
    public void test_DynamicConfigAddCacheConfigCodec_encodeResponse() {
        int fileClientMessageIndex = 752;
        ClientMessage encoded = DynamicConfigAddCacheConfigCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddFlakeIdGeneratorConfigCodec_decodeRequest() {
        int fileClientMessageIndex = 753;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        DynamicConfigAddFlakeIdGeneratorConfigCodec.RequestParameters parameters = DynamicConfigAddFlakeIdGeneratorConfigCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(anInt, parameters.prefetchCount));
        assertTrue(isEqual(aLong, parameters.prefetchValidity));
        assertTrue(isEqual(aBoolean, parameters.statisticsEnabled));
        assertTrue(isEqual(aLong, parameters.nodeIdOffset));
        assertTrue(isEqual(aLong, parameters.epochStart));
        assertTrue(isEqual(anInt, parameters.bitsSequence));
        assertTrue(isEqual(anInt, parameters.bitsNodeId));
        assertTrue(isEqual(aLong, parameters.allowedFutureMillis));
    }

    @Test
    public void test_DynamicConfigAddFlakeIdGeneratorConfigCodec_encodeResponse() {
        int fileClientMessageIndex = 754;
        ClientMessage encoded = DynamicConfigAddFlakeIdGeneratorConfigCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_DynamicConfigAddPNCounterConfigCodec_decodeRequest() {
        int fileClientMessageIndex = 755;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        DynamicConfigAddPNCounterConfigCodec.RequestParameters parameters = DynamicConfigAddPNCounterConfigCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(anInt, parameters.replicaCount));
        assertTrue(isEqual(aBoolean, parameters.statisticsEnabled));
        assertTrue(isEqual(null, parameters.splitBrainProtectionName));
    }

    @Test
    public void test_DynamicConfigAddPNCounterConfigCodec_encodeResponse() {
        int fileClientMessageIndex = 756;
        ClientMessage encoded = DynamicConfigAddPNCounterConfigCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_FlakeIdGeneratorNewIdBatchCodec_decodeRequest() {
        int fileClientMessageIndex = 757;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        FlakeIdGeneratorNewIdBatchCodec.RequestParameters parameters = FlakeIdGeneratorNewIdBatchCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(anInt, parameters.batchSize));
    }

    @Test
    public void test_FlakeIdGeneratorNewIdBatchCodec_encodeResponse() {
        int fileClientMessageIndex = 758;
        ClientMessage encoded = FlakeIdGeneratorNewIdBatchCodec.encodeResponse(aLong, aLong, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_PNCounterGetCodec_decodeRequest() {
        int fileClientMessageIndex = 759;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        PNCounterGetCodec.RequestParameters parameters = PNCounterGetCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aListOfUuidToLong, parameters.replicaTimestamps));
        assertTrue(isEqual(aUUID, parameters.targetReplicaUUID));
    }

    @Test
    public void test_PNCounterGetCodec_encodeResponse() {
        int fileClientMessageIndex = 760;
        ClientMessage encoded = PNCounterGetCodec.encodeResponse(aLong, aListOfUuidToLong, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_PNCounterAddCodec_decodeRequest() {
        int fileClientMessageIndex = 761;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        PNCounterAddCodec.RequestParameters parameters = PNCounterAddCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aLong, parameters.delta));
        assertTrue(isEqual(aBoolean, parameters.getBeforeUpdate));
        assertTrue(isEqual(aListOfUuidToLong, parameters.replicaTimestamps));
        assertTrue(isEqual(aUUID, parameters.targetReplicaUUID));
    }

    @Test
    public void test_PNCounterAddCodec_encodeResponse() {
        int fileClientMessageIndex = 762;
        ClientMessage encoded = PNCounterAddCodec.encodeResponse(aLong, aListOfUuidToLong, anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_PNCounterGetConfiguredReplicaCountCodec_decodeRequest() {
        int fileClientMessageIndex = 763;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, PNCounterGetConfiguredReplicaCountCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_PNCounterGetConfiguredReplicaCountCodec_encodeResponse() {
        int fileClientMessageIndex = 764;
        ClientMessage encoded = PNCounterGetConfiguredReplicaCountCodec.encodeResponse(anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CPGroupCreateCPGroupCodec_decodeRequest() {
        int fileClientMessageIndex = 765;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, CPGroupCreateCPGroupCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_CPGroupCreateCPGroupCodec_encodeResponse() {
        int fileClientMessageIndex = 766;
        ClientMessage encoded = CPGroupCreateCPGroupCodec.encodeResponse(aRaftGroupId);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CPGroupDestroyCPObjectCodec_decodeRequest() {
        int fileClientMessageIndex = 767;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CPGroupDestroyCPObjectCodec.RequestParameters parameters = CPGroupDestroyCPObjectCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aRaftGroupId, parameters.groupId));
        assertTrue(isEqual(aString, parameters.serviceName));
        assertTrue(isEqual(aString, parameters.objectName));
    }

    @Test
    public void test_CPGroupDestroyCPObjectCodec_encodeResponse() {
        int fileClientMessageIndex = 768;
        ClientMessage encoded = CPGroupDestroyCPObjectCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CPSessionCreateSessionCodec_decodeRequest() {
        int fileClientMessageIndex = 769;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CPSessionCreateSessionCodec.RequestParameters parameters = CPSessionCreateSessionCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aRaftGroupId, parameters.groupId));
        assertTrue(isEqual(aString, parameters.endpointName));
    }

    @Test
    public void test_CPSessionCreateSessionCodec_encodeResponse() {
        int fileClientMessageIndex = 770;
        ClientMessage encoded = CPSessionCreateSessionCodec.encodeResponse(aLong, aLong, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CPSessionCloseSessionCodec_decodeRequest() {
        int fileClientMessageIndex = 771;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CPSessionCloseSessionCodec.RequestParameters parameters = CPSessionCloseSessionCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aRaftGroupId, parameters.groupId));
        assertTrue(isEqual(aLong, parameters.sessionId));
    }

    @Test
    public void test_CPSessionCloseSessionCodec_encodeResponse() {
        int fileClientMessageIndex = 772;
        ClientMessage encoded = CPSessionCloseSessionCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CPSessionHeartbeatSessionCodec_decodeRequest() {
        int fileClientMessageIndex = 773;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        CPSessionHeartbeatSessionCodec.RequestParameters parameters = CPSessionHeartbeatSessionCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aRaftGroupId, parameters.groupId));
        assertTrue(isEqual(aLong, parameters.sessionId));
    }

    @Test
    public void test_CPSessionHeartbeatSessionCodec_encodeResponse() {
        int fileClientMessageIndex = 774;
        ClientMessage encoded = CPSessionHeartbeatSessionCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CPSessionGenerateThreadIdCodec_decodeRequest() {
        int fileClientMessageIndex = 775;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aRaftGroupId, CPSessionGenerateThreadIdCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_CPSessionGenerateThreadIdCodec_encodeResponse() {
        int fileClientMessageIndex = 776;
        ClientMessage encoded = CPSessionGenerateThreadIdCodec.encodeResponse(aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCReadMetricsCodec_decodeRequest() {
        int fileClientMessageIndex = 777;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MCReadMetricsCodec.RequestParameters parameters = MCReadMetricsCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aUUID, parameters.uuid));
        assertTrue(isEqual(aLong, parameters.fromSequence));
    }

    @Test
    public void test_MCReadMetricsCodec_encodeResponse() {
        int fileClientMessageIndex = 778;
        ClientMessage encoded = MCReadMetricsCodec.encodeResponse(aListOfLongToByteArray, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCChangeClusterStateCodec_decodeRequest() {
        int fileClientMessageIndex = 779;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(anInt, MCChangeClusterStateCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_MCChangeClusterStateCodec_encodeResponse() {
        int fileClientMessageIndex = 780;
        ClientMessage encoded = MCChangeClusterStateCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCGetMapConfigCodec_decodeRequest() {
        int fileClientMessageIndex = 781;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, MCGetMapConfigCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_MCGetMapConfigCodec_encodeResponse() {
        int fileClientMessageIndex = 782;
        ClientMessage encoded = MCGetMapConfigCodec.encodeResponse(anInt, anInt, anInt, anInt, anInt, anInt, anInt, aBoolean, anInt, aString, aListOfIndexConfigs);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCUpdateMapConfigCodec_decodeRequest() {
        int fileClientMessageIndex = 783;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MCUpdateMapConfigCodec.RequestParameters parameters = MCUpdateMapConfigCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.mapName));
        assertTrue(isEqual(anInt, parameters.timeToLiveSeconds));
        assertTrue(isEqual(anInt, parameters.maxIdleSeconds));
        assertTrue(isEqual(anInt, parameters.evictionPolicy));
        assertTrue(isEqual(aBoolean, parameters.readBackupData));
        assertTrue(isEqual(anInt, parameters.maxSize));
        assertTrue(isEqual(anInt, parameters.maxSizePolicy));
    }

    @Test
    public void test_MCUpdateMapConfigCodec_encodeResponse() {
        int fileClientMessageIndex = 784;
        ClientMessage encoded = MCUpdateMapConfigCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCGetMemberConfigCodec_decodeRequest() {
        int fileClientMessageIndex = 785;
    }

    @Test
    public void test_MCGetMemberConfigCodec_encodeResponse() {
        int fileClientMessageIndex = 786;
        ClientMessage encoded = MCGetMemberConfigCodec.encodeResponse(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCRunGcCodec_decodeRequest() {
        int fileClientMessageIndex = 787;
    }

    @Test
    public void test_MCRunGcCodec_encodeResponse() {
        int fileClientMessageIndex = 788;
        ClientMessage encoded = MCRunGcCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCGetThreadDumpCodec_decodeRequest() {
        int fileClientMessageIndex = 789;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, MCGetThreadDumpCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_MCGetThreadDumpCodec_encodeResponse() {
        int fileClientMessageIndex = 790;
        ClientMessage encoded = MCGetThreadDumpCodec.encodeResponse(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCShutdownMemberCodec_decodeRequest() {
        int fileClientMessageIndex = 791;
    }

    @Test
    public void test_MCShutdownMemberCodec_encodeResponse() {
        int fileClientMessageIndex = 792;
        ClientMessage encoded = MCShutdownMemberCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCPromoteLiteMemberCodec_decodeRequest() {
        int fileClientMessageIndex = 793;
    }

    @Test
    public void test_MCPromoteLiteMemberCodec_encodeResponse() {
        int fileClientMessageIndex = 794;
        ClientMessage encoded = MCPromoteLiteMemberCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCGetSystemPropertiesCodec_decodeRequest() {
        int fileClientMessageIndex = 795;
    }

    @Test
    public void test_MCGetSystemPropertiesCodec_encodeResponse() {
        int fileClientMessageIndex = 796;
        ClientMessage encoded = MCGetSystemPropertiesCodec.encodeResponse(aListOfStringToString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCGetTimedMemberStateCodec_decodeRequest() {
        int fileClientMessageIndex = 797;
    }

    @Test
    public void test_MCGetTimedMemberStateCodec_encodeResponse() {
        int fileClientMessageIndex = 798;
        ClientMessage encoded = MCGetTimedMemberStateCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCMatchMCConfigCodec_decodeRequest() {
        int fileClientMessageIndex = 799;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, MCMatchMCConfigCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_MCMatchMCConfigCodec_encodeResponse() {
        int fileClientMessageIndex = 800;
        ClientMessage encoded = MCMatchMCConfigCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCApplyMCConfigCodec_decodeRequest() {
        int fileClientMessageIndex = 801;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MCApplyMCConfigCodec.RequestParameters parameters = MCApplyMCConfigCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.eTag));
        assertTrue(isEqual(anInt, parameters.clientBwListMode));
        assertTrue(isEqual(aListOfClientBwListEntries, parameters.clientBwListEntries));
    }

    @Test
    public void test_MCApplyMCConfigCodec_encodeResponse() {
        int fileClientMessageIndex = 802;
        ClientMessage encoded = MCApplyMCConfigCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCGetClusterMetadataCodec_decodeRequest() {
        int fileClientMessageIndex = 803;
    }

    @Test
    public void test_MCGetClusterMetadataCodec_encodeResponse() {
        int fileClientMessageIndex = 804;
        ClientMessage encoded = MCGetClusterMetadataCodec.encodeResponse(aByte, aString, null, aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCShutdownClusterCodec_decodeRequest() {
        int fileClientMessageIndex = 805;
    }

    @Test
    public void test_MCShutdownClusterCodec_encodeResponse() {
        int fileClientMessageIndex = 806;
        ClientMessage encoded = MCShutdownClusterCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCChangeClusterVersionCodec_decodeRequest() {
        int fileClientMessageIndex = 807;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MCChangeClusterVersionCodec.RequestParameters parameters = MCChangeClusterVersionCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aByte, parameters.majorVersion));
        assertTrue(isEqual(aByte, parameters.minorVersion));
    }

    @Test
    public void test_MCChangeClusterVersionCodec_encodeResponse() {
        int fileClientMessageIndex = 808;
        ClientMessage encoded = MCChangeClusterVersionCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCRunScriptCodec_decodeRequest() {
        int fileClientMessageIndex = 809;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MCRunScriptCodec.RequestParameters parameters = MCRunScriptCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.engine));
        assertTrue(isEqual(aString, parameters.script));
    }

    @Test
    public void test_MCRunScriptCodec_encodeResponse() {
        int fileClientMessageIndex = 810;
        ClientMessage encoded = MCRunScriptCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCRunConsoleCommandCodec_decodeRequest() {
        int fileClientMessageIndex = 811;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MCRunConsoleCommandCodec.RequestParameters parameters = MCRunConsoleCommandCodec.decodeRequest(fromFile);
        assertTrue(isEqual(null, parameters.namespace));
        assertTrue(isEqual(aString, parameters.command));
    }

    @Test
    public void test_MCRunConsoleCommandCodec_encodeResponse() {
        int fileClientMessageIndex = 812;
        ClientMessage encoded = MCRunConsoleCommandCodec.encodeResponse(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCChangeWanReplicationStateCodec_decodeRequest() {
        int fileClientMessageIndex = 813;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MCChangeWanReplicationStateCodec.RequestParameters parameters = MCChangeWanReplicationStateCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.wanReplicationName));
        assertTrue(isEqual(aString, parameters.wanPublisherId));
        assertTrue(isEqual(aByte, parameters.newState));
    }

    @Test
    public void test_MCChangeWanReplicationStateCodec_encodeResponse() {
        int fileClientMessageIndex = 814;
        ClientMessage encoded = MCChangeWanReplicationStateCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCClearWanQueuesCodec_decodeRequest() {
        int fileClientMessageIndex = 815;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MCClearWanQueuesCodec.RequestParameters parameters = MCClearWanQueuesCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.wanReplicationName));
        assertTrue(isEqual(aString, parameters.wanPublisherId));
    }

    @Test
    public void test_MCClearWanQueuesCodec_encodeResponse() {
        int fileClientMessageIndex = 816;
        ClientMessage encoded = MCClearWanQueuesCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCAddWanBatchPublisherConfigCodec_decodeRequest() {
        int fileClientMessageIndex = 817;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MCAddWanBatchPublisherConfigCodec.RequestParameters parameters = MCAddWanBatchPublisherConfigCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aString, parameters.targetCluster));
        assertTrue(isEqual(null, parameters.publisherId));
        assertTrue(isEqual(aString, parameters.endpoints));
        assertTrue(isEqual(anInt, parameters.queueCapacity));
        assertTrue(isEqual(anInt, parameters.batchSize));
        assertTrue(isEqual(anInt, parameters.batchMaxDelayMillis));
        assertTrue(isEqual(anInt, parameters.responseTimeoutMillis));
        assertTrue(isEqual(anInt, parameters.ackType));
        assertTrue(isEqual(anInt, parameters.queueFullBehavior));
    }

    @Test
    public void test_MCAddWanBatchPublisherConfigCodec_encodeResponse() {
        int fileClientMessageIndex = 818;
        ClientMessage encoded = MCAddWanBatchPublisherConfigCodec.encodeResponse(aListOfStrings, aListOfStrings);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCWanSyncMapCodec_decodeRequest() {
        int fileClientMessageIndex = 819;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MCWanSyncMapCodec.RequestParameters parameters = MCWanSyncMapCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.wanReplicationName));
        assertTrue(isEqual(aString, parameters.wanPublisherId));
        assertTrue(isEqual(anInt, parameters.wanSyncType));
        assertTrue(isEqual(null, parameters.mapName));
    }

    @Test
    public void test_MCWanSyncMapCodec_encodeResponse() {
        int fileClientMessageIndex = 820;
        ClientMessage encoded = MCWanSyncMapCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCCheckWanConsistencyCodec_decodeRequest() {
        int fileClientMessageIndex = 821;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        MCCheckWanConsistencyCodec.RequestParameters parameters = MCCheckWanConsistencyCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.wanReplicationName));
        assertTrue(isEqual(aString, parameters.wanPublisherId));
        assertTrue(isEqual(null, parameters.mapName));
    }

    @Test
    public void test_MCCheckWanConsistencyCodec_encodeResponse() {
        int fileClientMessageIndex = 822;
        ClientMessage encoded = MCCheckWanConsistencyCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCPollMCEventsCodec_decodeRequest() {
        int fileClientMessageIndex = 823;
    }

    @Test
    public void test_MCPollMCEventsCodec_encodeResponse() {
        int fileClientMessageIndex = 824;
        ClientMessage encoded = MCPollMCEventsCodec.encodeResponse(aListOfMCEvents);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCGetCPMembersCodec_decodeRequest() {
        int fileClientMessageIndex = 825;
    }

    @Test
    public void test_MCGetCPMembersCodec_encodeResponse() {
        int fileClientMessageIndex = 826;
        ClientMessage encoded = MCGetCPMembersCodec.encodeResponse(aListOfUUIDToUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCPromoteToCPMemberCodec_decodeRequest() {
        int fileClientMessageIndex = 827;
    }

    @Test
    public void test_MCPromoteToCPMemberCodec_encodeResponse() {
        int fileClientMessageIndex = 828;
        ClientMessage encoded = MCPromoteToCPMemberCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCRemoveCPMemberCodec_decodeRequest() {
        int fileClientMessageIndex = 829;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, MCRemoveCPMemberCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_MCRemoveCPMemberCodec_encodeResponse() {
        int fileClientMessageIndex = 830;
        ClientMessage encoded = MCRemoveCPMemberCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCResetCPSubsystemCodec_decodeRequest() {
        int fileClientMessageIndex = 831;
    }

    @Test
    public void test_MCResetCPSubsystemCodec_encodeResponse() {
        int fileClientMessageIndex = 832;
        ClientMessage encoded = MCResetCPSubsystemCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCTriggerPartialStartCodec_decodeRequest() {
        int fileClientMessageIndex = 833;
    }

    @Test
    public void test_MCTriggerPartialStartCodec_encodeResponse() {
        int fileClientMessageIndex = 834;
        ClientMessage encoded = MCTriggerPartialStartCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCTriggerForceStartCodec_decodeRequest() {
        int fileClientMessageIndex = 835;
    }

    @Test
    public void test_MCTriggerForceStartCodec_encodeResponse() {
        int fileClientMessageIndex = 836;
        ClientMessage encoded = MCTriggerForceStartCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCTriggerHotRestartBackupCodec_decodeRequest() {
        int fileClientMessageIndex = 837;
    }

    @Test
    public void test_MCTriggerHotRestartBackupCodec_encodeResponse() {
        int fileClientMessageIndex = 838;
        ClientMessage encoded = MCTriggerHotRestartBackupCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCInterruptHotRestartBackupCodec_decodeRequest() {
        int fileClientMessageIndex = 839;
    }

    @Test
    public void test_MCInterruptHotRestartBackupCodec_encodeResponse() {
        int fileClientMessageIndex = 840;
        ClientMessage encoded = MCInterruptHotRestartBackupCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCResetQueueAgeStatisticsCodec_decodeRequest() {
        int fileClientMessageIndex = 841;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, MCResetQueueAgeStatisticsCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_MCResetQueueAgeStatisticsCodec_encodeResponse() {
        int fileClientMessageIndex = 842;
        ClientMessage encoded = MCResetQueueAgeStatisticsCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCReloadConfigCodec_decodeRequest() {
        int fileClientMessageIndex = 843;
    }

    @Test
    public void test_MCReloadConfigCodec_encodeResponse() {
        int fileClientMessageIndex = 844;
        ClientMessage encoded = MCReloadConfigCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_MCUpdateConfigCodec_decodeRequest() {
        int fileClientMessageIndex = 845;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, MCUpdateConfigCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_MCUpdateConfigCodec_encodeResponse() {
        int fileClientMessageIndex = 846;
        ClientMessage encoded = MCUpdateConfigCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SqlExecute_reservedCodec_decodeRequest() {
        int fileClientMessageIndex = 847;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        SqlExecute_reservedCodec.RequestParameters parameters = SqlExecute_reservedCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.sql));
        assertTrue(isEqual(aListOfData, parameters.parameters));
        assertTrue(isEqual(aLong, parameters.timeoutMillis));
        assertTrue(isEqual(anInt, parameters.cursorBufferSize));
    }

    @Test
    public void test_SqlExecute_reservedCodec_encodeResponse() {
        int fileClientMessageIndex = 848;
        ClientMessage encoded = SqlExecute_reservedCodec.encodeResponse(null, null, null, aBoolean, aLong, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SqlFetch_reservedCodec_decodeRequest() {
        int fileClientMessageIndex = 849;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        SqlFetch_reservedCodec.RequestParameters parameters = SqlFetch_reservedCodec.decodeRequest(fromFile);
        assertTrue(isEqual(anSqlQueryId, parameters.queryId));
        assertTrue(isEqual(anInt, parameters.cursorBufferSize));
    }

    @Test
    public void test_SqlFetch_reservedCodec_encodeResponse() {
        int fileClientMessageIndex = 850;
        ClientMessage encoded = SqlFetch_reservedCodec.encodeResponse(null, aBoolean, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SqlCloseCodec_decodeRequest() {
        int fileClientMessageIndex = 851;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(anSqlQueryId, SqlCloseCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_SqlCloseCodec_encodeResponse() {
        int fileClientMessageIndex = 852;
        ClientMessage encoded = SqlCloseCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SqlExecuteCodec_decodeRequest() {
        int fileClientMessageIndex = 853;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        SqlExecuteCodec.RequestParameters parameters = SqlExecuteCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.sql));
        assertTrue(isEqual(aListOfData, parameters.parameters));
        assertTrue(isEqual(aLong, parameters.timeoutMillis));
        assertTrue(isEqual(anInt, parameters.cursorBufferSize));
        assertTrue(isEqual(null, parameters.schema));
        assertTrue(isEqual(aByte, parameters.expectedResultType));
        assertTrue(isEqual(anSqlQueryId, parameters.queryId));
        assertTrue(parameters.isSkipUpdateStatisticsExists);
        assertTrue(isEqual(aBoolean, parameters.skipUpdateStatistics));
    }

    @Test
    public void test_SqlExecuteCodec_encodeResponse() {
        int fileClientMessageIndex = 854;
        ClientMessage encoded = SqlExecuteCodec.encodeResponse(null, null, aLong, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SqlFetchCodec_decodeRequest() {
        int fileClientMessageIndex = 855;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        SqlFetchCodec.RequestParameters parameters = SqlFetchCodec.decodeRequest(fromFile);
        assertTrue(isEqual(anSqlQueryId, parameters.queryId));
        assertTrue(isEqual(anInt, parameters.cursorBufferSize));
    }

    @Test
    public void test_SqlFetchCodec_encodeResponse() {
        int fileClientMessageIndex = 856;
        ClientMessage encoded = SqlFetchCodec.encodeResponse(null, null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_SqlMappingDdlCodec_decodeRequest() {
        int fileClientMessageIndex = 857;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, SqlMappingDdlCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_SqlMappingDdlCodec_encodeResponse() {
        int fileClientMessageIndex = 858;
        ClientMessage encoded = SqlMappingDdlCodec.encodeResponse(null);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CPSubsystemAddMembershipListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 859;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, CPSubsystemAddMembershipListenerCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_CPSubsystemAddMembershipListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 860;
        ClientMessage encoded = CPSubsystemAddMembershipListenerCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CPSubsystemAddMembershipListenerCodec_encodeMembershipEventEvent() {
        int fileClientMessageIndex = 861;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = CPSubsystemAddMembershipListenerCodec.encodeMembershipEventEvent(aCpMember, aByte);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CPSubsystemRemoveMembershipListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 862;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, CPSubsystemRemoveMembershipListenerCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_CPSubsystemRemoveMembershipListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 863;
        ClientMessage encoded = CPSubsystemRemoveMembershipListenerCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CPSubsystemAddGroupAvailabilityListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 864;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aBoolean, CPSubsystemAddGroupAvailabilityListenerCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_CPSubsystemAddGroupAvailabilityListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 865;
        ClientMessage encoded = CPSubsystemAddGroupAvailabilityListenerCodec.encodeResponse(aUUID);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CPSubsystemAddGroupAvailabilityListenerCodec_encodeGroupAvailabilityEventEvent() {
        int fileClientMessageIndex = 866;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        ClientMessage encoded = CPSubsystemAddGroupAvailabilityListenerCodec.encodeGroupAvailabilityEventEvent(aRaftGroupId, aListOfCpMembers, aListOfCpMembers);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_CPSubsystemRemoveGroupAvailabilityListenerCodec_decodeRequest() {
        int fileClientMessageIndex = 867;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aUUID, CPSubsystemRemoveGroupAvailabilityListenerCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_CPSubsystemRemoveGroupAvailabilityListenerCodec_encodeResponse() {
        int fileClientMessageIndex = 868;
        ClientMessage encoded = CPSubsystemRemoveGroupAvailabilityListenerCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetSubmitJobCodec_decodeRequest() {
        int fileClientMessageIndex = 869;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        JetSubmitJobCodec.RequestParameters parameters = JetSubmitJobCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aLong, parameters.jobId));
        assertTrue(isEqual(aData, parameters.dag));
        assertTrue(isEqual(null, parameters.jobConfig));
        assertTrue(parameters.isLightJobCoordinatorExists);
        assertTrue(isEqual(null, parameters.lightJobCoordinator));
    }

    @Test
    public void test_JetSubmitJobCodec_encodeResponse() {
        int fileClientMessageIndex = 870;
        ClientMessage encoded = JetSubmitJobCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetTerminateJobCodec_decodeRequest() {
        int fileClientMessageIndex = 871;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        JetTerminateJobCodec.RequestParameters parameters = JetTerminateJobCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aLong, parameters.jobId));
        assertTrue(isEqual(anInt, parameters.terminateMode));
        assertTrue(parameters.isLightJobCoordinatorExists);
        assertTrue(isEqual(null, parameters.lightJobCoordinator));
    }

    @Test
    public void test_JetTerminateJobCodec_encodeResponse() {
        int fileClientMessageIndex = 872;
        ClientMessage encoded = JetTerminateJobCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetGetJobStatusCodec_decodeRequest() {
        int fileClientMessageIndex = 873;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aLong, JetGetJobStatusCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_JetGetJobStatusCodec_encodeResponse() {
        int fileClientMessageIndex = 874;
        ClientMessage encoded = JetGetJobStatusCodec.encodeResponse(anInt);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetGetJobIdsCodec_decodeRequest() {
        int fileClientMessageIndex = 875;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        JetGetJobIdsCodec.RequestParameters parameters = JetGetJobIdsCodec.decodeRequest(fromFile);
        assertTrue(parameters.isOnlyNameExists);
        assertTrue(isEqual(null, parameters.onlyName));
        assertTrue(parameters.isOnlyJobIdExists);
        assertTrue(isEqual(aLong, parameters.onlyJobId));
    }

    @Test
    public void test_JetGetJobIdsCodec_encodeResponse() {
        int fileClientMessageIndex = 876;
        ClientMessage encoded = JetGetJobIdsCodec.encodeResponse(aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetJoinSubmittedJobCodec_decodeRequest() {
        int fileClientMessageIndex = 877;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        JetJoinSubmittedJobCodec.RequestParameters parameters = JetJoinSubmittedJobCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aLong, parameters.jobId));
        assertTrue(parameters.isLightJobCoordinatorExists);
        assertTrue(isEqual(null, parameters.lightJobCoordinator));
    }

    @Test
    public void test_JetJoinSubmittedJobCodec_encodeResponse() {
        int fileClientMessageIndex = 878;
        ClientMessage encoded = JetJoinSubmittedJobCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetGetJobSubmissionTimeCodec_decodeRequest() {
        int fileClientMessageIndex = 879;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        JetGetJobSubmissionTimeCodec.RequestParameters parameters = JetGetJobSubmissionTimeCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aLong, parameters.jobId));
        assertTrue(parameters.isLightJobCoordinatorExists);
        assertTrue(isEqual(null, parameters.lightJobCoordinator));
    }

    @Test
    public void test_JetGetJobSubmissionTimeCodec_encodeResponse() {
        int fileClientMessageIndex = 880;
        ClientMessage encoded = JetGetJobSubmissionTimeCodec.encodeResponse(aLong);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetGetJobConfigCodec_decodeRequest() {
        int fileClientMessageIndex = 881;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        JetGetJobConfigCodec.RequestParameters parameters = JetGetJobConfigCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aLong, parameters.jobId));
        assertTrue(parameters.isLightJobCoordinatorExists);
        assertTrue(isEqual(null, parameters.lightJobCoordinator));
    }

    @Test
    public void test_JetGetJobConfigCodec_encodeResponse() {
        int fileClientMessageIndex = 882;
        ClientMessage encoded = JetGetJobConfigCodec.encodeResponse(aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetResumeJobCodec_decodeRequest() {
        int fileClientMessageIndex = 883;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aLong, JetResumeJobCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_JetResumeJobCodec_encodeResponse() {
        int fileClientMessageIndex = 884;
        ClientMessage encoded = JetResumeJobCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetExportSnapshotCodec_decodeRequest() {
        int fileClientMessageIndex = 885;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        JetExportSnapshotCodec.RequestParameters parameters = JetExportSnapshotCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aLong, parameters.jobId));
        assertTrue(isEqual(aString, parameters.name));
        assertTrue(isEqual(aBoolean, parameters.cancelJob));
    }

    @Test
    public void test_JetExportSnapshotCodec_encodeResponse() {
        int fileClientMessageIndex = 886;
        ClientMessage encoded = JetExportSnapshotCodec.encodeResponse();
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetGetJobSummaryListCodec_decodeRequest() {
        int fileClientMessageIndex = 887;
    }

    @Test
    public void test_JetGetJobSummaryListCodec_encodeResponse() {
        int fileClientMessageIndex = 888;
        ClientMessage encoded = JetGetJobSummaryListCodec.encodeResponse(aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetExistsDistributedObjectCodec_decodeRequest() {
        int fileClientMessageIndex = 889;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        JetExistsDistributedObjectCodec.RequestParameters parameters = JetExistsDistributedObjectCodec.decodeRequest(fromFile);
        assertTrue(isEqual(aString, parameters.serviceName));
        assertTrue(isEqual(aString, parameters.objectName));
    }

    @Test
    public void test_JetExistsDistributedObjectCodec_encodeResponse() {
        int fileClientMessageIndex = 890;
        ClientMessage encoded = JetExistsDistributedObjectCodec.encodeResponse(aBoolean);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetGetJobMetricsCodec_decodeRequest() {
        int fileClientMessageIndex = 891;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aLong, JetGetJobMetricsCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_JetGetJobMetricsCodec_encodeResponse() {
        int fileClientMessageIndex = 892;
        ClientMessage encoded = JetGetJobMetricsCodec.encodeResponse(aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    @Test
    public void test_JetGetJobSuspensionCauseCodec_decodeRequest() {
        int fileClientMessageIndex = 893;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aLong, JetGetJobSuspensionCauseCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_JetGetJobSuspensionCauseCodec_encodeResponse() {
        int fileClientMessageIndex = 894;
        ClientMessage encoded = JetGetJobSuspensionCauseCodec.encodeResponse(aData);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
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