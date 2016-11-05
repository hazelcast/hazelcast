package com.hazelcast.client.protocol.compatibility;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.*;
import com.hazelcast.client.impl.protocol.util.SafeBuffer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.aBoolean;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.aByte;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.aData;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.aListOfEntry;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.aLong;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.aMember;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.aPartitionTable;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.aQueryCacheEventData;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.aString;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.anAddress;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.anInt;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.anXid;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.cacheEventDatas;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.datas;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.distributedObjectInfos;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.isEqual;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.jobPartitionStates;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.members;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.queryCacheEventDatas;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ServerCompatibilityNullTest_1_1 {
    private static final int FRAME_LEN_FIELD_SIZE = 4;

    @org.junit.Test
    public void test()
            throws IOException {
        InputStream input = getClass().getResourceAsStream("/1.1.protocol.compatibility.null.binary");
        DataInputStream inputStream = new DataInputStream(input);
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ClientAuthenticationCodec.RequestParameters params = ClientAuthenticationCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.username));
            assertTrue(isEqual(aString, params.password));
            assertTrue(isEqual(null, params.uuid));
            assertTrue(isEqual(null, params.ownerUuid));
            assertTrue(isEqual(aBoolean, params.isOwnerConnection));
            assertTrue(isEqual(aString, params.clientType));
            assertTrue(isEqual(aByte, params.serializationVersion));
            assertFalse(params.clientHazelcastVersionExist);
        }
        {
            ClientMessage clientMessage = ClientAuthenticationCodec.encodeResponse(aByte, null, null, null, aByte, aString, null);
            int length = inputStream.readInt();
            // Since the test is generated for protocol version (1.1) which is earlier than latest change in the message
            // (version 1.3), only the bytes after frame length fields are compared
            int frameLength = clientMessage.getFrameLength();
            assertTrue(frameLength >= length);
            inputStream.skipBytes(FRAME_LEN_FIELD_SIZE);
            byte[] bytes = new byte[length - FRAME_LEN_FIELD_SIZE];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOfRange(clientMessage.buffer().byteArray(), FRAME_LEN_FIELD_SIZE, length), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ClientAuthenticationCustomCodec.RequestParameters params = ClientAuthenticationCustomCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aData, params.credentials));
            assertTrue(isEqual(null, params.uuid));
            assertTrue(isEqual(null, params.ownerUuid));
            assertTrue(isEqual(aBoolean, params.isOwnerConnection));
            assertTrue(isEqual(aString, params.clientType));
            assertTrue(isEqual(aByte, params.serializationVersion));
            assertFalse(params.clientHazelcastVersionExist);
        }
        {
            ClientMessage clientMessage = ClientAuthenticationCustomCodec
                    .encodeResponse(aByte, null, null, null, aByte, aString, null);
            int length = inputStream.readInt();
            // Since the test is generated for protocol version (1.1) which is earlier than latest change in the message
            // (version 1.3), only the bytes after frame length fields are compared
            int frameLength = clientMessage.getFrameLength();
            assertTrue(frameLength >= length);
            inputStream.skipBytes(FRAME_LEN_FIELD_SIZE);
            byte[] bytes = new byte[length - FRAME_LEN_FIELD_SIZE];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOfRange(clientMessage.buffer().byteArray(), FRAME_LEN_FIELD_SIZE, length), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ClientAddMembershipListenerCodec.RequestParameters params = ClientAddMembershipListenerCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aBoolean, params.localOnly));
        }
        {
            ClientMessage clientMessage = ClientAddMembershipListenerCodec.encodeResponse(aString);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            {
                ClientMessage clientMessage = ClientAddMembershipListenerCodec.encodeMemberEvent(aMember, anInt);
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.read(bytes);
                assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
            }
            {
                ClientMessage clientMessage = ClientAddMembershipListenerCodec.encodeMemberListEvent(members);
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.read(bytes);
                assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
            }
            {
                ClientMessage clientMessage = ClientAddMembershipListenerCodec
                        .encodeMemberAttributeChangeEvent(aString, aString, anInt, null);
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.read(bytes);
                assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
            }
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ClientCreateProxyCodec.RequestParameters params = ClientCreateProxyCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.serviceName));
            assertTrue(isEqual(anAddress, params.target));
        }
        {
            ClientMessage clientMessage = ClientCreateProxyCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ClientDestroyProxyCodec.RequestParameters params = ClientDestroyProxyCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.serviceName));
        }
        {
            ClientMessage clientMessage = ClientDestroyProxyCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ClientGetPartitionsCodec.RequestParameters params = ClientGetPartitionsCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
        }
        {
            ClientMessage clientMessage = ClientGetPartitionsCodec.encodeResponse(aPartitionTable);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ClientRemoveAllListenersCodec.RequestParameters params = ClientRemoveAllListenersCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
        }
        {
            ClientMessage clientMessage = ClientRemoveAllListenersCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ClientAddPartitionLostListenerCodec.RequestParameters params = ClientAddPartitionLostListenerCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aBoolean, params.localOnly));
        }
        {
            ClientMessage clientMessage = ClientAddPartitionLostListenerCodec.encodeResponse(aString);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            {
                ClientMessage clientMessage = ClientAddPartitionLostListenerCodec.encodePartitionLostEvent(anInt, anInt, null);
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.read(bytes);
                assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
            }
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ClientRemovePartitionLostListenerCodec.RequestParameters params = ClientRemovePartitionLostListenerCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.registrationId));
        }
        {
            ClientMessage clientMessage = ClientRemovePartitionLostListenerCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ClientGetDistributedObjectsCodec.RequestParameters params = ClientGetDistributedObjectsCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
        }
        {
            ClientMessage clientMessage = ClientGetDistributedObjectsCodec.encodeResponse(distributedObjectInfos);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ClientAddDistributedObjectListenerCodec.RequestParameters params = ClientAddDistributedObjectListenerCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aBoolean, params.localOnly));
        }
        {
            ClientMessage clientMessage = ClientAddDistributedObjectListenerCodec.encodeResponse(aString);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            {
                ClientMessage clientMessage = ClientAddDistributedObjectListenerCodec
                        .encodeDistributedObjectEvent(aString, aString, aString);
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.read(bytes);
                assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
            }
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ClientRemoveDistributedObjectListenerCodec.RequestParameters params = ClientRemoveDistributedObjectListenerCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.registrationId));
        }
        {
            ClientMessage clientMessage = ClientRemoveDistributedObjectListenerCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ClientPingCodec.RequestParameters params = ClientPingCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
        }
        {
            ClientMessage clientMessage = ClientPingCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapPutCodec.RequestParameters params = MapPutCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.ttl));
        }
        {
            ClientMessage clientMessage = MapPutCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapGetCodec.RequestParameters params = MapGetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
        }
        {
            ClientMessage clientMessage = MapGetCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapRemoveCodec.RequestParameters params = MapRemoveCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
        }
        {
            ClientMessage clientMessage = MapRemoveCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapReplaceCodec.RequestParameters params = MapReplaceCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(aLong, params.threadId));
        }
        {
            ClientMessage clientMessage = MapReplaceCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapReplaceIfSameCodec.RequestParameters params = MapReplaceIfSameCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.testValue));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(aLong, params.threadId));
        }
        {
            ClientMessage clientMessage = MapReplaceIfSameCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapContainsKeyCodec.RequestParameters params = MapContainsKeyCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
        }
        {
            ClientMessage clientMessage = MapContainsKeyCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapContainsValueCodec.RequestParameters params = MapContainsValueCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.value));
        }
        {
            ClientMessage clientMessage = MapContainsValueCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapRemoveIfSameCodec.RequestParameters params = MapRemoveIfSameCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(aLong, params.threadId));
        }
        {
            ClientMessage clientMessage = MapRemoveIfSameCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapDeleteCodec.RequestParameters params = MapDeleteCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
        }
        {
            ClientMessage clientMessage = MapDeleteCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapFlushCodec.RequestParameters params = MapFlushCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = MapFlushCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapTryRemoveCodec.RequestParameters params = MapTryRemoveCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.timeout));
        }
        {
            ClientMessage clientMessage = MapTryRemoveCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapTryPutCodec.RequestParameters params = MapTryPutCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.timeout));
        }
        {
            ClientMessage clientMessage = MapTryPutCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapPutTransientCodec.RequestParameters params = MapPutTransientCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.ttl));
        }
        {
            ClientMessage clientMessage = MapPutTransientCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapPutIfAbsentCodec.RequestParameters params = MapPutIfAbsentCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.ttl));
        }
        {
            ClientMessage clientMessage = MapPutIfAbsentCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapSetCodec.RequestParameters params = MapSetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.ttl));
        }
        {
            ClientMessage clientMessage = MapSetCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapLockCodec.RequestParameters params = MapLockCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.ttl));
            assertFalse(params.referenceIdExist);
        }
        {
            ClientMessage clientMessage = MapLockCodec.encodeResponse();
            int length = inputStream.readInt();
            // Since the test is generated for protocol version (1.1) which is earlier than latest change in the message
            // (version 1.2), only the bytes after frame length fields are compared
            int frameLength = clientMessage.getFrameLength();
            assertTrue(frameLength >= length);
            inputStream.skipBytes(FRAME_LEN_FIELD_SIZE);
            byte[] bytes = new byte[length - FRAME_LEN_FIELD_SIZE];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOfRange(clientMessage.buffer().byteArray(), FRAME_LEN_FIELD_SIZE, length), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapTryLockCodec.RequestParameters params = MapTryLockCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.lease));
            assertTrue(isEqual(aLong, params.timeout));
            assertFalse(params.referenceIdExist);
        }
        {
            ClientMessage clientMessage = MapTryLockCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            // Since the test is generated for protocol version (1.1) which is earlier than latest change in the message
            // (version 1.2), only the bytes after frame length fields are compared
            int frameLength = clientMessage.getFrameLength();
            assertTrue(frameLength >= length);
            inputStream.skipBytes(FRAME_LEN_FIELD_SIZE);
            byte[] bytes = new byte[length - FRAME_LEN_FIELD_SIZE];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOfRange(clientMessage.buffer().byteArray(), FRAME_LEN_FIELD_SIZE, length), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapIsLockedCodec.RequestParameters params = MapIsLockedCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
        }
        {
            ClientMessage clientMessage = MapIsLockedCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapUnlockCodec.RequestParameters params = MapUnlockCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
            assertFalse(params.referenceIdExist);
        }
        {
            ClientMessage clientMessage = MapUnlockCodec.encodeResponse();
            int length = inputStream.readInt();
            // Since the test is generated for protocol version (1.1) which is earlier than latest change in the message
            // (version 1.2), only the bytes after frame length fields are compared
            int frameLength = clientMessage.getFrameLength();
            assertTrue(frameLength >= length);
            inputStream.skipBytes(FRAME_LEN_FIELD_SIZE);
            byte[] bytes = new byte[length - FRAME_LEN_FIELD_SIZE];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOfRange(clientMessage.buffer().byteArray(), FRAME_LEN_FIELD_SIZE, length), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapAddInterceptorCodec.RequestParameters params = MapAddInterceptorCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.interceptor));
        }
        {
            ClientMessage clientMessage = MapAddInterceptorCodec.encodeResponse(aString);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapRemoveInterceptorCodec.RequestParameters params = MapRemoveInterceptorCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.id));
        }
        {
            ClientMessage clientMessage = MapRemoveInterceptorCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapAddEntryListenerToKeyWithPredicateCodec.RequestParameters params = MapAddEntryListenerToKeyWithPredicateCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.predicate));
            assertTrue(isEqual(aBoolean, params.includeValue));
            assertTrue(isEqual(anInt, params.listenerFlags));
            assertTrue(isEqual(aBoolean, params.localOnly));
        }
        {
            ClientMessage clientMessage = MapAddEntryListenerToKeyWithPredicateCodec.encodeResponse(aString);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            {
                ClientMessage clientMessage = MapAddEntryListenerToKeyWithPredicateCodec
                        .encodeEntryEvent(null, null, null, null, anInt, aString, anInt);
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.read(bytes);
                assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
            }
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapAddEntryListenerWithPredicateCodec.RequestParameters params = MapAddEntryListenerWithPredicateCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.predicate));
            assertTrue(isEqual(aBoolean, params.includeValue));
            assertTrue(isEqual(anInt, params.listenerFlags));
            assertTrue(isEqual(aBoolean, params.localOnly));
        }
        {
            ClientMessage clientMessage = MapAddEntryListenerWithPredicateCodec.encodeResponse(aString);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            {
                ClientMessage clientMessage = MapAddEntryListenerWithPredicateCodec
                        .encodeEntryEvent(null, null, null, null, anInt, aString, anInt);
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.read(bytes);
                assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
            }
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapAddEntryListenerToKeyCodec.RequestParameters params = MapAddEntryListenerToKeyCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aBoolean, params.includeValue));
            assertTrue(isEqual(anInt, params.listenerFlags));
            assertTrue(isEqual(aBoolean, params.localOnly));
        }
        {
            ClientMessage clientMessage = MapAddEntryListenerToKeyCodec.encodeResponse(aString);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            {
                ClientMessage clientMessage = MapAddEntryListenerToKeyCodec
                        .encodeEntryEvent(null, null, null, null, anInt, aString, anInt);
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.read(bytes);
                assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
            }
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapAddEntryListenerCodec.RequestParameters params = MapAddEntryListenerCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aBoolean, params.includeValue));
            assertTrue(isEqual(anInt, params.listenerFlags));
            assertTrue(isEqual(aBoolean, params.localOnly));
        }
        {
            ClientMessage clientMessage = MapAddEntryListenerCodec.encodeResponse(aString);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            {
                ClientMessage clientMessage = MapAddEntryListenerCodec
                        .encodeEntryEvent(null, null, null, null, anInt, aString, anInt);
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.read(bytes);
                assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
            }
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapAddNearCacheEntryListenerCodec.RequestParameters params = MapAddNearCacheEntryListenerCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.listenerFlags));
            assertTrue(isEqual(aBoolean, params.localOnly));
        }
        {
            ClientMessage clientMessage = MapAddNearCacheEntryListenerCodec.encodeResponse(aString);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            {
                ClientMessage clientMessage = MapAddNearCacheEntryListenerCodec.encodeIMapInvalidationEvent(null);
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.read(bytes);
                assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
            }
            {
                ClientMessage clientMessage = MapAddNearCacheEntryListenerCodec.encodeIMapBatchInvalidationEvent(datas);
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.read(bytes);
                assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
            }
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapRemoveEntryListenerCodec.RequestParameters params = MapRemoveEntryListenerCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.registrationId));
        }
        {
            ClientMessage clientMessage = MapRemoveEntryListenerCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapAddPartitionLostListenerCodec.RequestParameters params = MapAddPartitionLostListenerCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aBoolean, params.localOnly));
        }
        {
            ClientMessage clientMessage = MapAddPartitionLostListenerCodec.encodeResponse(aString);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            {
                ClientMessage clientMessage = MapAddPartitionLostListenerCodec.encodeMapPartitionLostEvent(anInt, aString);
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.read(bytes);
                assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
            }
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapRemovePartitionLostListenerCodec.RequestParameters params = MapRemovePartitionLostListenerCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.registrationId));
        }
        {
            ClientMessage clientMessage = MapRemovePartitionLostListenerCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapGetEntryViewCodec.RequestParameters params = MapGetEntryViewCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
        }
        {
            ClientMessage clientMessage = MapGetEntryViewCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapEvictCodec.RequestParameters params = MapEvictCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
        }
        {
            ClientMessage clientMessage = MapEvictCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapEvictAllCodec.RequestParameters params = MapEvictAllCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = MapEvictAllCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapLoadAllCodec.RequestParameters params = MapLoadAllCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aBoolean, params.replaceExistingValues));
        }
        {
            ClientMessage clientMessage = MapLoadAllCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapLoadGivenKeysCodec.RequestParameters params = MapLoadGivenKeysCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.keys));
            assertTrue(isEqual(aBoolean, params.replaceExistingValues));
        }
        {
            ClientMessage clientMessage = MapLoadGivenKeysCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapKeySetCodec.RequestParameters params = MapKeySetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = MapKeySetCodec.encodeResponse(datas);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapGetAllCodec.RequestParameters params = MapGetAllCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.keys));
        }
        {
            ClientMessage clientMessage = MapGetAllCodec.encodeResponse(aListOfEntry);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapValuesCodec.RequestParameters params = MapValuesCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = MapValuesCodec.encodeResponse(datas);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapEntrySetCodec.RequestParameters params = MapEntrySetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = MapEntrySetCodec.encodeResponse(aListOfEntry);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapKeySetWithPredicateCodec.RequestParameters params = MapKeySetWithPredicateCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.predicate));
        }
        {
            ClientMessage clientMessage = MapKeySetWithPredicateCodec.encodeResponse(datas);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapValuesWithPredicateCodec.RequestParameters params = MapValuesWithPredicateCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.predicate));
        }
        {
            ClientMessage clientMessage = MapValuesWithPredicateCodec.encodeResponse(datas);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapEntriesWithPredicateCodec.RequestParameters params = MapEntriesWithPredicateCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.predicate));
        }
        {
            ClientMessage clientMessage = MapEntriesWithPredicateCodec.encodeResponse(aListOfEntry);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapAddIndexCodec.RequestParameters params = MapAddIndexCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.attribute));
            assertTrue(isEqual(aBoolean, params.ordered));
        }
        {
            ClientMessage clientMessage = MapAddIndexCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapSizeCodec.RequestParameters params = MapSizeCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = MapSizeCodec.encodeResponse(anInt);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapIsEmptyCodec.RequestParameters params = MapIsEmptyCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = MapIsEmptyCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapPutAllCodec.RequestParameters params = MapPutAllCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aListOfEntry, params.entries));
        }
        {
            ClientMessage clientMessage = MapPutAllCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapClearCodec.RequestParameters params = MapClearCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = MapClearCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapExecuteOnKeyCodec.RequestParameters params = MapExecuteOnKeyCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.entryProcessor));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
        }
        {
            ClientMessage clientMessage = MapExecuteOnKeyCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapSubmitToKeyCodec.RequestParameters params = MapSubmitToKeyCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.entryProcessor));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
        }
        {
            ClientMessage clientMessage = MapSubmitToKeyCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapExecuteOnAllKeysCodec.RequestParameters params = MapExecuteOnAllKeysCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.entryProcessor));
        }
        {
            ClientMessage clientMessage = MapExecuteOnAllKeysCodec.encodeResponse(aListOfEntry);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapExecuteWithPredicateCodec.RequestParameters params = MapExecuteWithPredicateCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.entryProcessor));
            assertTrue(isEqual(aData, params.predicate));
        }
        {
            ClientMessage clientMessage = MapExecuteWithPredicateCodec.encodeResponse(aListOfEntry);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapExecuteOnKeysCodec.RequestParameters params = MapExecuteOnKeysCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.entryProcessor));
            assertTrue(isEqual(datas, params.keys));
        }
        {
            ClientMessage clientMessage = MapExecuteOnKeysCodec.encodeResponse(aListOfEntry);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapForceUnlockCodec.RequestParameters params = MapForceUnlockCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertFalse(params.referenceIdExist);
        }
        {
            ClientMessage clientMessage = MapForceUnlockCodec.encodeResponse();
            int length = inputStream.readInt();
            // Since the test is generated for protocol version (1.1) which is earlier than latest change in the message
            // (version 1.2), only the bytes after frame length fields are compared
            int frameLength = clientMessage.getFrameLength();
            assertTrue(frameLength >= length);
            inputStream.skipBytes(FRAME_LEN_FIELD_SIZE);
            byte[] bytes = new byte[length - FRAME_LEN_FIELD_SIZE];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOfRange(clientMessage.buffer().byteArray(), FRAME_LEN_FIELD_SIZE, length), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapKeySetWithPagingPredicateCodec.RequestParameters params = MapKeySetWithPagingPredicateCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.predicate));
        }
        {
            ClientMessage clientMessage = MapKeySetWithPagingPredicateCodec.encodeResponse(datas);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapValuesWithPagingPredicateCodec.RequestParameters params = MapValuesWithPagingPredicateCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.predicate));
        }
        {
            ClientMessage clientMessage = MapValuesWithPagingPredicateCodec.encodeResponse(aListOfEntry);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapEntriesWithPagingPredicateCodec.RequestParameters params = MapEntriesWithPagingPredicateCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.predicate));
        }
        {
            ClientMessage clientMessage = MapEntriesWithPagingPredicateCodec.encodeResponse(aListOfEntry);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapClearNearCacheCodec.RequestParameters params = MapClearNearCacheCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anAddress, params.target));
        }
        {
            ClientMessage clientMessage = MapClearNearCacheCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapFetchKeysCodec.RequestParameters params = MapFetchKeysCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.partitionId));
            assertTrue(isEqual(anInt, params.tableIndex));
            assertTrue(isEqual(anInt, params.batch));
        }
        {
            ClientMessage clientMessage = MapFetchKeysCodec.encodeResponse(anInt, datas);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapFetchEntriesCodec.RequestParameters params = MapFetchEntriesCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.partitionId));
            assertTrue(isEqual(anInt, params.tableIndex));
            assertTrue(isEqual(anInt, params.batch));
        }
        {
            ClientMessage clientMessage = MapFetchEntriesCodec.encodeResponse(anInt, aListOfEntry);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MultiMapPutCodec.RequestParameters params = MultiMapPutCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(aLong, params.threadId));
        }
        {
            ClientMessage clientMessage = MultiMapPutCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MultiMapGetCodec.RequestParameters params = MultiMapGetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
        }
        {
            ClientMessage clientMessage = MultiMapGetCodec.encodeResponse(datas);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MultiMapRemoveCodec.RequestParameters params = MultiMapRemoveCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
        }
        {
            ClientMessage clientMessage = MultiMapRemoveCodec.encodeResponse(datas);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MultiMapKeySetCodec.RequestParameters params = MultiMapKeySetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = MultiMapKeySetCodec.encodeResponse(datas);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MultiMapValuesCodec.RequestParameters params = MultiMapValuesCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = MultiMapValuesCodec.encodeResponse(datas);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MultiMapEntrySetCodec.RequestParameters params = MultiMapEntrySetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = MultiMapEntrySetCodec.encodeResponse(aListOfEntry);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MultiMapContainsKeyCodec.RequestParameters params = MultiMapContainsKeyCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
        }
        {
            ClientMessage clientMessage = MultiMapContainsKeyCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MultiMapContainsValueCodec.RequestParameters params = MultiMapContainsValueCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.value));
        }
        {
            ClientMessage clientMessage = MultiMapContainsValueCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MultiMapContainsEntryCodec.RequestParameters params = MultiMapContainsEntryCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(aLong, params.threadId));
        }
        {
            ClientMessage clientMessage = MultiMapContainsEntryCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MultiMapSizeCodec.RequestParameters params = MultiMapSizeCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = MultiMapSizeCodec.encodeResponse(anInt);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MultiMapClearCodec.RequestParameters params = MultiMapClearCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = MultiMapClearCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MultiMapValueCountCodec.RequestParameters params = MultiMapValueCountCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
        }
        {
            ClientMessage clientMessage = MultiMapValueCountCodec.encodeResponse(anInt);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MultiMapAddEntryListenerToKeyCodec.RequestParameters params = MultiMapAddEntryListenerToKeyCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aBoolean, params.includeValue));
            assertTrue(isEqual(aBoolean, params.localOnly));
        }
        {
            ClientMessage clientMessage = MultiMapAddEntryListenerToKeyCodec.encodeResponse(aString);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            {
                ClientMessage clientMessage = MultiMapAddEntryListenerToKeyCodec
                        .encodeEntryEvent(null, null, null, null, anInt, aString, anInt);
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.read(bytes);
                assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
            }
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MultiMapAddEntryListenerCodec.RequestParameters params = MultiMapAddEntryListenerCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aBoolean, params.includeValue));
            assertTrue(isEqual(aBoolean, params.localOnly));
        }
        {
            ClientMessage clientMessage = MultiMapAddEntryListenerCodec.encodeResponse(aString);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            {
                ClientMessage clientMessage = MultiMapAddEntryListenerCodec
                        .encodeEntryEvent(null, null, null, null, anInt, aString, anInt);
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.read(bytes);
                assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
            }
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MultiMapRemoveEntryListenerCodec.RequestParameters params = MultiMapRemoveEntryListenerCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.registrationId));
        }
        {
            ClientMessage clientMessage = MultiMapRemoveEntryListenerCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MultiMapLockCodec.RequestParameters params = MultiMapLockCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.ttl));
            assertFalse(params.referenceIdExist);
        }
        {
            ClientMessage clientMessage = MultiMapLockCodec.encodeResponse();
            int length = inputStream.readInt();
            // Since the test is generated for protocol version (1.1) which is earlier than latest change in the message
            // (version 1.2), only the bytes after frame length fields are compared
            int frameLength = clientMessage.getFrameLength();
            assertTrue(frameLength >= length);
            inputStream.skipBytes(FRAME_LEN_FIELD_SIZE);
            byte[] bytes = new byte[length - FRAME_LEN_FIELD_SIZE];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOfRange(clientMessage.buffer().byteArray(), FRAME_LEN_FIELD_SIZE, length), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MultiMapTryLockCodec.RequestParameters params = MultiMapTryLockCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.lease));
            assertTrue(isEqual(aLong, params.timeout));
            assertFalse(params.referenceIdExist);
        }
        {
            ClientMessage clientMessage = MultiMapTryLockCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            // Since the test is generated for protocol version (1.1) which is earlier than latest change in the message
            // (version 1.2), only the bytes after frame length fields are compared
            int frameLength = clientMessage.getFrameLength();
            assertTrue(frameLength >= length);
            inputStream.skipBytes(FRAME_LEN_FIELD_SIZE);
            byte[] bytes = new byte[length - FRAME_LEN_FIELD_SIZE];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOfRange(clientMessage.buffer().byteArray(), FRAME_LEN_FIELD_SIZE, length), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MultiMapIsLockedCodec.RequestParameters params = MultiMapIsLockedCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
        }
        {
            ClientMessage clientMessage = MultiMapIsLockedCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MultiMapUnlockCodec.RequestParameters params = MultiMapUnlockCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
            assertFalse(params.referenceIdExist);
        }
        {
            ClientMessage clientMessage = MultiMapUnlockCodec.encodeResponse();
            int length = inputStream.readInt();
            // Since the test is generated for protocol version (1.1) which is earlier than latest change in the message
            // (version 1.2), only the bytes after frame length fields are compared
            int frameLength = clientMessage.getFrameLength();
            assertTrue(frameLength >= length);
            inputStream.skipBytes(FRAME_LEN_FIELD_SIZE);
            byte[] bytes = new byte[length - FRAME_LEN_FIELD_SIZE];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOfRange(clientMessage.buffer().byteArray(), FRAME_LEN_FIELD_SIZE, length), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MultiMapForceUnlockCodec.RequestParameters params = MultiMapForceUnlockCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertFalse(params.referenceIdExist);
        }
        {
            ClientMessage clientMessage = MultiMapForceUnlockCodec.encodeResponse();
            int length = inputStream.readInt();
            // Since the test is generated for protocol version (1.1) which is earlier than latest change in the message
            // (version 1.2), only the bytes after frame length fields are compared
            int frameLength = clientMessage.getFrameLength();
            assertTrue(frameLength >= length);
            inputStream.skipBytes(FRAME_LEN_FIELD_SIZE);
            byte[] bytes = new byte[length - FRAME_LEN_FIELD_SIZE];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOfRange(clientMessage.buffer().byteArray(), FRAME_LEN_FIELD_SIZE, length), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MultiMapRemoveEntryCodec.RequestParameters params = MultiMapRemoveEntryCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(aLong, params.threadId));
        }
        {
            ClientMessage clientMessage = MultiMapRemoveEntryCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            QueueOfferCodec.RequestParameters params = QueueOfferCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(aLong, params.timeoutMillis));
        }
        {
            ClientMessage clientMessage = QueueOfferCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            QueuePutCodec.RequestParameters params = QueuePutCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.value));
        }
        {
            ClientMessage clientMessage = QueuePutCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            QueueSizeCodec.RequestParameters params = QueueSizeCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = QueueSizeCodec.encodeResponse(anInt);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            QueueRemoveCodec.RequestParameters params = QueueRemoveCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.value));
        }
        {
            ClientMessage clientMessage = QueueRemoveCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            QueuePollCodec.RequestParameters params = QueuePollCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.timeoutMillis));
        }
        {
            ClientMessage clientMessage = QueuePollCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            QueueTakeCodec.RequestParameters params = QueueTakeCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = QueueTakeCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            QueuePeekCodec.RequestParameters params = QueuePeekCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = QueuePeekCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            QueueIteratorCodec.RequestParameters params = QueueIteratorCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = QueueIteratorCodec.encodeResponse(datas);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            QueueDrainToCodec.RequestParameters params = QueueDrainToCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = QueueDrainToCodec.encodeResponse(datas);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            QueueDrainToMaxSizeCodec.RequestParameters params = QueueDrainToMaxSizeCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.maxSize));
        }
        {
            ClientMessage clientMessage = QueueDrainToMaxSizeCodec.encodeResponse(datas);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            QueueContainsCodec.RequestParameters params = QueueContainsCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.value));
        }
        {
            ClientMessage clientMessage = QueueContainsCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            QueueContainsAllCodec.RequestParameters params = QueueContainsAllCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.dataList));
        }
        {
            ClientMessage clientMessage = QueueContainsAllCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            QueueCompareAndRemoveAllCodec.RequestParameters params = QueueCompareAndRemoveAllCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.dataList));
        }
        {
            ClientMessage clientMessage = QueueCompareAndRemoveAllCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            QueueCompareAndRetainAllCodec.RequestParameters params = QueueCompareAndRetainAllCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.dataList));
        }
        {
            ClientMessage clientMessage = QueueCompareAndRetainAllCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            QueueClearCodec.RequestParameters params = QueueClearCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = QueueClearCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            QueueAddAllCodec.RequestParameters params = QueueAddAllCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.dataList));
        }
        {
            ClientMessage clientMessage = QueueAddAllCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            QueueAddListenerCodec.RequestParameters params = QueueAddListenerCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aBoolean, params.includeValue));
            assertTrue(isEqual(aBoolean, params.localOnly));
        }
        {
            ClientMessage clientMessage = QueueAddListenerCodec.encodeResponse(aString);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            {
                ClientMessage clientMessage = QueueAddListenerCodec.encodeItemEvent(null, aString, anInt);
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.read(bytes);
                assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
            }
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            QueueRemoveListenerCodec.RequestParameters params = QueueRemoveListenerCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.registrationId));
        }
        {
            ClientMessage clientMessage = QueueRemoveListenerCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            QueueRemainingCapacityCodec.RequestParameters params = QueueRemainingCapacityCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = QueueRemainingCapacityCodec.encodeResponse(anInt);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            QueueIsEmptyCodec.RequestParameters params = QueueIsEmptyCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = QueueIsEmptyCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TopicPublishCodec.RequestParameters params = TopicPublishCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.message));
        }
        {
            ClientMessage clientMessage = TopicPublishCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TopicAddMessageListenerCodec.RequestParameters params = TopicAddMessageListenerCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aBoolean, params.localOnly));
        }
        {
            ClientMessage clientMessage = TopicAddMessageListenerCodec.encodeResponse(aString);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            {
                ClientMessage clientMessage = TopicAddMessageListenerCodec.encodeTopicEvent(aData, aLong, aString);
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.read(bytes);
                assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
            }
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TopicRemoveMessageListenerCodec.RequestParameters params = TopicRemoveMessageListenerCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.registrationId));
        }
        {
            ClientMessage clientMessage = TopicRemoveMessageListenerCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ListSizeCodec.RequestParameters params = ListSizeCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = ListSizeCodec.encodeResponse(anInt);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ListContainsCodec.RequestParameters params = ListContainsCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.value));
        }
        {
            ClientMessage clientMessage = ListContainsCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ListContainsAllCodec.RequestParameters params = ListContainsAllCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.values));
        }
        {
            ClientMessage clientMessage = ListContainsAllCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ListAddCodec.RequestParameters params = ListAddCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.value));
        }
        {
            ClientMessage clientMessage = ListAddCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ListRemoveCodec.RequestParameters params = ListRemoveCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.value));
        }
        {
            ClientMessage clientMessage = ListRemoveCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ListAddAllCodec.RequestParameters params = ListAddAllCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.valueList));
        }
        {
            ClientMessage clientMessage = ListAddAllCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ListCompareAndRemoveAllCodec.RequestParameters params = ListCompareAndRemoveAllCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.values));
        }
        {
            ClientMessage clientMessage = ListCompareAndRemoveAllCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ListCompareAndRetainAllCodec.RequestParameters params = ListCompareAndRetainAllCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.values));
        }
        {
            ClientMessage clientMessage = ListCompareAndRetainAllCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ListClearCodec.RequestParameters params = ListClearCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = ListClearCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ListGetAllCodec.RequestParameters params = ListGetAllCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = ListGetAllCodec.encodeResponse(datas);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ListAddListenerCodec.RequestParameters params = ListAddListenerCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aBoolean, params.includeValue));
            assertTrue(isEqual(aBoolean, params.localOnly));
        }
        {
            ClientMessage clientMessage = ListAddListenerCodec.encodeResponse(aString);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            {
                ClientMessage clientMessage = ListAddListenerCodec.encodeItemEvent(null, aString, anInt);
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.read(bytes);
                assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
            }
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ListRemoveListenerCodec.RequestParameters params = ListRemoveListenerCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.registrationId));
        }
        {
            ClientMessage clientMessage = ListRemoveListenerCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ListIsEmptyCodec.RequestParameters params = ListIsEmptyCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = ListIsEmptyCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ListAddAllWithIndexCodec.RequestParameters params = ListAddAllWithIndexCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.index));
            assertTrue(isEqual(datas, params.valueList));
        }
        {
            ClientMessage clientMessage = ListAddAllWithIndexCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ListGetCodec.RequestParameters params = ListGetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.index));
        }
        {
            ClientMessage clientMessage = ListGetCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ListSetCodec.RequestParameters params = ListSetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.index));
            assertTrue(isEqual(aData, params.value));
        }
        {
            ClientMessage clientMessage = ListSetCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ListAddWithIndexCodec.RequestParameters params = ListAddWithIndexCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.index));
            assertTrue(isEqual(aData, params.value));
        }
        {
            ClientMessage clientMessage = ListAddWithIndexCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ListRemoveWithIndexCodec.RequestParameters params = ListRemoveWithIndexCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.index));
        }
        {
            ClientMessage clientMessage = ListRemoveWithIndexCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ListLastIndexOfCodec.RequestParameters params = ListLastIndexOfCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.value));
        }
        {
            ClientMessage clientMessage = ListLastIndexOfCodec.encodeResponse(anInt);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ListIndexOfCodec.RequestParameters params = ListIndexOfCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.value));
        }
        {
            ClientMessage clientMessage = ListIndexOfCodec.encodeResponse(anInt);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ListSubCodec.RequestParameters params = ListSubCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.from));
            assertTrue(isEqual(anInt, params.to));
        }
        {
            ClientMessage clientMessage = ListSubCodec.encodeResponse(datas);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ListIteratorCodec.RequestParameters params = ListIteratorCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = ListIteratorCodec.encodeResponse(datas);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ListListIteratorCodec.RequestParameters params = ListListIteratorCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.index));
        }
        {
            ClientMessage clientMessage = ListListIteratorCodec.encodeResponse(datas);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            SetSizeCodec.RequestParameters params = SetSizeCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = SetSizeCodec.encodeResponse(anInt);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            SetContainsCodec.RequestParameters params = SetContainsCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.value));
        }
        {
            ClientMessage clientMessage = SetContainsCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            SetContainsAllCodec.RequestParameters params = SetContainsAllCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.items));
        }
        {
            ClientMessage clientMessage = SetContainsAllCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            SetAddCodec.RequestParameters params = SetAddCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.value));
        }
        {
            ClientMessage clientMessage = SetAddCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            SetRemoveCodec.RequestParameters params = SetRemoveCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.value));
        }
        {
            ClientMessage clientMessage = SetRemoveCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            SetAddAllCodec.RequestParameters params = SetAddAllCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.valueList));
        }
        {
            ClientMessage clientMessage = SetAddAllCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            SetCompareAndRemoveAllCodec.RequestParameters params = SetCompareAndRemoveAllCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.values));
        }
        {
            ClientMessage clientMessage = SetCompareAndRemoveAllCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            SetCompareAndRetainAllCodec.RequestParameters params = SetCompareAndRetainAllCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.values));
        }
        {
            ClientMessage clientMessage = SetCompareAndRetainAllCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            SetClearCodec.RequestParameters params = SetClearCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = SetClearCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            SetGetAllCodec.RequestParameters params = SetGetAllCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = SetGetAllCodec.encodeResponse(datas);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            SetAddListenerCodec.RequestParameters params = SetAddListenerCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aBoolean, params.includeValue));
            assertTrue(isEqual(aBoolean, params.localOnly));
        }
        {
            ClientMessage clientMessage = SetAddListenerCodec.encodeResponse(aString);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            {
                ClientMessage clientMessage = SetAddListenerCodec.encodeItemEvent(null, aString, anInt);
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.read(bytes);
                assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
            }
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            SetRemoveListenerCodec.RequestParameters params = SetRemoveListenerCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.registrationId));
        }
        {
            ClientMessage clientMessage = SetRemoveListenerCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            SetIsEmptyCodec.RequestParameters params = SetIsEmptyCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = SetIsEmptyCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            LockIsLockedCodec.RequestParameters params = LockIsLockedCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = LockIsLockedCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            LockIsLockedByCurrentThreadCodec.RequestParameters params = LockIsLockedByCurrentThreadCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.threadId));
        }
        {
            ClientMessage clientMessage = LockIsLockedByCurrentThreadCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            LockGetLockCountCodec.RequestParameters params = LockGetLockCountCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = LockGetLockCountCodec.encodeResponse(anInt);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            LockGetRemainingLeaseTimeCodec.RequestParameters params = LockGetRemainingLeaseTimeCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = LockGetRemainingLeaseTimeCodec.encodeResponse(aLong);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            LockLockCodec.RequestParameters params = LockLockCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.leaseTime));
            assertTrue(isEqual(aLong, params.threadId));
            assertFalse(params.referenceIdExist);
        }
        {
            ClientMessage clientMessage = LockLockCodec.encodeResponse();
            int length = inputStream.readInt();
            // Since the test is generated for protocol version (1.1) which is earlier than latest change in the message
            // (version 1.2), only the bytes after frame length fields are compared
            int frameLength = clientMessage.getFrameLength();
            assertTrue(frameLength >= length);
            inputStream.skipBytes(FRAME_LEN_FIELD_SIZE);
            byte[] bytes = new byte[length - FRAME_LEN_FIELD_SIZE];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOfRange(clientMessage.buffer().byteArray(), FRAME_LEN_FIELD_SIZE, length), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            LockUnlockCodec.RequestParameters params = LockUnlockCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.threadId));
            assertFalse(params.referenceIdExist);
        }
        {
            ClientMessage clientMessage = LockUnlockCodec.encodeResponse();
            int length = inputStream.readInt();
            // Since the test is generated for protocol version (1.1) which is earlier than latest change in the message
            // (version 1.2), only the bytes after frame length fields are compared
            int frameLength = clientMessage.getFrameLength();
            assertTrue(frameLength >= length);
            inputStream.skipBytes(FRAME_LEN_FIELD_SIZE);
            byte[] bytes = new byte[length - FRAME_LEN_FIELD_SIZE];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOfRange(clientMessage.buffer().byteArray(), FRAME_LEN_FIELD_SIZE, length), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            LockForceUnlockCodec.RequestParameters params = LockForceUnlockCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertFalse(params.referenceIdExist);
        }
        {
            ClientMessage clientMessage = LockForceUnlockCodec.encodeResponse();
            int length = inputStream.readInt();
            // Since the test is generated for protocol version (1.1) which is earlier than latest change in the message
            // (version 1.2), only the bytes after frame length fields are compared
            int frameLength = clientMessage.getFrameLength();
            assertTrue(frameLength >= length);
            inputStream.skipBytes(FRAME_LEN_FIELD_SIZE);
            byte[] bytes = new byte[length - FRAME_LEN_FIELD_SIZE];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOfRange(clientMessage.buffer().byteArray(), FRAME_LEN_FIELD_SIZE, length), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            LockTryLockCodec.RequestParameters params = LockTryLockCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.lease));
            assertTrue(isEqual(aLong, params.timeout));
            assertFalse(params.referenceIdExist);
        }
        {
            ClientMessage clientMessage = LockTryLockCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            // Since the test is generated for protocol version (1.1) which is earlier than latest change in the message
            // (version 1.2), only the bytes after frame length fields are compared
            int frameLength = clientMessage.getFrameLength();
            assertTrue(frameLength >= length);
            inputStream.skipBytes(FRAME_LEN_FIELD_SIZE);
            byte[] bytes = new byte[length - FRAME_LEN_FIELD_SIZE];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOfRange(clientMessage.buffer().byteArray(), FRAME_LEN_FIELD_SIZE, length), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ConditionAwaitCodec.RequestParameters params = ConditionAwaitCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.timeout));
            assertTrue(isEqual(aString, params.lockName));
            assertFalse(params.referenceIdExist);
        }
        {
            ClientMessage clientMessage = ConditionAwaitCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            // Since the test is generated for protocol version (1.1) which is earlier than latest change in the message
            // (version 1.2), only the bytes after frame length fields are compared
            int frameLength = clientMessage.getFrameLength();
            assertTrue(frameLength >= length);
            inputStream.skipBytes(FRAME_LEN_FIELD_SIZE);
            byte[] bytes = new byte[length - FRAME_LEN_FIELD_SIZE];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOfRange(clientMessage.buffer().byteArray(), FRAME_LEN_FIELD_SIZE, length), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ConditionBeforeAwaitCodec.RequestParameters params = ConditionBeforeAwaitCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aString, params.lockName));
            assertFalse(params.referenceIdExist);
        }
        {
            ClientMessage clientMessage = ConditionBeforeAwaitCodec.encodeResponse();
            int length = inputStream.readInt();
            // Since the test is generated for protocol version (1.1) which is earlier than latest change in the message
            // (version 1.2), only the bytes after frame length fields are compared
            int frameLength = clientMessage.getFrameLength();
            assertTrue(frameLength >= length);
            inputStream.skipBytes(FRAME_LEN_FIELD_SIZE);
            byte[] bytes = new byte[length - FRAME_LEN_FIELD_SIZE];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOfRange(clientMessage.buffer().byteArray(), FRAME_LEN_FIELD_SIZE, length), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ConditionSignalCodec.RequestParameters params = ConditionSignalCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aString, params.lockName));
        }
        {
            ClientMessage clientMessage = ConditionSignalCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ConditionSignalAllCodec.RequestParameters params = ConditionSignalAllCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aString, params.lockName));
        }
        {
            ClientMessage clientMessage = ConditionSignalAllCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ExecutorServiceShutdownCodec.RequestParameters params = ExecutorServiceShutdownCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = ExecutorServiceShutdownCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ExecutorServiceIsShutdownCodec.RequestParameters params = ExecutorServiceIsShutdownCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = ExecutorServiceIsShutdownCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ExecutorServiceCancelOnPartitionCodec.RequestParameters params = ExecutorServiceCancelOnPartitionCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.uuid));
            assertTrue(isEqual(anInt, params.partitionId));
            assertTrue(isEqual(aBoolean, params.interrupt));
        }
        {
            ClientMessage clientMessage = ExecutorServiceCancelOnPartitionCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ExecutorServiceCancelOnAddressCodec.RequestParameters params = ExecutorServiceCancelOnAddressCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.uuid));
            assertTrue(isEqual(anAddress, params.address));
            assertTrue(isEqual(aBoolean, params.interrupt));
        }
        {
            ClientMessage clientMessage = ExecutorServiceCancelOnAddressCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ExecutorServiceSubmitToPartitionCodec.RequestParameters params = ExecutorServiceSubmitToPartitionCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.uuid));
            assertTrue(isEqual(aData, params.callable));
            assertTrue(isEqual(anInt, params.partitionId));
        }
        {
            ClientMessage clientMessage = ExecutorServiceSubmitToPartitionCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ExecutorServiceSubmitToAddressCodec.RequestParameters params = ExecutorServiceSubmitToAddressCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.uuid));
            assertTrue(isEqual(aData, params.callable));
            assertTrue(isEqual(anAddress, params.address));
        }
        {
            ClientMessage clientMessage = ExecutorServiceSubmitToAddressCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            AtomicLongApplyCodec.RequestParameters params = AtomicLongApplyCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.function));
        }
        {
            ClientMessage clientMessage = AtomicLongApplyCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            AtomicLongAlterCodec.RequestParameters params = AtomicLongAlterCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.function));
        }
        {
            ClientMessage clientMessage = AtomicLongAlterCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            AtomicLongAlterAndGetCodec.RequestParameters params = AtomicLongAlterAndGetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.function));
        }
        {
            ClientMessage clientMessage = AtomicLongAlterAndGetCodec.encodeResponse(aLong);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            AtomicLongGetAndAlterCodec.RequestParameters params = AtomicLongGetAndAlterCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.function));
        }
        {
            ClientMessage clientMessage = AtomicLongGetAndAlterCodec.encodeResponse(aLong);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            AtomicLongAddAndGetCodec.RequestParameters params = AtomicLongAddAndGetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.delta));
        }
        {
            ClientMessage clientMessage = AtomicLongAddAndGetCodec.encodeResponse(aLong);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            AtomicLongCompareAndSetCodec.RequestParameters params = AtomicLongCompareAndSetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.expected));
            assertTrue(isEqual(aLong, params.updated));
        }
        {
            ClientMessage clientMessage = AtomicLongCompareAndSetCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            AtomicLongDecrementAndGetCodec.RequestParameters params = AtomicLongDecrementAndGetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = AtomicLongDecrementAndGetCodec.encodeResponse(aLong);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            AtomicLongGetCodec.RequestParameters params = AtomicLongGetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = AtomicLongGetCodec.encodeResponse(aLong);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            AtomicLongGetAndAddCodec.RequestParameters params = AtomicLongGetAndAddCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.delta));
        }
        {
            ClientMessage clientMessage = AtomicLongGetAndAddCodec.encodeResponse(aLong);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            AtomicLongGetAndSetCodec.RequestParameters params = AtomicLongGetAndSetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.newValue));
        }
        {
            ClientMessage clientMessage = AtomicLongGetAndSetCodec.encodeResponse(aLong);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            AtomicLongIncrementAndGetCodec.RequestParameters params = AtomicLongIncrementAndGetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = AtomicLongIncrementAndGetCodec.encodeResponse(aLong);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            AtomicLongGetAndIncrementCodec.RequestParameters params = AtomicLongGetAndIncrementCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = AtomicLongGetAndIncrementCodec.encodeResponse(aLong);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            AtomicLongSetCodec.RequestParameters params = AtomicLongSetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.newValue));
        }
        {
            ClientMessage clientMessage = AtomicLongSetCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            AtomicReferenceApplyCodec.RequestParameters params = AtomicReferenceApplyCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.function));
        }
        {
            ClientMessage clientMessage = AtomicReferenceApplyCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            AtomicReferenceAlterCodec.RequestParameters params = AtomicReferenceAlterCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.function));
        }
        {
            ClientMessage clientMessage = AtomicReferenceAlterCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            AtomicReferenceAlterAndGetCodec.RequestParameters params = AtomicReferenceAlterAndGetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.function));
        }
        {
            ClientMessage clientMessage = AtomicReferenceAlterAndGetCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            AtomicReferenceGetAndAlterCodec.RequestParameters params = AtomicReferenceGetAndAlterCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.function));
        }
        {
            ClientMessage clientMessage = AtomicReferenceGetAndAlterCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            AtomicReferenceContainsCodec.RequestParameters params = AtomicReferenceContainsCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(null, params.expected));
        }
        {
            ClientMessage clientMessage = AtomicReferenceContainsCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            AtomicReferenceCompareAndSetCodec.RequestParameters params = AtomicReferenceCompareAndSetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(null, params.expected));
            assertTrue(isEqual(null, params.updated));
        }
        {
            ClientMessage clientMessage = AtomicReferenceCompareAndSetCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            AtomicReferenceGetCodec.RequestParameters params = AtomicReferenceGetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = AtomicReferenceGetCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            AtomicReferenceSetCodec.RequestParameters params = AtomicReferenceSetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(null, params.newValue));
        }
        {
            ClientMessage clientMessage = AtomicReferenceSetCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            AtomicReferenceClearCodec.RequestParameters params = AtomicReferenceClearCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = AtomicReferenceClearCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            AtomicReferenceGetAndSetCodec.RequestParameters params = AtomicReferenceGetAndSetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(null, params.newValue));
        }
        {
            ClientMessage clientMessage = AtomicReferenceGetAndSetCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            AtomicReferenceSetAndGetCodec.RequestParameters params = AtomicReferenceSetAndGetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(null, params.newValue));
        }
        {
            ClientMessage clientMessage = AtomicReferenceSetAndGetCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            AtomicReferenceIsNullCodec.RequestParameters params = AtomicReferenceIsNullCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = AtomicReferenceIsNullCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CountDownLatchAwaitCodec.RequestParameters params = CountDownLatchAwaitCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.timeout));
        }
        {
            ClientMessage clientMessage = CountDownLatchAwaitCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CountDownLatchCountDownCodec.RequestParameters params = CountDownLatchCountDownCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = CountDownLatchCountDownCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CountDownLatchGetCountCodec.RequestParameters params = CountDownLatchGetCountCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = CountDownLatchGetCountCodec.encodeResponse(anInt);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CountDownLatchTrySetCountCodec.RequestParameters params = CountDownLatchTrySetCountCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.count));
        }
        {
            ClientMessage clientMessage = CountDownLatchTrySetCountCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            SemaphoreInitCodec.RequestParameters params = SemaphoreInitCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.permits));
        }
        {
            ClientMessage clientMessage = SemaphoreInitCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            SemaphoreAcquireCodec.RequestParameters params = SemaphoreAcquireCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.permits));
        }
        {
            ClientMessage clientMessage = SemaphoreAcquireCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            SemaphoreAvailablePermitsCodec.RequestParameters params = SemaphoreAvailablePermitsCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = SemaphoreAvailablePermitsCodec.encodeResponse(anInt);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            SemaphoreDrainPermitsCodec.RequestParameters params = SemaphoreDrainPermitsCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = SemaphoreDrainPermitsCodec.encodeResponse(anInt);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            SemaphoreReducePermitsCodec.RequestParameters params = SemaphoreReducePermitsCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.reduction));
        }
        {
            ClientMessage clientMessage = SemaphoreReducePermitsCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            SemaphoreReleaseCodec.RequestParameters params = SemaphoreReleaseCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.permits));
        }
        {
            ClientMessage clientMessage = SemaphoreReleaseCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            SemaphoreTryAcquireCodec.RequestParameters params = SemaphoreTryAcquireCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.permits));
            assertTrue(isEqual(aLong, params.timeout));
        }
        {
            ClientMessage clientMessage = SemaphoreTryAcquireCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ReplicatedMapPutCodec.RequestParameters params = ReplicatedMapPutCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(aLong, params.ttl));
        }
        {
            ClientMessage clientMessage = ReplicatedMapPutCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ReplicatedMapSizeCodec.RequestParameters params = ReplicatedMapSizeCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = ReplicatedMapSizeCodec.encodeResponse(anInt);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ReplicatedMapIsEmptyCodec.RequestParameters params = ReplicatedMapIsEmptyCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = ReplicatedMapIsEmptyCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ReplicatedMapContainsKeyCodec.RequestParameters params = ReplicatedMapContainsKeyCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
        }
        {
            ClientMessage clientMessage = ReplicatedMapContainsKeyCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ReplicatedMapContainsValueCodec.RequestParameters params = ReplicatedMapContainsValueCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.value));
        }
        {
            ClientMessage clientMessage = ReplicatedMapContainsValueCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ReplicatedMapGetCodec.RequestParameters params = ReplicatedMapGetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
        }
        {
            ClientMessage clientMessage = ReplicatedMapGetCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ReplicatedMapRemoveCodec.RequestParameters params = ReplicatedMapRemoveCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
        }
        {
            ClientMessage clientMessage = ReplicatedMapRemoveCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ReplicatedMapPutAllCodec.RequestParameters params = ReplicatedMapPutAllCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aListOfEntry, params.entries));
        }
        {
            ClientMessage clientMessage = ReplicatedMapPutAllCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ReplicatedMapClearCodec.RequestParameters params = ReplicatedMapClearCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = ReplicatedMapClearCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.RequestParameters params = ReplicatedMapAddEntryListenerToKeyWithPredicateCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.predicate));
            assertTrue(isEqual(aBoolean, params.localOnly));
        }
        {
            ClientMessage clientMessage = ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.encodeResponse(aString);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            {
                ClientMessage clientMessage = ReplicatedMapAddEntryListenerToKeyWithPredicateCodec
                        .encodeEntryEvent(null, null, null, null, anInt, aString, anInt);
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.read(bytes);
                assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
            }
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ReplicatedMapAddEntryListenerWithPredicateCodec.RequestParameters params = ReplicatedMapAddEntryListenerWithPredicateCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.predicate));
            assertTrue(isEqual(aBoolean, params.localOnly));
        }
        {
            ClientMessage clientMessage = ReplicatedMapAddEntryListenerWithPredicateCodec.encodeResponse(aString);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            {
                ClientMessage clientMessage = ReplicatedMapAddEntryListenerWithPredicateCodec
                        .encodeEntryEvent(null, null, null, null, anInt, aString, anInt);
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.read(bytes);
                assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
            }
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ReplicatedMapAddEntryListenerToKeyCodec.RequestParameters params = ReplicatedMapAddEntryListenerToKeyCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aBoolean, params.localOnly));
        }
        {
            ClientMessage clientMessage = ReplicatedMapAddEntryListenerToKeyCodec.encodeResponse(aString);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            {
                ClientMessage clientMessage = ReplicatedMapAddEntryListenerToKeyCodec
                        .encodeEntryEvent(null, null, null, null, anInt, aString, anInt);
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.read(bytes);
                assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
            }
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ReplicatedMapAddEntryListenerCodec.RequestParameters params = ReplicatedMapAddEntryListenerCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aBoolean, params.localOnly));
        }
        {
            ClientMessage clientMessage = ReplicatedMapAddEntryListenerCodec.encodeResponse(aString);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            {
                ClientMessage clientMessage = ReplicatedMapAddEntryListenerCodec
                        .encodeEntryEvent(null, null, null, null, anInt, aString, anInt);
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.read(bytes);
                assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
            }
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ReplicatedMapRemoveEntryListenerCodec.RequestParameters params = ReplicatedMapRemoveEntryListenerCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.registrationId));
        }
        {
            ClientMessage clientMessage = ReplicatedMapRemoveEntryListenerCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ReplicatedMapKeySetCodec.RequestParameters params = ReplicatedMapKeySetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = ReplicatedMapKeySetCodec.encodeResponse(datas);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ReplicatedMapValuesCodec.RequestParameters params = ReplicatedMapValuesCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = ReplicatedMapValuesCodec.encodeResponse(datas);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ReplicatedMapEntrySetCodec.RequestParameters params = ReplicatedMapEntrySetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = ReplicatedMapEntrySetCodec.encodeResponse(aListOfEntry);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            ReplicatedMapAddNearCacheEntryListenerCodec.RequestParameters params = ReplicatedMapAddNearCacheEntryListenerCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aBoolean, params.includeValue));
            assertTrue(isEqual(aBoolean, params.localOnly));
        }
        {
            ClientMessage clientMessage = ReplicatedMapAddNearCacheEntryListenerCodec.encodeResponse(aString);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            {
                ClientMessage clientMessage = ReplicatedMapAddNearCacheEntryListenerCodec
                        .encodeEntryEvent(null, null, null, null, anInt, aString, anInt);
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.read(bytes);
                assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
            }
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapReduceCancelCodec.RequestParameters params = MapReduceCancelCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.jobId));
        }
        {
            ClientMessage clientMessage = MapReduceCancelCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapReduceJobProcessInformationCodec.RequestParameters params = MapReduceJobProcessInformationCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.jobId));
        }
        {
            ClientMessage clientMessage = MapReduceJobProcessInformationCodec.encodeResponse(jobPartitionStates, anInt);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapReduceForMapCodec.RequestParameters params = MapReduceForMapCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.jobId));
            assertTrue(isEqual(null, params.predicate));
            assertTrue(isEqual(aData, params.mapper));
            assertTrue(isEqual(null, params.combinerFactory));
            assertTrue(isEqual(null, params.reducerFactory));
            assertTrue(isEqual(aString, params.mapName));
            assertTrue(isEqual(anInt, params.chunkSize));
            assertTrue(isEqual(null, params.keys));
            assertTrue(isEqual(null, params.topologyChangedStrategy));
        }
        {
            ClientMessage clientMessage = MapReduceForMapCodec.encodeResponse(aListOfEntry);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapReduceForListCodec.RequestParameters params = MapReduceForListCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.jobId));
            assertTrue(isEqual(null, params.predicate));
            assertTrue(isEqual(aData, params.mapper));
            assertTrue(isEqual(null, params.combinerFactory));
            assertTrue(isEqual(null, params.reducerFactory));
            assertTrue(isEqual(aString, params.listName));
            assertTrue(isEqual(anInt, params.chunkSize));
            assertTrue(isEqual(null, params.keys));
            assertTrue(isEqual(null, params.topologyChangedStrategy));
        }
        {
            ClientMessage clientMessage = MapReduceForListCodec.encodeResponse(aListOfEntry);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapReduceForSetCodec.RequestParameters params = MapReduceForSetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.jobId));
            assertTrue(isEqual(null, params.predicate));
            assertTrue(isEqual(aData, params.mapper));
            assertTrue(isEqual(null, params.combinerFactory));
            assertTrue(isEqual(null, params.reducerFactory));
            assertTrue(isEqual(aString, params.setName));
            assertTrue(isEqual(anInt, params.chunkSize));
            assertTrue(isEqual(null, params.keys));
            assertTrue(isEqual(null, params.topologyChangedStrategy));
        }
        {
            ClientMessage clientMessage = MapReduceForSetCodec.encodeResponse(aListOfEntry);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapReduceForMultiMapCodec.RequestParameters params = MapReduceForMultiMapCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.jobId));
            assertTrue(isEqual(null, params.predicate));
            assertTrue(isEqual(aData, params.mapper));
            assertTrue(isEqual(null, params.combinerFactory));
            assertTrue(isEqual(null, params.reducerFactory));
            assertTrue(isEqual(aString, params.multiMapName));
            assertTrue(isEqual(anInt, params.chunkSize));
            assertTrue(isEqual(null, params.keys));
            assertTrue(isEqual(null, params.topologyChangedStrategy));
        }
        {
            ClientMessage clientMessage = MapReduceForMultiMapCodec.encodeResponse(aListOfEntry);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            MapReduceForCustomCodec.RequestParameters params = MapReduceForCustomCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.jobId));
            assertTrue(isEqual(null, params.predicate));
            assertTrue(isEqual(aData, params.mapper));
            assertTrue(isEqual(null, params.combinerFactory));
            assertTrue(isEqual(null, params.reducerFactory));
            assertTrue(isEqual(aData, params.keyValueSource));
            assertTrue(isEqual(anInt, params.chunkSize));
            assertTrue(isEqual(null, params.keys));
            assertTrue(isEqual(null, params.topologyChangedStrategy));
        }
        {
            ClientMessage clientMessage = MapReduceForCustomCodec.encodeResponse(aListOfEntry);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalMapContainsKeyCodec.RequestParameters params = TransactionalMapContainsKeyCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
        }
        {
            ClientMessage clientMessage = TransactionalMapContainsKeyCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalMapGetCodec.RequestParameters params = TransactionalMapGetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
        }
        {
            ClientMessage clientMessage = TransactionalMapGetCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalMapGetForUpdateCodec.RequestParameters params = TransactionalMapGetForUpdateCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
        }
        {
            ClientMessage clientMessage = TransactionalMapGetForUpdateCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalMapSizeCodec.RequestParameters params = TransactionalMapSizeCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
        }
        {
            ClientMessage clientMessage = TransactionalMapSizeCodec.encodeResponse(anInt);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalMapIsEmptyCodec.RequestParameters params = TransactionalMapIsEmptyCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
        }
        {
            ClientMessage clientMessage = TransactionalMapIsEmptyCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalMapPutCodec.RequestParameters params = TransactionalMapPutCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(aLong, params.ttl));
        }
        {
            ClientMessage clientMessage = TransactionalMapPutCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalMapSetCodec.RequestParameters params = TransactionalMapSetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
        }
        {
            ClientMessage clientMessage = TransactionalMapSetCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalMapPutIfAbsentCodec.RequestParameters params = TransactionalMapPutIfAbsentCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
        }
        {
            ClientMessage clientMessage = TransactionalMapPutIfAbsentCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalMapReplaceCodec.RequestParameters params = TransactionalMapReplaceCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
        }
        {
            ClientMessage clientMessage = TransactionalMapReplaceCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalMapReplaceIfSameCodec.RequestParameters params = TransactionalMapReplaceIfSameCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.oldValue));
            assertTrue(isEqual(aData, params.newValue));
        }
        {
            ClientMessage clientMessage = TransactionalMapReplaceIfSameCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalMapRemoveCodec.RequestParameters params = TransactionalMapRemoveCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
        }
        {
            ClientMessage clientMessage = TransactionalMapRemoveCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalMapDeleteCodec.RequestParameters params = TransactionalMapDeleteCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
        }
        {
            ClientMessage clientMessage = TransactionalMapDeleteCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalMapRemoveIfSameCodec.RequestParameters params = TransactionalMapRemoveIfSameCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
        }
        {
            ClientMessage clientMessage = TransactionalMapRemoveIfSameCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalMapKeySetCodec.RequestParameters params = TransactionalMapKeySetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
        }
        {
            ClientMessage clientMessage = TransactionalMapKeySetCodec.encodeResponse(datas);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalMapKeySetWithPredicateCodec.RequestParameters params = TransactionalMapKeySetWithPredicateCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.predicate));
        }
        {
            ClientMessage clientMessage = TransactionalMapKeySetWithPredicateCodec.encodeResponse(datas);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalMapValuesCodec.RequestParameters params = TransactionalMapValuesCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
        }
        {
            ClientMessage clientMessage = TransactionalMapValuesCodec.encodeResponse(datas);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalMapValuesWithPredicateCodec.RequestParameters params = TransactionalMapValuesWithPredicateCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.predicate));
        }
        {
            ClientMessage clientMessage = TransactionalMapValuesWithPredicateCodec.encodeResponse(datas);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalMultiMapPutCodec.RequestParameters params = TransactionalMultiMapPutCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
        }
        {
            ClientMessage clientMessage = TransactionalMultiMapPutCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalMultiMapGetCodec.RequestParameters params = TransactionalMultiMapGetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
        }
        {
            ClientMessage clientMessage = TransactionalMultiMapGetCodec.encodeResponse(datas);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalMultiMapRemoveCodec.RequestParameters params = TransactionalMultiMapRemoveCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
        }
        {
            ClientMessage clientMessage = TransactionalMultiMapRemoveCodec.encodeResponse(datas);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalMultiMapRemoveEntryCodec.RequestParameters params = TransactionalMultiMapRemoveEntryCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
        }
        {
            ClientMessage clientMessage = TransactionalMultiMapRemoveEntryCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalMultiMapValueCountCodec.RequestParameters params = TransactionalMultiMapValueCountCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
        }
        {
            ClientMessage clientMessage = TransactionalMultiMapValueCountCodec.encodeResponse(anInt);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalMultiMapSizeCodec.RequestParameters params = TransactionalMultiMapSizeCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
        }
        {
            ClientMessage clientMessage = TransactionalMultiMapSizeCodec.encodeResponse(anInt);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalSetAddCodec.RequestParameters params = TransactionalSetAddCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.item));
        }
        {
            ClientMessage clientMessage = TransactionalSetAddCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalSetRemoveCodec.RequestParameters params = TransactionalSetRemoveCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.item));
        }
        {
            ClientMessage clientMessage = TransactionalSetRemoveCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalSetSizeCodec.RequestParameters params = TransactionalSetSizeCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
        }
        {
            ClientMessage clientMessage = TransactionalSetSizeCodec.encodeResponse(anInt);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalListAddCodec.RequestParameters params = TransactionalListAddCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.item));
        }
        {
            ClientMessage clientMessage = TransactionalListAddCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalListRemoveCodec.RequestParameters params = TransactionalListRemoveCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.item));
        }
        {
            ClientMessage clientMessage = TransactionalListRemoveCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalListSizeCodec.RequestParameters params = TransactionalListSizeCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
        }
        {
            ClientMessage clientMessage = TransactionalListSizeCodec.encodeResponse(anInt);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalQueueOfferCodec.RequestParameters params = TransactionalQueueOfferCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.item));
            assertTrue(isEqual(aLong, params.timeout));
        }
        {
            ClientMessage clientMessage = TransactionalQueueOfferCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalQueueTakeCodec.RequestParameters params = TransactionalQueueTakeCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
        }
        {
            ClientMessage clientMessage = TransactionalQueueTakeCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalQueuePollCodec.RequestParameters params = TransactionalQueuePollCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.timeout));
        }
        {
            ClientMessage clientMessage = TransactionalQueuePollCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalQueuePeekCodec.RequestParameters params = TransactionalQueuePeekCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.timeout));
        }
        {
            ClientMessage clientMessage = TransactionalQueuePeekCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionalQueueSizeCodec.RequestParameters params = TransactionalQueueSizeCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
        }
        {
            ClientMessage clientMessage = TransactionalQueueSizeCodec.encodeResponse(anInt);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CacheAddEntryListenerCodec.RequestParameters params = CacheAddEntryListenerCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aBoolean, params.localOnly));
        }
        {
            ClientMessage clientMessage = CacheAddEntryListenerCodec.encodeResponse(aString);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            {
                ClientMessage clientMessage = CacheAddEntryListenerCodec.encodeCacheEvent(anInt, cacheEventDatas, anInt);
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.read(bytes);
                assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
            }
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CacheAddInvalidationListenerCodec.RequestParameters params = CacheAddInvalidationListenerCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aBoolean, params.localOnly));
        }
        {
            ClientMessage clientMessage = CacheAddInvalidationListenerCodec.encodeResponse(aString);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            {
                ClientMessage clientMessage = CacheAddInvalidationListenerCodec.encodeCacheInvalidationEvent(aString, null, null);
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.read(bytes);
                assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
            }
            {
                ClientMessage clientMessage = CacheAddInvalidationListenerCodec
                        .encodeCacheBatchInvalidationEvent(aString, datas, null);
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.read(bytes);
                assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
            }
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CacheClearCodec.RequestParameters params = CacheClearCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = CacheClearCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CacheRemoveAllKeysCodec.RequestParameters params = CacheRemoveAllKeysCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.keys));
            assertTrue(isEqual(anInt, params.completionId));
        }
        {
            ClientMessage clientMessage = CacheRemoveAllKeysCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CacheRemoveAllCodec.RequestParameters params = CacheRemoveAllCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.completionId));
        }
        {
            ClientMessage clientMessage = CacheRemoveAllCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CacheContainsKeyCodec.RequestParameters params = CacheContainsKeyCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
        }
        {
            ClientMessage clientMessage = CacheContainsKeyCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CacheCreateConfigCodec.RequestParameters params = CacheCreateConfigCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aData, params.cacheConfig));
            assertTrue(isEqual(aBoolean, params.createAlsoOnOthers));
        }
        {
            ClientMessage clientMessage = CacheCreateConfigCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CacheDestroyCodec.RequestParameters params = CacheDestroyCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = CacheDestroyCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CacheEntryProcessorCodec.RequestParameters params = CacheEntryProcessorCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.entryProcessor));
            assertTrue(isEqual(datas, params.arguments));
            assertTrue(isEqual(anInt, params.completionId));
        }
        {
            ClientMessage clientMessage = CacheEntryProcessorCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CacheGetAllCodec.RequestParameters params = CacheGetAllCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.keys));
            assertTrue(isEqual(null, params.expiryPolicy));
        }
        {
            ClientMessage clientMessage = CacheGetAllCodec.encodeResponse(aListOfEntry);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CacheGetAndRemoveCodec.RequestParameters params = CacheGetAndRemoveCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(anInt, params.completionId));
        }
        {
            ClientMessage clientMessage = CacheGetAndRemoveCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CacheGetAndReplaceCodec.RequestParameters params = CacheGetAndReplaceCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(null, params.expiryPolicy));
            assertTrue(isEqual(anInt, params.completionId));
        }
        {
            ClientMessage clientMessage = CacheGetAndReplaceCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CacheGetConfigCodec.RequestParameters params = CacheGetConfigCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.simpleName));
        }
        {
            ClientMessage clientMessage = CacheGetConfigCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CacheGetCodec.RequestParameters params = CacheGetCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(null, params.expiryPolicy));
        }
        {
            ClientMessage clientMessage = CacheGetCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CacheIterateCodec.RequestParameters params = CacheIterateCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.partitionId));
            assertTrue(isEqual(anInt, params.tableIndex));
            assertTrue(isEqual(anInt, params.batch));
        }
        {
            ClientMessage clientMessage = CacheIterateCodec.encodeResponse(anInt, datas);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CacheListenerRegistrationCodec.RequestParameters params = CacheListenerRegistrationCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.listenerConfig));
            assertTrue(isEqual(aBoolean, params.shouldRegister));
            assertTrue(isEqual(anAddress, params.address));
        }
        {
            ClientMessage clientMessage = CacheListenerRegistrationCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CacheLoadAllCodec.RequestParameters params = CacheLoadAllCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.keys));
            assertTrue(isEqual(aBoolean, params.replaceExistingValues));
        }
        {
            ClientMessage clientMessage = CacheLoadAllCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CacheManagementConfigCodec.RequestParameters params = CacheManagementConfigCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aBoolean, params.isStat));
            assertTrue(isEqual(aBoolean, params.enabled));
            assertTrue(isEqual(anAddress, params.address));
        }
        {
            ClientMessage clientMessage = CacheManagementConfigCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CachePutIfAbsentCodec.RequestParameters params = CachePutIfAbsentCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(null, params.expiryPolicy));
            assertTrue(isEqual(anInt, params.completionId));
        }
        {
            ClientMessage clientMessage = CachePutIfAbsentCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CachePutCodec.RequestParameters params = CachePutCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(null, params.expiryPolicy));
            assertTrue(isEqual(aBoolean, params.get));
            assertTrue(isEqual(anInt, params.completionId));
        }
        {
            ClientMessage clientMessage = CachePutCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CacheRemoveEntryListenerCodec.RequestParameters params = CacheRemoveEntryListenerCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.registrationId));
        }
        {
            ClientMessage clientMessage = CacheRemoveEntryListenerCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CacheRemoveInvalidationListenerCodec.RequestParameters params = CacheRemoveInvalidationListenerCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.registrationId));
        }
        {
            ClientMessage clientMessage = CacheRemoveInvalidationListenerCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CacheRemoveCodec.RequestParameters params = CacheRemoveCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(null, params.currentValue));
            assertTrue(isEqual(anInt, params.completionId));
        }
        {
            ClientMessage clientMessage = CacheRemoveCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CacheReplaceCodec.RequestParameters params = CacheReplaceCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(null, params.oldValue));
            assertTrue(isEqual(aData, params.newValue));
            assertTrue(isEqual(null, params.expiryPolicy));
            assertTrue(isEqual(anInt, params.completionId));
        }
        {
            ClientMessage clientMessage = CacheReplaceCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CacheSizeCodec.RequestParameters params = CacheSizeCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = CacheSizeCodec.encodeResponse(anInt);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CacheAddPartitionLostListenerCodec.RequestParameters params = CacheAddPartitionLostListenerCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aBoolean, params.localOnly));
        }
        {
            ClientMessage clientMessage = CacheAddPartitionLostListenerCodec.encodeResponse(aString);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            {
                ClientMessage clientMessage = CacheAddPartitionLostListenerCodec.encodeCachePartitionLostEvent(anInt, aString);
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.read(bytes);
                assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
            }
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CacheRemovePartitionLostListenerCodec.RequestParameters params = CacheRemovePartitionLostListenerCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.registrationId));
        }
        {
            ClientMessage clientMessage = CacheRemovePartitionLostListenerCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CachePutAllCodec.RequestParameters params = CachePutAllCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aListOfEntry, params.entries));
            assertTrue(isEqual(null, params.expiryPolicy));
            assertTrue(isEqual(anInt, params.completionId));
        }
        {
            ClientMessage clientMessage = CachePutAllCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            CacheIterateEntriesCodec.RequestParameters params = CacheIterateEntriesCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.partitionId));
            assertTrue(isEqual(anInt, params.tableIndex));
            assertTrue(isEqual(anInt, params.batch));
        }
        {
            ClientMessage clientMessage = CacheIterateEntriesCodec.encodeResponse(anInt, aListOfEntry);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            XATransactionClearRemoteCodec.RequestParameters params = XATransactionClearRemoteCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(anXid, params.xid));
        }
        {
            ClientMessage clientMessage = XATransactionClearRemoteCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            XATransactionCollectTransactionsCodec.RequestParameters params = XATransactionCollectTransactionsCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
        }
        {
            ClientMessage clientMessage = XATransactionCollectTransactionsCodec.encodeResponse(datas);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            XATransactionFinalizeCodec.RequestParameters params = XATransactionFinalizeCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(anXid, params.xid));
            assertTrue(isEqual(aBoolean, params.isCommit));
        }
        {
            ClientMessage clientMessage = XATransactionFinalizeCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            XATransactionCommitCodec.RequestParameters params = XATransactionCommitCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.transactionId));
            assertTrue(isEqual(aBoolean, params.onePhase));
        }
        {
            ClientMessage clientMessage = XATransactionCommitCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            XATransactionCreateCodec.RequestParameters params = XATransactionCreateCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(anXid, params.xid));
            assertTrue(isEqual(aLong, params.timeout));
        }
        {
            ClientMessage clientMessage = XATransactionCreateCodec.encodeResponse(aString);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            XATransactionPrepareCodec.RequestParameters params = XATransactionPrepareCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.transactionId));
        }
        {
            ClientMessage clientMessage = XATransactionPrepareCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            XATransactionRollbackCodec.RequestParameters params = XATransactionRollbackCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.transactionId));
        }
        {
            ClientMessage clientMessage = XATransactionRollbackCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionCommitCodec.RequestParameters params = TransactionCommitCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.transactionId));
            assertTrue(isEqual(aLong, params.threadId));
        }
        {
            ClientMessage clientMessage = TransactionCommitCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionCreateCodec.RequestParameters params = TransactionCreateCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aLong, params.timeout));
            assertTrue(isEqual(anInt, params.durability));
            assertTrue(isEqual(anInt, params.transactionType));
            assertTrue(isEqual(aLong, params.threadId));
        }
        {
            ClientMessage clientMessage = TransactionCreateCodec.encodeResponse(aString);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            TransactionRollbackCodec.RequestParameters params = TransactionRollbackCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.transactionId));
            assertTrue(isEqual(aLong, params.threadId));
        }
        {
            ClientMessage clientMessage = TransactionRollbackCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            EnterpriseMapPublisherCreateWithValueCodec.RequestParameters params = EnterpriseMapPublisherCreateWithValueCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.mapName));
            assertTrue(isEqual(aString, params.cacheName));
            assertTrue(isEqual(aData, params.predicate));
            assertTrue(isEqual(anInt, params.batchSize));
            assertTrue(isEqual(anInt, params.bufferSize));
            assertTrue(isEqual(aLong, params.delaySeconds));
            assertTrue(isEqual(aBoolean, params.populate));
            assertTrue(isEqual(aBoolean, params.coalesce));
        }
        {
            ClientMessage clientMessage = EnterpriseMapPublisherCreateWithValueCodec.encodeResponse(aListOfEntry);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            EnterpriseMapPublisherCreateCodec.RequestParameters params = EnterpriseMapPublisherCreateCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.mapName));
            assertTrue(isEqual(aString, params.cacheName));
            assertTrue(isEqual(aData, params.predicate));
            assertTrue(isEqual(anInt, params.batchSize));
            assertTrue(isEqual(anInt, params.bufferSize));
            assertTrue(isEqual(aLong, params.delaySeconds));
            assertTrue(isEqual(aBoolean, params.populate));
            assertTrue(isEqual(aBoolean, params.coalesce));
        }
        {
            ClientMessage clientMessage = EnterpriseMapPublisherCreateCodec.encodeResponse(datas);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            EnterpriseMapMadePublishableCodec.RequestParameters params = EnterpriseMapMadePublishableCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.mapName));
            assertTrue(isEqual(aString, params.cacheName));
        }
        {
            ClientMessage clientMessage = EnterpriseMapMadePublishableCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            EnterpriseMapAddListenerCodec.RequestParameters params = EnterpriseMapAddListenerCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.listenerName));
            assertTrue(isEqual(aBoolean, params.localOnly));
        }
        {
            ClientMessage clientMessage = EnterpriseMapAddListenerCodec.encodeResponse(aString);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            {
                ClientMessage clientMessage = EnterpriseMapAddListenerCodec.encodeQueryCacheSingleEvent(aQueryCacheEventData);
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.read(bytes);
                assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
            }
            {
                ClientMessage clientMessage = EnterpriseMapAddListenerCodec
                        .encodeQueryCacheBatchEvent(queryCacheEventDatas, aString, anInt);
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.read(bytes);
                assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
            }
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            EnterpriseMapSetReadCursorCodec.RequestParameters params = EnterpriseMapSetReadCursorCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.mapName));
            assertTrue(isEqual(aString, params.cacheName));
            assertTrue(isEqual(aLong, params.sequence));
        }
        {
            ClientMessage clientMessage = EnterpriseMapSetReadCursorCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            EnterpriseMapDestroyCacheCodec.RequestParameters params = EnterpriseMapDestroyCacheCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.mapName));
            assertTrue(isEqual(aString, params.cacheName));
        }
        {
            ClientMessage clientMessage = EnterpriseMapDestroyCacheCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            RingbufferSizeCodec.RequestParameters params = RingbufferSizeCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = RingbufferSizeCodec.encodeResponse(aLong);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            RingbufferTailSequenceCodec.RequestParameters params = RingbufferTailSequenceCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = RingbufferTailSequenceCodec.encodeResponse(aLong);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            RingbufferHeadSequenceCodec.RequestParameters params = RingbufferHeadSequenceCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = RingbufferHeadSequenceCodec.encodeResponse(aLong);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            RingbufferCapacityCodec.RequestParameters params = RingbufferCapacityCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = RingbufferCapacityCodec.encodeResponse(aLong);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            RingbufferRemainingCapacityCodec.RequestParameters params = RingbufferRemainingCapacityCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = RingbufferRemainingCapacityCodec.encodeResponse(aLong);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            RingbufferAddCodec.RequestParameters params = RingbufferAddCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.overflowPolicy));
            assertTrue(isEqual(aData, params.value));
        }
        {
            ClientMessage clientMessage = RingbufferAddCodec.encodeResponse(aLong);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            RingbufferReadOneCodec.RequestParameters params = RingbufferReadOneCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.sequence));
        }
        {
            ClientMessage clientMessage = RingbufferReadOneCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            RingbufferAddAllCodec.RequestParameters params = RingbufferAddAllCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.valueList));
            assertTrue(isEqual(anInt, params.overflowPolicy));
        }
        {
            ClientMessage clientMessage = RingbufferAddAllCodec.encodeResponse(aLong);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            RingbufferReadManyCodec.RequestParameters params = RingbufferReadManyCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.startSequence));
            assertTrue(isEqual(anInt, params.minCount));
            assertTrue(isEqual(anInt, params.maxCount));
            assertTrue(isEqual(null, params.filter));
        }
        {
            ClientMessage clientMessage = RingbufferReadManyCodec.encodeResponse(anInt, datas);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            DurableExecutorShutdownCodec.RequestParameters params = DurableExecutorShutdownCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = DurableExecutorShutdownCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            DurableExecutorIsShutdownCodec.RequestParameters params = DurableExecutorIsShutdownCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
        }
        {
            ClientMessage clientMessage = DurableExecutorIsShutdownCodec.encodeResponse(aBoolean);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            DurableExecutorSubmitToPartitionCodec.RequestParameters params = DurableExecutorSubmitToPartitionCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.callable));
        }
        {
            ClientMessage clientMessage = DurableExecutorSubmitToPartitionCodec.encodeResponse(anInt);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            DurableExecutorRetrieveResultCodec.RequestParameters params = DurableExecutorRetrieveResultCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.sequence));
        }
        {
            ClientMessage clientMessage = DurableExecutorRetrieveResultCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            DurableExecutorDisposeResultCodec.RequestParameters params = DurableExecutorDisposeResultCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.sequence));
        }
        {
            ClientMessage clientMessage = DurableExecutorDisposeResultCodec.encodeResponse();
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        {
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            DurableExecutorRetrieveAndDisposeResultCodec.RequestParameters params = DurableExecutorRetrieveAndDisposeResultCodec
                    .decodeRequest(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.sequence));
        }
        {
            ClientMessage clientMessage = DurableExecutorRetrieveAndDisposeResultCodec.encodeResponse(null);
            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
            assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));
        }
        inputStream.close();
        input.close();
    }
}

