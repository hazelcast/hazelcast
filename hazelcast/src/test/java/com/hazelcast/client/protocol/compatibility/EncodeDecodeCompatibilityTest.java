/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.MemberImpl;
import com.hazelcast.client.impl.client.DistributedObjectInfo;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.*;
import com.hazelcast.core.Member;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.map.impl.querycache.event.DefaultQueryCacheEventData;
import com.hazelcast.map.impl.querycache.event.QueryCacheEventData;
import com.hazelcast.mapreduce.JobPartitionState;
import com.hazelcast.mapreduce.impl.task.JobPartitionStateImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.transaction.impl.xa.SerializableXID;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import java.lang.reflect.Array;
import java.net.UnknownHostException;
import javax.transaction.xa.Xid;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.*;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EncodeDecodeCompatibilityTest {

    @org.junit.Test
            public void test() {
{
    ClientMessage clientMessage = ClientAuthenticationCodec.encodeRequest(    aString ,    aString ,    aString ,    aString ,    aBoolean ,    aString ,    aByte ,    aString   );
    ClientAuthenticationCodec.RequestParameters params = ClientAuthenticationCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.username));
            assertTrue(isEqual(aString, params.password));
            assertTrue(isEqual(aString, params.uuid));
            assertTrue(isEqual(aString, params.ownerUuid));
            assertTrue(isEqual(aBoolean, params.isOwnerConnection));
            assertTrue(isEqual(aString, params.clientType));
            assertTrue(isEqual(aByte, params.serializationVersion));
            assertTrue(isEqual(aString, params.clientHazelcastVersion));
}
{
    ClientMessage clientMessage = ClientAuthenticationCodec.encodeResponse(    aByte ,    anAddress ,    aString ,    aString ,    aByte ,    aString ,    members   );
    ClientAuthenticationCodec.ResponseParameters params = ClientAuthenticationCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aByte, params.status));
            assertTrue(isEqual(anAddress, params.address));
            assertTrue(isEqual(aString, params.uuid));
            assertTrue(isEqual(aString, params.ownerUuid));
            assertTrue(isEqual(aByte, params.serializationVersion));
            assertTrue(isEqual(aString, params.serverHazelcastVersion));
            assertTrue(isEqual(members, params.clientUnregisteredMembers));
}
{
    ClientMessage clientMessage = ClientAuthenticationCustomCodec.encodeRequest(    aData ,    aString ,    aString ,    aBoolean ,    aString ,    aByte ,    aString   );
    ClientAuthenticationCustomCodec.RequestParameters params = ClientAuthenticationCustomCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.credentials));
            assertTrue(isEqual(aString, params.uuid));
            assertTrue(isEqual(aString, params.ownerUuid));
            assertTrue(isEqual(aBoolean, params.isOwnerConnection));
            assertTrue(isEqual(aString, params.clientType));
            assertTrue(isEqual(aByte, params.serializationVersion));
            assertTrue(isEqual(aString, params.clientHazelcastVersion));
}
{
    ClientMessage clientMessage = ClientAuthenticationCustomCodec.encodeResponse(    aByte ,    anAddress ,    aString ,    aString ,    aByte ,    aString ,    members   );
    ClientAuthenticationCustomCodec.ResponseParameters params = ClientAuthenticationCustomCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aByte, params.status));
            assertTrue(isEqual(anAddress, params.address));
            assertTrue(isEqual(aString, params.uuid));
            assertTrue(isEqual(aString, params.ownerUuid));
            assertTrue(isEqual(aByte, params.serializationVersion));
            assertTrue(isEqual(aString, params.serverHazelcastVersion));
            assertTrue(isEqual(members, params.clientUnregisteredMembers));
}
{
    ClientMessage clientMessage = ClientAddMembershipListenerCodec.encodeRequest(    aBoolean   );
    ClientAddMembershipListenerCodec.RequestParameters params = ClientAddMembershipListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.localOnly));
}
{
    ClientMessage clientMessage = ClientAddMembershipListenerCodec.encodeResponse(    aString   );
    ClientAddMembershipListenerCodec.ResponseParameters params = ClientAddMembershipListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.response));
}
{
    class ClientAddMembershipListenerCodecHandler extends ClientAddMembershipListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  Member
 member ,   int
 eventType   ) {
                          assertTrue(isEqual(aMember, member));
                          assertTrue(isEqual(anInt, eventType));
        }
        @Override
        public void handle(  Collection<Member> members   ) {
                          assertTrue(isEqual(members, members));
        }
        @Override
        public void handle(  String
 uuid ,   String
 key ,   int
 operationType ,   String
 value   ) {
                          assertTrue(isEqual(aString, uuid));
                          assertTrue(isEqual(aString, key));
                          assertTrue(isEqual(anInt, operationType));
                          assertTrue(isEqual(aString, value));
        }
    }
    ClientAddMembershipListenerCodecHandler handler = new ClientAddMembershipListenerCodecHandler();
    {
        ClientMessage clientMessage = ClientAddMembershipListenerCodec.encodeMemberEvent( aMember ,  anInt   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
    {
        ClientMessage clientMessage = ClientAddMembershipListenerCodec.encodeMemberListEvent( members   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
    {
        ClientMessage clientMessage = ClientAddMembershipListenerCodec.encodeMemberAttributeChangeEvent( aString ,  aString ,  anInt ,  aString   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
}
{
    ClientMessage clientMessage = ClientCreateProxyCodec.encodeRequest(    aString ,    aString ,    anAddress   );
    ClientCreateProxyCodec.RequestParameters params = ClientCreateProxyCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.serviceName));
            assertTrue(isEqual(anAddress, params.target));
}
{
    ClientMessage clientMessage = ClientCreateProxyCodec.encodeResponse( );
    ClientCreateProxyCodec.ResponseParameters params = ClientCreateProxyCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = ClientDestroyProxyCodec.encodeRequest(    aString ,    aString   );
    ClientDestroyProxyCodec.RequestParameters params = ClientDestroyProxyCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.serviceName));
}
{
    ClientMessage clientMessage = ClientDestroyProxyCodec.encodeResponse( );
    ClientDestroyProxyCodec.ResponseParameters params = ClientDestroyProxyCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = ClientGetPartitionsCodec.encodeRequest( );
    ClientGetPartitionsCodec.RequestParameters params = ClientGetPartitionsCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = ClientGetPartitionsCodec.encodeResponse(    aPartitionTable ,    anInt   );
    ClientGetPartitionsCodec.ResponseParameters params = ClientGetPartitionsCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aPartitionTable, params.partitions));
            assertTrue(isEqual(anInt, params.partitionStateVersion));
}
{
    ClientMessage clientMessage = ClientRemoveAllListenersCodec.encodeRequest( );
    ClientRemoveAllListenersCodec.RequestParameters params = ClientRemoveAllListenersCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = ClientRemoveAllListenersCodec.encodeResponse( );
    ClientRemoveAllListenersCodec.ResponseParameters params = ClientRemoveAllListenersCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = ClientAddPartitionLostListenerCodec.encodeRequest(    aBoolean   );
    ClientAddPartitionLostListenerCodec.RequestParameters params = ClientAddPartitionLostListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.localOnly));
}
{
    ClientMessage clientMessage = ClientAddPartitionLostListenerCodec.encodeResponse(    aString   );
    ClientAddPartitionLostListenerCodec.ResponseParameters params = ClientAddPartitionLostListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.response));
}
{
    class ClientAddPartitionLostListenerCodecHandler extends ClientAddPartitionLostListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  int
 partitionId ,   int
 lostBackupCount ,   Address
 source   ) {
                          assertTrue(isEqual(anInt, partitionId));
                          assertTrue(isEqual(anInt, lostBackupCount));
                          assertTrue(isEqual(anAddress, source));
        }
    }
    ClientAddPartitionLostListenerCodecHandler handler = new ClientAddPartitionLostListenerCodecHandler();
    {
        ClientMessage clientMessage = ClientAddPartitionLostListenerCodec.encodePartitionLostEvent( anInt ,  anInt ,  anAddress   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
}
{
    ClientMessage clientMessage = ClientRemovePartitionLostListenerCodec.encodeRequest(    aString   );
    ClientRemovePartitionLostListenerCodec.RequestParameters params = ClientRemovePartitionLostListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.registrationId));
}
{
    ClientMessage clientMessage = ClientRemovePartitionLostListenerCodec.encodeResponse(    aBoolean   );
    ClientRemovePartitionLostListenerCodec.ResponseParameters params = ClientRemovePartitionLostListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = ClientGetDistributedObjectsCodec.encodeRequest( );
    ClientGetDistributedObjectsCodec.RequestParameters params = ClientGetDistributedObjectsCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = ClientGetDistributedObjectsCodec.encodeResponse(    distributedObjectInfos   );
    ClientGetDistributedObjectsCodec.ResponseParameters params = ClientGetDistributedObjectsCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(distributedObjectInfos, params.response));
}
{
    ClientMessage clientMessage = ClientAddDistributedObjectListenerCodec.encodeRequest(    aBoolean   );
    ClientAddDistributedObjectListenerCodec.RequestParameters params = ClientAddDistributedObjectListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.localOnly));
}
{
    ClientMessage clientMessage = ClientAddDistributedObjectListenerCodec.encodeResponse(    aString   );
    ClientAddDistributedObjectListenerCodec.ResponseParameters params = ClientAddDistributedObjectListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.response));
}
{
    class ClientAddDistributedObjectListenerCodecHandler extends ClientAddDistributedObjectListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  String
 name ,   String
 serviceName ,   String
 eventType   ) {
                          assertTrue(isEqual(aString, name));
                          assertTrue(isEqual(aString, serviceName));
                          assertTrue(isEqual(aString, eventType));
        }
    }
    ClientAddDistributedObjectListenerCodecHandler handler = new ClientAddDistributedObjectListenerCodecHandler();
    {
        ClientMessage clientMessage = ClientAddDistributedObjectListenerCodec.encodeDistributedObjectEvent( aString ,  aString ,  aString   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
}
{
    ClientMessage clientMessage = ClientRemoveDistributedObjectListenerCodec.encodeRequest(    aString   );
    ClientRemoveDistributedObjectListenerCodec.RequestParameters params = ClientRemoveDistributedObjectListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.registrationId));
}
{
    ClientMessage clientMessage = ClientRemoveDistributedObjectListenerCodec.encodeResponse(    aBoolean   );
    ClientRemoveDistributedObjectListenerCodec.ResponseParameters params = ClientRemoveDistributedObjectListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = ClientPingCodec.encodeRequest( );
    ClientPingCodec.RequestParameters params = ClientPingCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = ClientPingCodec.encodeResponse( );
    ClientPingCodec.ResponseParameters params = ClientPingCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = ClientStatisticsCodec.encodeRequest(    aString   );
    ClientStatisticsCodec.RequestParameters params = ClientStatisticsCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.stats));
}
{
    ClientMessage clientMessage = ClientStatisticsCodec.encodeResponse( );
    ClientStatisticsCodec.ResponseParameters params = ClientStatisticsCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = ClientDeployClassesCodec.encodeRequest(    aListOfStringToByteArrEntry   );
    ClientDeployClassesCodec.RequestParameters params = ClientDeployClassesCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aListOfStringToByteArrEntry, params.classDefinitions));
}
{
    ClientMessage clientMessage = ClientDeployClassesCodec.encodeResponse( );
    ClientDeployClassesCodec.ResponseParameters params = ClientDeployClassesCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = ClientAddPartitionListenerCodec.encodeRequest( );
    ClientAddPartitionListenerCodec.RequestParameters params = ClientAddPartitionListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = ClientAddPartitionListenerCodec.encodeResponse( );
    ClientAddPartitionListenerCodec.ResponseParameters params = ClientAddPartitionListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    class ClientAddPartitionListenerCodecHandler extends ClientAddPartitionListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  Collection<Map.Entry<Address,List<Integer>>> partitions ,   int
 partitionStateVersion   ) {
                          assertTrue(isEqual(aPartitionTable, partitions));
                          assertTrue(isEqual(anInt, partitionStateVersion));
        }
    }
    ClientAddPartitionListenerCodecHandler handler = new ClientAddPartitionListenerCodecHandler();
    {
        ClientMessage clientMessage = ClientAddPartitionListenerCodec.encodePartitionsEvent( aPartitionTable ,  anInt   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
}
{
    ClientMessage clientMessage = ClientCreateProxiesCodec.encodeRequest(    aListOfStringToString   );
    ClientCreateProxiesCodec.RequestParameters params = ClientCreateProxiesCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aListOfStringToString, params.proxies));
}
{
    ClientMessage clientMessage = ClientCreateProxiesCodec.encodeResponse( );
    ClientCreateProxiesCodec.ResponseParameters params = ClientCreateProxiesCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = MapPutCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong ,    aLong   );
    MapPutCodec.RequestParameters params = MapPutCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.ttl));
}
{
    ClientMessage clientMessage = MapPutCodec.encodeResponse(    aData   );
    MapPutCodec.ResponseParameters params = MapPutCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = MapGetCodec.encodeRequest(    aString ,    aData ,    aLong   );
    MapGetCodec.RequestParameters params = MapGetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = MapGetCodec.encodeResponse(    aData   );
    MapGetCodec.ResponseParameters params = MapGetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = MapRemoveCodec.encodeRequest(    aString ,    aData ,    aLong   );
    MapRemoveCodec.RequestParameters params = MapRemoveCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = MapRemoveCodec.encodeResponse(    aData   );
    MapRemoveCodec.ResponseParameters params = MapRemoveCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = MapReplaceCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
    MapReplaceCodec.RequestParameters params = MapReplaceCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = MapReplaceCodec.encodeResponse(    aData   );
    MapReplaceCodec.ResponseParameters params = MapReplaceCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = MapReplaceIfSameCodec.encodeRequest(    aString ,    aData ,    aData ,    aData ,    aLong   );
    MapReplaceIfSameCodec.RequestParameters params = MapReplaceIfSameCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.testValue));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = MapReplaceIfSameCodec.encodeResponse(    aBoolean   );
    MapReplaceIfSameCodec.ResponseParameters params = MapReplaceIfSameCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = MapContainsKeyCodec.encodeRequest(    aString ,    aData ,    aLong   );
    MapContainsKeyCodec.RequestParameters params = MapContainsKeyCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = MapContainsKeyCodec.encodeResponse(    aBoolean   );
    MapContainsKeyCodec.ResponseParameters params = MapContainsKeyCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = MapContainsValueCodec.encodeRequest(    aString ,    aData   );
    MapContainsValueCodec.RequestParameters params = MapContainsValueCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.value));
}
{
    ClientMessage clientMessage = MapContainsValueCodec.encodeResponse(    aBoolean   );
    MapContainsValueCodec.ResponseParameters params = MapContainsValueCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = MapRemoveIfSameCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
    MapRemoveIfSameCodec.RequestParameters params = MapRemoveIfSameCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = MapRemoveIfSameCodec.encodeResponse(    aBoolean   );
    MapRemoveIfSameCodec.ResponseParameters params = MapRemoveIfSameCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = MapDeleteCodec.encodeRequest(    aString ,    aData ,    aLong   );
    MapDeleteCodec.RequestParameters params = MapDeleteCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = MapDeleteCodec.encodeResponse( );
    MapDeleteCodec.ResponseParameters params = MapDeleteCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = MapFlushCodec.encodeRequest(    aString   );
    MapFlushCodec.RequestParameters params = MapFlushCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = MapFlushCodec.encodeResponse( );
    MapFlushCodec.ResponseParameters params = MapFlushCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = MapTryRemoveCodec.encodeRequest(    aString ,    aData ,    aLong ,    aLong   );
    MapTryRemoveCodec.RequestParameters params = MapTryRemoveCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.timeout));
}
{
    ClientMessage clientMessage = MapTryRemoveCodec.encodeResponse(    aBoolean   );
    MapTryRemoveCodec.ResponseParameters params = MapTryRemoveCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = MapTryPutCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong ,    aLong   );
    MapTryPutCodec.RequestParameters params = MapTryPutCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.timeout));
}
{
    ClientMessage clientMessage = MapTryPutCodec.encodeResponse(    aBoolean   );
    MapTryPutCodec.ResponseParameters params = MapTryPutCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = MapPutTransientCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong ,    aLong   );
    MapPutTransientCodec.RequestParameters params = MapPutTransientCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.ttl));
}
{
    ClientMessage clientMessage = MapPutTransientCodec.encodeResponse( );
    MapPutTransientCodec.ResponseParameters params = MapPutTransientCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = MapPutIfAbsentCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong ,    aLong   );
    MapPutIfAbsentCodec.RequestParameters params = MapPutIfAbsentCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.ttl));
}
{
    ClientMessage clientMessage = MapPutIfAbsentCodec.encodeResponse(    aData   );
    MapPutIfAbsentCodec.ResponseParameters params = MapPutIfAbsentCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = MapSetCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong ,    aLong   );
    MapSetCodec.RequestParameters params = MapSetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.ttl));
}
{
    ClientMessage clientMessage = MapSetCodec.encodeResponse( );
    MapSetCodec.ResponseParameters params = MapSetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = MapLockCodec.encodeRequest(    aString ,    aData ,    aLong ,    aLong ,    aLong   );
    MapLockCodec.RequestParameters params = MapLockCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.ttl));
            assertTrue(isEqual(aLong, params.referenceId));
}
{
    ClientMessage clientMessage = MapLockCodec.encodeResponse( );
    MapLockCodec.ResponseParameters params = MapLockCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = MapTryLockCodec.encodeRequest(    aString ,    aData ,    aLong ,    aLong ,    aLong ,    aLong   );
    MapTryLockCodec.RequestParameters params = MapTryLockCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.lease));
            assertTrue(isEqual(aLong, params.timeout));
            assertTrue(isEqual(aLong, params.referenceId));
}
{
    ClientMessage clientMessage = MapTryLockCodec.encodeResponse(    aBoolean   );
    MapTryLockCodec.ResponseParameters params = MapTryLockCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = MapIsLockedCodec.encodeRequest(    aString ,    aData   );
    MapIsLockedCodec.RequestParameters params = MapIsLockedCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
}
{
    ClientMessage clientMessage = MapIsLockedCodec.encodeResponse(    aBoolean   );
    MapIsLockedCodec.ResponseParameters params = MapIsLockedCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = MapUnlockCodec.encodeRequest(    aString ,    aData ,    aLong ,    aLong   );
    MapUnlockCodec.RequestParameters params = MapUnlockCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.referenceId));
}
{
    ClientMessage clientMessage = MapUnlockCodec.encodeResponse( );
    MapUnlockCodec.ResponseParameters params = MapUnlockCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = MapAddInterceptorCodec.encodeRequest(    aString ,    aData   );
    MapAddInterceptorCodec.RequestParameters params = MapAddInterceptorCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.interceptor));
}
{
    ClientMessage clientMessage = MapAddInterceptorCodec.encodeResponse(    aString   );
    MapAddInterceptorCodec.ResponseParameters params = MapAddInterceptorCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.response));
}
{
    ClientMessage clientMessage = MapRemoveInterceptorCodec.encodeRequest(    aString ,    aString   );
    MapRemoveInterceptorCodec.RequestParameters params = MapRemoveInterceptorCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.id));
}
{
    ClientMessage clientMessage = MapRemoveInterceptorCodec.encodeResponse(    aBoolean   );
    MapRemoveInterceptorCodec.ResponseParameters params = MapRemoveInterceptorCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = MapAddEntryListenerToKeyWithPredicateCodec.encodeRequest(    aString ,    aData ,    aData ,    aBoolean ,    anInt ,    aBoolean   );
    MapAddEntryListenerToKeyWithPredicateCodec.RequestParameters params = MapAddEntryListenerToKeyWithPredicateCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.predicate));
            assertTrue(isEqual(aBoolean, params.includeValue));
            assertTrue(isEqual(anInt, params.listenerFlags));
            assertTrue(isEqual(aBoolean, params.localOnly));
}
{
    ClientMessage clientMessage = MapAddEntryListenerToKeyWithPredicateCodec.encodeResponse(    aString   );
    MapAddEntryListenerToKeyWithPredicateCodec.ResponseParameters params = MapAddEntryListenerToKeyWithPredicateCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.response));
}
{
    class MapAddEntryListenerToKeyWithPredicateCodecHandler extends MapAddEntryListenerToKeyWithPredicateCodec.AbstractEventHandler {
        @Override
        public void handle(  Data
 key ,   Data
 value ,   Data
 oldValue ,   Data
 mergingValue ,   int
 eventType ,   String
 uuid ,   int
 numberOfAffectedEntries   ) {
                          assertTrue(isEqual(aData, key));
                          assertTrue(isEqual(aData, value));
                          assertTrue(isEqual(aData, oldValue));
                          assertTrue(isEqual(aData, mergingValue));
                          assertTrue(isEqual(anInt, eventType));
                          assertTrue(isEqual(aString, uuid));
                          assertTrue(isEqual(anInt, numberOfAffectedEntries));
        }
    }
    MapAddEntryListenerToKeyWithPredicateCodecHandler handler = new MapAddEntryListenerToKeyWithPredicateCodecHandler();
    {
        ClientMessage clientMessage = MapAddEntryListenerToKeyWithPredicateCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
}
{
    ClientMessage clientMessage = MapAddEntryListenerWithPredicateCodec.encodeRequest(    aString ,    aData ,    aBoolean ,    anInt ,    aBoolean   );
    MapAddEntryListenerWithPredicateCodec.RequestParameters params = MapAddEntryListenerWithPredicateCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.predicate));
            assertTrue(isEqual(aBoolean, params.includeValue));
            assertTrue(isEqual(anInt, params.listenerFlags));
            assertTrue(isEqual(aBoolean, params.localOnly));
}
{
    ClientMessage clientMessage = MapAddEntryListenerWithPredicateCodec.encodeResponse(    aString   );
    MapAddEntryListenerWithPredicateCodec.ResponseParameters params = MapAddEntryListenerWithPredicateCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.response));
}
{
    class MapAddEntryListenerWithPredicateCodecHandler extends MapAddEntryListenerWithPredicateCodec.AbstractEventHandler {
        @Override
        public void handle(  Data
 key ,   Data
 value ,   Data
 oldValue ,   Data
 mergingValue ,   int
 eventType ,   String
 uuid ,   int
 numberOfAffectedEntries   ) {
                          assertTrue(isEqual(aData, key));
                          assertTrue(isEqual(aData, value));
                          assertTrue(isEqual(aData, oldValue));
                          assertTrue(isEqual(aData, mergingValue));
                          assertTrue(isEqual(anInt, eventType));
                          assertTrue(isEqual(aString, uuid));
                          assertTrue(isEqual(anInt, numberOfAffectedEntries));
        }
    }
    MapAddEntryListenerWithPredicateCodecHandler handler = new MapAddEntryListenerWithPredicateCodecHandler();
    {
        ClientMessage clientMessage = MapAddEntryListenerWithPredicateCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
}
{
    ClientMessage clientMessage = MapAddEntryListenerToKeyCodec.encodeRequest(    aString ,    aData ,    aBoolean ,    anInt ,    aBoolean   );
    MapAddEntryListenerToKeyCodec.RequestParameters params = MapAddEntryListenerToKeyCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aBoolean, params.includeValue));
            assertTrue(isEqual(anInt, params.listenerFlags));
            assertTrue(isEqual(aBoolean, params.localOnly));
}
{
    ClientMessage clientMessage = MapAddEntryListenerToKeyCodec.encodeResponse(    aString   );
    MapAddEntryListenerToKeyCodec.ResponseParameters params = MapAddEntryListenerToKeyCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.response));
}
{
    class MapAddEntryListenerToKeyCodecHandler extends MapAddEntryListenerToKeyCodec.AbstractEventHandler {
        @Override
        public void handle(  Data
 key ,   Data
 value ,   Data
 oldValue ,   Data
 mergingValue ,   int
 eventType ,   String
 uuid ,   int
 numberOfAffectedEntries   ) {
                          assertTrue(isEqual(aData, key));
                          assertTrue(isEqual(aData, value));
                          assertTrue(isEqual(aData, oldValue));
                          assertTrue(isEqual(aData, mergingValue));
                          assertTrue(isEqual(anInt, eventType));
                          assertTrue(isEqual(aString, uuid));
                          assertTrue(isEqual(anInt, numberOfAffectedEntries));
        }
    }
    MapAddEntryListenerToKeyCodecHandler handler = new MapAddEntryListenerToKeyCodecHandler();
    {
        ClientMessage clientMessage = MapAddEntryListenerToKeyCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
}
{
    ClientMessage clientMessage = MapAddEntryListenerCodec.encodeRequest(    aString ,    aBoolean ,    anInt ,    aBoolean   );
    MapAddEntryListenerCodec.RequestParameters params = MapAddEntryListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aBoolean, params.includeValue));
            assertTrue(isEqual(anInt, params.listenerFlags));
            assertTrue(isEqual(aBoolean, params.localOnly));
}
{
    ClientMessage clientMessage = MapAddEntryListenerCodec.encodeResponse(    aString   );
    MapAddEntryListenerCodec.ResponseParameters params = MapAddEntryListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.response));
}
{
    class MapAddEntryListenerCodecHandler extends MapAddEntryListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  Data
 key ,   Data
 value ,   Data
 oldValue ,   Data
 mergingValue ,   int
 eventType ,   String
 uuid ,   int
 numberOfAffectedEntries   ) {
                          assertTrue(isEqual(aData, key));
                          assertTrue(isEqual(aData, value));
                          assertTrue(isEqual(aData, oldValue));
                          assertTrue(isEqual(aData, mergingValue));
                          assertTrue(isEqual(anInt, eventType));
                          assertTrue(isEqual(aString, uuid));
                          assertTrue(isEqual(anInt, numberOfAffectedEntries));
        }
    }
    MapAddEntryListenerCodecHandler handler = new MapAddEntryListenerCodecHandler();
    {
        ClientMessage clientMessage = MapAddEntryListenerCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
}
{
    ClientMessage clientMessage = MapAddNearCacheEntryListenerCodec.encodeRequest(    aString ,    anInt ,    aBoolean   );
    MapAddNearCacheEntryListenerCodec.RequestParameters params = MapAddNearCacheEntryListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.listenerFlags));
            assertTrue(isEqual(aBoolean, params.localOnly));
}
{
    ClientMessage clientMessage = MapAddNearCacheEntryListenerCodec.encodeResponse(    aString   );
    MapAddNearCacheEntryListenerCodec.ResponseParameters params = MapAddNearCacheEntryListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.response));
}
{
    class MapAddNearCacheEntryListenerCodecHandler extends MapAddNearCacheEntryListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  Data
 key ,   String
 sourceUuid ,   java.util.UUID
 partitionUuid ,   long
 sequence   ) {
                          assertTrue(isEqual(aData, key));
                          assertTrue(isEqual(aString, sourceUuid));
                          assertTrue(isEqual(aUUID, partitionUuid));
                          assertTrue(isEqual(aLong, sequence));
        }
        @Override
        public void handle(  Collection<Data> keys ,   Collection<String> sourceUuids ,   Collection<java.util.UUID> partitionUuids ,   Collection<Long> sequences   ) {
                          assertTrue(isEqual(datas, keys));
                          assertTrue(isEqual(strings, sourceUuids));
                          assertTrue(isEqual(uuids, partitionUuids));
                          assertTrue(isEqual(longs, sequences));
        }
    }
    MapAddNearCacheEntryListenerCodecHandler handler = new MapAddNearCacheEntryListenerCodecHandler();
    {
        ClientMessage clientMessage = MapAddNearCacheEntryListenerCodec.encodeIMapInvalidationEvent( aData ,  aString ,  aUUID ,  aLong   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
    {
        ClientMessage clientMessage = MapAddNearCacheEntryListenerCodec.encodeIMapBatchInvalidationEvent( datas ,  strings ,  uuids ,  longs   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
}
{
    ClientMessage clientMessage = MapRemoveEntryListenerCodec.encodeRequest(    aString ,    aString   );
    MapRemoveEntryListenerCodec.RequestParameters params = MapRemoveEntryListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.registrationId));
}
{
    ClientMessage clientMessage = MapRemoveEntryListenerCodec.encodeResponse(    aBoolean   );
    MapRemoveEntryListenerCodec.ResponseParameters params = MapRemoveEntryListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = MapAddPartitionLostListenerCodec.encodeRequest(    aString ,    aBoolean   );
    MapAddPartitionLostListenerCodec.RequestParameters params = MapAddPartitionLostListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aBoolean, params.localOnly));
}
{
    ClientMessage clientMessage = MapAddPartitionLostListenerCodec.encodeResponse(    aString   );
    MapAddPartitionLostListenerCodec.ResponseParameters params = MapAddPartitionLostListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.response));
}
{
    class MapAddPartitionLostListenerCodecHandler extends MapAddPartitionLostListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  int
 partitionId ,   String
 uuid   ) {
                          assertTrue(isEqual(anInt, partitionId));
                          assertTrue(isEqual(aString, uuid));
        }
    }
    MapAddPartitionLostListenerCodecHandler handler = new MapAddPartitionLostListenerCodecHandler();
    {
        ClientMessage clientMessage = MapAddPartitionLostListenerCodec.encodeMapPartitionLostEvent( anInt ,  aString   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
}
{
    ClientMessage clientMessage = MapRemovePartitionLostListenerCodec.encodeRequest(    aString ,    aString   );
    MapRemovePartitionLostListenerCodec.RequestParameters params = MapRemovePartitionLostListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.registrationId));
}
{
    ClientMessage clientMessage = MapRemovePartitionLostListenerCodec.encodeResponse(    aBoolean   );
    MapRemovePartitionLostListenerCodec.ResponseParameters params = MapRemovePartitionLostListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = MapGetEntryViewCodec.encodeRequest(    aString ,    aData ,    aLong   );
    MapGetEntryViewCodec.RequestParameters params = MapGetEntryViewCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = MapGetEntryViewCodec.encodeResponse(    anEntryView   );
    MapGetEntryViewCodec.ResponseParameters params = MapGetEntryViewCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anEntryView, params.response));
}
{
    ClientMessage clientMessage = MapEvictCodec.encodeRequest(    aString ,    aData ,    aLong   );
    MapEvictCodec.RequestParameters params = MapEvictCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = MapEvictCodec.encodeResponse(    aBoolean   );
    MapEvictCodec.ResponseParameters params = MapEvictCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = MapEvictAllCodec.encodeRequest(    aString   );
    MapEvictAllCodec.RequestParameters params = MapEvictAllCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = MapEvictAllCodec.encodeResponse( );
    MapEvictAllCodec.ResponseParameters params = MapEvictAllCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = MapLoadAllCodec.encodeRequest(    aString ,    aBoolean   );
    MapLoadAllCodec.RequestParameters params = MapLoadAllCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aBoolean, params.replaceExistingValues));
}
{
    ClientMessage clientMessage = MapLoadAllCodec.encodeResponse( );
    MapLoadAllCodec.ResponseParameters params = MapLoadAllCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = MapLoadGivenKeysCodec.encodeRequest(    aString ,    datas ,    aBoolean   );
    MapLoadGivenKeysCodec.RequestParameters params = MapLoadGivenKeysCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.keys));
            assertTrue(isEqual(aBoolean, params.replaceExistingValues));
}
{
    ClientMessage clientMessage = MapLoadGivenKeysCodec.encodeResponse( );
    MapLoadGivenKeysCodec.ResponseParameters params = MapLoadGivenKeysCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = MapKeySetCodec.encodeRequest(    aString   );
    MapKeySetCodec.RequestParameters params = MapKeySetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = MapKeySetCodec.encodeResponse(    datas   );
    MapKeySetCodec.ResponseParameters params = MapKeySetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(datas, params.response));
}
{
    ClientMessage clientMessage = MapGetAllCodec.encodeRequest(    aString ,    datas   );
    MapGetAllCodec.RequestParameters params = MapGetAllCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.keys));
}
{
    ClientMessage clientMessage = MapGetAllCodec.encodeResponse(    aListOfEntry   );
    MapGetAllCodec.ResponseParameters params = MapGetAllCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aListOfEntry, params.response));
}
{
    ClientMessage clientMessage = MapValuesCodec.encodeRequest(    aString   );
    MapValuesCodec.RequestParameters params = MapValuesCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = MapValuesCodec.encodeResponse(    datas   );
    MapValuesCodec.ResponseParameters params = MapValuesCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(datas, params.response));
}
{
    ClientMessage clientMessage = MapEntrySetCodec.encodeRequest(    aString   );
    MapEntrySetCodec.RequestParameters params = MapEntrySetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = MapEntrySetCodec.encodeResponse(    aListOfEntry   );
    MapEntrySetCodec.ResponseParameters params = MapEntrySetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aListOfEntry, params.response));
}
{
    ClientMessage clientMessage = MapKeySetWithPredicateCodec.encodeRequest(    aString ,    aData   );
    MapKeySetWithPredicateCodec.RequestParameters params = MapKeySetWithPredicateCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.predicate));
}
{
    ClientMessage clientMessage = MapKeySetWithPredicateCodec.encodeResponse(    datas   );
    MapKeySetWithPredicateCodec.ResponseParameters params = MapKeySetWithPredicateCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(datas, params.response));
}
{
    ClientMessage clientMessage = MapValuesWithPredicateCodec.encodeRequest(    aString ,    aData   );
    MapValuesWithPredicateCodec.RequestParameters params = MapValuesWithPredicateCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.predicate));
}
{
    ClientMessage clientMessage = MapValuesWithPredicateCodec.encodeResponse(    datas   );
    MapValuesWithPredicateCodec.ResponseParameters params = MapValuesWithPredicateCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(datas, params.response));
}
{
    ClientMessage clientMessage = MapEntriesWithPredicateCodec.encodeRequest(    aString ,    aData   );
    MapEntriesWithPredicateCodec.RequestParameters params = MapEntriesWithPredicateCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.predicate));
}
{
    ClientMessage clientMessage = MapEntriesWithPredicateCodec.encodeResponse(    aListOfEntry   );
    MapEntriesWithPredicateCodec.ResponseParameters params = MapEntriesWithPredicateCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aListOfEntry, params.response));
}
{
    ClientMessage clientMessage = MapAddIndexCodec.encodeRequest(    aString ,    aString ,    aBoolean   );
    MapAddIndexCodec.RequestParameters params = MapAddIndexCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.attribute));
            assertTrue(isEqual(aBoolean, params.ordered));
}
{
    ClientMessage clientMessage = MapAddIndexCodec.encodeResponse( );
    MapAddIndexCodec.ResponseParameters params = MapAddIndexCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = MapSizeCodec.encodeRequest(    aString   );
    MapSizeCodec.RequestParameters params = MapSizeCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = MapSizeCodec.encodeResponse(    anInt   );
    MapSizeCodec.ResponseParameters params = MapSizeCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anInt, params.response));
}
{
    ClientMessage clientMessage = MapIsEmptyCodec.encodeRequest(    aString   );
    MapIsEmptyCodec.RequestParameters params = MapIsEmptyCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = MapIsEmptyCodec.encodeResponse(    aBoolean   );
    MapIsEmptyCodec.ResponseParameters params = MapIsEmptyCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = MapPutAllCodec.encodeRequest(    aString ,    aListOfEntry   );
    MapPutAllCodec.RequestParameters params = MapPutAllCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aListOfEntry, params.entries));
}
{
    ClientMessage clientMessage = MapPutAllCodec.encodeResponse( );
    MapPutAllCodec.ResponseParameters params = MapPutAllCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = MapClearCodec.encodeRequest(    aString   );
    MapClearCodec.RequestParameters params = MapClearCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = MapClearCodec.encodeResponse( );
    MapClearCodec.ResponseParameters params = MapClearCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = MapExecuteOnKeyCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
    MapExecuteOnKeyCodec.RequestParameters params = MapExecuteOnKeyCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.entryProcessor));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = MapExecuteOnKeyCodec.encodeResponse(    aData   );
    MapExecuteOnKeyCodec.ResponseParameters params = MapExecuteOnKeyCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = MapSubmitToKeyCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
    MapSubmitToKeyCodec.RequestParameters params = MapSubmitToKeyCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.entryProcessor));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = MapSubmitToKeyCodec.encodeResponse(    aData   );
    MapSubmitToKeyCodec.ResponseParameters params = MapSubmitToKeyCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = MapExecuteOnAllKeysCodec.encodeRequest(    aString ,    aData   );
    MapExecuteOnAllKeysCodec.RequestParameters params = MapExecuteOnAllKeysCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.entryProcessor));
}
{
    ClientMessage clientMessage = MapExecuteOnAllKeysCodec.encodeResponse(    aListOfEntry   );
    MapExecuteOnAllKeysCodec.ResponseParameters params = MapExecuteOnAllKeysCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aListOfEntry, params.response));
}
{
    ClientMessage clientMessage = MapExecuteWithPredicateCodec.encodeRequest(    aString ,    aData ,    aData   );
    MapExecuteWithPredicateCodec.RequestParameters params = MapExecuteWithPredicateCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.entryProcessor));
            assertTrue(isEqual(aData, params.predicate));
}
{
    ClientMessage clientMessage = MapExecuteWithPredicateCodec.encodeResponse(    aListOfEntry   );
    MapExecuteWithPredicateCodec.ResponseParameters params = MapExecuteWithPredicateCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aListOfEntry, params.response));
}
{
    ClientMessage clientMessage = MapExecuteOnKeysCodec.encodeRequest(    aString ,    aData ,    datas   );
    MapExecuteOnKeysCodec.RequestParameters params = MapExecuteOnKeysCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.entryProcessor));
            assertTrue(isEqual(datas, params.keys));
}
{
    ClientMessage clientMessage = MapExecuteOnKeysCodec.encodeResponse(    aListOfEntry   );
    MapExecuteOnKeysCodec.ResponseParameters params = MapExecuteOnKeysCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aListOfEntry, params.response));
}
{
    ClientMessage clientMessage = MapForceUnlockCodec.encodeRequest(    aString ,    aData ,    aLong   );
    MapForceUnlockCodec.RequestParameters params = MapForceUnlockCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.referenceId));
}
{
    ClientMessage clientMessage = MapForceUnlockCodec.encodeResponse( );
    MapForceUnlockCodec.ResponseParameters params = MapForceUnlockCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = MapKeySetWithPagingPredicateCodec.encodeRequest(    aString ,    aData   );
    MapKeySetWithPagingPredicateCodec.RequestParameters params = MapKeySetWithPagingPredicateCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.predicate));
}
{
    ClientMessage clientMessage = MapKeySetWithPagingPredicateCodec.encodeResponse(    datas   );
    MapKeySetWithPagingPredicateCodec.ResponseParameters params = MapKeySetWithPagingPredicateCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(datas, params.response));
}
{
    ClientMessage clientMessage = MapValuesWithPagingPredicateCodec.encodeRequest(    aString ,    aData   );
    MapValuesWithPagingPredicateCodec.RequestParameters params = MapValuesWithPagingPredicateCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.predicate));
}
{
    ClientMessage clientMessage = MapValuesWithPagingPredicateCodec.encodeResponse(    aListOfEntry   );
    MapValuesWithPagingPredicateCodec.ResponseParameters params = MapValuesWithPagingPredicateCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aListOfEntry, params.response));
}
{
    ClientMessage clientMessage = MapEntriesWithPagingPredicateCodec.encodeRequest(    aString ,    aData   );
    MapEntriesWithPagingPredicateCodec.RequestParameters params = MapEntriesWithPagingPredicateCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.predicate));
}
{
    ClientMessage clientMessage = MapEntriesWithPagingPredicateCodec.encodeResponse(    aListOfEntry   );
    MapEntriesWithPagingPredicateCodec.ResponseParameters params = MapEntriesWithPagingPredicateCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aListOfEntry, params.response));
}
{
    ClientMessage clientMessage = MapClearNearCacheCodec.encodeRequest(    aString ,    anAddress   );
    MapClearNearCacheCodec.RequestParameters params = MapClearNearCacheCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anAddress, params.target));
}
{
    ClientMessage clientMessage = MapClearNearCacheCodec.encodeResponse( );
    MapClearNearCacheCodec.ResponseParameters params = MapClearNearCacheCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = MapFetchKeysCodec.encodeRequest(    aString ,    anInt ,    anInt ,    anInt   );
    MapFetchKeysCodec.RequestParameters params = MapFetchKeysCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.partitionId));
            assertTrue(isEqual(anInt, params.tableIndex));
            assertTrue(isEqual(anInt, params.batch));
}
{
    ClientMessage clientMessage = MapFetchKeysCodec.encodeResponse(    anInt ,    datas   );
    MapFetchKeysCodec.ResponseParameters params = MapFetchKeysCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anInt, params.tableIndex));
            assertTrue(isEqual(datas, params.keys));
}
{
    ClientMessage clientMessage = MapFetchEntriesCodec.encodeRequest(    aString ,    anInt ,    anInt ,    anInt   );
    MapFetchEntriesCodec.RequestParameters params = MapFetchEntriesCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.partitionId));
            assertTrue(isEqual(anInt, params.tableIndex));
            assertTrue(isEqual(anInt, params.batch));
}
{
    ClientMessage clientMessage = MapFetchEntriesCodec.encodeResponse(    anInt ,    aListOfEntry   );
    MapFetchEntriesCodec.ResponseParameters params = MapFetchEntriesCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anInt, params.tableIndex));
            assertTrue(isEqual(aListOfEntry, params.entries));
}
{
    ClientMessage clientMessage = MapAggregateCodec.encodeRequest(    aString ,    aData   );
    MapAggregateCodec.RequestParameters params = MapAggregateCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.aggregator));
}
{
    ClientMessage clientMessage = MapAggregateCodec.encodeResponse(    aData   );
    MapAggregateCodec.ResponseParameters params = MapAggregateCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = MapAggregateWithPredicateCodec.encodeRequest(    aString ,    aData ,    aData   );
    MapAggregateWithPredicateCodec.RequestParameters params = MapAggregateWithPredicateCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.aggregator));
            assertTrue(isEqual(aData, params.predicate));
}
{
    ClientMessage clientMessage = MapAggregateWithPredicateCodec.encodeResponse(    aData   );
    MapAggregateWithPredicateCodec.ResponseParameters params = MapAggregateWithPredicateCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = MapProjectCodec.encodeRequest(    aString ,    aData   );
    MapProjectCodec.RequestParameters params = MapProjectCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.projection));
}
{
    ClientMessage clientMessage = MapProjectCodec.encodeResponse(    datas   );
    MapProjectCodec.ResponseParameters params = MapProjectCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(datas, params.response));
}
{
    ClientMessage clientMessage = MapProjectWithPredicateCodec.encodeRequest(    aString ,    aData ,    aData   );
    MapProjectWithPredicateCodec.RequestParameters params = MapProjectWithPredicateCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.projection));
            assertTrue(isEqual(aData, params.predicate));
}
{
    ClientMessage clientMessage = MapProjectWithPredicateCodec.encodeResponse(    datas   );
    MapProjectWithPredicateCodec.ResponseParameters params = MapProjectWithPredicateCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(datas, params.response));
}
{
    ClientMessage clientMessage = MapFetchNearCacheInvalidationMetadataCodec.encodeRequest(    strings ,    anAddress   );
    MapFetchNearCacheInvalidationMetadataCodec.RequestParameters params = MapFetchNearCacheInvalidationMetadataCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(strings, params.names));
            assertTrue(isEqual(anAddress, params.address));
}
{
    ClientMessage clientMessage = MapFetchNearCacheInvalidationMetadataCodec.encodeResponse(    aNamePartitionSequenceList ,    aPartitionUuidList   );
    MapFetchNearCacheInvalidationMetadataCodec.ResponseParameters params = MapFetchNearCacheInvalidationMetadataCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aNamePartitionSequenceList, params.namePartitionSequenceList));
            assertTrue(isEqual(aPartitionUuidList, params.partitionUuidList));
}
{
    ClientMessage clientMessage = MapAssignAndGetUuidsCodec.encodeRequest( );
    MapAssignAndGetUuidsCodec.RequestParameters params = MapAssignAndGetUuidsCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = MapAssignAndGetUuidsCodec.encodeResponse(    aPartitionUuidList   );
    MapAssignAndGetUuidsCodec.ResponseParameters params = MapAssignAndGetUuidsCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aPartitionUuidList, params.partitionUuidList));
}
{
    ClientMessage clientMessage = MapRemoveAllCodec.encodeRequest(    aString ,    aData   );
    MapRemoveAllCodec.RequestParameters params = MapRemoveAllCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.predicate));
}
{
    ClientMessage clientMessage = MapRemoveAllCodec.encodeResponse( );
    MapRemoveAllCodec.ResponseParameters params = MapRemoveAllCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = MapAddNearCacheInvalidationListenerCodec.encodeRequest(    aString ,    anInt ,    aBoolean   );
    MapAddNearCacheInvalidationListenerCodec.RequestParameters params = MapAddNearCacheInvalidationListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.listenerFlags));
            assertTrue(isEqual(aBoolean, params.localOnly));
}
{
    ClientMessage clientMessage = MapAddNearCacheInvalidationListenerCodec.encodeResponse(    aString   );
    MapAddNearCacheInvalidationListenerCodec.ResponseParameters params = MapAddNearCacheInvalidationListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.response));
}
{
    class MapAddNearCacheInvalidationListenerCodecHandler extends MapAddNearCacheInvalidationListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  Data
 key ,   String
 sourceUuid ,   java.util.UUID
 partitionUuid ,   long
 sequence   ) {
                          assertTrue(isEqual(aData, key));
                          assertTrue(isEqual(aString, sourceUuid));
                          assertTrue(isEqual(aUUID, partitionUuid));
                          assertTrue(isEqual(aLong, sequence));
        }
        @Override
        public void handle(  Collection<Data> keys ,   Collection<String> sourceUuids ,   Collection<java.util.UUID> partitionUuids ,   Collection<Long> sequences   ) {
                          assertTrue(isEqual(datas, keys));
                          assertTrue(isEqual(strings, sourceUuids));
                          assertTrue(isEqual(uuids, partitionUuids));
                          assertTrue(isEqual(longs, sequences));
        }
    }
    MapAddNearCacheInvalidationListenerCodecHandler handler = new MapAddNearCacheInvalidationListenerCodecHandler();
    {
        ClientMessage clientMessage = MapAddNearCacheInvalidationListenerCodec.encodeIMapInvalidationEvent( aData ,  aString ,  aUUID ,  aLong   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
    {
        ClientMessage clientMessage = MapAddNearCacheInvalidationListenerCodec.encodeIMapBatchInvalidationEvent( datas ,  strings ,  uuids ,  longs   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
}
{
    ClientMessage clientMessage = MapFetchWithQueryCodec.encodeRequest(    aString ,    anInt ,    anInt ,    aData ,    aData   );
    MapFetchWithQueryCodec.RequestParameters params = MapFetchWithQueryCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.tableIndex));
            assertTrue(isEqual(anInt, params.batch));
            assertTrue(isEqual(aData, params.projection));
            assertTrue(isEqual(aData, params.predicate));
}
{
    ClientMessage clientMessage = MapFetchWithQueryCodec.encodeResponse(    datas ,    anInt   );
    MapFetchWithQueryCodec.ResponseParameters params = MapFetchWithQueryCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(datas, params.results));
            assertTrue(isEqual(anInt, params.nextTableIndexToReadFrom));
}
{
    ClientMessage clientMessage = MapEventJournalSubscribeCodec.encodeRequest(    aString   );
    MapEventJournalSubscribeCodec.RequestParameters params = MapEventJournalSubscribeCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = MapEventJournalSubscribeCodec.encodeResponse(    aLong ,    aLong   );
    MapEventJournalSubscribeCodec.ResponseParameters params = MapEventJournalSubscribeCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aLong, params.oldestSequence));
            assertTrue(isEqual(aLong, params.newestSequence));
}
{
    ClientMessage clientMessage = MapEventJournalReadCodec.encodeRequest(    aString ,    aLong ,    anInt ,    anInt ,    aData ,    aData   );
    MapEventJournalReadCodec.RequestParameters params = MapEventJournalReadCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.startSequence));
            assertTrue(isEqual(anInt, params.minSize));
            assertTrue(isEqual(anInt, params.maxSize));
            assertTrue(isEqual(aData, params.predicate));
            assertTrue(isEqual(aData, params.projection));
}
{
    ClientMessage clientMessage = MapEventJournalReadCodec.encodeResponse(    anInt ,    datas ,    arrLongs ,    aLong   );
    MapEventJournalReadCodec.ResponseParameters params = MapEventJournalReadCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anInt, params.readCount));
            assertTrue(isEqual(datas, params.items));
            assertTrue(isEqual(arrLongs, params.itemSeqs));
            assertTrue(isEqual(aLong, params.nextSeq));
}
{
    ClientMessage clientMessage = MultiMapPutCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
    MultiMapPutCodec.RequestParameters params = MultiMapPutCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = MultiMapPutCodec.encodeResponse(    aBoolean   );
    MultiMapPutCodec.ResponseParameters params = MultiMapPutCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = MultiMapGetCodec.encodeRequest(    aString ,    aData ,    aLong   );
    MultiMapGetCodec.RequestParameters params = MultiMapGetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = MultiMapGetCodec.encodeResponse(    datas   );
    MultiMapGetCodec.ResponseParameters params = MultiMapGetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(datas, params.response));
}
{
    ClientMessage clientMessage = MultiMapRemoveCodec.encodeRequest(    aString ,    aData ,    aLong   );
    MultiMapRemoveCodec.RequestParameters params = MultiMapRemoveCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = MultiMapRemoveCodec.encodeResponse(    datas   );
    MultiMapRemoveCodec.ResponseParameters params = MultiMapRemoveCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(datas, params.response));
}
{
    ClientMessage clientMessage = MultiMapKeySetCodec.encodeRequest(    aString   );
    MultiMapKeySetCodec.RequestParameters params = MultiMapKeySetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = MultiMapKeySetCodec.encodeResponse(    datas   );
    MultiMapKeySetCodec.ResponseParameters params = MultiMapKeySetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(datas, params.response));
}
{
    ClientMessage clientMessage = MultiMapValuesCodec.encodeRequest(    aString   );
    MultiMapValuesCodec.RequestParameters params = MultiMapValuesCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = MultiMapValuesCodec.encodeResponse(    datas   );
    MultiMapValuesCodec.ResponseParameters params = MultiMapValuesCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(datas, params.response));
}
{
    ClientMessage clientMessage = MultiMapEntrySetCodec.encodeRequest(    aString   );
    MultiMapEntrySetCodec.RequestParameters params = MultiMapEntrySetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = MultiMapEntrySetCodec.encodeResponse(    aListOfEntry   );
    MultiMapEntrySetCodec.ResponseParameters params = MultiMapEntrySetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aListOfEntry, params.response));
}
{
    ClientMessage clientMessage = MultiMapContainsKeyCodec.encodeRequest(    aString ,    aData ,    aLong   );
    MultiMapContainsKeyCodec.RequestParameters params = MultiMapContainsKeyCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = MultiMapContainsKeyCodec.encodeResponse(    aBoolean   );
    MultiMapContainsKeyCodec.ResponseParameters params = MultiMapContainsKeyCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = MultiMapContainsValueCodec.encodeRequest(    aString ,    aData   );
    MultiMapContainsValueCodec.RequestParameters params = MultiMapContainsValueCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.value));
}
{
    ClientMessage clientMessage = MultiMapContainsValueCodec.encodeResponse(    aBoolean   );
    MultiMapContainsValueCodec.ResponseParameters params = MultiMapContainsValueCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = MultiMapContainsEntryCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
    MultiMapContainsEntryCodec.RequestParameters params = MultiMapContainsEntryCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = MultiMapContainsEntryCodec.encodeResponse(    aBoolean   );
    MultiMapContainsEntryCodec.ResponseParameters params = MultiMapContainsEntryCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = MultiMapSizeCodec.encodeRequest(    aString   );
    MultiMapSizeCodec.RequestParameters params = MultiMapSizeCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = MultiMapSizeCodec.encodeResponse(    anInt   );
    MultiMapSizeCodec.ResponseParameters params = MultiMapSizeCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anInt, params.response));
}
{
    ClientMessage clientMessage = MultiMapClearCodec.encodeRequest(    aString   );
    MultiMapClearCodec.RequestParameters params = MultiMapClearCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = MultiMapClearCodec.encodeResponse( );
    MultiMapClearCodec.ResponseParameters params = MultiMapClearCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = MultiMapValueCountCodec.encodeRequest(    aString ,    aData ,    aLong   );
    MultiMapValueCountCodec.RequestParameters params = MultiMapValueCountCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = MultiMapValueCountCodec.encodeResponse(    anInt   );
    MultiMapValueCountCodec.ResponseParameters params = MultiMapValueCountCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anInt, params.response));
}
{
    ClientMessage clientMessage = MultiMapAddEntryListenerToKeyCodec.encodeRequest(    aString ,    aData ,    aBoolean ,    aBoolean   );
    MultiMapAddEntryListenerToKeyCodec.RequestParameters params = MultiMapAddEntryListenerToKeyCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aBoolean, params.includeValue));
            assertTrue(isEqual(aBoolean, params.localOnly));
}
{
    ClientMessage clientMessage = MultiMapAddEntryListenerToKeyCodec.encodeResponse(    aString   );
    MultiMapAddEntryListenerToKeyCodec.ResponseParameters params = MultiMapAddEntryListenerToKeyCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.response));
}
{
    class MultiMapAddEntryListenerToKeyCodecHandler extends MultiMapAddEntryListenerToKeyCodec.AbstractEventHandler {
        @Override
        public void handle(  Data
 key ,   Data
 value ,   Data
 oldValue ,   Data
 mergingValue ,   int
 eventType ,   String
 uuid ,   int
 numberOfAffectedEntries   ) {
                          assertTrue(isEqual(aData, key));
                          assertTrue(isEqual(aData, value));
                          assertTrue(isEqual(aData, oldValue));
                          assertTrue(isEqual(aData, mergingValue));
                          assertTrue(isEqual(anInt, eventType));
                          assertTrue(isEqual(aString, uuid));
                          assertTrue(isEqual(anInt, numberOfAffectedEntries));
        }
    }
    MultiMapAddEntryListenerToKeyCodecHandler handler = new MultiMapAddEntryListenerToKeyCodecHandler();
    {
        ClientMessage clientMessage = MultiMapAddEntryListenerToKeyCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
}
{
    ClientMessage clientMessage = MultiMapAddEntryListenerCodec.encodeRequest(    aString ,    aBoolean ,    aBoolean   );
    MultiMapAddEntryListenerCodec.RequestParameters params = MultiMapAddEntryListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aBoolean, params.includeValue));
            assertTrue(isEqual(aBoolean, params.localOnly));
}
{
    ClientMessage clientMessage = MultiMapAddEntryListenerCodec.encodeResponse(    aString   );
    MultiMapAddEntryListenerCodec.ResponseParameters params = MultiMapAddEntryListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.response));
}
{
    class MultiMapAddEntryListenerCodecHandler extends MultiMapAddEntryListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  Data
 key ,   Data
 value ,   Data
 oldValue ,   Data
 mergingValue ,   int
 eventType ,   String
 uuid ,   int
 numberOfAffectedEntries   ) {
                          assertTrue(isEqual(aData, key));
                          assertTrue(isEqual(aData, value));
                          assertTrue(isEqual(aData, oldValue));
                          assertTrue(isEqual(aData, mergingValue));
                          assertTrue(isEqual(anInt, eventType));
                          assertTrue(isEqual(aString, uuid));
                          assertTrue(isEqual(anInt, numberOfAffectedEntries));
        }
    }
    MultiMapAddEntryListenerCodecHandler handler = new MultiMapAddEntryListenerCodecHandler();
    {
        ClientMessage clientMessage = MultiMapAddEntryListenerCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
}
{
    ClientMessage clientMessage = MultiMapRemoveEntryListenerCodec.encodeRequest(    aString ,    aString   );
    MultiMapRemoveEntryListenerCodec.RequestParameters params = MultiMapRemoveEntryListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.registrationId));
}
{
    ClientMessage clientMessage = MultiMapRemoveEntryListenerCodec.encodeResponse(    aBoolean   );
    MultiMapRemoveEntryListenerCodec.ResponseParameters params = MultiMapRemoveEntryListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = MultiMapLockCodec.encodeRequest(    aString ,    aData ,    aLong ,    aLong ,    aLong   );
    MultiMapLockCodec.RequestParameters params = MultiMapLockCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.ttl));
            assertTrue(isEqual(aLong, params.referenceId));
}
{
    ClientMessage clientMessage = MultiMapLockCodec.encodeResponse( );
    MultiMapLockCodec.ResponseParameters params = MultiMapLockCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = MultiMapTryLockCodec.encodeRequest(    aString ,    aData ,    aLong ,    aLong ,    aLong ,    aLong   );
    MultiMapTryLockCodec.RequestParameters params = MultiMapTryLockCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.lease));
            assertTrue(isEqual(aLong, params.timeout));
            assertTrue(isEqual(aLong, params.referenceId));
}
{
    ClientMessage clientMessage = MultiMapTryLockCodec.encodeResponse(    aBoolean   );
    MultiMapTryLockCodec.ResponseParameters params = MultiMapTryLockCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = MultiMapIsLockedCodec.encodeRequest(    aString ,    aData   );
    MultiMapIsLockedCodec.RequestParameters params = MultiMapIsLockedCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
}
{
    ClientMessage clientMessage = MultiMapIsLockedCodec.encodeResponse(    aBoolean   );
    MultiMapIsLockedCodec.ResponseParameters params = MultiMapIsLockedCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = MultiMapUnlockCodec.encodeRequest(    aString ,    aData ,    aLong ,    aLong   );
    MultiMapUnlockCodec.RequestParameters params = MultiMapUnlockCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.referenceId));
}
{
    ClientMessage clientMessage = MultiMapUnlockCodec.encodeResponse( );
    MultiMapUnlockCodec.ResponseParameters params = MultiMapUnlockCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = MultiMapForceUnlockCodec.encodeRequest(    aString ,    aData ,    aLong   );
    MultiMapForceUnlockCodec.RequestParameters params = MultiMapForceUnlockCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.referenceId));
}
{
    ClientMessage clientMessage = MultiMapForceUnlockCodec.encodeResponse( );
    MultiMapForceUnlockCodec.ResponseParameters params = MultiMapForceUnlockCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = MultiMapRemoveEntryCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
    MultiMapRemoveEntryCodec.RequestParameters params = MultiMapRemoveEntryCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = MultiMapRemoveEntryCodec.encodeResponse(    aBoolean   );
    MultiMapRemoveEntryCodec.ResponseParameters params = MultiMapRemoveEntryCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = MultiMapDeleteCodec.encodeRequest(    aString ,    aData ,    aLong   );
    MultiMapDeleteCodec.RequestParameters params = MultiMapDeleteCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = MultiMapDeleteCodec.encodeResponse( );
    MultiMapDeleteCodec.ResponseParameters params = MultiMapDeleteCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = QueueOfferCodec.encodeRequest(    aString ,    aData ,    aLong   );
    QueueOfferCodec.RequestParameters params = QueueOfferCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(aLong, params.timeoutMillis));
}
{
    ClientMessage clientMessage = QueueOfferCodec.encodeResponse(    aBoolean   );
    QueueOfferCodec.ResponseParameters params = QueueOfferCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = QueuePutCodec.encodeRequest(    aString ,    aData   );
    QueuePutCodec.RequestParameters params = QueuePutCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.value));
}
{
    ClientMessage clientMessage = QueuePutCodec.encodeResponse( );
    QueuePutCodec.ResponseParameters params = QueuePutCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = QueueSizeCodec.encodeRequest(    aString   );
    QueueSizeCodec.RequestParameters params = QueueSizeCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = QueueSizeCodec.encodeResponse(    anInt   );
    QueueSizeCodec.ResponseParameters params = QueueSizeCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anInt, params.response));
}
{
    ClientMessage clientMessage = QueueRemoveCodec.encodeRequest(    aString ,    aData   );
    QueueRemoveCodec.RequestParameters params = QueueRemoveCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.value));
}
{
    ClientMessage clientMessage = QueueRemoveCodec.encodeResponse(    aBoolean   );
    QueueRemoveCodec.ResponseParameters params = QueueRemoveCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = QueuePollCodec.encodeRequest(    aString ,    aLong   );
    QueuePollCodec.RequestParameters params = QueuePollCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.timeoutMillis));
}
{
    ClientMessage clientMessage = QueuePollCodec.encodeResponse(    aData   );
    QueuePollCodec.ResponseParameters params = QueuePollCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = QueueTakeCodec.encodeRequest(    aString   );
    QueueTakeCodec.RequestParameters params = QueueTakeCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = QueueTakeCodec.encodeResponse(    aData   );
    QueueTakeCodec.ResponseParameters params = QueueTakeCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = QueuePeekCodec.encodeRequest(    aString   );
    QueuePeekCodec.RequestParameters params = QueuePeekCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = QueuePeekCodec.encodeResponse(    aData   );
    QueuePeekCodec.ResponseParameters params = QueuePeekCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = QueueIteratorCodec.encodeRequest(    aString   );
    QueueIteratorCodec.RequestParameters params = QueueIteratorCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = QueueIteratorCodec.encodeResponse(    datas   );
    QueueIteratorCodec.ResponseParameters params = QueueIteratorCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(datas, params.response));
}
{
    ClientMessage clientMessage = QueueDrainToCodec.encodeRequest(    aString   );
    QueueDrainToCodec.RequestParameters params = QueueDrainToCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = QueueDrainToCodec.encodeResponse(    datas   );
    QueueDrainToCodec.ResponseParameters params = QueueDrainToCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(datas, params.response));
}
{
    ClientMessage clientMessage = QueueDrainToMaxSizeCodec.encodeRequest(    aString ,    anInt   );
    QueueDrainToMaxSizeCodec.RequestParameters params = QueueDrainToMaxSizeCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.maxSize));
}
{
    ClientMessage clientMessage = QueueDrainToMaxSizeCodec.encodeResponse(    datas   );
    QueueDrainToMaxSizeCodec.ResponseParameters params = QueueDrainToMaxSizeCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(datas, params.response));
}
{
    ClientMessage clientMessage = QueueContainsCodec.encodeRequest(    aString ,    aData   );
    QueueContainsCodec.RequestParameters params = QueueContainsCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.value));
}
{
    ClientMessage clientMessage = QueueContainsCodec.encodeResponse(    aBoolean   );
    QueueContainsCodec.ResponseParameters params = QueueContainsCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = QueueContainsAllCodec.encodeRequest(    aString ,    datas   );
    QueueContainsAllCodec.RequestParameters params = QueueContainsAllCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.dataList));
}
{
    ClientMessage clientMessage = QueueContainsAllCodec.encodeResponse(    aBoolean   );
    QueueContainsAllCodec.ResponseParameters params = QueueContainsAllCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = QueueCompareAndRemoveAllCodec.encodeRequest(    aString ,    datas   );
    QueueCompareAndRemoveAllCodec.RequestParameters params = QueueCompareAndRemoveAllCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.dataList));
}
{
    ClientMessage clientMessage = QueueCompareAndRemoveAllCodec.encodeResponse(    aBoolean   );
    QueueCompareAndRemoveAllCodec.ResponseParameters params = QueueCompareAndRemoveAllCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = QueueCompareAndRetainAllCodec.encodeRequest(    aString ,    datas   );
    QueueCompareAndRetainAllCodec.RequestParameters params = QueueCompareAndRetainAllCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.dataList));
}
{
    ClientMessage clientMessage = QueueCompareAndRetainAllCodec.encodeResponse(    aBoolean   );
    QueueCompareAndRetainAllCodec.ResponseParameters params = QueueCompareAndRetainAllCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = QueueClearCodec.encodeRequest(    aString   );
    QueueClearCodec.RequestParameters params = QueueClearCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = QueueClearCodec.encodeResponse( );
    QueueClearCodec.ResponseParameters params = QueueClearCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = QueueAddAllCodec.encodeRequest(    aString ,    datas   );
    QueueAddAllCodec.RequestParameters params = QueueAddAllCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.dataList));
}
{
    ClientMessage clientMessage = QueueAddAllCodec.encodeResponse(    aBoolean   );
    QueueAddAllCodec.ResponseParameters params = QueueAddAllCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = QueueAddListenerCodec.encodeRequest(    aString ,    aBoolean ,    aBoolean   );
    QueueAddListenerCodec.RequestParameters params = QueueAddListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aBoolean, params.includeValue));
            assertTrue(isEqual(aBoolean, params.localOnly));
}
{
    ClientMessage clientMessage = QueueAddListenerCodec.encodeResponse(    aString   );
    QueueAddListenerCodec.ResponseParameters params = QueueAddListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.response));
}
{
    class QueueAddListenerCodecHandler extends QueueAddListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  Data
 item ,   String
 uuid ,   int
 eventType   ) {
                          assertTrue(isEqual(aData, item));
                          assertTrue(isEqual(aString, uuid));
                          assertTrue(isEqual(anInt, eventType));
        }
    }
    QueueAddListenerCodecHandler handler = new QueueAddListenerCodecHandler();
    {
        ClientMessage clientMessage = QueueAddListenerCodec.encodeItemEvent( aData ,  aString ,  anInt   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
}
{
    ClientMessage clientMessage = QueueRemoveListenerCodec.encodeRequest(    aString ,    aString   );
    QueueRemoveListenerCodec.RequestParameters params = QueueRemoveListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.registrationId));
}
{
    ClientMessage clientMessage = QueueRemoveListenerCodec.encodeResponse(    aBoolean   );
    QueueRemoveListenerCodec.ResponseParameters params = QueueRemoveListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = QueueRemainingCapacityCodec.encodeRequest(    aString   );
    QueueRemainingCapacityCodec.RequestParameters params = QueueRemainingCapacityCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = QueueRemainingCapacityCodec.encodeResponse(    anInt   );
    QueueRemainingCapacityCodec.ResponseParameters params = QueueRemainingCapacityCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anInt, params.response));
}
{
    ClientMessage clientMessage = QueueIsEmptyCodec.encodeRequest(    aString   );
    QueueIsEmptyCodec.RequestParameters params = QueueIsEmptyCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = QueueIsEmptyCodec.encodeResponse(    aBoolean   );
    QueueIsEmptyCodec.ResponseParameters params = QueueIsEmptyCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = TopicPublishCodec.encodeRequest(    aString ,    aData   );
    TopicPublishCodec.RequestParameters params = TopicPublishCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.message));
}
{
    ClientMessage clientMessage = TopicPublishCodec.encodeResponse( );
    TopicPublishCodec.ResponseParameters params = TopicPublishCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = TopicAddMessageListenerCodec.encodeRequest(    aString ,    aBoolean   );
    TopicAddMessageListenerCodec.RequestParameters params = TopicAddMessageListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aBoolean, params.localOnly));
}
{
    ClientMessage clientMessage = TopicAddMessageListenerCodec.encodeResponse(    aString   );
    TopicAddMessageListenerCodec.ResponseParameters params = TopicAddMessageListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.response));
}
{
    class TopicAddMessageListenerCodecHandler extends TopicAddMessageListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  Data
 item ,   long
 publishTime ,   String
 uuid   ) {
                          assertTrue(isEqual(aData, item));
                          assertTrue(isEqual(aLong, publishTime));
                          assertTrue(isEqual(aString, uuid));
        }
    }
    TopicAddMessageListenerCodecHandler handler = new TopicAddMessageListenerCodecHandler();
    {
        ClientMessage clientMessage = TopicAddMessageListenerCodec.encodeTopicEvent( aData ,  aLong ,  aString   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
}
{
    ClientMessage clientMessage = TopicRemoveMessageListenerCodec.encodeRequest(    aString ,    aString   );
    TopicRemoveMessageListenerCodec.RequestParameters params = TopicRemoveMessageListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.registrationId));
}
{
    ClientMessage clientMessage = TopicRemoveMessageListenerCodec.encodeResponse(    aBoolean   );
    TopicRemoveMessageListenerCodec.ResponseParameters params = TopicRemoveMessageListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = ListSizeCodec.encodeRequest(    aString   );
    ListSizeCodec.RequestParameters params = ListSizeCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = ListSizeCodec.encodeResponse(    anInt   );
    ListSizeCodec.ResponseParameters params = ListSizeCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anInt, params.response));
}
{
    ClientMessage clientMessage = ListContainsCodec.encodeRequest(    aString ,    aData   );
    ListContainsCodec.RequestParameters params = ListContainsCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.value));
}
{
    ClientMessage clientMessage = ListContainsCodec.encodeResponse(    aBoolean   );
    ListContainsCodec.ResponseParameters params = ListContainsCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = ListContainsAllCodec.encodeRequest(    aString ,    datas   );
    ListContainsAllCodec.RequestParameters params = ListContainsAllCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.values));
}
{
    ClientMessage clientMessage = ListContainsAllCodec.encodeResponse(    aBoolean   );
    ListContainsAllCodec.ResponseParameters params = ListContainsAllCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = ListAddCodec.encodeRequest(    aString ,    aData   );
    ListAddCodec.RequestParameters params = ListAddCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.value));
}
{
    ClientMessage clientMessage = ListAddCodec.encodeResponse(    aBoolean   );
    ListAddCodec.ResponseParameters params = ListAddCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = ListRemoveCodec.encodeRequest(    aString ,    aData   );
    ListRemoveCodec.RequestParameters params = ListRemoveCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.value));
}
{
    ClientMessage clientMessage = ListRemoveCodec.encodeResponse(    aBoolean   );
    ListRemoveCodec.ResponseParameters params = ListRemoveCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = ListAddAllCodec.encodeRequest(    aString ,    datas   );
    ListAddAllCodec.RequestParameters params = ListAddAllCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.valueList));
}
{
    ClientMessage clientMessage = ListAddAllCodec.encodeResponse(    aBoolean   );
    ListAddAllCodec.ResponseParameters params = ListAddAllCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = ListCompareAndRemoveAllCodec.encodeRequest(    aString ,    datas   );
    ListCompareAndRemoveAllCodec.RequestParameters params = ListCompareAndRemoveAllCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.values));
}
{
    ClientMessage clientMessage = ListCompareAndRemoveAllCodec.encodeResponse(    aBoolean   );
    ListCompareAndRemoveAllCodec.ResponseParameters params = ListCompareAndRemoveAllCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = ListCompareAndRetainAllCodec.encodeRequest(    aString ,    datas   );
    ListCompareAndRetainAllCodec.RequestParameters params = ListCompareAndRetainAllCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.values));
}
{
    ClientMessage clientMessage = ListCompareAndRetainAllCodec.encodeResponse(    aBoolean   );
    ListCompareAndRetainAllCodec.ResponseParameters params = ListCompareAndRetainAllCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = ListClearCodec.encodeRequest(    aString   );
    ListClearCodec.RequestParameters params = ListClearCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = ListClearCodec.encodeResponse( );
    ListClearCodec.ResponseParameters params = ListClearCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = ListGetAllCodec.encodeRequest(    aString   );
    ListGetAllCodec.RequestParameters params = ListGetAllCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = ListGetAllCodec.encodeResponse(    datas   );
    ListGetAllCodec.ResponseParameters params = ListGetAllCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(datas, params.response));
}
{
    ClientMessage clientMessage = ListAddListenerCodec.encodeRequest(    aString ,    aBoolean ,    aBoolean   );
    ListAddListenerCodec.RequestParameters params = ListAddListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aBoolean, params.includeValue));
            assertTrue(isEqual(aBoolean, params.localOnly));
}
{
    ClientMessage clientMessage = ListAddListenerCodec.encodeResponse(    aString   );
    ListAddListenerCodec.ResponseParameters params = ListAddListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.response));
}
{
    class ListAddListenerCodecHandler extends ListAddListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  Data
 item ,   String
 uuid ,   int
 eventType   ) {
                          assertTrue(isEqual(aData, item));
                          assertTrue(isEqual(aString, uuid));
                          assertTrue(isEqual(anInt, eventType));
        }
    }
    ListAddListenerCodecHandler handler = new ListAddListenerCodecHandler();
    {
        ClientMessage clientMessage = ListAddListenerCodec.encodeItemEvent( aData ,  aString ,  anInt   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
}
{
    ClientMessage clientMessage = ListRemoveListenerCodec.encodeRequest(    aString ,    aString   );
    ListRemoveListenerCodec.RequestParameters params = ListRemoveListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.registrationId));
}
{
    ClientMessage clientMessage = ListRemoveListenerCodec.encodeResponse(    aBoolean   );
    ListRemoveListenerCodec.ResponseParameters params = ListRemoveListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = ListIsEmptyCodec.encodeRequest(    aString   );
    ListIsEmptyCodec.RequestParameters params = ListIsEmptyCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = ListIsEmptyCodec.encodeResponse(    aBoolean   );
    ListIsEmptyCodec.ResponseParameters params = ListIsEmptyCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = ListAddAllWithIndexCodec.encodeRequest(    aString ,    anInt ,    datas   );
    ListAddAllWithIndexCodec.RequestParameters params = ListAddAllWithIndexCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.index));
            assertTrue(isEqual(datas, params.valueList));
}
{
    ClientMessage clientMessage = ListAddAllWithIndexCodec.encodeResponse(    aBoolean   );
    ListAddAllWithIndexCodec.ResponseParameters params = ListAddAllWithIndexCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = ListGetCodec.encodeRequest(    aString ,    anInt   );
    ListGetCodec.RequestParameters params = ListGetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.index));
}
{
    ClientMessage clientMessage = ListGetCodec.encodeResponse(    aData   );
    ListGetCodec.ResponseParameters params = ListGetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = ListSetCodec.encodeRequest(    aString ,    anInt ,    aData   );
    ListSetCodec.RequestParameters params = ListSetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.index));
            assertTrue(isEqual(aData, params.value));
}
{
    ClientMessage clientMessage = ListSetCodec.encodeResponse(    aData   );
    ListSetCodec.ResponseParameters params = ListSetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = ListAddWithIndexCodec.encodeRequest(    aString ,    anInt ,    aData   );
    ListAddWithIndexCodec.RequestParameters params = ListAddWithIndexCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.index));
            assertTrue(isEqual(aData, params.value));
}
{
    ClientMessage clientMessage = ListAddWithIndexCodec.encodeResponse( );
    ListAddWithIndexCodec.ResponseParameters params = ListAddWithIndexCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = ListRemoveWithIndexCodec.encodeRequest(    aString ,    anInt   );
    ListRemoveWithIndexCodec.RequestParameters params = ListRemoveWithIndexCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.index));
}
{
    ClientMessage clientMessage = ListRemoveWithIndexCodec.encodeResponse(    aData   );
    ListRemoveWithIndexCodec.ResponseParameters params = ListRemoveWithIndexCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = ListLastIndexOfCodec.encodeRequest(    aString ,    aData   );
    ListLastIndexOfCodec.RequestParameters params = ListLastIndexOfCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.value));
}
{
    ClientMessage clientMessage = ListLastIndexOfCodec.encodeResponse(    anInt   );
    ListLastIndexOfCodec.ResponseParameters params = ListLastIndexOfCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anInt, params.response));
}
{
    ClientMessage clientMessage = ListIndexOfCodec.encodeRequest(    aString ,    aData   );
    ListIndexOfCodec.RequestParameters params = ListIndexOfCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.value));
}
{
    ClientMessage clientMessage = ListIndexOfCodec.encodeResponse(    anInt   );
    ListIndexOfCodec.ResponseParameters params = ListIndexOfCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anInt, params.response));
}
{
    ClientMessage clientMessage = ListSubCodec.encodeRequest(    aString ,    anInt ,    anInt   );
    ListSubCodec.RequestParameters params = ListSubCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.from));
            assertTrue(isEqual(anInt, params.to));
}
{
    ClientMessage clientMessage = ListSubCodec.encodeResponse(    datas   );
    ListSubCodec.ResponseParameters params = ListSubCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(datas, params.response));
}
{
    ClientMessage clientMessage = ListIteratorCodec.encodeRequest(    aString   );
    ListIteratorCodec.RequestParameters params = ListIteratorCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = ListIteratorCodec.encodeResponse(    datas   );
    ListIteratorCodec.ResponseParameters params = ListIteratorCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(datas, params.response));
}
{
    ClientMessage clientMessage = ListListIteratorCodec.encodeRequest(    aString ,    anInt   );
    ListListIteratorCodec.RequestParameters params = ListListIteratorCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.index));
}
{
    ClientMessage clientMessage = ListListIteratorCodec.encodeResponse(    datas   );
    ListListIteratorCodec.ResponseParameters params = ListListIteratorCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(datas, params.response));
}
{
    ClientMessage clientMessage = SetSizeCodec.encodeRequest(    aString   );
    SetSizeCodec.RequestParameters params = SetSizeCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = SetSizeCodec.encodeResponse(    anInt   );
    SetSizeCodec.ResponseParameters params = SetSizeCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anInt, params.response));
}
{
    ClientMessage clientMessage = SetContainsCodec.encodeRequest(    aString ,    aData   );
    SetContainsCodec.RequestParameters params = SetContainsCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.value));
}
{
    ClientMessage clientMessage = SetContainsCodec.encodeResponse(    aBoolean   );
    SetContainsCodec.ResponseParameters params = SetContainsCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = SetContainsAllCodec.encodeRequest(    aString ,    datas   );
    SetContainsAllCodec.RequestParameters params = SetContainsAllCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.items));
}
{
    ClientMessage clientMessage = SetContainsAllCodec.encodeResponse(    aBoolean   );
    SetContainsAllCodec.ResponseParameters params = SetContainsAllCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = SetAddCodec.encodeRequest(    aString ,    aData   );
    SetAddCodec.RequestParameters params = SetAddCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.value));
}
{
    ClientMessage clientMessage = SetAddCodec.encodeResponse(    aBoolean   );
    SetAddCodec.ResponseParameters params = SetAddCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = SetRemoveCodec.encodeRequest(    aString ,    aData   );
    SetRemoveCodec.RequestParameters params = SetRemoveCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.value));
}
{
    ClientMessage clientMessage = SetRemoveCodec.encodeResponse(    aBoolean   );
    SetRemoveCodec.ResponseParameters params = SetRemoveCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = SetAddAllCodec.encodeRequest(    aString ,    datas   );
    SetAddAllCodec.RequestParameters params = SetAddAllCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.valueList));
}
{
    ClientMessage clientMessage = SetAddAllCodec.encodeResponse(    aBoolean   );
    SetAddAllCodec.ResponseParameters params = SetAddAllCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = SetCompareAndRemoveAllCodec.encodeRequest(    aString ,    datas   );
    SetCompareAndRemoveAllCodec.RequestParameters params = SetCompareAndRemoveAllCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.values));
}
{
    ClientMessage clientMessage = SetCompareAndRemoveAllCodec.encodeResponse(    aBoolean   );
    SetCompareAndRemoveAllCodec.ResponseParameters params = SetCompareAndRemoveAllCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = SetCompareAndRetainAllCodec.encodeRequest(    aString ,    datas   );
    SetCompareAndRetainAllCodec.RequestParameters params = SetCompareAndRetainAllCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.values));
}
{
    ClientMessage clientMessage = SetCompareAndRetainAllCodec.encodeResponse(    aBoolean   );
    SetCompareAndRetainAllCodec.ResponseParameters params = SetCompareAndRetainAllCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = SetClearCodec.encodeRequest(    aString   );
    SetClearCodec.RequestParameters params = SetClearCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = SetClearCodec.encodeResponse( );
    SetClearCodec.ResponseParameters params = SetClearCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = SetGetAllCodec.encodeRequest(    aString   );
    SetGetAllCodec.RequestParameters params = SetGetAllCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = SetGetAllCodec.encodeResponse(    datas   );
    SetGetAllCodec.ResponseParameters params = SetGetAllCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(datas, params.response));
}
{
    ClientMessage clientMessage = SetAddListenerCodec.encodeRequest(    aString ,    aBoolean ,    aBoolean   );
    SetAddListenerCodec.RequestParameters params = SetAddListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aBoolean, params.includeValue));
            assertTrue(isEqual(aBoolean, params.localOnly));
}
{
    ClientMessage clientMessage = SetAddListenerCodec.encodeResponse(    aString   );
    SetAddListenerCodec.ResponseParameters params = SetAddListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.response));
}
{
    class SetAddListenerCodecHandler extends SetAddListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  Data
 item ,   String
 uuid ,   int
 eventType   ) {
                          assertTrue(isEqual(aData, item));
                          assertTrue(isEqual(aString, uuid));
                          assertTrue(isEqual(anInt, eventType));
        }
    }
    SetAddListenerCodecHandler handler = new SetAddListenerCodecHandler();
    {
        ClientMessage clientMessage = SetAddListenerCodec.encodeItemEvent( aData ,  aString ,  anInt   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
}
{
    ClientMessage clientMessage = SetRemoveListenerCodec.encodeRequest(    aString ,    aString   );
    SetRemoveListenerCodec.RequestParameters params = SetRemoveListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.registrationId));
}
{
    ClientMessage clientMessage = SetRemoveListenerCodec.encodeResponse(    aBoolean   );
    SetRemoveListenerCodec.ResponseParameters params = SetRemoveListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = SetIsEmptyCodec.encodeRequest(    aString   );
    SetIsEmptyCodec.RequestParameters params = SetIsEmptyCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = SetIsEmptyCodec.encodeResponse(    aBoolean   );
    SetIsEmptyCodec.ResponseParameters params = SetIsEmptyCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = LockIsLockedCodec.encodeRequest(    aString   );
    LockIsLockedCodec.RequestParameters params = LockIsLockedCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = LockIsLockedCodec.encodeResponse(    aBoolean   );
    LockIsLockedCodec.ResponseParameters params = LockIsLockedCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = LockIsLockedByCurrentThreadCodec.encodeRequest(    aString ,    aLong   );
    LockIsLockedByCurrentThreadCodec.RequestParameters params = LockIsLockedByCurrentThreadCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = LockIsLockedByCurrentThreadCodec.encodeResponse(    aBoolean   );
    LockIsLockedByCurrentThreadCodec.ResponseParameters params = LockIsLockedByCurrentThreadCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = LockGetLockCountCodec.encodeRequest(    aString   );
    LockGetLockCountCodec.RequestParameters params = LockGetLockCountCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = LockGetLockCountCodec.encodeResponse(    anInt   );
    LockGetLockCountCodec.ResponseParameters params = LockGetLockCountCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anInt, params.response));
}
{
    ClientMessage clientMessage = LockGetRemainingLeaseTimeCodec.encodeRequest(    aString   );
    LockGetRemainingLeaseTimeCodec.RequestParameters params = LockGetRemainingLeaseTimeCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = LockGetRemainingLeaseTimeCodec.encodeResponse(    aLong   );
    LockGetRemainingLeaseTimeCodec.ResponseParameters params = LockGetRemainingLeaseTimeCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aLong, params.response));
}
{
    ClientMessage clientMessage = LockLockCodec.encodeRequest(    aString ,    aLong ,    aLong ,    aLong   );
    LockLockCodec.RequestParameters params = LockLockCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.leaseTime));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.referenceId));
}
{
    ClientMessage clientMessage = LockLockCodec.encodeResponse( );
    LockLockCodec.ResponseParameters params = LockLockCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = LockUnlockCodec.encodeRequest(    aString ,    aLong ,    aLong   );
    LockUnlockCodec.RequestParameters params = LockUnlockCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.referenceId));
}
{
    ClientMessage clientMessage = LockUnlockCodec.encodeResponse( );
    LockUnlockCodec.ResponseParameters params = LockUnlockCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = LockForceUnlockCodec.encodeRequest(    aString ,    aLong   );
    LockForceUnlockCodec.RequestParameters params = LockForceUnlockCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.referenceId));
}
{
    ClientMessage clientMessage = LockForceUnlockCodec.encodeResponse( );
    LockForceUnlockCodec.ResponseParameters params = LockForceUnlockCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = LockTryLockCodec.encodeRequest(    aString ,    aLong ,    aLong ,    aLong ,    aLong   );
    LockTryLockCodec.RequestParameters params = LockTryLockCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.lease));
            assertTrue(isEqual(aLong, params.timeout));
            assertTrue(isEqual(aLong, params.referenceId));
}
{
    ClientMessage clientMessage = LockTryLockCodec.encodeResponse(    aBoolean   );
    LockTryLockCodec.ResponseParameters params = LockTryLockCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = ConditionAwaitCodec.encodeRequest(    aString ,    aLong ,    aLong ,    aString ,    aLong   );
    ConditionAwaitCodec.RequestParameters params = ConditionAwaitCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.timeout));
            assertTrue(isEqual(aString, params.lockName));
            assertTrue(isEqual(aLong, params.referenceId));
}
{
    ClientMessage clientMessage = ConditionAwaitCodec.encodeResponse(    aBoolean   );
    ConditionAwaitCodec.ResponseParameters params = ConditionAwaitCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = ConditionBeforeAwaitCodec.encodeRequest(    aString ,    aLong ,    aString ,    aLong   );
    ConditionBeforeAwaitCodec.RequestParameters params = ConditionBeforeAwaitCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aString, params.lockName));
            assertTrue(isEqual(aLong, params.referenceId));
}
{
    ClientMessage clientMessage = ConditionBeforeAwaitCodec.encodeResponse( );
    ConditionBeforeAwaitCodec.ResponseParameters params = ConditionBeforeAwaitCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = ConditionSignalCodec.encodeRequest(    aString ,    aLong ,    aString   );
    ConditionSignalCodec.RequestParameters params = ConditionSignalCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aString, params.lockName));
}
{
    ClientMessage clientMessage = ConditionSignalCodec.encodeResponse( );
    ConditionSignalCodec.ResponseParameters params = ConditionSignalCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = ConditionSignalAllCodec.encodeRequest(    aString ,    aLong ,    aString   );
    ConditionSignalAllCodec.RequestParameters params = ConditionSignalAllCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aString, params.lockName));
}
{
    ClientMessage clientMessage = ConditionSignalAllCodec.encodeResponse( );
    ConditionSignalAllCodec.ResponseParameters params = ConditionSignalAllCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = ExecutorServiceShutdownCodec.encodeRequest(    aString   );
    ExecutorServiceShutdownCodec.RequestParameters params = ExecutorServiceShutdownCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = ExecutorServiceShutdownCodec.encodeResponse( );
    ExecutorServiceShutdownCodec.ResponseParameters params = ExecutorServiceShutdownCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = ExecutorServiceIsShutdownCodec.encodeRequest(    aString   );
    ExecutorServiceIsShutdownCodec.RequestParameters params = ExecutorServiceIsShutdownCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = ExecutorServiceIsShutdownCodec.encodeResponse(    aBoolean   );
    ExecutorServiceIsShutdownCodec.ResponseParameters params = ExecutorServiceIsShutdownCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = ExecutorServiceCancelOnPartitionCodec.encodeRequest(    aString ,    anInt ,    aBoolean   );
    ExecutorServiceCancelOnPartitionCodec.RequestParameters params = ExecutorServiceCancelOnPartitionCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.uuid));
            assertTrue(isEqual(anInt, params.partitionId));
            assertTrue(isEqual(aBoolean, params.interrupt));
}
{
    ClientMessage clientMessage = ExecutorServiceCancelOnPartitionCodec.encodeResponse(    aBoolean   );
    ExecutorServiceCancelOnPartitionCodec.ResponseParameters params = ExecutorServiceCancelOnPartitionCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = ExecutorServiceCancelOnAddressCodec.encodeRequest(    aString ,    anAddress ,    aBoolean   );
    ExecutorServiceCancelOnAddressCodec.RequestParameters params = ExecutorServiceCancelOnAddressCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.uuid));
            assertTrue(isEqual(anAddress, params.address));
            assertTrue(isEqual(aBoolean, params.interrupt));
}
{
    ClientMessage clientMessage = ExecutorServiceCancelOnAddressCodec.encodeResponse(    aBoolean   );
    ExecutorServiceCancelOnAddressCodec.ResponseParameters params = ExecutorServiceCancelOnAddressCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = ExecutorServiceSubmitToPartitionCodec.encodeRequest(    aString ,    aString ,    aData ,    anInt   );
    ExecutorServiceSubmitToPartitionCodec.RequestParameters params = ExecutorServiceSubmitToPartitionCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.uuid));
            assertTrue(isEqual(aData, params.callable));
            assertTrue(isEqual(anInt, params.partitionId));
}
{
    ClientMessage clientMessage = ExecutorServiceSubmitToPartitionCodec.encodeResponse(    aData   );
    ExecutorServiceSubmitToPartitionCodec.ResponseParameters params = ExecutorServiceSubmitToPartitionCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = ExecutorServiceSubmitToAddressCodec.encodeRequest(    aString ,    aString ,    aData ,    anAddress   );
    ExecutorServiceSubmitToAddressCodec.RequestParameters params = ExecutorServiceSubmitToAddressCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.uuid));
            assertTrue(isEqual(aData, params.callable));
            assertTrue(isEqual(anAddress, params.address));
}
{
    ClientMessage clientMessage = ExecutorServiceSubmitToAddressCodec.encodeResponse(    aData   );
    ExecutorServiceSubmitToAddressCodec.ResponseParameters params = ExecutorServiceSubmitToAddressCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = AtomicLongApplyCodec.encodeRequest(    aString ,    aData   );
    AtomicLongApplyCodec.RequestParameters params = AtomicLongApplyCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.function));
}
{
    ClientMessage clientMessage = AtomicLongApplyCodec.encodeResponse(    aData   );
    AtomicLongApplyCodec.ResponseParameters params = AtomicLongApplyCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = AtomicLongAlterCodec.encodeRequest(    aString ,    aData   );
    AtomicLongAlterCodec.RequestParameters params = AtomicLongAlterCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.function));
}
{
    ClientMessage clientMessage = AtomicLongAlterCodec.encodeResponse( );
    AtomicLongAlterCodec.ResponseParameters params = AtomicLongAlterCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = AtomicLongAlterAndGetCodec.encodeRequest(    aString ,    aData   );
    AtomicLongAlterAndGetCodec.RequestParameters params = AtomicLongAlterAndGetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.function));
}
{
    ClientMessage clientMessage = AtomicLongAlterAndGetCodec.encodeResponse(    aLong   );
    AtomicLongAlterAndGetCodec.ResponseParameters params = AtomicLongAlterAndGetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aLong, params.response));
}
{
    ClientMessage clientMessage = AtomicLongGetAndAlterCodec.encodeRequest(    aString ,    aData   );
    AtomicLongGetAndAlterCodec.RequestParameters params = AtomicLongGetAndAlterCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.function));
}
{
    ClientMessage clientMessage = AtomicLongGetAndAlterCodec.encodeResponse(    aLong   );
    AtomicLongGetAndAlterCodec.ResponseParameters params = AtomicLongGetAndAlterCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aLong, params.response));
}
{
    ClientMessage clientMessage = AtomicLongAddAndGetCodec.encodeRequest(    aString ,    aLong   );
    AtomicLongAddAndGetCodec.RequestParameters params = AtomicLongAddAndGetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.delta));
}
{
    ClientMessage clientMessage = AtomicLongAddAndGetCodec.encodeResponse(    aLong   );
    AtomicLongAddAndGetCodec.ResponseParameters params = AtomicLongAddAndGetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aLong, params.response));
}
{
    ClientMessage clientMessage = AtomicLongCompareAndSetCodec.encodeRequest(    aString ,    aLong ,    aLong   );
    AtomicLongCompareAndSetCodec.RequestParameters params = AtomicLongCompareAndSetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.expected));
            assertTrue(isEqual(aLong, params.updated));
}
{
    ClientMessage clientMessage = AtomicLongCompareAndSetCodec.encodeResponse(    aBoolean   );
    AtomicLongCompareAndSetCodec.ResponseParameters params = AtomicLongCompareAndSetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = AtomicLongDecrementAndGetCodec.encodeRequest(    aString   );
    AtomicLongDecrementAndGetCodec.RequestParameters params = AtomicLongDecrementAndGetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = AtomicLongDecrementAndGetCodec.encodeResponse(    aLong   );
    AtomicLongDecrementAndGetCodec.ResponseParameters params = AtomicLongDecrementAndGetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aLong, params.response));
}
{
    ClientMessage clientMessage = AtomicLongGetCodec.encodeRequest(    aString   );
    AtomicLongGetCodec.RequestParameters params = AtomicLongGetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = AtomicLongGetCodec.encodeResponse(    aLong   );
    AtomicLongGetCodec.ResponseParameters params = AtomicLongGetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aLong, params.response));
}
{
    ClientMessage clientMessage = AtomicLongGetAndAddCodec.encodeRequest(    aString ,    aLong   );
    AtomicLongGetAndAddCodec.RequestParameters params = AtomicLongGetAndAddCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.delta));
}
{
    ClientMessage clientMessage = AtomicLongGetAndAddCodec.encodeResponse(    aLong   );
    AtomicLongGetAndAddCodec.ResponseParameters params = AtomicLongGetAndAddCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aLong, params.response));
}
{
    ClientMessage clientMessage = AtomicLongGetAndSetCodec.encodeRequest(    aString ,    aLong   );
    AtomicLongGetAndSetCodec.RequestParameters params = AtomicLongGetAndSetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.newValue));
}
{
    ClientMessage clientMessage = AtomicLongGetAndSetCodec.encodeResponse(    aLong   );
    AtomicLongGetAndSetCodec.ResponseParameters params = AtomicLongGetAndSetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aLong, params.response));
}
{
    ClientMessage clientMessage = AtomicLongIncrementAndGetCodec.encodeRequest(    aString   );
    AtomicLongIncrementAndGetCodec.RequestParameters params = AtomicLongIncrementAndGetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = AtomicLongIncrementAndGetCodec.encodeResponse(    aLong   );
    AtomicLongIncrementAndGetCodec.ResponseParameters params = AtomicLongIncrementAndGetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aLong, params.response));
}
{
    ClientMessage clientMessage = AtomicLongGetAndIncrementCodec.encodeRequest(    aString   );
    AtomicLongGetAndIncrementCodec.RequestParameters params = AtomicLongGetAndIncrementCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = AtomicLongGetAndIncrementCodec.encodeResponse(    aLong   );
    AtomicLongGetAndIncrementCodec.ResponseParameters params = AtomicLongGetAndIncrementCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aLong, params.response));
}
{
    ClientMessage clientMessage = AtomicLongSetCodec.encodeRequest(    aString ,    aLong   );
    AtomicLongSetCodec.RequestParameters params = AtomicLongSetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.newValue));
}
{
    ClientMessage clientMessage = AtomicLongSetCodec.encodeResponse( );
    AtomicLongSetCodec.ResponseParameters params = AtomicLongSetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = AtomicReferenceApplyCodec.encodeRequest(    aString ,    aData   );
    AtomicReferenceApplyCodec.RequestParameters params = AtomicReferenceApplyCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.function));
}
{
    ClientMessage clientMessage = AtomicReferenceApplyCodec.encodeResponse(    aData   );
    AtomicReferenceApplyCodec.ResponseParameters params = AtomicReferenceApplyCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = AtomicReferenceAlterCodec.encodeRequest(    aString ,    aData   );
    AtomicReferenceAlterCodec.RequestParameters params = AtomicReferenceAlterCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.function));
}
{
    ClientMessage clientMessage = AtomicReferenceAlterCodec.encodeResponse( );
    AtomicReferenceAlterCodec.ResponseParameters params = AtomicReferenceAlterCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = AtomicReferenceAlterAndGetCodec.encodeRequest(    aString ,    aData   );
    AtomicReferenceAlterAndGetCodec.RequestParameters params = AtomicReferenceAlterAndGetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.function));
}
{
    ClientMessage clientMessage = AtomicReferenceAlterAndGetCodec.encodeResponse(    aData   );
    AtomicReferenceAlterAndGetCodec.ResponseParameters params = AtomicReferenceAlterAndGetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = AtomicReferenceGetAndAlterCodec.encodeRequest(    aString ,    aData   );
    AtomicReferenceGetAndAlterCodec.RequestParameters params = AtomicReferenceGetAndAlterCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.function));
}
{
    ClientMessage clientMessage = AtomicReferenceGetAndAlterCodec.encodeResponse(    aData   );
    AtomicReferenceGetAndAlterCodec.ResponseParameters params = AtomicReferenceGetAndAlterCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = AtomicReferenceContainsCodec.encodeRequest(    aString ,    aData   );
    AtomicReferenceContainsCodec.RequestParameters params = AtomicReferenceContainsCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.expected));
}
{
    ClientMessage clientMessage = AtomicReferenceContainsCodec.encodeResponse(    aBoolean   );
    AtomicReferenceContainsCodec.ResponseParameters params = AtomicReferenceContainsCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = AtomicReferenceCompareAndSetCodec.encodeRequest(    aString ,    aData ,    aData   );
    AtomicReferenceCompareAndSetCodec.RequestParameters params = AtomicReferenceCompareAndSetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.expected));
            assertTrue(isEqual(aData, params.updated));
}
{
    ClientMessage clientMessage = AtomicReferenceCompareAndSetCodec.encodeResponse(    aBoolean   );
    AtomicReferenceCompareAndSetCodec.ResponseParameters params = AtomicReferenceCompareAndSetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = AtomicReferenceGetCodec.encodeRequest(    aString   );
    AtomicReferenceGetCodec.RequestParameters params = AtomicReferenceGetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = AtomicReferenceGetCodec.encodeResponse(    aData   );
    AtomicReferenceGetCodec.ResponseParameters params = AtomicReferenceGetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = AtomicReferenceSetCodec.encodeRequest(    aString ,    aData   );
    AtomicReferenceSetCodec.RequestParameters params = AtomicReferenceSetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.newValue));
}
{
    ClientMessage clientMessage = AtomicReferenceSetCodec.encodeResponse( );
    AtomicReferenceSetCodec.ResponseParameters params = AtomicReferenceSetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = AtomicReferenceClearCodec.encodeRequest(    aString   );
    AtomicReferenceClearCodec.RequestParameters params = AtomicReferenceClearCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = AtomicReferenceClearCodec.encodeResponse( );
    AtomicReferenceClearCodec.ResponseParameters params = AtomicReferenceClearCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = AtomicReferenceGetAndSetCodec.encodeRequest(    aString ,    aData   );
    AtomicReferenceGetAndSetCodec.RequestParameters params = AtomicReferenceGetAndSetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.newValue));
}
{
    ClientMessage clientMessage = AtomicReferenceGetAndSetCodec.encodeResponse(    aData   );
    AtomicReferenceGetAndSetCodec.ResponseParameters params = AtomicReferenceGetAndSetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = AtomicReferenceSetAndGetCodec.encodeRequest(    aString ,    aData   );
    AtomicReferenceSetAndGetCodec.RequestParameters params = AtomicReferenceSetAndGetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.newValue));
}
{
    ClientMessage clientMessage = AtomicReferenceSetAndGetCodec.encodeResponse(    aData   );
    AtomicReferenceSetAndGetCodec.ResponseParameters params = AtomicReferenceSetAndGetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = AtomicReferenceIsNullCodec.encodeRequest(    aString   );
    AtomicReferenceIsNullCodec.RequestParameters params = AtomicReferenceIsNullCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = AtomicReferenceIsNullCodec.encodeResponse(    aBoolean   );
    AtomicReferenceIsNullCodec.ResponseParameters params = AtomicReferenceIsNullCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = CountDownLatchAwaitCodec.encodeRequest(    aString ,    aLong   );
    CountDownLatchAwaitCodec.RequestParameters params = CountDownLatchAwaitCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.timeout));
}
{
    ClientMessage clientMessage = CountDownLatchAwaitCodec.encodeResponse(    aBoolean   );
    CountDownLatchAwaitCodec.ResponseParameters params = CountDownLatchAwaitCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = CountDownLatchCountDownCodec.encodeRequest(    aString   );
    CountDownLatchCountDownCodec.RequestParameters params = CountDownLatchCountDownCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = CountDownLatchCountDownCodec.encodeResponse( );
    CountDownLatchCountDownCodec.ResponseParameters params = CountDownLatchCountDownCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = CountDownLatchGetCountCodec.encodeRequest(    aString   );
    CountDownLatchGetCountCodec.RequestParameters params = CountDownLatchGetCountCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = CountDownLatchGetCountCodec.encodeResponse(    anInt   );
    CountDownLatchGetCountCodec.ResponseParameters params = CountDownLatchGetCountCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anInt, params.response));
}
{
    ClientMessage clientMessage = CountDownLatchTrySetCountCodec.encodeRequest(    aString ,    anInt   );
    CountDownLatchTrySetCountCodec.RequestParameters params = CountDownLatchTrySetCountCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.count));
}
{
    ClientMessage clientMessage = CountDownLatchTrySetCountCodec.encodeResponse(    aBoolean   );
    CountDownLatchTrySetCountCodec.ResponseParameters params = CountDownLatchTrySetCountCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = SemaphoreInitCodec.encodeRequest(    aString ,    anInt   );
    SemaphoreInitCodec.RequestParameters params = SemaphoreInitCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.permits));
}
{
    ClientMessage clientMessage = SemaphoreInitCodec.encodeResponse(    aBoolean   );
    SemaphoreInitCodec.ResponseParameters params = SemaphoreInitCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = SemaphoreAcquireCodec.encodeRequest(    aString ,    anInt   );
    SemaphoreAcquireCodec.RequestParameters params = SemaphoreAcquireCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.permits));
}
{
    ClientMessage clientMessage = SemaphoreAcquireCodec.encodeResponse( );
    SemaphoreAcquireCodec.ResponseParameters params = SemaphoreAcquireCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = SemaphoreAvailablePermitsCodec.encodeRequest(    aString   );
    SemaphoreAvailablePermitsCodec.RequestParameters params = SemaphoreAvailablePermitsCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = SemaphoreAvailablePermitsCodec.encodeResponse(    anInt   );
    SemaphoreAvailablePermitsCodec.ResponseParameters params = SemaphoreAvailablePermitsCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anInt, params.response));
}
{
    ClientMessage clientMessage = SemaphoreDrainPermitsCodec.encodeRequest(    aString   );
    SemaphoreDrainPermitsCodec.RequestParameters params = SemaphoreDrainPermitsCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = SemaphoreDrainPermitsCodec.encodeResponse(    anInt   );
    SemaphoreDrainPermitsCodec.ResponseParameters params = SemaphoreDrainPermitsCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anInt, params.response));
}
{
    ClientMessage clientMessage = SemaphoreReducePermitsCodec.encodeRequest(    aString ,    anInt   );
    SemaphoreReducePermitsCodec.RequestParameters params = SemaphoreReducePermitsCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.reduction));
}
{
    ClientMessage clientMessage = SemaphoreReducePermitsCodec.encodeResponse( );
    SemaphoreReducePermitsCodec.ResponseParameters params = SemaphoreReducePermitsCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = SemaphoreReleaseCodec.encodeRequest(    aString ,    anInt   );
    SemaphoreReleaseCodec.RequestParameters params = SemaphoreReleaseCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.permits));
}
{
    ClientMessage clientMessage = SemaphoreReleaseCodec.encodeResponse( );
    SemaphoreReleaseCodec.ResponseParameters params = SemaphoreReleaseCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = SemaphoreTryAcquireCodec.encodeRequest(    aString ,    anInt ,    aLong   );
    SemaphoreTryAcquireCodec.RequestParameters params = SemaphoreTryAcquireCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.permits));
            assertTrue(isEqual(aLong, params.timeout));
}
{
    ClientMessage clientMessage = SemaphoreTryAcquireCodec.encodeResponse(    aBoolean   );
    SemaphoreTryAcquireCodec.ResponseParameters params = SemaphoreTryAcquireCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = SemaphoreIncreasePermitsCodec.encodeRequest(    aString ,    anInt   );
    SemaphoreIncreasePermitsCodec.RequestParameters params = SemaphoreIncreasePermitsCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.increase));
}
{
    ClientMessage clientMessage = SemaphoreIncreasePermitsCodec.encodeResponse( );
    SemaphoreIncreasePermitsCodec.ResponseParameters params = SemaphoreIncreasePermitsCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = ReplicatedMapPutCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
    ReplicatedMapPutCodec.RequestParameters params = ReplicatedMapPutCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(aLong, params.ttl));
}
{
    ClientMessage clientMessage = ReplicatedMapPutCodec.encodeResponse(    aData   );
    ReplicatedMapPutCodec.ResponseParameters params = ReplicatedMapPutCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = ReplicatedMapSizeCodec.encodeRequest(    aString   );
    ReplicatedMapSizeCodec.RequestParameters params = ReplicatedMapSizeCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = ReplicatedMapSizeCodec.encodeResponse(    anInt   );
    ReplicatedMapSizeCodec.ResponseParameters params = ReplicatedMapSizeCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anInt, params.response));
}
{
    ClientMessage clientMessage = ReplicatedMapIsEmptyCodec.encodeRequest(    aString   );
    ReplicatedMapIsEmptyCodec.RequestParameters params = ReplicatedMapIsEmptyCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = ReplicatedMapIsEmptyCodec.encodeResponse(    aBoolean   );
    ReplicatedMapIsEmptyCodec.ResponseParameters params = ReplicatedMapIsEmptyCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = ReplicatedMapContainsKeyCodec.encodeRequest(    aString ,    aData   );
    ReplicatedMapContainsKeyCodec.RequestParameters params = ReplicatedMapContainsKeyCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
}
{
    ClientMessage clientMessage = ReplicatedMapContainsKeyCodec.encodeResponse(    aBoolean   );
    ReplicatedMapContainsKeyCodec.ResponseParameters params = ReplicatedMapContainsKeyCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = ReplicatedMapContainsValueCodec.encodeRequest(    aString ,    aData   );
    ReplicatedMapContainsValueCodec.RequestParameters params = ReplicatedMapContainsValueCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.value));
}
{
    ClientMessage clientMessage = ReplicatedMapContainsValueCodec.encodeResponse(    aBoolean   );
    ReplicatedMapContainsValueCodec.ResponseParameters params = ReplicatedMapContainsValueCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = ReplicatedMapGetCodec.encodeRequest(    aString ,    aData   );
    ReplicatedMapGetCodec.RequestParameters params = ReplicatedMapGetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
}
{
    ClientMessage clientMessage = ReplicatedMapGetCodec.encodeResponse(    aData   );
    ReplicatedMapGetCodec.ResponseParameters params = ReplicatedMapGetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = ReplicatedMapRemoveCodec.encodeRequest(    aString ,    aData   );
    ReplicatedMapRemoveCodec.RequestParameters params = ReplicatedMapRemoveCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
}
{
    ClientMessage clientMessage = ReplicatedMapRemoveCodec.encodeResponse(    aData   );
    ReplicatedMapRemoveCodec.ResponseParameters params = ReplicatedMapRemoveCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = ReplicatedMapPutAllCodec.encodeRequest(    aString ,    aListOfEntry   );
    ReplicatedMapPutAllCodec.RequestParameters params = ReplicatedMapPutAllCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aListOfEntry, params.entries));
}
{
    ClientMessage clientMessage = ReplicatedMapPutAllCodec.encodeResponse( );
    ReplicatedMapPutAllCodec.ResponseParameters params = ReplicatedMapPutAllCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = ReplicatedMapClearCodec.encodeRequest(    aString   );
    ReplicatedMapClearCodec.RequestParameters params = ReplicatedMapClearCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = ReplicatedMapClearCodec.encodeResponse( );
    ReplicatedMapClearCodec.ResponseParameters params = ReplicatedMapClearCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.encodeRequest(    aString ,    aData ,    aData ,    aBoolean   );
    ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.RequestParameters params = ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.predicate));
            assertTrue(isEqual(aBoolean, params.localOnly));
}
{
    ClientMessage clientMessage = ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.encodeResponse(    aString   );
    ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.ResponseParameters params = ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.response));
}
{
    class ReplicatedMapAddEntryListenerToKeyWithPredicateCodecHandler extends ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.AbstractEventHandler {
        @Override
        public void handle(  Data
 key ,   Data
 value ,   Data
 oldValue ,   Data
 mergingValue ,   int
 eventType ,   String
 uuid ,   int
 numberOfAffectedEntries   ) {
                          assertTrue(isEqual(aData, key));
                          assertTrue(isEqual(aData, value));
                          assertTrue(isEqual(aData, oldValue));
                          assertTrue(isEqual(aData, mergingValue));
                          assertTrue(isEqual(anInt, eventType));
                          assertTrue(isEqual(aString, uuid));
                          assertTrue(isEqual(anInt, numberOfAffectedEntries));
        }
    }
    ReplicatedMapAddEntryListenerToKeyWithPredicateCodecHandler handler = new ReplicatedMapAddEntryListenerToKeyWithPredicateCodecHandler();
    {
        ClientMessage clientMessage = ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
}
{
    ClientMessage clientMessage = ReplicatedMapAddEntryListenerWithPredicateCodec.encodeRequest(    aString ,    aData ,    aBoolean   );
    ReplicatedMapAddEntryListenerWithPredicateCodec.RequestParameters params = ReplicatedMapAddEntryListenerWithPredicateCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.predicate));
            assertTrue(isEqual(aBoolean, params.localOnly));
}
{
    ClientMessage clientMessage = ReplicatedMapAddEntryListenerWithPredicateCodec.encodeResponse(    aString   );
    ReplicatedMapAddEntryListenerWithPredicateCodec.ResponseParameters params = ReplicatedMapAddEntryListenerWithPredicateCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.response));
}
{
    class ReplicatedMapAddEntryListenerWithPredicateCodecHandler extends ReplicatedMapAddEntryListenerWithPredicateCodec.AbstractEventHandler {
        @Override
        public void handle(  Data
 key ,   Data
 value ,   Data
 oldValue ,   Data
 mergingValue ,   int
 eventType ,   String
 uuid ,   int
 numberOfAffectedEntries   ) {
                          assertTrue(isEqual(aData, key));
                          assertTrue(isEqual(aData, value));
                          assertTrue(isEqual(aData, oldValue));
                          assertTrue(isEqual(aData, mergingValue));
                          assertTrue(isEqual(anInt, eventType));
                          assertTrue(isEqual(aString, uuid));
                          assertTrue(isEqual(anInt, numberOfAffectedEntries));
        }
    }
    ReplicatedMapAddEntryListenerWithPredicateCodecHandler handler = new ReplicatedMapAddEntryListenerWithPredicateCodecHandler();
    {
        ClientMessage clientMessage = ReplicatedMapAddEntryListenerWithPredicateCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
}
{
    ClientMessage clientMessage = ReplicatedMapAddEntryListenerToKeyCodec.encodeRequest(    aString ,    aData ,    aBoolean   );
    ReplicatedMapAddEntryListenerToKeyCodec.RequestParameters params = ReplicatedMapAddEntryListenerToKeyCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aBoolean, params.localOnly));
}
{
    ClientMessage clientMessage = ReplicatedMapAddEntryListenerToKeyCodec.encodeResponse(    aString   );
    ReplicatedMapAddEntryListenerToKeyCodec.ResponseParameters params = ReplicatedMapAddEntryListenerToKeyCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.response));
}
{
    class ReplicatedMapAddEntryListenerToKeyCodecHandler extends ReplicatedMapAddEntryListenerToKeyCodec.AbstractEventHandler {
        @Override
        public void handle(  Data
 key ,   Data
 value ,   Data
 oldValue ,   Data
 mergingValue ,   int
 eventType ,   String
 uuid ,   int
 numberOfAffectedEntries   ) {
                          assertTrue(isEqual(aData, key));
                          assertTrue(isEqual(aData, value));
                          assertTrue(isEqual(aData, oldValue));
                          assertTrue(isEqual(aData, mergingValue));
                          assertTrue(isEqual(anInt, eventType));
                          assertTrue(isEqual(aString, uuid));
                          assertTrue(isEqual(anInt, numberOfAffectedEntries));
        }
    }
    ReplicatedMapAddEntryListenerToKeyCodecHandler handler = new ReplicatedMapAddEntryListenerToKeyCodecHandler();
    {
        ClientMessage clientMessage = ReplicatedMapAddEntryListenerToKeyCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
}
{
    ClientMessage clientMessage = ReplicatedMapAddEntryListenerCodec.encodeRequest(    aString ,    aBoolean   );
    ReplicatedMapAddEntryListenerCodec.RequestParameters params = ReplicatedMapAddEntryListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aBoolean, params.localOnly));
}
{
    ClientMessage clientMessage = ReplicatedMapAddEntryListenerCodec.encodeResponse(    aString   );
    ReplicatedMapAddEntryListenerCodec.ResponseParameters params = ReplicatedMapAddEntryListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.response));
}
{
    class ReplicatedMapAddEntryListenerCodecHandler extends ReplicatedMapAddEntryListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  Data
 key ,   Data
 value ,   Data
 oldValue ,   Data
 mergingValue ,   int
 eventType ,   String
 uuid ,   int
 numberOfAffectedEntries   ) {
                          assertTrue(isEqual(aData, key));
                          assertTrue(isEqual(aData, value));
                          assertTrue(isEqual(aData, oldValue));
                          assertTrue(isEqual(aData, mergingValue));
                          assertTrue(isEqual(anInt, eventType));
                          assertTrue(isEqual(aString, uuid));
                          assertTrue(isEqual(anInt, numberOfAffectedEntries));
        }
    }
    ReplicatedMapAddEntryListenerCodecHandler handler = new ReplicatedMapAddEntryListenerCodecHandler();
    {
        ClientMessage clientMessage = ReplicatedMapAddEntryListenerCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
}
{
    ClientMessage clientMessage = ReplicatedMapRemoveEntryListenerCodec.encodeRequest(    aString ,    aString   );
    ReplicatedMapRemoveEntryListenerCodec.RequestParameters params = ReplicatedMapRemoveEntryListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.registrationId));
}
{
    ClientMessage clientMessage = ReplicatedMapRemoveEntryListenerCodec.encodeResponse(    aBoolean   );
    ReplicatedMapRemoveEntryListenerCodec.ResponseParameters params = ReplicatedMapRemoveEntryListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = ReplicatedMapKeySetCodec.encodeRequest(    aString   );
    ReplicatedMapKeySetCodec.RequestParameters params = ReplicatedMapKeySetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = ReplicatedMapKeySetCodec.encodeResponse(    datas   );
    ReplicatedMapKeySetCodec.ResponseParameters params = ReplicatedMapKeySetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(datas, params.response));
}
{
    ClientMessage clientMessage = ReplicatedMapValuesCodec.encodeRequest(    aString   );
    ReplicatedMapValuesCodec.RequestParameters params = ReplicatedMapValuesCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = ReplicatedMapValuesCodec.encodeResponse(    datas   );
    ReplicatedMapValuesCodec.ResponseParameters params = ReplicatedMapValuesCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(datas, params.response));
}
{
    ClientMessage clientMessage = ReplicatedMapEntrySetCodec.encodeRequest(    aString   );
    ReplicatedMapEntrySetCodec.RequestParameters params = ReplicatedMapEntrySetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = ReplicatedMapEntrySetCodec.encodeResponse(    aListOfEntry   );
    ReplicatedMapEntrySetCodec.ResponseParameters params = ReplicatedMapEntrySetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aListOfEntry, params.response));
}
{
    ClientMessage clientMessage = ReplicatedMapAddNearCacheEntryListenerCodec.encodeRequest(    aString ,    aBoolean ,    aBoolean   );
    ReplicatedMapAddNearCacheEntryListenerCodec.RequestParameters params = ReplicatedMapAddNearCacheEntryListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aBoolean, params.includeValue));
            assertTrue(isEqual(aBoolean, params.localOnly));
}
{
    ClientMessage clientMessage = ReplicatedMapAddNearCacheEntryListenerCodec.encodeResponse(    aString   );
    ReplicatedMapAddNearCacheEntryListenerCodec.ResponseParameters params = ReplicatedMapAddNearCacheEntryListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.response));
}
{
    class ReplicatedMapAddNearCacheEntryListenerCodecHandler extends ReplicatedMapAddNearCacheEntryListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  Data
 key ,   Data
 value ,   Data
 oldValue ,   Data
 mergingValue ,   int
 eventType ,   String
 uuid ,   int
 numberOfAffectedEntries   ) {
                          assertTrue(isEqual(aData, key));
                          assertTrue(isEqual(aData, value));
                          assertTrue(isEqual(aData, oldValue));
                          assertTrue(isEqual(aData, mergingValue));
                          assertTrue(isEqual(anInt, eventType));
                          assertTrue(isEqual(aString, uuid));
                          assertTrue(isEqual(anInt, numberOfAffectedEntries));
        }
    }
    ReplicatedMapAddNearCacheEntryListenerCodecHandler handler = new ReplicatedMapAddNearCacheEntryListenerCodecHandler();
    {
        ClientMessage clientMessage = ReplicatedMapAddNearCacheEntryListenerCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
}
{
    ClientMessage clientMessage = MapReduceCancelCodec.encodeRequest(    aString ,    aString   );
    MapReduceCancelCodec.RequestParameters params = MapReduceCancelCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.jobId));
}
{
    ClientMessage clientMessage = MapReduceCancelCodec.encodeResponse(    aBoolean   );
    MapReduceCancelCodec.ResponseParameters params = MapReduceCancelCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = MapReduceJobProcessInformationCodec.encodeRequest(    aString ,    aString   );
    MapReduceJobProcessInformationCodec.RequestParameters params = MapReduceJobProcessInformationCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.jobId));
}
{
    ClientMessage clientMessage = MapReduceJobProcessInformationCodec.encodeResponse(    jobPartitionStates ,    anInt   );
    MapReduceJobProcessInformationCodec.ResponseParameters params = MapReduceJobProcessInformationCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(jobPartitionStates, params.jobPartitionStates));
            assertTrue(isEqual(anInt, params.processRecords));
}
{
    ClientMessage clientMessage = MapReduceForMapCodec.encodeRequest(    aString ,    aString ,    aData ,    aData ,    aData ,    aData ,    aString ,    anInt ,    datas ,    aString   );
    MapReduceForMapCodec.RequestParameters params = MapReduceForMapCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.jobId));
            assertTrue(isEqual(aData, params.predicate));
            assertTrue(isEqual(aData, params.mapper));
            assertTrue(isEqual(aData, params.combinerFactory));
            assertTrue(isEqual(aData, params.reducerFactory));
            assertTrue(isEqual(aString, params.mapName));
            assertTrue(isEqual(anInt, params.chunkSize));
            assertTrue(isEqual(datas, params.keys));
            assertTrue(isEqual(aString, params.topologyChangedStrategy));
}
{
    ClientMessage clientMessage = MapReduceForMapCodec.encodeResponse(    aListOfEntry   );
    MapReduceForMapCodec.ResponseParameters params = MapReduceForMapCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aListOfEntry, params.response));
}
{
    ClientMessage clientMessage = MapReduceForListCodec.encodeRequest(    aString ,    aString ,    aData ,    aData ,    aData ,    aData ,    aString ,    anInt ,    datas ,    aString   );
    MapReduceForListCodec.RequestParameters params = MapReduceForListCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.jobId));
            assertTrue(isEqual(aData, params.predicate));
            assertTrue(isEqual(aData, params.mapper));
            assertTrue(isEqual(aData, params.combinerFactory));
            assertTrue(isEqual(aData, params.reducerFactory));
            assertTrue(isEqual(aString, params.listName));
            assertTrue(isEqual(anInt, params.chunkSize));
            assertTrue(isEqual(datas, params.keys));
            assertTrue(isEqual(aString, params.topologyChangedStrategy));
}
{
    ClientMessage clientMessage = MapReduceForListCodec.encodeResponse(    aListOfEntry   );
    MapReduceForListCodec.ResponseParameters params = MapReduceForListCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aListOfEntry, params.response));
}
{
    ClientMessage clientMessage = MapReduceForSetCodec.encodeRequest(    aString ,    aString ,    aData ,    aData ,    aData ,    aData ,    aString ,    anInt ,    datas ,    aString   );
    MapReduceForSetCodec.RequestParameters params = MapReduceForSetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.jobId));
            assertTrue(isEqual(aData, params.predicate));
            assertTrue(isEqual(aData, params.mapper));
            assertTrue(isEqual(aData, params.combinerFactory));
            assertTrue(isEqual(aData, params.reducerFactory));
            assertTrue(isEqual(aString, params.setName));
            assertTrue(isEqual(anInt, params.chunkSize));
            assertTrue(isEqual(datas, params.keys));
            assertTrue(isEqual(aString, params.topologyChangedStrategy));
}
{
    ClientMessage clientMessage = MapReduceForSetCodec.encodeResponse(    aListOfEntry   );
    MapReduceForSetCodec.ResponseParameters params = MapReduceForSetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aListOfEntry, params.response));
}
{
    ClientMessage clientMessage = MapReduceForMultiMapCodec.encodeRequest(    aString ,    aString ,    aData ,    aData ,    aData ,    aData ,    aString ,    anInt ,    datas ,    aString   );
    MapReduceForMultiMapCodec.RequestParameters params = MapReduceForMultiMapCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.jobId));
            assertTrue(isEqual(aData, params.predicate));
            assertTrue(isEqual(aData, params.mapper));
            assertTrue(isEqual(aData, params.combinerFactory));
            assertTrue(isEqual(aData, params.reducerFactory));
            assertTrue(isEqual(aString, params.multiMapName));
            assertTrue(isEqual(anInt, params.chunkSize));
            assertTrue(isEqual(datas, params.keys));
            assertTrue(isEqual(aString, params.topologyChangedStrategy));
}
{
    ClientMessage clientMessage = MapReduceForMultiMapCodec.encodeResponse(    aListOfEntry   );
    MapReduceForMultiMapCodec.ResponseParameters params = MapReduceForMultiMapCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aListOfEntry, params.response));
}
{
    ClientMessage clientMessage = MapReduceForCustomCodec.encodeRequest(    aString ,    aString ,    aData ,    aData ,    aData ,    aData ,    aData ,    anInt ,    datas ,    aString   );
    MapReduceForCustomCodec.RequestParameters params = MapReduceForCustomCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.jobId));
            assertTrue(isEqual(aData, params.predicate));
            assertTrue(isEqual(aData, params.mapper));
            assertTrue(isEqual(aData, params.combinerFactory));
            assertTrue(isEqual(aData, params.reducerFactory));
            assertTrue(isEqual(aData, params.keyValueSource));
            assertTrue(isEqual(anInt, params.chunkSize));
            assertTrue(isEqual(datas, params.keys));
            assertTrue(isEqual(aString, params.topologyChangedStrategy));
}
{
    ClientMessage clientMessage = MapReduceForCustomCodec.encodeResponse(    aListOfEntry   );
    MapReduceForCustomCodec.ResponseParameters params = MapReduceForCustomCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aListOfEntry, params.response));
}
{
    ClientMessage clientMessage = TransactionalMapContainsKeyCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
    TransactionalMapContainsKeyCodec.RequestParameters params = TransactionalMapContainsKeyCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
}
{
    ClientMessage clientMessage = TransactionalMapContainsKeyCodec.encodeResponse(    aBoolean   );
    TransactionalMapContainsKeyCodec.ResponseParameters params = TransactionalMapContainsKeyCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = TransactionalMapGetCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
    TransactionalMapGetCodec.RequestParameters params = TransactionalMapGetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
}
{
    ClientMessage clientMessage = TransactionalMapGetCodec.encodeResponse(    aData   );
    TransactionalMapGetCodec.ResponseParameters params = TransactionalMapGetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = TransactionalMapGetForUpdateCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
    TransactionalMapGetForUpdateCodec.RequestParameters params = TransactionalMapGetForUpdateCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
}
{
    ClientMessage clientMessage = TransactionalMapGetForUpdateCodec.encodeResponse(    aData   );
    TransactionalMapGetForUpdateCodec.ResponseParameters params = TransactionalMapGetForUpdateCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = TransactionalMapSizeCodec.encodeRequest(    aString ,    aString ,    aLong   );
    TransactionalMapSizeCodec.RequestParameters params = TransactionalMapSizeCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = TransactionalMapSizeCodec.encodeResponse(    anInt   );
    TransactionalMapSizeCodec.ResponseParameters params = TransactionalMapSizeCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anInt, params.response));
}
{
    ClientMessage clientMessage = TransactionalMapIsEmptyCodec.encodeRequest(    aString ,    aString ,    aLong   );
    TransactionalMapIsEmptyCodec.RequestParameters params = TransactionalMapIsEmptyCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = TransactionalMapIsEmptyCodec.encodeResponse(    aBoolean   );
    TransactionalMapIsEmptyCodec.ResponseParameters params = TransactionalMapIsEmptyCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = TransactionalMapPutCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData ,    aLong   );
    TransactionalMapPutCodec.RequestParameters params = TransactionalMapPutCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(aLong, params.ttl));
}
{
    ClientMessage clientMessage = TransactionalMapPutCodec.encodeResponse(    aData   );
    TransactionalMapPutCodec.ResponseParameters params = TransactionalMapPutCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = TransactionalMapSetCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData   );
    TransactionalMapSetCodec.RequestParameters params = TransactionalMapSetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
}
{
    ClientMessage clientMessage = TransactionalMapSetCodec.encodeResponse( );
    TransactionalMapSetCodec.ResponseParameters params = TransactionalMapSetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = TransactionalMapPutIfAbsentCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData   );
    TransactionalMapPutIfAbsentCodec.RequestParameters params = TransactionalMapPutIfAbsentCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
}
{
    ClientMessage clientMessage = TransactionalMapPutIfAbsentCodec.encodeResponse(    aData   );
    TransactionalMapPutIfAbsentCodec.ResponseParameters params = TransactionalMapPutIfAbsentCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = TransactionalMapReplaceCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData   );
    TransactionalMapReplaceCodec.RequestParameters params = TransactionalMapReplaceCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
}
{
    ClientMessage clientMessage = TransactionalMapReplaceCodec.encodeResponse(    aData   );
    TransactionalMapReplaceCodec.ResponseParameters params = TransactionalMapReplaceCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = TransactionalMapReplaceIfSameCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData ,    aData   );
    TransactionalMapReplaceIfSameCodec.RequestParameters params = TransactionalMapReplaceIfSameCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.oldValue));
            assertTrue(isEqual(aData, params.newValue));
}
{
    ClientMessage clientMessage = TransactionalMapReplaceIfSameCodec.encodeResponse(    aBoolean   );
    TransactionalMapReplaceIfSameCodec.ResponseParameters params = TransactionalMapReplaceIfSameCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = TransactionalMapRemoveCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
    TransactionalMapRemoveCodec.RequestParameters params = TransactionalMapRemoveCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
}
{
    ClientMessage clientMessage = TransactionalMapRemoveCodec.encodeResponse(    aData   );
    TransactionalMapRemoveCodec.ResponseParameters params = TransactionalMapRemoveCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = TransactionalMapDeleteCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
    TransactionalMapDeleteCodec.RequestParameters params = TransactionalMapDeleteCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
}
{
    ClientMessage clientMessage = TransactionalMapDeleteCodec.encodeResponse( );
    TransactionalMapDeleteCodec.ResponseParameters params = TransactionalMapDeleteCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = TransactionalMapRemoveIfSameCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData   );
    TransactionalMapRemoveIfSameCodec.RequestParameters params = TransactionalMapRemoveIfSameCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
}
{
    ClientMessage clientMessage = TransactionalMapRemoveIfSameCodec.encodeResponse(    aBoolean   );
    TransactionalMapRemoveIfSameCodec.ResponseParameters params = TransactionalMapRemoveIfSameCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = TransactionalMapKeySetCodec.encodeRequest(    aString ,    aString ,    aLong   );
    TransactionalMapKeySetCodec.RequestParameters params = TransactionalMapKeySetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = TransactionalMapKeySetCodec.encodeResponse(    datas   );
    TransactionalMapKeySetCodec.ResponseParameters params = TransactionalMapKeySetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(datas, params.response));
}
{
    ClientMessage clientMessage = TransactionalMapKeySetWithPredicateCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
    TransactionalMapKeySetWithPredicateCodec.RequestParameters params = TransactionalMapKeySetWithPredicateCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.predicate));
}
{
    ClientMessage clientMessage = TransactionalMapKeySetWithPredicateCodec.encodeResponse(    datas   );
    TransactionalMapKeySetWithPredicateCodec.ResponseParameters params = TransactionalMapKeySetWithPredicateCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(datas, params.response));
}
{
    ClientMessage clientMessage = TransactionalMapValuesCodec.encodeRequest(    aString ,    aString ,    aLong   );
    TransactionalMapValuesCodec.RequestParameters params = TransactionalMapValuesCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = TransactionalMapValuesCodec.encodeResponse(    datas   );
    TransactionalMapValuesCodec.ResponseParameters params = TransactionalMapValuesCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(datas, params.response));
}
{
    ClientMessage clientMessage = TransactionalMapValuesWithPredicateCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
    TransactionalMapValuesWithPredicateCodec.RequestParameters params = TransactionalMapValuesWithPredicateCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.predicate));
}
{
    ClientMessage clientMessage = TransactionalMapValuesWithPredicateCodec.encodeResponse(    datas   );
    TransactionalMapValuesWithPredicateCodec.ResponseParameters params = TransactionalMapValuesWithPredicateCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(datas, params.response));
}
{
    ClientMessage clientMessage = TransactionalMapContainsValueCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
    TransactionalMapContainsValueCodec.RequestParameters params = TransactionalMapContainsValueCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.value));
}
{
    ClientMessage clientMessage = TransactionalMapContainsValueCodec.encodeResponse(    aBoolean   );
    TransactionalMapContainsValueCodec.ResponseParameters params = TransactionalMapContainsValueCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = TransactionalMultiMapPutCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData   );
    TransactionalMultiMapPutCodec.RequestParameters params = TransactionalMultiMapPutCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
}
{
    ClientMessage clientMessage = TransactionalMultiMapPutCodec.encodeResponse(    aBoolean   );
    TransactionalMultiMapPutCodec.ResponseParameters params = TransactionalMultiMapPutCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = TransactionalMultiMapGetCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
    TransactionalMultiMapGetCodec.RequestParameters params = TransactionalMultiMapGetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
}
{
    ClientMessage clientMessage = TransactionalMultiMapGetCodec.encodeResponse(    datas   );
    TransactionalMultiMapGetCodec.ResponseParameters params = TransactionalMultiMapGetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(datas, params.response));
}
{
    ClientMessage clientMessage = TransactionalMultiMapRemoveCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
    TransactionalMultiMapRemoveCodec.RequestParameters params = TransactionalMultiMapRemoveCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
}
{
    ClientMessage clientMessage = TransactionalMultiMapRemoveCodec.encodeResponse(    datas   );
    TransactionalMultiMapRemoveCodec.ResponseParameters params = TransactionalMultiMapRemoveCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(datas, params.response));
}
{
    ClientMessage clientMessage = TransactionalMultiMapRemoveEntryCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData   );
    TransactionalMultiMapRemoveEntryCodec.RequestParameters params = TransactionalMultiMapRemoveEntryCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
}
{
    ClientMessage clientMessage = TransactionalMultiMapRemoveEntryCodec.encodeResponse(    aBoolean   );
    TransactionalMultiMapRemoveEntryCodec.ResponseParameters params = TransactionalMultiMapRemoveEntryCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = TransactionalMultiMapValueCountCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
    TransactionalMultiMapValueCountCodec.RequestParameters params = TransactionalMultiMapValueCountCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.key));
}
{
    ClientMessage clientMessage = TransactionalMultiMapValueCountCodec.encodeResponse(    anInt   );
    TransactionalMultiMapValueCountCodec.ResponseParameters params = TransactionalMultiMapValueCountCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anInt, params.response));
}
{
    ClientMessage clientMessage = TransactionalMultiMapSizeCodec.encodeRequest(    aString ,    aString ,    aLong   );
    TransactionalMultiMapSizeCodec.RequestParameters params = TransactionalMultiMapSizeCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = TransactionalMultiMapSizeCodec.encodeResponse(    anInt   );
    TransactionalMultiMapSizeCodec.ResponseParameters params = TransactionalMultiMapSizeCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anInt, params.response));
}
{
    ClientMessage clientMessage = TransactionalSetAddCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
    TransactionalSetAddCodec.RequestParameters params = TransactionalSetAddCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.item));
}
{
    ClientMessage clientMessage = TransactionalSetAddCodec.encodeResponse(    aBoolean   );
    TransactionalSetAddCodec.ResponseParameters params = TransactionalSetAddCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = TransactionalSetRemoveCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
    TransactionalSetRemoveCodec.RequestParameters params = TransactionalSetRemoveCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.item));
}
{
    ClientMessage clientMessage = TransactionalSetRemoveCodec.encodeResponse(    aBoolean   );
    TransactionalSetRemoveCodec.ResponseParameters params = TransactionalSetRemoveCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = TransactionalSetSizeCodec.encodeRequest(    aString ,    aString ,    aLong   );
    TransactionalSetSizeCodec.RequestParameters params = TransactionalSetSizeCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = TransactionalSetSizeCodec.encodeResponse(    anInt   );
    TransactionalSetSizeCodec.ResponseParameters params = TransactionalSetSizeCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anInt, params.response));
}
{
    ClientMessage clientMessage = TransactionalListAddCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
    TransactionalListAddCodec.RequestParameters params = TransactionalListAddCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.item));
}
{
    ClientMessage clientMessage = TransactionalListAddCodec.encodeResponse(    aBoolean   );
    TransactionalListAddCodec.ResponseParameters params = TransactionalListAddCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = TransactionalListRemoveCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
    TransactionalListRemoveCodec.RequestParameters params = TransactionalListRemoveCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.item));
}
{
    ClientMessage clientMessage = TransactionalListRemoveCodec.encodeResponse(    aBoolean   );
    TransactionalListRemoveCodec.ResponseParameters params = TransactionalListRemoveCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = TransactionalListSizeCodec.encodeRequest(    aString ,    aString ,    aLong   );
    TransactionalListSizeCodec.RequestParameters params = TransactionalListSizeCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = TransactionalListSizeCodec.encodeResponse(    anInt   );
    TransactionalListSizeCodec.ResponseParameters params = TransactionalListSizeCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anInt, params.response));
}
{
    ClientMessage clientMessage = TransactionalQueueOfferCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aLong   );
    TransactionalQueueOfferCodec.RequestParameters params = TransactionalQueueOfferCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aData, params.item));
            assertTrue(isEqual(aLong, params.timeout));
}
{
    ClientMessage clientMessage = TransactionalQueueOfferCodec.encodeResponse(    aBoolean   );
    TransactionalQueueOfferCodec.ResponseParameters params = TransactionalQueueOfferCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = TransactionalQueueTakeCodec.encodeRequest(    aString ,    aString ,    aLong   );
    TransactionalQueueTakeCodec.RequestParameters params = TransactionalQueueTakeCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = TransactionalQueueTakeCodec.encodeResponse(    aData   );
    TransactionalQueueTakeCodec.ResponseParameters params = TransactionalQueueTakeCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = TransactionalQueuePollCodec.encodeRequest(    aString ,    aString ,    aLong ,    aLong   );
    TransactionalQueuePollCodec.RequestParameters params = TransactionalQueuePollCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.timeout));
}
{
    ClientMessage clientMessage = TransactionalQueuePollCodec.encodeResponse(    aData   );
    TransactionalQueuePollCodec.ResponseParameters params = TransactionalQueuePollCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = TransactionalQueuePeekCodec.encodeRequest(    aString ,    aString ,    aLong ,    aLong   );
    TransactionalQueuePeekCodec.RequestParameters params = TransactionalQueuePeekCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
            assertTrue(isEqual(aLong, params.timeout));
}
{
    ClientMessage clientMessage = TransactionalQueuePeekCodec.encodeResponse(    aData   );
    TransactionalQueuePeekCodec.ResponseParameters params = TransactionalQueuePeekCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = TransactionalQueueSizeCodec.encodeRequest(    aString ,    aString ,    aLong   );
    TransactionalQueueSizeCodec.RequestParameters params = TransactionalQueueSizeCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.txnId));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = TransactionalQueueSizeCodec.encodeResponse(    anInt   );
    TransactionalQueueSizeCodec.ResponseParameters params = TransactionalQueueSizeCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anInt, params.response));
}
{
    ClientMessage clientMessage = CacheAddEntryListenerCodec.encodeRequest(    aString ,    aBoolean   );
    CacheAddEntryListenerCodec.RequestParameters params = CacheAddEntryListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aBoolean, params.localOnly));
}
{
    ClientMessage clientMessage = CacheAddEntryListenerCodec.encodeResponse(    aString   );
    CacheAddEntryListenerCodec.ResponseParameters params = CacheAddEntryListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.response));
}
{
    class CacheAddEntryListenerCodecHandler extends CacheAddEntryListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  int
 type ,   Collection<com.hazelcast.cache.impl.CacheEventData> keys ,   int
 completionId   ) {
                          assertTrue(isEqual(anInt, type));
                          assertTrue(isEqual(cacheEventDatas, keys));
                          assertTrue(isEqual(anInt, completionId));
        }
    }
    CacheAddEntryListenerCodecHandler handler = new CacheAddEntryListenerCodecHandler();
    {
        ClientMessage clientMessage = CacheAddEntryListenerCodec.encodeCacheEvent( anInt ,  cacheEventDatas ,  anInt   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
}
{
    ClientMessage clientMessage = CacheAddInvalidationListenerCodec.encodeRequest(    aString ,    aBoolean   );
    CacheAddInvalidationListenerCodec.RequestParameters params = CacheAddInvalidationListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aBoolean, params.localOnly));
}
{
    ClientMessage clientMessage = CacheAddInvalidationListenerCodec.encodeResponse(    aString   );
    CacheAddInvalidationListenerCodec.ResponseParameters params = CacheAddInvalidationListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.response));
}
{
    class CacheAddInvalidationListenerCodecHandler extends CacheAddInvalidationListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  String
 name ,   Data
 key ,   String
 sourceUuid ,   java.util.UUID
 partitionUuid ,   long
 sequence   ) {
                          assertTrue(isEqual(aString, name));
                          assertTrue(isEqual(aData, key));
                          assertTrue(isEqual(aString, sourceUuid));
                          assertTrue(isEqual(aUUID, partitionUuid));
                          assertTrue(isEqual(aLong, sequence));
        }
        @Override
        public void handle(  String
 name ,   Collection<Data> keys ,   Collection<String> sourceUuids ,   Collection<java.util.UUID> partitionUuids ,   Collection<Long> sequences   ) {
                          assertTrue(isEqual(aString, name));
                          assertTrue(isEqual(datas, keys));
                          assertTrue(isEqual(strings, sourceUuids));
                          assertTrue(isEqual(uuids, partitionUuids));
                          assertTrue(isEqual(longs, sequences));
        }
    }
    CacheAddInvalidationListenerCodecHandler handler = new CacheAddInvalidationListenerCodecHandler();
    {
        ClientMessage clientMessage = CacheAddInvalidationListenerCodec.encodeCacheInvalidationEvent( aString ,  aData ,  aString ,  aUUID ,  aLong   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
    {
        ClientMessage clientMessage = CacheAddInvalidationListenerCodec.encodeCacheBatchInvalidationEvent( aString ,  datas ,  strings ,  uuids ,  longs   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
}
{
    ClientMessage clientMessage = CacheClearCodec.encodeRequest(    aString   );
    CacheClearCodec.RequestParameters params = CacheClearCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = CacheClearCodec.encodeResponse( );
    CacheClearCodec.ResponseParameters params = CacheClearCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = CacheRemoveAllKeysCodec.encodeRequest(    aString ,    datas ,    anInt   );
    CacheRemoveAllKeysCodec.RequestParameters params = CacheRemoveAllKeysCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.keys));
            assertTrue(isEqual(anInt, params.completionId));
}
{
    ClientMessage clientMessage = CacheRemoveAllKeysCodec.encodeResponse( );
    CacheRemoveAllKeysCodec.ResponseParameters params = CacheRemoveAllKeysCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = CacheRemoveAllCodec.encodeRequest(    aString ,    anInt   );
    CacheRemoveAllCodec.RequestParameters params = CacheRemoveAllCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.completionId));
}
{
    ClientMessage clientMessage = CacheRemoveAllCodec.encodeResponse( );
    CacheRemoveAllCodec.ResponseParameters params = CacheRemoveAllCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = CacheContainsKeyCodec.encodeRequest(    aString ,    aData   );
    CacheContainsKeyCodec.RequestParameters params = CacheContainsKeyCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
}
{
    ClientMessage clientMessage = CacheContainsKeyCodec.encodeResponse(    aBoolean   );
    CacheContainsKeyCodec.ResponseParameters params = CacheContainsKeyCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = CacheCreateConfigCodec.encodeRequest(    aData ,    aBoolean   );
    CacheCreateConfigCodec.RequestParameters params = CacheCreateConfigCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.cacheConfig));
            assertTrue(isEqual(aBoolean, params.createAlsoOnOthers));
}
{
    ClientMessage clientMessage = CacheCreateConfigCodec.encodeResponse(    aData   );
    CacheCreateConfigCodec.ResponseParameters params = CacheCreateConfigCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = CacheDestroyCodec.encodeRequest(    aString   );
    CacheDestroyCodec.RequestParameters params = CacheDestroyCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = CacheDestroyCodec.encodeResponse( );
    CacheDestroyCodec.ResponseParameters params = CacheDestroyCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = CacheEntryProcessorCodec.encodeRequest(    aString ,    aData ,    aData ,    datas ,    anInt   );
    CacheEntryProcessorCodec.RequestParameters params = CacheEntryProcessorCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.entryProcessor));
            assertTrue(isEqual(datas, params.arguments));
            assertTrue(isEqual(anInt, params.completionId));
}
{
    ClientMessage clientMessage = CacheEntryProcessorCodec.encodeResponse(    aData   );
    CacheEntryProcessorCodec.ResponseParameters params = CacheEntryProcessorCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = CacheGetAllCodec.encodeRequest(    aString ,    datas ,    aData   );
    CacheGetAllCodec.RequestParameters params = CacheGetAllCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.keys));
            assertTrue(isEqual(aData, params.expiryPolicy));
}
{
    ClientMessage clientMessage = CacheGetAllCodec.encodeResponse(    aListOfEntry   );
    CacheGetAllCodec.ResponseParameters params = CacheGetAllCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aListOfEntry, params.response));
}
{
    ClientMessage clientMessage = CacheGetAndRemoveCodec.encodeRequest(    aString ,    aData ,    anInt   );
    CacheGetAndRemoveCodec.RequestParameters params = CacheGetAndRemoveCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(anInt, params.completionId));
}
{
    ClientMessage clientMessage = CacheGetAndRemoveCodec.encodeResponse(    aData   );
    CacheGetAndRemoveCodec.ResponseParameters params = CacheGetAndRemoveCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = CacheGetAndReplaceCodec.encodeRequest(    aString ,    aData ,    aData ,    aData ,    anInt   );
    CacheGetAndReplaceCodec.RequestParameters params = CacheGetAndReplaceCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(aData, params.expiryPolicy));
            assertTrue(isEqual(anInt, params.completionId));
}
{
    ClientMessage clientMessage = CacheGetAndReplaceCodec.encodeResponse(    aData   );
    CacheGetAndReplaceCodec.ResponseParameters params = CacheGetAndReplaceCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = CacheGetConfigCodec.encodeRequest(    aString ,    aString   );
    CacheGetConfigCodec.RequestParameters params = CacheGetConfigCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.simpleName));
}
{
    ClientMessage clientMessage = CacheGetConfigCodec.encodeResponse(    aData   );
    CacheGetConfigCodec.ResponseParameters params = CacheGetConfigCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = CacheGetCodec.encodeRequest(    aString ,    aData ,    aData   );
    CacheGetCodec.RequestParameters params = CacheGetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.expiryPolicy));
}
{
    ClientMessage clientMessage = CacheGetCodec.encodeResponse(    aData   );
    CacheGetCodec.ResponseParameters params = CacheGetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = CacheIterateCodec.encodeRequest(    aString ,    anInt ,    anInt ,    anInt   );
    CacheIterateCodec.RequestParameters params = CacheIterateCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.partitionId));
            assertTrue(isEqual(anInt, params.tableIndex));
            assertTrue(isEqual(anInt, params.batch));
}
{
    ClientMessage clientMessage = CacheIterateCodec.encodeResponse(    anInt ,    datas   );
    CacheIterateCodec.ResponseParameters params = CacheIterateCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anInt, params.tableIndex));
            assertTrue(isEqual(datas, params.keys));
}
{
    ClientMessage clientMessage = CacheListenerRegistrationCodec.encodeRequest(    aString ,    aData ,    aBoolean ,    anAddress   );
    CacheListenerRegistrationCodec.RequestParameters params = CacheListenerRegistrationCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.listenerConfig));
            assertTrue(isEqual(aBoolean, params.shouldRegister));
            assertTrue(isEqual(anAddress, params.address));
}
{
    ClientMessage clientMessage = CacheListenerRegistrationCodec.encodeResponse( );
    CacheListenerRegistrationCodec.ResponseParameters params = CacheListenerRegistrationCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = CacheLoadAllCodec.encodeRequest(    aString ,    datas ,    aBoolean   );
    CacheLoadAllCodec.RequestParameters params = CacheLoadAllCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.keys));
            assertTrue(isEqual(aBoolean, params.replaceExistingValues));
}
{
    ClientMessage clientMessage = CacheLoadAllCodec.encodeResponse( );
    CacheLoadAllCodec.ResponseParameters params = CacheLoadAllCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = CacheManagementConfigCodec.encodeRequest(    aString ,    aBoolean ,    aBoolean ,    anAddress   );
    CacheManagementConfigCodec.RequestParameters params = CacheManagementConfigCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aBoolean, params.isStat));
            assertTrue(isEqual(aBoolean, params.enabled));
            assertTrue(isEqual(anAddress, params.address));
}
{
    ClientMessage clientMessage = CacheManagementConfigCodec.encodeResponse( );
    CacheManagementConfigCodec.ResponseParameters params = CacheManagementConfigCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = CachePutIfAbsentCodec.encodeRequest(    aString ,    aData ,    aData ,    aData ,    anInt   );
    CachePutIfAbsentCodec.RequestParameters params = CachePutIfAbsentCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(aData, params.expiryPolicy));
            assertTrue(isEqual(anInt, params.completionId));
}
{
    ClientMessage clientMessage = CachePutIfAbsentCodec.encodeResponse(    aBoolean   );
    CachePutIfAbsentCodec.ResponseParameters params = CachePutIfAbsentCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = CachePutCodec.encodeRequest(    aString ,    aData ,    aData ,    aData ,    aBoolean ,    anInt   );
    CachePutCodec.RequestParameters params = CachePutCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.value));
            assertTrue(isEqual(aData, params.expiryPolicy));
            assertTrue(isEqual(aBoolean, params.get));
            assertTrue(isEqual(anInt, params.completionId));
}
{
    ClientMessage clientMessage = CachePutCodec.encodeResponse(    aData   );
    CachePutCodec.ResponseParameters params = CachePutCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = CacheRemoveEntryListenerCodec.encodeRequest(    aString ,    aString   );
    CacheRemoveEntryListenerCodec.RequestParameters params = CacheRemoveEntryListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.registrationId));
}
{
    ClientMessage clientMessage = CacheRemoveEntryListenerCodec.encodeResponse(    aBoolean   );
    CacheRemoveEntryListenerCodec.ResponseParameters params = CacheRemoveEntryListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = CacheRemoveInvalidationListenerCodec.encodeRequest(    aString ,    aString   );
    CacheRemoveInvalidationListenerCodec.RequestParameters params = CacheRemoveInvalidationListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.registrationId));
}
{
    ClientMessage clientMessage = CacheRemoveInvalidationListenerCodec.encodeResponse(    aBoolean   );
    CacheRemoveInvalidationListenerCodec.ResponseParameters params = CacheRemoveInvalidationListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = CacheRemoveCodec.encodeRequest(    aString ,    aData ,    aData ,    anInt   );
    CacheRemoveCodec.RequestParameters params = CacheRemoveCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.currentValue));
            assertTrue(isEqual(anInt, params.completionId));
}
{
    ClientMessage clientMessage = CacheRemoveCodec.encodeResponse(    aBoolean   );
    CacheRemoveCodec.ResponseParameters params = CacheRemoveCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = CacheReplaceCodec.encodeRequest(    aString ,    aData ,    aData ,    aData ,    aData ,    anInt   );
    CacheReplaceCodec.RequestParameters params = CacheReplaceCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.key));
            assertTrue(isEqual(aData, params.oldValue));
            assertTrue(isEqual(aData, params.newValue));
            assertTrue(isEqual(aData, params.expiryPolicy));
            assertTrue(isEqual(anInt, params.completionId));
}
{
    ClientMessage clientMessage = CacheReplaceCodec.encodeResponse(    aData   );
    CacheReplaceCodec.ResponseParameters params = CacheReplaceCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = CacheSizeCodec.encodeRequest(    aString   );
    CacheSizeCodec.RequestParameters params = CacheSizeCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = CacheSizeCodec.encodeResponse(    anInt   );
    CacheSizeCodec.ResponseParameters params = CacheSizeCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anInt, params.response));
}
{
    ClientMessage clientMessage = CacheAddPartitionLostListenerCodec.encodeRequest(    aString ,    aBoolean   );
    CacheAddPartitionLostListenerCodec.RequestParameters params = CacheAddPartitionLostListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aBoolean, params.localOnly));
}
{
    ClientMessage clientMessage = CacheAddPartitionLostListenerCodec.encodeResponse(    aString   );
    CacheAddPartitionLostListenerCodec.ResponseParameters params = CacheAddPartitionLostListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.response));
}
{
    class CacheAddPartitionLostListenerCodecHandler extends CacheAddPartitionLostListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  int
 partitionId ,   String
 uuid   ) {
                          assertTrue(isEqual(anInt, partitionId));
                          assertTrue(isEqual(aString, uuid));
        }
    }
    CacheAddPartitionLostListenerCodecHandler handler = new CacheAddPartitionLostListenerCodecHandler();
    {
        ClientMessage clientMessage = CacheAddPartitionLostListenerCodec.encodeCachePartitionLostEvent( anInt ,  aString   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
}
{
    ClientMessage clientMessage = CacheRemovePartitionLostListenerCodec.encodeRequest(    aString ,    aString   );
    CacheRemovePartitionLostListenerCodec.RequestParameters params = CacheRemovePartitionLostListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.registrationId));
}
{
    ClientMessage clientMessage = CacheRemovePartitionLostListenerCodec.encodeResponse(    aBoolean   );
    CacheRemovePartitionLostListenerCodec.ResponseParameters params = CacheRemovePartitionLostListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = CachePutAllCodec.encodeRequest(    aString ,    aListOfEntry ,    aData ,    anInt   );
    CachePutAllCodec.RequestParameters params = CachePutAllCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aListOfEntry, params.entries));
            assertTrue(isEqual(aData, params.expiryPolicy));
            assertTrue(isEqual(anInt, params.completionId));
}
{
    ClientMessage clientMessage = CachePutAllCodec.encodeResponse( );
    CachePutAllCodec.ResponseParameters params = CachePutAllCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = CacheIterateEntriesCodec.encodeRequest(    aString ,    anInt ,    anInt ,    anInt   );
    CacheIterateEntriesCodec.RequestParameters params = CacheIterateEntriesCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.partitionId));
            assertTrue(isEqual(anInt, params.tableIndex));
            assertTrue(isEqual(anInt, params.batch));
}
{
    ClientMessage clientMessage = CacheIterateEntriesCodec.encodeResponse(    anInt ,    aListOfEntry   );
    CacheIterateEntriesCodec.ResponseParameters params = CacheIterateEntriesCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anInt, params.tableIndex));
            assertTrue(isEqual(aListOfEntry, params.entries));
}
{
    ClientMessage clientMessage = CacheAddNearCacheInvalidationListenerCodec.encodeRequest(    aString ,    aBoolean   );
    CacheAddNearCacheInvalidationListenerCodec.RequestParameters params = CacheAddNearCacheInvalidationListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aBoolean, params.localOnly));
}
{
    ClientMessage clientMessage = CacheAddNearCacheInvalidationListenerCodec.encodeResponse(    aString   );
    CacheAddNearCacheInvalidationListenerCodec.ResponseParameters params = CacheAddNearCacheInvalidationListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.response));
}
{
    class CacheAddNearCacheInvalidationListenerCodecHandler extends CacheAddNearCacheInvalidationListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  String
 name ,   Data
 key ,   String
 sourceUuid ,   java.util.UUID
 partitionUuid ,   long
 sequence   ) {
                          assertTrue(isEqual(aString, name));
                          assertTrue(isEqual(aData, key));
                          assertTrue(isEqual(aString, sourceUuid));
                          assertTrue(isEqual(aUUID, partitionUuid));
                          assertTrue(isEqual(aLong, sequence));
        }
        @Override
        public void handle(  String
 name ,   Collection<Data> keys ,   Collection<String> sourceUuids ,   Collection<java.util.UUID> partitionUuids ,   Collection<Long> sequences   ) {
                          assertTrue(isEqual(aString, name));
                          assertTrue(isEqual(datas, keys));
                          assertTrue(isEqual(strings, sourceUuids));
                          assertTrue(isEqual(uuids, partitionUuids));
                          assertTrue(isEqual(longs, sequences));
        }
    }
    CacheAddNearCacheInvalidationListenerCodecHandler handler = new CacheAddNearCacheInvalidationListenerCodecHandler();
    {
        ClientMessage clientMessage = CacheAddNearCacheInvalidationListenerCodec.encodeCacheInvalidationEvent( aString ,  aData ,  aString ,  aUUID ,  aLong   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
    {
        ClientMessage clientMessage = CacheAddNearCacheInvalidationListenerCodec.encodeCacheBatchInvalidationEvent( aString ,  datas ,  strings ,  uuids ,  longs   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
}
{
    ClientMessage clientMessage = CacheFetchNearCacheInvalidationMetadataCodec.encodeRequest(    strings ,    anAddress   );
    CacheFetchNearCacheInvalidationMetadataCodec.RequestParameters params = CacheFetchNearCacheInvalidationMetadataCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(strings, params.names));
            assertTrue(isEqual(anAddress, params.address));
}
{
    ClientMessage clientMessage = CacheFetchNearCacheInvalidationMetadataCodec.encodeResponse(    aNamePartitionSequenceList ,    aPartitionUuidList   );
    CacheFetchNearCacheInvalidationMetadataCodec.ResponseParameters params = CacheFetchNearCacheInvalidationMetadataCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aNamePartitionSequenceList, params.namePartitionSequenceList));
            assertTrue(isEqual(aPartitionUuidList, params.partitionUuidList));
}
{
    ClientMessage clientMessage = CacheAssignAndGetUuidsCodec.encodeRequest( );
    CacheAssignAndGetUuidsCodec.RequestParameters params = CacheAssignAndGetUuidsCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = CacheAssignAndGetUuidsCodec.encodeResponse(    aPartitionUuidList   );
    CacheAssignAndGetUuidsCodec.ResponseParameters params = CacheAssignAndGetUuidsCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aPartitionUuidList, params.partitionUuidList));
}
{
    ClientMessage clientMessage = CacheEventJournalSubscribeCodec.encodeRequest(    aString   );
    CacheEventJournalSubscribeCodec.RequestParameters params = CacheEventJournalSubscribeCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = CacheEventJournalSubscribeCodec.encodeResponse(    aLong ,    aLong   );
    CacheEventJournalSubscribeCodec.ResponseParameters params = CacheEventJournalSubscribeCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aLong, params.oldestSequence));
            assertTrue(isEqual(aLong, params.newestSequence));
}
{
    ClientMessage clientMessage = CacheEventJournalReadCodec.encodeRequest(    aString ,    aLong ,    anInt ,    anInt ,    aData ,    aData   );
    CacheEventJournalReadCodec.RequestParameters params = CacheEventJournalReadCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.startSequence));
            assertTrue(isEqual(anInt, params.minSize));
            assertTrue(isEqual(anInt, params.maxSize));
            assertTrue(isEqual(aData, params.predicate));
            assertTrue(isEqual(aData, params.projection));
}
{
    ClientMessage clientMessage = CacheEventJournalReadCodec.encodeResponse(    anInt ,    datas ,    arrLongs ,    aLong   );
    CacheEventJournalReadCodec.ResponseParameters params = CacheEventJournalReadCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anInt, params.readCount));
            assertTrue(isEqual(datas, params.items));
            assertTrue(isEqual(arrLongs, params.itemSeqs));
            assertTrue(isEqual(aLong, params.nextSeq));
}
{
    ClientMessage clientMessage = XATransactionClearRemoteCodec.encodeRequest(    anXid   );
    XATransactionClearRemoteCodec.RequestParameters params = XATransactionClearRemoteCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anXid, params.xid));
}
{
    ClientMessage clientMessage = XATransactionClearRemoteCodec.encodeResponse( );
    XATransactionClearRemoteCodec.ResponseParameters params = XATransactionClearRemoteCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = XATransactionCollectTransactionsCodec.encodeRequest( );
    XATransactionCollectTransactionsCodec.RequestParameters params = XATransactionCollectTransactionsCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = XATransactionCollectTransactionsCodec.encodeResponse(    datas   );
    XATransactionCollectTransactionsCodec.ResponseParameters params = XATransactionCollectTransactionsCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(datas, params.response));
}
{
    ClientMessage clientMessage = XATransactionFinalizeCodec.encodeRequest(    anXid ,    aBoolean   );
    XATransactionFinalizeCodec.RequestParameters params = XATransactionFinalizeCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anXid, params.xid));
            assertTrue(isEqual(aBoolean, params.isCommit));
}
{
    ClientMessage clientMessage = XATransactionFinalizeCodec.encodeResponse( );
    XATransactionFinalizeCodec.ResponseParameters params = XATransactionFinalizeCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = XATransactionCommitCodec.encodeRequest(    aString ,    aBoolean   );
    XATransactionCommitCodec.RequestParameters params = XATransactionCommitCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.transactionId));
            assertTrue(isEqual(aBoolean, params.onePhase));
}
{
    ClientMessage clientMessage = XATransactionCommitCodec.encodeResponse( );
    XATransactionCommitCodec.ResponseParameters params = XATransactionCommitCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = XATransactionCreateCodec.encodeRequest(    anXid ,    aLong   );
    XATransactionCreateCodec.RequestParameters params = XATransactionCreateCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anXid, params.xid));
            assertTrue(isEqual(aLong, params.timeout));
}
{
    ClientMessage clientMessage = XATransactionCreateCodec.encodeResponse(    aString   );
    XATransactionCreateCodec.ResponseParameters params = XATransactionCreateCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.response));
}
{
    ClientMessage clientMessage = XATransactionPrepareCodec.encodeRequest(    aString   );
    XATransactionPrepareCodec.RequestParameters params = XATransactionPrepareCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.transactionId));
}
{
    ClientMessage clientMessage = XATransactionPrepareCodec.encodeResponse( );
    XATransactionPrepareCodec.ResponseParameters params = XATransactionPrepareCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = XATransactionRollbackCodec.encodeRequest(    aString   );
    XATransactionRollbackCodec.RequestParameters params = XATransactionRollbackCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.transactionId));
}
{
    ClientMessage clientMessage = XATransactionRollbackCodec.encodeResponse( );
    XATransactionRollbackCodec.ResponseParameters params = XATransactionRollbackCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = TransactionCommitCodec.encodeRequest(    aString ,    aLong   );
    TransactionCommitCodec.RequestParameters params = TransactionCommitCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.transactionId));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = TransactionCommitCodec.encodeResponse( );
    TransactionCommitCodec.ResponseParameters params = TransactionCommitCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = TransactionCreateCodec.encodeRequest(    aLong ,    anInt ,    anInt ,    aLong   );
    TransactionCreateCodec.RequestParameters params = TransactionCreateCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aLong, params.timeout));
            assertTrue(isEqual(anInt, params.durability));
            assertTrue(isEqual(anInt, params.transactionType));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = TransactionCreateCodec.encodeResponse(    aString   );
    TransactionCreateCodec.ResponseParameters params = TransactionCreateCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.response));
}
{
    ClientMessage clientMessage = TransactionRollbackCodec.encodeRequest(    aString ,    aLong   );
    TransactionRollbackCodec.RequestParameters params = TransactionRollbackCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.transactionId));
            assertTrue(isEqual(aLong, params.threadId));
}
{
    ClientMessage clientMessage = TransactionRollbackCodec.encodeResponse( );
    TransactionRollbackCodec.ResponseParameters params = TransactionRollbackCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = ContinuousQueryPublisherCreateWithValueCodec.encodeRequest(    aString ,    aString ,    aData ,    anInt ,    anInt ,    aLong ,    aBoolean ,    aBoolean   );
    ContinuousQueryPublisherCreateWithValueCodec.RequestParameters params = ContinuousQueryPublisherCreateWithValueCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
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
    ClientMessage clientMessage = ContinuousQueryPublisherCreateWithValueCodec.encodeResponse(    aListOfEntry   );
    ContinuousQueryPublisherCreateWithValueCodec.ResponseParameters params = ContinuousQueryPublisherCreateWithValueCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aListOfEntry, params.response));
}
{
    ClientMessage clientMessage = ContinuousQueryPublisherCreateCodec.encodeRequest(    aString ,    aString ,    aData ,    anInt ,    anInt ,    aLong ,    aBoolean ,    aBoolean   );
    ContinuousQueryPublisherCreateCodec.RequestParameters params = ContinuousQueryPublisherCreateCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
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
    ClientMessage clientMessage = ContinuousQueryPublisherCreateCodec.encodeResponse(    datas   );
    ContinuousQueryPublisherCreateCodec.ResponseParameters params = ContinuousQueryPublisherCreateCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(datas, params.response));
}
{
    ClientMessage clientMessage = ContinuousQueryMadePublishableCodec.encodeRequest(    aString ,    aString   );
    ContinuousQueryMadePublishableCodec.RequestParameters params = ContinuousQueryMadePublishableCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.mapName));
            assertTrue(isEqual(aString, params.cacheName));
}
{
    ClientMessage clientMessage = ContinuousQueryMadePublishableCodec.encodeResponse(    aBoolean   );
    ContinuousQueryMadePublishableCodec.ResponseParameters params = ContinuousQueryMadePublishableCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = ContinuousQueryAddListenerCodec.encodeRequest(    aString ,    aBoolean   );
    ContinuousQueryAddListenerCodec.RequestParameters params = ContinuousQueryAddListenerCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.listenerName));
            assertTrue(isEqual(aBoolean, params.localOnly));
}
{
    ClientMessage clientMessage = ContinuousQueryAddListenerCodec.encodeResponse(    aString   );
    ContinuousQueryAddListenerCodec.ResponseParameters params = ContinuousQueryAddListenerCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.response));
}
{
    class ContinuousQueryAddListenerCodecHandler extends ContinuousQueryAddListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  QueryCacheEventData
 data   ) {
                          assertTrue(isEqual(aQueryCacheEventData, data));
        }
        @Override
        public void handle(  Collection<QueryCacheEventData> events ,   String
 source ,   int
 partitionId   ) {
                          assertTrue(isEqual(queryCacheEventDatas, events));
                          assertTrue(isEqual(aString, source));
                          assertTrue(isEqual(anInt, partitionId));
        }
    }
    ContinuousQueryAddListenerCodecHandler handler = new ContinuousQueryAddListenerCodecHandler();
    {
        ClientMessage clientMessage = ContinuousQueryAddListenerCodec.encodeQueryCacheSingleEvent( aQueryCacheEventData   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
    {
        ClientMessage clientMessage = ContinuousQueryAddListenerCodec.encodeQueryCacheBatchEvent( queryCacheEventDatas ,  aString ,  anInt   );
        handler.handle(ClientMessage.createForDecode(clientMessage.buffer(), 0));
     }
}
{
    ClientMessage clientMessage = ContinuousQuerySetReadCursorCodec.encodeRequest(    aString ,    aString ,    aLong   );
    ContinuousQuerySetReadCursorCodec.RequestParameters params = ContinuousQuerySetReadCursorCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.mapName));
            assertTrue(isEqual(aString, params.cacheName));
            assertTrue(isEqual(aLong, params.sequence));
}
{
    ClientMessage clientMessage = ContinuousQuerySetReadCursorCodec.encodeResponse(    aBoolean   );
    ContinuousQuerySetReadCursorCodec.ResponseParameters params = ContinuousQuerySetReadCursorCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = ContinuousQueryDestroyCacheCodec.encodeRequest(    aString ,    aString   );
    ContinuousQueryDestroyCacheCodec.RequestParameters params = ContinuousQueryDestroyCacheCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.mapName));
            assertTrue(isEqual(aString, params.cacheName));
}
{
    ClientMessage clientMessage = ContinuousQueryDestroyCacheCodec.encodeResponse(    aBoolean   );
    ContinuousQueryDestroyCacheCodec.ResponseParameters params = ContinuousQueryDestroyCacheCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = RingbufferSizeCodec.encodeRequest(    aString   );
    RingbufferSizeCodec.RequestParameters params = RingbufferSizeCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = RingbufferSizeCodec.encodeResponse(    aLong   );
    RingbufferSizeCodec.ResponseParameters params = RingbufferSizeCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aLong, params.response));
}
{
    ClientMessage clientMessage = RingbufferTailSequenceCodec.encodeRequest(    aString   );
    RingbufferTailSequenceCodec.RequestParameters params = RingbufferTailSequenceCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = RingbufferTailSequenceCodec.encodeResponse(    aLong   );
    RingbufferTailSequenceCodec.ResponseParameters params = RingbufferTailSequenceCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aLong, params.response));
}
{
    ClientMessage clientMessage = RingbufferHeadSequenceCodec.encodeRequest(    aString   );
    RingbufferHeadSequenceCodec.RequestParameters params = RingbufferHeadSequenceCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = RingbufferHeadSequenceCodec.encodeResponse(    aLong   );
    RingbufferHeadSequenceCodec.ResponseParameters params = RingbufferHeadSequenceCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aLong, params.response));
}
{
    ClientMessage clientMessage = RingbufferCapacityCodec.encodeRequest(    aString   );
    RingbufferCapacityCodec.RequestParameters params = RingbufferCapacityCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = RingbufferCapacityCodec.encodeResponse(    aLong   );
    RingbufferCapacityCodec.ResponseParameters params = RingbufferCapacityCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aLong, params.response));
}
{
    ClientMessage clientMessage = RingbufferRemainingCapacityCodec.encodeRequest(    aString   );
    RingbufferRemainingCapacityCodec.RequestParameters params = RingbufferRemainingCapacityCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = RingbufferRemainingCapacityCodec.encodeResponse(    aLong   );
    RingbufferRemainingCapacityCodec.ResponseParameters params = RingbufferRemainingCapacityCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aLong, params.response));
}
{
    ClientMessage clientMessage = RingbufferAddCodec.encodeRequest(    aString ,    anInt ,    aData   );
    RingbufferAddCodec.RequestParameters params = RingbufferAddCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.overflowPolicy));
            assertTrue(isEqual(aData, params.value));
}
{
    ClientMessage clientMessage = RingbufferAddCodec.encodeResponse(    aLong   );
    RingbufferAddCodec.ResponseParameters params = RingbufferAddCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aLong, params.response));
}
{
    ClientMessage clientMessage = RingbufferReadOneCodec.encodeRequest(    aString ,    aLong   );
    RingbufferReadOneCodec.RequestParameters params = RingbufferReadOneCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.sequence));
}
{
    ClientMessage clientMessage = RingbufferReadOneCodec.encodeResponse(    aData   );
    RingbufferReadOneCodec.ResponseParameters params = RingbufferReadOneCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = RingbufferAddAllCodec.encodeRequest(    aString ,    datas ,    anInt   );
    RingbufferAddAllCodec.RequestParameters params = RingbufferAddAllCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(datas, params.valueList));
            assertTrue(isEqual(anInt, params.overflowPolicy));
}
{
    ClientMessage clientMessage = RingbufferAddAllCodec.encodeResponse(    aLong   );
    RingbufferAddAllCodec.ResponseParameters params = RingbufferAddAllCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aLong, params.response));
}
{
    ClientMessage clientMessage = RingbufferReadManyCodec.encodeRequest(    aString ,    aLong ,    anInt ,    anInt ,    aData   );
    RingbufferReadManyCodec.RequestParameters params = RingbufferReadManyCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.startSequence));
            assertTrue(isEqual(anInt, params.minCount));
            assertTrue(isEqual(anInt, params.maxCount));
            assertTrue(isEqual(aData, params.filter));
}
{
    ClientMessage clientMessage = RingbufferReadManyCodec.encodeResponse(    anInt ,    datas ,    arrLongs ,    aLong   );
    RingbufferReadManyCodec.ResponseParameters params = RingbufferReadManyCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anInt, params.readCount));
            assertTrue(isEqual(datas, params.items));
            assertTrue(isEqual(arrLongs, params.itemSeqs));
            assertTrue(isEqual(aLong, params.nextSeq));
}
{
    ClientMessage clientMessage = DurableExecutorShutdownCodec.encodeRequest(    aString   );
    DurableExecutorShutdownCodec.RequestParameters params = DurableExecutorShutdownCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = DurableExecutorShutdownCodec.encodeResponse( );
    DurableExecutorShutdownCodec.ResponseParameters params = DurableExecutorShutdownCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = DurableExecutorIsShutdownCodec.encodeRequest(    aString   );
    DurableExecutorIsShutdownCodec.RequestParameters params = DurableExecutorIsShutdownCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = DurableExecutorIsShutdownCodec.encodeResponse(    aBoolean   );
    DurableExecutorIsShutdownCodec.ResponseParameters params = DurableExecutorIsShutdownCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = DurableExecutorSubmitToPartitionCodec.encodeRequest(    aString ,    aData   );
    DurableExecutorSubmitToPartitionCodec.RequestParameters params = DurableExecutorSubmitToPartitionCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aData, params.callable));
}
{
    ClientMessage clientMessage = DurableExecutorSubmitToPartitionCodec.encodeResponse(    anInt   );
    DurableExecutorSubmitToPartitionCodec.ResponseParameters params = DurableExecutorSubmitToPartitionCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anInt, params.response));
}
{
    ClientMessage clientMessage = DurableExecutorRetrieveResultCodec.encodeRequest(    aString ,    anInt   );
    DurableExecutorRetrieveResultCodec.RequestParameters params = DurableExecutorRetrieveResultCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.sequence));
}
{
    ClientMessage clientMessage = DurableExecutorRetrieveResultCodec.encodeResponse(    aData   );
    DurableExecutorRetrieveResultCodec.ResponseParameters params = DurableExecutorRetrieveResultCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = DurableExecutorDisposeResultCodec.encodeRequest(    aString ,    anInt   );
    DurableExecutorDisposeResultCodec.RequestParameters params = DurableExecutorDisposeResultCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.sequence));
}
{
    ClientMessage clientMessage = DurableExecutorDisposeResultCodec.encodeResponse( );
    DurableExecutorDisposeResultCodec.ResponseParameters params = DurableExecutorDisposeResultCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = DurableExecutorRetrieveAndDisposeResultCodec.encodeRequest(    aString ,    anInt   );
    DurableExecutorRetrieveAndDisposeResultCodec.RequestParameters params = DurableExecutorRetrieveAndDisposeResultCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.sequence));
}
{
    ClientMessage clientMessage = DurableExecutorRetrieveAndDisposeResultCodec.encodeResponse(    aData   );
    DurableExecutorRetrieveAndDisposeResultCodec.ResponseParameters params = DurableExecutorRetrieveAndDisposeResultCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = CardinalityEstimatorAddCodec.encodeRequest(    aString ,    aLong   );
    CardinalityEstimatorAddCodec.RequestParameters params = CardinalityEstimatorAddCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.hash));
}
{
    ClientMessage clientMessage = CardinalityEstimatorAddCodec.encodeResponse( );
    CardinalityEstimatorAddCodec.ResponseParameters params = CardinalityEstimatorAddCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = CardinalityEstimatorEstimateCodec.encodeRequest(    aString   );
    CardinalityEstimatorEstimateCodec.RequestParameters params = CardinalityEstimatorEstimateCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = CardinalityEstimatorEstimateCodec.encodeResponse(    aLong   );
    CardinalityEstimatorEstimateCodec.ResponseParameters params = CardinalityEstimatorEstimateCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aLong, params.response));
}
{
    ClientMessage clientMessage = ScheduledExecutorShutdownCodec.encodeRequest(    aString ,    anAddress   );
    ScheduledExecutorShutdownCodec.RequestParameters params = ScheduledExecutorShutdownCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.schedulerName));
            assertTrue(isEqual(anAddress, params.address));
}
{
    ClientMessage clientMessage = ScheduledExecutorShutdownCodec.encodeResponse( );
    ScheduledExecutorShutdownCodec.ResponseParameters params = ScheduledExecutorShutdownCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = ScheduledExecutorSubmitToPartitionCodec.encodeRequest(    aString ,    aByte ,    aString ,    aData ,    aLong ,    aLong   );
    ScheduledExecutorSubmitToPartitionCodec.RequestParameters params = ScheduledExecutorSubmitToPartitionCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.schedulerName));
            assertTrue(isEqual(aByte, params.type));
            assertTrue(isEqual(aString, params.taskName));
            assertTrue(isEqual(aData, params.task));
            assertTrue(isEqual(aLong, params.initialDelayInMillis));
            assertTrue(isEqual(aLong, params.periodInMillis));
}
{
    ClientMessage clientMessage = ScheduledExecutorSubmitToPartitionCodec.encodeResponse( );
    ScheduledExecutorSubmitToPartitionCodec.ResponseParameters params = ScheduledExecutorSubmitToPartitionCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = ScheduledExecutorSubmitToAddressCodec.encodeRequest(    aString ,    anAddress ,    aByte ,    aString ,    aData ,    aLong ,    aLong   );
    ScheduledExecutorSubmitToAddressCodec.RequestParameters params = ScheduledExecutorSubmitToAddressCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.schedulerName));
            assertTrue(isEqual(anAddress, params.address));
            assertTrue(isEqual(aByte, params.type));
            assertTrue(isEqual(aString, params.taskName));
            assertTrue(isEqual(aData, params.task));
            assertTrue(isEqual(aLong, params.initialDelayInMillis));
            assertTrue(isEqual(aLong, params.periodInMillis));
}
{
    ClientMessage clientMessage = ScheduledExecutorSubmitToAddressCodec.encodeResponse( );
    ScheduledExecutorSubmitToAddressCodec.ResponseParameters params = ScheduledExecutorSubmitToAddressCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = ScheduledExecutorGetAllScheduledFuturesCodec.encodeRequest(    aString   );
    ScheduledExecutorGetAllScheduledFuturesCodec.RequestParameters params = ScheduledExecutorGetAllScheduledFuturesCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.schedulerName));
}
{
    ClientMessage clientMessage = ScheduledExecutorGetAllScheduledFuturesCodec.encodeResponse(    taskHandlers   );
    ScheduledExecutorGetAllScheduledFuturesCodec.ResponseParameters params = ScheduledExecutorGetAllScheduledFuturesCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(taskHandlers, params.handlers));
}
{
    ClientMessage clientMessage = ScheduledExecutorGetStatsFromPartitionCodec.encodeRequest(    aString ,    aString   );
    ScheduledExecutorGetStatsFromPartitionCodec.RequestParameters params = ScheduledExecutorGetStatsFromPartitionCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.schedulerName));
            assertTrue(isEqual(aString, params.taskName));
}
{
    ClientMessage clientMessage = ScheduledExecutorGetStatsFromPartitionCodec.encodeResponse(    aLong ,    aLong ,    aLong ,    aLong ,    aLong   );
    ScheduledExecutorGetStatsFromPartitionCodec.ResponseParameters params = ScheduledExecutorGetStatsFromPartitionCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aLong, params.lastIdleTimeNanos));
            assertTrue(isEqual(aLong, params.totalIdleTimeNanos));
            assertTrue(isEqual(aLong, params.totalRuns));
            assertTrue(isEqual(aLong, params.totalRunTimeNanos));
            assertTrue(isEqual(aLong, params.lastRunDurationNanos));
}
{
    ClientMessage clientMessage = ScheduledExecutorGetStatsFromAddressCodec.encodeRequest(    aString ,    aString ,    anAddress   );
    ScheduledExecutorGetStatsFromAddressCodec.RequestParameters params = ScheduledExecutorGetStatsFromAddressCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.schedulerName));
            assertTrue(isEqual(aString, params.taskName));
            assertTrue(isEqual(anAddress, params.address));
}
{
    ClientMessage clientMessage = ScheduledExecutorGetStatsFromAddressCodec.encodeResponse(    aLong ,    aLong ,    aLong ,    aLong ,    aLong   );
    ScheduledExecutorGetStatsFromAddressCodec.ResponseParameters params = ScheduledExecutorGetStatsFromAddressCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aLong, params.lastIdleTimeNanos));
            assertTrue(isEqual(aLong, params.totalIdleTimeNanos));
            assertTrue(isEqual(aLong, params.totalRuns));
            assertTrue(isEqual(aLong, params.totalRunTimeNanos));
            assertTrue(isEqual(aLong, params.lastRunDurationNanos));
}
{
    ClientMessage clientMessage = ScheduledExecutorGetDelayFromPartitionCodec.encodeRequest(    aString ,    aString   );
    ScheduledExecutorGetDelayFromPartitionCodec.RequestParameters params = ScheduledExecutorGetDelayFromPartitionCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.schedulerName));
            assertTrue(isEqual(aString, params.taskName));
}
{
    ClientMessage clientMessage = ScheduledExecutorGetDelayFromPartitionCodec.encodeResponse(    aLong   );
    ScheduledExecutorGetDelayFromPartitionCodec.ResponseParameters params = ScheduledExecutorGetDelayFromPartitionCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aLong, params.response));
}
{
    ClientMessage clientMessage = ScheduledExecutorGetDelayFromAddressCodec.encodeRequest(    aString ,    aString ,    anAddress   );
    ScheduledExecutorGetDelayFromAddressCodec.RequestParameters params = ScheduledExecutorGetDelayFromAddressCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.schedulerName));
            assertTrue(isEqual(aString, params.taskName));
            assertTrue(isEqual(anAddress, params.address));
}
{
    ClientMessage clientMessage = ScheduledExecutorGetDelayFromAddressCodec.encodeResponse(    aLong   );
    ScheduledExecutorGetDelayFromAddressCodec.ResponseParameters params = ScheduledExecutorGetDelayFromAddressCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aLong, params.response));
}
{
    ClientMessage clientMessage = ScheduledExecutorCancelFromPartitionCodec.encodeRequest(    aString ,    aString ,    aBoolean   );
    ScheduledExecutorCancelFromPartitionCodec.RequestParameters params = ScheduledExecutorCancelFromPartitionCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.schedulerName));
            assertTrue(isEqual(aString, params.taskName));
            assertTrue(isEqual(aBoolean, params.mayInterruptIfRunning));
}
{
    ClientMessage clientMessage = ScheduledExecutorCancelFromPartitionCodec.encodeResponse(    aBoolean   );
    ScheduledExecutorCancelFromPartitionCodec.ResponseParameters params = ScheduledExecutorCancelFromPartitionCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = ScheduledExecutorCancelFromAddressCodec.encodeRequest(    aString ,    aString ,    anAddress ,    aBoolean   );
    ScheduledExecutorCancelFromAddressCodec.RequestParameters params = ScheduledExecutorCancelFromAddressCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.schedulerName));
            assertTrue(isEqual(aString, params.taskName));
            assertTrue(isEqual(anAddress, params.address));
            assertTrue(isEqual(aBoolean, params.mayInterruptIfRunning));
}
{
    ClientMessage clientMessage = ScheduledExecutorCancelFromAddressCodec.encodeResponse(    aBoolean   );
    ScheduledExecutorCancelFromAddressCodec.ResponseParameters params = ScheduledExecutorCancelFromAddressCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = ScheduledExecutorIsCancelledFromPartitionCodec.encodeRequest(    aString ,    aString   );
    ScheduledExecutorIsCancelledFromPartitionCodec.RequestParameters params = ScheduledExecutorIsCancelledFromPartitionCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.schedulerName));
            assertTrue(isEqual(aString, params.taskName));
}
{
    ClientMessage clientMessage = ScheduledExecutorIsCancelledFromPartitionCodec.encodeResponse(    aBoolean   );
    ScheduledExecutorIsCancelledFromPartitionCodec.ResponseParameters params = ScheduledExecutorIsCancelledFromPartitionCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = ScheduledExecutorIsCancelledFromAddressCodec.encodeRequest(    aString ,    aString ,    anAddress   );
    ScheduledExecutorIsCancelledFromAddressCodec.RequestParameters params = ScheduledExecutorIsCancelledFromAddressCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.schedulerName));
            assertTrue(isEqual(aString, params.taskName));
            assertTrue(isEqual(anAddress, params.address));
}
{
    ClientMessage clientMessage = ScheduledExecutorIsCancelledFromAddressCodec.encodeResponse(    aBoolean   );
    ScheduledExecutorIsCancelledFromAddressCodec.ResponseParameters params = ScheduledExecutorIsCancelledFromAddressCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = ScheduledExecutorIsDoneFromPartitionCodec.encodeRequest(    aString ,    aString   );
    ScheduledExecutorIsDoneFromPartitionCodec.RequestParameters params = ScheduledExecutorIsDoneFromPartitionCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.schedulerName));
            assertTrue(isEqual(aString, params.taskName));
}
{
    ClientMessage clientMessage = ScheduledExecutorIsDoneFromPartitionCodec.encodeResponse(    aBoolean   );
    ScheduledExecutorIsDoneFromPartitionCodec.ResponseParameters params = ScheduledExecutorIsDoneFromPartitionCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = ScheduledExecutorIsDoneFromAddressCodec.encodeRequest(    aString ,    aString ,    anAddress   );
    ScheduledExecutorIsDoneFromAddressCodec.RequestParameters params = ScheduledExecutorIsDoneFromAddressCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.schedulerName));
            assertTrue(isEqual(aString, params.taskName));
            assertTrue(isEqual(anAddress, params.address));
}
{
    ClientMessage clientMessage = ScheduledExecutorIsDoneFromAddressCodec.encodeResponse(    aBoolean   );
    ScheduledExecutorIsDoneFromAddressCodec.ResponseParameters params = ScheduledExecutorIsDoneFromAddressCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aBoolean, params.response));
}
{
    ClientMessage clientMessage = ScheduledExecutorGetResultFromPartitionCodec.encodeRequest(    aString ,    aString   );
    ScheduledExecutorGetResultFromPartitionCodec.RequestParameters params = ScheduledExecutorGetResultFromPartitionCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.schedulerName));
            assertTrue(isEqual(aString, params.taskName));
}
{
    ClientMessage clientMessage = ScheduledExecutorGetResultFromPartitionCodec.encodeResponse(    aData   );
    ScheduledExecutorGetResultFromPartitionCodec.ResponseParameters params = ScheduledExecutorGetResultFromPartitionCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = ScheduledExecutorGetResultFromAddressCodec.encodeRequest(    aString ,    aString ,    anAddress   );
    ScheduledExecutorGetResultFromAddressCodec.RequestParameters params = ScheduledExecutorGetResultFromAddressCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.schedulerName));
            assertTrue(isEqual(aString, params.taskName));
            assertTrue(isEqual(anAddress, params.address));
}
{
    ClientMessage clientMessage = ScheduledExecutorGetResultFromAddressCodec.encodeResponse(    aData   );
    ScheduledExecutorGetResultFromAddressCodec.ResponseParameters params = ScheduledExecutorGetResultFromAddressCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aData, params.response));
}
{
    ClientMessage clientMessage = ScheduledExecutorDisposeFromPartitionCodec.encodeRequest(    aString ,    aString   );
    ScheduledExecutorDisposeFromPartitionCodec.RequestParameters params = ScheduledExecutorDisposeFromPartitionCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.schedulerName));
            assertTrue(isEqual(aString, params.taskName));
}
{
    ClientMessage clientMessage = ScheduledExecutorDisposeFromPartitionCodec.encodeResponse( );
    ScheduledExecutorDisposeFromPartitionCodec.ResponseParameters params = ScheduledExecutorDisposeFromPartitionCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = ScheduledExecutorDisposeFromAddressCodec.encodeRequest(    aString ,    aString ,    anAddress   );
    ScheduledExecutorDisposeFromAddressCodec.RequestParameters params = ScheduledExecutorDisposeFromAddressCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.schedulerName));
            assertTrue(isEqual(aString, params.taskName));
            assertTrue(isEqual(anAddress, params.address));
}
{
    ClientMessage clientMessage = ScheduledExecutorDisposeFromAddressCodec.encodeResponse( );
    ScheduledExecutorDisposeFromAddressCodec.ResponseParameters params = ScheduledExecutorDisposeFromAddressCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = DynamicConfigAddMultiMapConfigCodec.encodeRequest(    aString ,    aString ,    listenerConfigs ,    aBoolean ,    anInt ,    anInt ,    aBoolean ,    aString ,    aString ,    anInt   );
    DynamicConfigAddMultiMapConfigCodec.RequestParameters params = DynamicConfigAddMultiMapConfigCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.collectionType));
            assertTrue(isEqual(listenerConfigs, params.listenerConfigs));
            assertTrue(isEqual(aBoolean, params.binary));
            assertTrue(isEqual(anInt, params.backupCount));
            assertTrue(isEqual(anInt, params.asyncBackupCount));
            assertTrue(isEqual(aBoolean, params.statisticsEnabled));
            assertTrue(isEqual(aString, params.quorumName));
            assertTrue(isEqual(aString, params.mergePolicy));
            assertTrue(isEqual(anInt, params.mergeBatchSize));
}
{
    ClientMessage clientMessage = DynamicConfigAddMultiMapConfigCodec.encodeResponse( );
    DynamicConfigAddMultiMapConfigCodec.ResponseParameters params = DynamicConfigAddMultiMapConfigCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = DynamicConfigAddRingbufferConfigCodec.encodeRequest(    aString ,    anInt ,    anInt ,    anInt ,    anInt ,    aString ,    ringbufferStore ,    aString ,    aString ,    anInt   );
    DynamicConfigAddRingbufferConfigCodec.RequestParameters params = DynamicConfigAddRingbufferConfigCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.capacity));
            assertTrue(isEqual(anInt, params.backupCount));
            assertTrue(isEqual(anInt, params.asyncBackupCount));
            assertTrue(isEqual(anInt, params.timeToLiveSeconds));
            assertTrue(isEqual(aString, params.inMemoryFormat));
            assertTrue(isEqual(ringbufferStore, params.ringbufferStoreConfig));
            assertTrue(isEqual(aString, params.quorumName));
            assertTrue(isEqual(aString, params.mergePolicy));
            assertTrue(isEqual(anInt, params.mergeBatchSize));
}
{
    ClientMessage clientMessage = DynamicConfigAddRingbufferConfigCodec.encodeResponse( );
    DynamicConfigAddRingbufferConfigCodec.ResponseParameters params = DynamicConfigAddRingbufferConfigCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = DynamicConfigAddCardinalityEstimatorConfigCodec.encodeRequest(    aString ,    anInt ,    anInt ,    aString ,    aString ,    anInt   );
    DynamicConfigAddCardinalityEstimatorConfigCodec.RequestParameters params = DynamicConfigAddCardinalityEstimatorConfigCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.backupCount));
            assertTrue(isEqual(anInt, params.asyncBackupCount));
            assertTrue(isEqual(aString, params.quorumName));
            assertTrue(isEqual(aString, params.mergePolicy));
            assertTrue(isEqual(anInt, params.mergeBatchSize));
}
{
    ClientMessage clientMessage = DynamicConfigAddCardinalityEstimatorConfigCodec.encodeResponse( );
    DynamicConfigAddCardinalityEstimatorConfigCodec.ResponseParameters params = DynamicConfigAddCardinalityEstimatorConfigCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = DynamicConfigAddLockConfigCodec.encodeRequest(    aString ,    aString   );
    DynamicConfigAddLockConfigCodec.RequestParameters params = DynamicConfigAddLockConfigCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.quorumName));
}
{
    ClientMessage clientMessage = DynamicConfigAddLockConfigCodec.encodeResponse( );
    DynamicConfigAddLockConfigCodec.ResponseParameters params = DynamicConfigAddLockConfigCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = DynamicConfigAddListConfigCodec.encodeRequest(    aString ,    listenerConfigs ,    anInt ,    anInt ,    anInt ,    aBoolean ,    aString ,    aString ,    anInt   );
    DynamicConfigAddListConfigCodec.RequestParameters params = DynamicConfigAddListConfigCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(listenerConfigs, params.listenerConfigs));
            assertTrue(isEqual(anInt, params.backupCount));
            assertTrue(isEqual(anInt, params.asyncBackupCount));
            assertTrue(isEqual(anInt, params.maxSize));
            assertTrue(isEqual(aBoolean, params.statisticsEnabled));
            assertTrue(isEqual(aString, params.quorumName));
            assertTrue(isEqual(aString, params.mergePolicy));
            assertTrue(isEqual(anInt, params.mergeBatchSize));
}
{
    ClientMessage clientMessage = DynamicConfigAddListConfigCodec.encodeResponse( );
    DynamicConfigAddListConfigCodec.ResponseParameters params = DynamicConfigAddListConfigCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = DynamicConfigAddSetConfigCodec.encodeRequest(    aString ,    listenerConfigs ,    anInt ,    anInt ,    anInt ,    aBoolean ,    aString ,    aString ,    anInt   );
    DynamicConfigAddSetConfigCodec.RequestParameters params = DynamicConfigAddSetConfigCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(listenerConfigs, params.listenerConfigs));
            assertTrue(isEqual(anInt, params.backupCount));
            assertTrue(isEqual(anInt, params.asyncBackupCount));
            assertTrue(isEqual(anInt, params.maxSize));
            assertTrue(isEqual(aBoolean, params.statisticsEnabled));
            assertTrue(isEqual(aString, params.quorumName));
            assertTrue(isEqual(aString, params.mergePolicy));
            assertTrue(isEqual(anInt, params.mergeBatchSize));
}
{
    ClientMessage clientMessage = DynamicConfigAddSetConfigCodec.encodeResponse( );
    DynamicConfigAddSetConfigCodec.ResponseParameters params = DynamicConfigAddSetConfigCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = DynamicConfigAddReplicatedMapConfigCodec.encodeRequest(    aString ,    aString ,    aBoolean ,    aBoolean ,    aString ,    listenerConfigs ,    aString ,    anInt   );
    DynamicConfigAddReplicatedMapConfigCodec.RequestParameters params = DynamicConfigAddReplicatedMapConfigCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.inMemoryFormat));
            assertTrue(isEqual(aBoolean, params.asyncFillup));
            assertTrue(isEqual(aBoolean, params.statisticsEnabled));
            assertTrue(isEqual(aString, params.mergePolicy));
            assertTrue(isEqual(listenerConfigs, params.listenerConfigs));
            assertTrue(isEqual(aString, params.quorumName));
            assertTrue(isEqual(anInt, params.mergeBatchSize));
}
{
    ClientMessage clientMessage = DynamicConfigAddReplicatedMapConfigCodec.encodeResponse( );
    DynamicConfigAddReplicatedMapConfigCodec.ResponseParameters params = DynamicConfigAddReplicatedMapConfigCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = DynamicConfigAddTopicConfigCodec.encodeRequest(    aString ,    aBoolean ,    aBoolean ,    aBoolean ,    listenerConfigs   );
    DynamicConfigAddTopicConfigCodec.RequestParameters params = DynamicConfigAddTopicConfigCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aBoolean, params.globalOrderingEnabled));
            assertTrue(isEqual(aBoolean, params.statisticsEnabled));
            assertTrue(isEqual(aBoolean, params.multiThreadingEnabled));
            assertTrue(isEqual(listenerConfigs, params.listenerConfigs));
}
{
    ClientMessage clientMessage = DynamicConfigAddTopicConfigCodec.encodeResponse( );
    DynamicConfigAddTopicConfigCodec.ResponseParameters params = DynamicConfigAddTopicConfigCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = DynamicConfigAddExecutorConfigCodec.encodeRequest(    aString ,    anInt ,    anInt ,    aBoolean ,    aString   );
    DynamicConfigAddExecutorConfigCodec.RequestParameters params = DynamicConfigAddExecutorConfigCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.poolSize));
            assertTrue(isEqual(anInt, params.queueCapacity));
            assertTrue(isEqual(aBoolean, params.statisticsEnabled));
            assertTrue(isEqual(aString, params.quorumName));
}
{
    ClientMessage clientMessage = DynamicConfigAddExecutorConfigCodec.encodeResponse( );
    DynamicConfigAddExecutorConfigCodec.ResponseParameters params = DynamicConfigAddExecutorConfigCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = DynamicConfigAddDurableExecutorConfigCodec.encodeRequest(    aString ,    anInt ,    anInt ,    anInt ,    aString   );
    DynamicConfigAddDurableExecutorConfigCodec.RequestParameters params = DynamicConfigAddDurableExecutorConfigCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.poolSize));
            assertTrue(isEqual(anInt, params.durability));
            assertTrue(isEqual(anInt, params.capacity));
            assertTrue(isEqual(aString, params.quorumName));
}
{
    ClientMessage clientMessage = DynamicConfigAddDurableExecutorConfigCodec.encodeResponse( );
    DynamicConfigAddDurableExecutorConfigCodec.ResponseParameters params = DynamicConfigAddDurableExecutorConfigCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = DynamicConfigAddScheduledExecutorConfigCodec.encodeRequest(    aString ,    anInt ,    anInt ,    anInt ,    aString ,    aString ,    anInt   );
    DynamicConfigAddScheduledExecutorConfigCodec.RequestParameters params = DynamicConfigAddScheduledExecutorConfigCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.poolSize));
            assertTrue(isEqual(anInt, params.durability));
            assertTrue(isEqual(anInt, params.capacity));
            assertTrue(isEqual(aString, params.quorumName));
            assertTrue(isEqual(aString, params.mergePolicy));
            assertTrue(isEqual(anInt, params.mergeBatchSize));
}
{
    ClientMessage clientMessage = DynamicConfigAddScheduledExecutorConfigCodec.encodeResponse( );
    DynamicConfigAddScheduledExecutorConfigCodec.ResponseParameters params = DynamicConfigAddScheduledExecutorConfigCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = DynamicConfigAddSemaphoreConfigCodec.encodeRequest(    aString ,    anInt ,    anInt ,    anInt ,    aString   );
    DynamicConfigAddSemaphoreConfigCodec.RequestParameters params = DynamicConfigAddSemaphoreConfigCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.initialPermits));
            assertTrue(isEqual(anInt, params.backupCount));
            assertTrue(isEqual(anInt, params.asyncBackupCount));
            assertTrue(isEqual(aString, params.quorumName));
}
{
    ClientMessage clientMessage = DynamicConfigAddSemaphoreConfigCodec.encodeResponse( );
    DynamicConfigAddSemaphoreConfigCodec.ResponseParameters params = DynamicConfigAddSemaphoreConfigCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = DynamicConfigAddQueueConfigCodec.encodeRequest(    aString ,    listenerConfigs ,    anInt ,    anInt ,    anInt ,    anInt ,    aBoolean ,    aString ,    queueStoreConfig ,    aString ,    anInt   );
    DynamicConfigAddQueueConfigCodec.RequestParameters params = DynamicConfigAddQueueConfigCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(listenerConfigs, params.listenerConfigs));
            assertTrue(isEqual(anInt, params.backupCount));
            assertTrue(isEqual(anInt, params.asyncBackupCount));
            assertTrue(isEqual(anInt, params.maxSize));
            assertTrue(isEqual(anInt, params.emptyQueueTtl));
            assertTrue(isEqual(aBoolean, params.statisticsEnabled));
            assertTrue(isEqual(aString, params.quorumName));
            assertTrue(isEqual(queueStoreConfig, params.queueStoreConfig));
            assertTrue(isEqual(aString, params.mergePolicy));
            assertTrue(isEqual(anInt, params.mergeBatchSize));
}
{
    ClientMessage clientMessage = DynamicConfigAddQueueConfigCodec.encodeResponse( );
    DynamicConfigAddQueueConfigCodec.ResponseParameters params = DynamicConfigAddQueueConfigCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = DynamicConfigAddMapConfigCodec.encodeRequest(    aString ,    anInt ,    anInt ,    anInt ,    anInt ,    aString ,    aBoolean ,    aString ,    aString ,    aString ,    listenerConfigs ,    listenerConfigs ,    aBoolean ,    aString ,    aData ,    aString ,    anInt ,    mapStoreConfig ,    nearCacheConfig ,    wanReplicationRef ,    mapIndexConfigs ,    mapAttributeConfigs ,    queryCacheConfigs ,    aString ,    aData ,    hotRestartConfig ,    anInt   );
    DynamicConfigAddMapConfigCodec.RequestParameters params = DynamicConfigAddMapConfigCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.backupCount));
            assertTrue(isEqual(anInt, params.asyncBackupCount));
            assertTrue(isEqual(anInt, params.timeToLiveSeconds));
            assertTrue(isEqual(anInt, params.maxIdleSeconds));
            assertTrue(isEqual(aString, params.evictionPolicy));
            assertTrue(isEqual(aBoolean, params.readBackupData));
            assertTrue(isEqual(aString, params.cacheDeserializedValues));
            assertTrue(isEqual(aString, params.mergePolicy));
            assertTrue(isEqual(aString, params.inMemoryFormat));
            assertTrue(isEqual(listenerConfigs, params.listenerConfigs));
            assertTrue(isEqual(listenerConfigs, params.partitionLostListenerConfigs));
            assertTrue(isEqual(aBoolean, params.statisticsEnabled));
            assertTrue(isEqual(aString, params.quorumName));
            assertTrue(isEqual(aData, params.mapEvictionPolicy));
            assertTrue(isEqual(aString, params.maxSizeConfigMaxSizePolicy));
            assertTrue(isEqual(anInt, params.maxSizeConfigSize));
            assertTrue(isEqual(mapStoreConfig, params.mapStoreConfig));
            assertTrue(isEqual(nearCacheConfig, params.nearCacheConfig));
            assertTrue(isEqual(wanReplicationRef, params.wanReplicationRef));
            assertTrue(isEqual(mapIndexConfigs, params.mapIndexConfigs));
            assertTrue(isEqual(mapAttributeConfigs, params.mapAttributeConfigs));
            assertTrue(isEqual(queryCacheConfigs, params.queryCacheConfigs));
            assertTrue(isEqual(aString, params.partitioningStrategyClassName));
            assertTrue(isEqual(aData, params.partitioningStrategyImplementation));
            assertTrue(isEqual(hotRestartConfig, params.hotRestartConfig));
            assertTrue(isEqual(anInt, params.mergeBatchSize));
}
{
    ClientMessage clientMessage = DynamicConfigAddMapConfigCodec.encodeResponse( );
    DynamicConfigAddMapConfigCodec.ResponseParameters params = DynamicConfigAddMapConfigCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = DynamicConfigAddReliableTopicConfigCodec.encodeRequest(    aString ,    listenerConfigs ,    anInt ,    aBoolean ,    aString ,    aData   );
    DynamicConfigAddReliableTopicConfigCodec.RequestParameters params = DynamicConfigAddReliableTopicConfigCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(listenerConfigs, params.listenerConfigs));
            assertTrue(isEqual(anInt, params.readBatchSize));
            assertTrue(isEqual(aBoolean, params.statisticsEnabled));
            assertTrue(isEqual(aString, params.topicOverloadPolicy));
            assertTrue(isEqual(aData, params.executor));
}
{
    ClientMessage clientMessage = DynamicConfigAddReliableTopicConfigCodec.encodeResponse( );
    DynamicConfigAddReliableTopicConfigCodec.ResponseParameters params = DynamicConfigAddReliableTopicConfigCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = DynamicConfigAddCacheConfigCodec.encodeRequest(    aString ,    aString ,    aString ,    aBoolean ,    aBoolean ,    aBoolean ,    aBoolean ,    aString ,    aString ,    aString ,    aString ,    anInt ,    anInt ,    aString ,    aString ,    aString ,    aBoolean ,    listenerConfigs ,    aString ,    timedExpiryPolicyFactoryConfig ,    cacheEntryListenerConfigs ,    evictionConfig ,    wanReplicationRef ,    hotRestartConfig   );
    DynamicConfigAddCacheConfigCodec.RequestParameters params = DynamicConfigAddCacheConfigCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.keyType));
            assertTrue(isEqual(aString, params.valueType));
            assertTrue(isEqual(aBoolean, params.statisticsEnabled));
            assertTrue(isEqual(aBoolean, params.managementEnabled));
            assertTrue(isEqual(aBoolean, params.readThrough));
            assertTrue(isEqual(aBoolean, params.writeThrough));
            assertTrue(isEqual(aString, params.cacheLoaderFactory));
            assertTrue(isEqual(aString, params.cacheWriterFactory));
            assertTrue(isEqual(aString, params.cacheLoader));
            assertTrue(isEqual(aString, params.cacheWriter));
            assertTrue(isEqual(anInt, params.backupCount));
            assertTrue(isEqual(anInt, params.asyncBackupCount));
            assertTrue(isEqual(aString, params.inMemoryFormat));
            assertTrue(isEqual(aString, params.quorumName));
            assertTrue(isEqual(aString, params.mergePolicy));
            assertTrue(isEqual(aBoolean, params.disablePerEntryInvalidationEvents));
            assertTrue(isEqual(listenerConfigs, params.partitionLostListenerConfigs));
            assertTrue(isEqual(aString, params.expiryPolicyFactoryClassName));
            assertTrue(isEqual(timedExpiryPolicyFactoryConfig, params.timedExpiryPolicyFactoryConfig));
            assertTrue(isEqual(cacheEntryListenerConfigs, params.cacheEntryListeners));
            assertTrue(isEqual(evictionConfig, params.evictionConfig));
            assertTrue(isEqual(wanReplicationRef, params.wanReplicationRef));
            assertTrue(isEqual(hotRestartConfig, params.hotRestartConfig));
}
{
    ClientMessage clientMessage = DynamicConfigAddCacheConfigCodec.encodeResponse( );
    DynamicConfigAddCacheConfigCodec.ResponseParameters params = DynamicConfigAddCacheConfigCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = DynamicConfigAddEventJournalConfigCodec.encodeRequest(    aString ,    aString ,    aBoolean ,    anInt ,    anInt   );
    DynamicConfigAddEventJournalConfigCodec.RequestParameters params = DynamicConfigAddEventJournalConfigCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.mapName));
            assertTrue(isEqual(aString, params.cacheName));
            assertTrue(isEqual(aBoolean, params.enabled));
            assertTrue(isEqual(anInt, params.capacity));
            assertTrue(isEqual(anInt, params.timeToLiveSeconds));
}
{
    ClientMessage clientMessage = DynamicConfigAddEventJournalConfigCodec.encodeResponse( );
    DynamicConfigAddEventJournalConfigCodec.ResponseParameters params = DynamicConfigAddEventJournalConfigCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = DynamicConfigAddFlakeIdGeneratorConfigCodec.encodeRequest(    aString ,    anInt ,    aLong ,    aLong ,    aBoolean ,    aLong   );
    DynamicConfigAddFlakeIdGeneratorConfigCodec.RequestParameters params = DynamicConfigAddFlakeIdGeneratorConfigCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.prefetchCount));
            assertTrue(isEqual(aLong, params.prefetchValidity));
            assertTrue(isEqual(aLong, params.idOffset));
            assertTrue(isEqual(aBoolean, params.statisticsEnabled));
            assertTrue(isEqual(aLong, params.nodeIdOffset));
}
{
    ClientMessage clientMessage = DynamicConfigAddFlakeIdGeneratorConfigCodec.encodeResponse( );
    DynamicConfigAddFlakeIdGeneratorConfigCodec.ResponseParameters params = DynamicConfigAddFlakeIdGeneratorConfigCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = DynamicConfigAddAtomicLongConfigCodec.encodeRequest(    aString ,    aString ,    aString ,    anInt   );
    DynamicConfigAddAtomicLongConfigCodec.RequestParameters params = DynamicConfigAddAtomicLongConfigCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.quorumName));
            assertTrue(isEqual(aString, params.mergePolicy));
            assertTrue(isEqual(anInt, params.mergeBatchSize));
}
{
    ClientMessage clientMessage = DynamicConfigAddAtomicLongConfigCodec.encodeResponse( );
    DynamicConfigAddAtomicLongConfigCodec.ResponseParameters params = DynamicConfigAddAtomicLongConfigCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = DynamicConfigAddAtomicReferenceConfigCodec.encodeRequest(    aString ,    aString ,    aString ,    anInt   );
    DynamicConfigAddAtomicReferenceConfigCodec.RequestParameters params = DynamicConfigAddAtomicReferenceConfigCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.quorumName));
            assertTrue(isEqual(aString, params.mergePolicy));
            assertTrue(isEqual(anInt, params.mergeBatchSize));
}
{
    ClientMessage clientMessage = DynamicConfigAddAtomicReferenceConfigCodec.encodeResponse( );
    DynamicConfigAddAtomicReferenceConfigCodec.ResponseParameters params = DynamicConfigAddAtomicReferenceConfigCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = DynamicConfigAddCountDownLatchConfigCodec.encodeRequest(    aString ,    aString   );
    DynamicConfigAddCountDownLatchConfigCodec.RequestParameters params = DynamicConfigAddCountDownLatchConfigCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aString, params.quorumName));
}
{
    ClientMessage clientMessage = DynamicConfigAddCountDownLatchConfigCodec.encodeResponse( );
    DynamicConfigAddCountDownLatchConfigCodec.ResponseParameters params = DynamicConfigAddCountDownLatchConfigCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = DynamicConfigAddPNCounterConfigCodec.encodeRequest(    aString ,    anInt ,    aBoolean ,    aString   );
    DynamicConfigAddPNCounterConfigCodec.RequestParameters params = DynamicConfigAddPNCounterConfigCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.replicaCount));
            assertTrue(isEqual(aBoolean, params.statisticsEnabled));
            assertTrue(isEqual(aString, params.quorumName));
}
{
    ClientMessage clientMessage = DynamicConfigAddPNCounterConfigCodec.encodeResponse( );
    DynamicConfigAddPNCounterConfigCodec.ResponseParameters params = DynamicConfigAddPNCounterConfigCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
}
{
    ClientMessage clientMessage = FlakeIdGeneratorNewIdBatchCodec.encodeRequest(    aString ,    anInt   );
    FlakeIdGeneratorNewIdBatchCodec.RequestParameters params = FlakeIdGeneratorNewIdBatchCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(anInt, params.batchSize));
}
{
    ClientMessage clientMessage = FlakeIdGeneratorNewIdBatchCodec.encodeResponse(    aLong ,    aLong ,    anInt   );
    FlakeIdGeneratorNewIdBatchCodec.ResponseParameters params = FlakeIdGeneratorNewIdBatchCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aLong, params.base));
            assertTrue(isEqual(aLong, params.increment));
            assertTrue(isEqual(anInt, params.batchSize));
}
{
    ClientMessage clientMessage = PNCounterGetCodec.encodeRequest(    aString ,    aListOfStringToLong ,    anAddress   );
    PNCounterGetCodec.RequestParameters params = PNCounterGetCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aListOfStringToLong, params.replicaTimestamps));
            assertTrue(isEqual(anAddress, params.targetReplica));
}
{
    ClientMessage clientMessage = PNCounterGetCodec.encodeResponse(    aLong ,    aListOfStringToLong ,    anInt   );
    PNCounterGetCodec.ResponseParameters params = PNCounterGetCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aLong, params.value));
            assertTrue(isEqual(aListOfStringToLong, params.replicaTimestamps));
            assertTrue(isEqual(anInt, params.replicaCount));
}
{
    ClientMessage clientMessage = PNCounterAddCodec.encodeRequest(    aString ,    aLong ,    aBoolean ,    aListOfStringToLong ,    anAddress   );
    PNCounterAddCodec.RequestParameters params = PNCounterAddCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
            assertTrue(isEqual(aLong, params.delta));
            assertTrue(isEqual(aBoolean, params.getBeforeUpdate));
            assertTrue(isEqual(aListOfStringToLong, params.replicaTimestamps));
            assertTrue(isEqual(anAddress, params.targetReplica));
}
{
    ClientMessage clientMessage = PNCounterAddCodec.encodeResponse(    aLong ,    aListOfStringToLong ,    anInt   );
    PNCounterAddCodec.ResponseParameters params = PNCounterAddCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aLong, params.value));
            assertTrue(isEqual(aListOfStringToLong, params.replicaTimestamps));
            assertTrue(isEqual(anInt, params.replicaCount));
}
{
    ClientMessage clientMessage = PNCounterGetConfiguredReplicaCountCodec.encodeRequest(    aString   );
    PNCounterGetConfiguredReplicaCountCodec.RequestParameters params = PNCounterGetConfiguredReplicaCountCodec.decodeRequest(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(aString, params.name));
}
{
    ClientMessage clientMessage = PNCounterGetConfiguredReplicaCountCodec.encodeResponse(    anInt   );
    PNCounterGetConfiguredReplicaCountCodec.ResponseParameters params = PNCounterGetConfiguredReplicaCountCodec.decodeResponse(ClientMessage.createForDecode(clientMessage.buffer(), 0));
            assertTrue(isEqual(anInt, params.response));
}
    }
}

