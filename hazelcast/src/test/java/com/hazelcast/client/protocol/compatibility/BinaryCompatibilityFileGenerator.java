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

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
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


public class BinaryCompatibilityFileGenerator {
    public static void main(String[] args) throws IOException {
        OutputStream out = new FileOutputStream("1.6.protocol.compatibility.binary");
        DataOutputStream outputStream = new DataOutputStream(out);

{
    ClientMessage clientMessage = ClientAuthenticationCodec.encodeRequest(    aString ,    aString ,    aString ,    aString ,    aBoolean ,    aString ,    aByte ,    aString ,    aString ,    strings ,    anInt ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ClientAuthenticationCodec.encodeResponse(    aByte ,    anAddress ,    aString ,    aString ,    aByte ,    aString ,    members ,    anInt ,    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ClientAuthenticationCustomCodec.encodeRequest(    aData ,    aString ,    aString ,    aBoolean ,    aString ,    aByte ,    aString ,    aString ,    strings ,    anInt ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ClientAuthenticationCustomCodec.encodeResponse(    aByte ,    anAddress ,    aString ,    aString ,    aByte ,    aString ,    members ,    anInt ,    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ClientAddMembershipListenerCodec.encodeRequest(    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ClientAddMembershipListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = ClientAddMembershipListenerCodec.encodeMemberEvent( aMember ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
    {
        ClientMessage clientMessage = ClientAddMembershipListenerCodec.encodeMemberListEvent( members   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
    {
        ClientMessage clientMessage = ClientAddMembershipListenerCodec.encodeMemberAttributeChangeEvent( aString ,  aString ,  anInt ,  aString   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = ClientCreateProxyCodec.encodeRequest(    aString ,    aString ,    anAddress   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ClientCreateProxyCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ClientDestroyProxyCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ClientDestroyProxyCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ClientGetPartitionsCodec.encodeRequest( );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ClientGetPartitionsCodec.encodeResponse(    aPartitionTable ,    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ClientRemoveAllListenersCodec.encodeRequest( );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ClientRemoveAllListenersCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ClientAddPartitionLostListenerCodec.encodeRequest(    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ClientAddPartitionLostListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = ClientAddPartitionLostListenerCodec.encodePartitionLostEvent( anInt ,  anInt ,  anAddress   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = ClientRemovePartitionLostListenerCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ClientRemovePartitionLostListenerCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ClientGetDistributedObjectsCodec.encodeRequest( );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ClientGetDistributedObjectsCodec.encodeResponse(    distributedObjectInfos   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ClientAddDistributedObjectListenerCodec.encodeRequest(    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ClientAddDistributedObjectListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = ClientAddDistributedObjectListenerCodec.encodeDistributedObjectEvent( aString ,  aString ,  aString   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = ClientRemoveDistributedObjectListenerCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ClientRemoveDistributedObjectListenerCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ClientPingCodec.encodeRequest( );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ClientPingCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ClientStatisticsCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ClientStatisticsCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ClientDeployClassesCodec.encodeRequest(    aListOfStringToByteArrEntry   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ClientDeployClassesCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ClientAddPartitionListenerCodec.encodeRequest( );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ClientAddPartitionListenerCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = ClientAddPartitionListenerCodec.encodePartitionsEvent( aPartitionTable ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = ClientCreateProxiesCodec.encodeRequest(    aListOfStringToString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ClientCreateProxiesCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ClientIsFailoverSupportedCodec.encodeRequest( );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ClientIsFailoverSupportedCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapPutCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapPutCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapGetCodec.encodeRequest(    aString ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapGetCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapRemoveCodec.encodeRequest(    aString ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapRemoveCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapReplaceCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapReplaceCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapReplaceIfSameCodec.encodeRequest(    aString ,    aData ,    aData ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapReplaceIfSameCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapContainsKeyCodec.encodeRequest(    aString ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapContainsKeyCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapContainsValueCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapContainsValueCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapRemoveIfSameCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapRemoveIfSameCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapDeleteCodec.encodeRequest(    aString ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapDeleteCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapFlushCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapFlushCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapTryRemoveCodec.encodeRequest(    aString ,    aData ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapTryRemoveCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapTryPutCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapTryPutCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapPutTransientCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapPutTransientCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapPutIfAbsentCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapPutIfAbsentCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapSetCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapSetCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapLockCodec.encodeRequest(    aString ,    aData ,    aLong ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapLockCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapTryLockCodec.encodeRequest(    aString ,    aData ,    aLong ,    aLong ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapTryLockCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapIsLockedCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapIsLockedCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapUnlockCodec.encodeRequest(    aString ,    aData ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapUnlockCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapAddInterceptorCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapAddInterceptorCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapRemoveInterceptorCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapRemoveInterceptorCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapAddEntryListenerToKeyWithPredicateCodec.encodeRequest(    aString ,    aData ,    aData ,    aBoolean ,    anInt ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapAddEntryListenerToKeyWithPredicateCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = MapAddEntryListenerToKeyWithPredicateCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = MapAddEntryListenerWithPredicateCodec.encodeRequest(    aString ,    aData ,    aBoolean ,    anInt ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapAddEntryListenerWithPredicateCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = MapAddEntryListenerWithPredicateCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = MapAddEntryListenerToKeyCodec.encodeRequest(    aString ,    aData ,    aBoolean ,    anInt ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapAddEntryListenerToKeyCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = MapAddEntryListenerToKeyCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = MapAddEntryListenerCodec.encodeRequest(    aString ,    aBoolean ,    anInt ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapAddEntryListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = MapAddEntryListenerCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = MapAddNearCacheEntryListenerCodec.encodeRequest(    aString ,    anInt ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapAddNearCacheEntryListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = MapAddNearCacheEntryListenerCodec.encodeIMapInvalidationEvent( aData ,  aString ,  aUUID ,  aLong   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
    {
        ClientMessage clientMessage = MapAddNearCacheEntryListenerCodec.encodeIMapBatchInvalidationEvent( datas ,  strings ,  uuids ,  longs   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = MapRemoveEntryListenerCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapRemoveEntryListenerCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapAddPartitionLostListenerCodec.encodeRequest(    aString ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapAddPartitionLostListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = MapAddPartitionLostListenerCodec.encodeMapPartitionLostEvent( anInt ,  aString   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = MapRemovePartitionLostListenerCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapRemovePartitionLostListenerCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapGetEntryViewCodec.encodeRequest(    aString ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapGetEntryViewCodec.encodeResponse(    anEntryView ,    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapEvictCodec.encodeRequest(    aString ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapEvictCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapEvictAllCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapEvictAllCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapLoadAllCodec.encodeRequest(    aString ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapLoadAllCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapLoadGivenKeysCodec.encodeRequest(    aString ,    datas ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapLoadGivenKeysCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapKeySetCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapKeySetCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapGetAllCodec.encodeRequest(    aString ,    datas   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapGetAllCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapValuesCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapValuesCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapEntrySetCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapEntrySetCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapKeySetWithPredicateCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapKeySetWithPredicateCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapValuesWithPredicateCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapValuesWithPredicateCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapEntriesWithPredicateCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapEntriesWithPredicateCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapAddIndexCodec.encodeRequest(    aString ,    aString ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapAddIndexCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapSizeCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapSizeCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapIsEmptyCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapIsEmptyCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapPutAllCodec.encodeRequest(    aString ,    aListOfEntry   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapPutAllCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapClearCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapClearCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapExecuteOnKeyCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapExecuteOnKeyCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapSubmitToKeyCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapSubmitToKeyCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapExecuteOnAllKeysCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapExecuteOnAllKeysCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapExecuteWithPredicateCodec.encodeRequest(    aString ,    aData ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapExecuteWithPredicateCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapExecuteOnKeysCodec.encodeRequest(    aString ,    aData ,    datas   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapExecuteOnKeysCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapForceUnlockCodec.encodeRequest(    aString ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapForceUnlockCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapKeySetWithPagingPredicateCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapKeySetWithPagingPredicateCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapValuesWithPagingPredicateCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapValuesWithPagingPredicateCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapEntriesWithPagingPredicateCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapEntriesWithPagingPredicateCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapClearNearCacheCodec.encodeRequest(    aString ,    anAddress   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapClearNearCacheCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapFetchKeysCodec.encodeRequest(    aString ,    anInt ,    anInt ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapFetchKeysCodec.encodeResponse(    anInt ,    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapFetchEntriesCodec.encodeRequest(    aString ,    anInt ,    anInt ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapFetchEntriesCodec.encodeResponse(    anInt ,    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapAggregateCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapAggregateCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapAggregateWithPredicateCodec.encodeRequest(    aString ,    aData ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapAggregateWithPredicateCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapProjectCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapProjectCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapProjectWithPredicateCodec.encodeRequest(    aString ,    aData ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapProjectWithPredicateCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapFetchNearCacheInvalidationMetadataCodec.encodeRequest(    strings ,    anAddress   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapFetchNearCacheInvalidationMetadataCodec.encodeResponse(    aNamePartitionSequenceList ,    aPartitionUuidList   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapAssignAndGetUuidsCodec.encodeRequest( );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapAssignAndGetUuidsCodec.encodeResponse(    aPartitionUuidList   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapRemoveAllCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapRemoveAllCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapAddNearCacheInvalidationListenerCodec.encodeRequest(    aString ,    anInt ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapAddNearCacheInvalidationListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = MapAddNearCacheInvalidationListenerCodec.encodeIMapInvalidationEvent( aData ,  aString ,  aUUID ,  aLong   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
    {
        ClientMessage clientMessage = MapAddNearCacheInvalidationListenerCodec.encodeIMapBatchInvalidationEvent( datas ,  strings ,  uuids ,  longs   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = MapFetchWithQueryCodec.encodeRequest(    aString ,    anInt ,    anInt ,    aData ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapFetchWithQueryCodec.encodeResponse(    datas ,    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapEventJournalSubscribeCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapEventJournalSubscribeCodec.encodeResponse(    aLong ,    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapEventJournalReadCodec.encodeRequest(    aString ,    aLong ,    anInt ,    anInt ,    aData ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapEventJournalReadCodec.encodeResponse(    anInt ,    datas ,    arrLongs ,    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapSetTtlCodec.encodeRequest(    aString ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapSetTtlCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapPutWithMaxIdleCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapPutWithMaxIdleCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapPutTransientWithMaxIdleCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapPutTransientWithMaxIdleCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapPutIfAbsentWithMaxIdleCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapPutIfAbsentWithMaxIdleCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapSetWithMaxIdleCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapSetWithMaxIdleCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapPutCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapPutCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapGetCodec.encodeRequest(    aString ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapGetCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapRemoveCodec.encodeRequest(    aString ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapRemoveCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapKeySetCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapKeySetCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapValuesCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapValuesCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapEntrySetCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapEntrySetCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapContainsKeyCodec.encodeRequest(    aString ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapContainsKeyCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapContainsValueCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapContainsValueCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapContainsEntryCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapContainsEntryCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapSizeCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapSizeCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapClearCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapClearCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapValueCountCodec.encodeRequest(    aString ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapValueCountCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapAddEntryListenerToKeyCodec.encodeRequest(    aString ,    aData ,    aBoolean ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapAddEntryListenerToKeyCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = MultiMapAddEntryListenerToKeyCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = MultiMapAddEntryListenerCodec.encodeRequest(    aString ,    aBoolean ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapAddEntryListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = MultiMapAddEntryListenerCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = MultiMapRemoveEntryListenerCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapRemoveEntryListenerCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapLockCodec.encodeRequest(    aString ,    aData ,    aLong ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapLockCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapTryLockCodec.encodeRequest(    aString ,    aData ,    aLong ,    aLong ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapTryLockCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapIsLockedCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapIsLockedCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapUnlockCodec.encodeRequest(    aString ,    aData ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapUnlockCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapForceUnlockCodec.encodeRequest(    aString ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapForceUnlockCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapRemoveEntryCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapRemoveEntryCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MultiMapDeleteCodec.encodeRequest(    aString ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MultiMapDeleteCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueOfferCodec.encodeRequest(    aString ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueOfferCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueuePutCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueuePutCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueSizeCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueSizeCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueRemoveCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueRemoveCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueuePollCodec.encodeRequest(    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueuePollCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueTakeCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueTakeCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueuePeekCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueuePeekCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueIteratorCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueIteratorCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueDrainToCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueDrainToCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueDrainToMaxSizeCodec.encodeRequest(    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueDrainToMaxSizeCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueContainsCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueContainsCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueContainsAllCodec.encodeRequest(    aString ,    datas   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueContainsAllCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueCompareAndRemoveAllCodec.encodeRequest(    aString ,    datas   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueCompareAndRemoveAllCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueCompareAndRetainAllCodec.encodeRequest(    aString ,    datas   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueCompareAndRetainAllCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueClearCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueClearCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueAddAllCodec.encodeRequest(    aString ,    datas   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueAddAllCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueAddListenerCodec.encodeRequest(    aString ,    aBoolean ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueAddListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = QueueAddListenerCodec.encodeItemEvent( aData ,  aString ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = QueueRemoveListenerCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueRemoveListenerCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueRemainingCapacityCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueRemainingCapacityCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = QueueIsEmptyCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = QueueIsEmptyCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TopicPublishCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TopicPublishCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TopicAddMessageListenerCodec.encodeRequest(    aString ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TopicAddMessageListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = TopicAddMessageListenerCodec.encodeTopicEvent( aData ,  aLong ,  aString   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = TopicRemoveMessageListenerCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TopicRemoveMessageListenerCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListSizeCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListSizeCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListContainsCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListContainsCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListContainsAllCodec.encodeRequest(    aString ,    datas   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListContainsAllCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListAddCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListAddCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListRemoveCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListRemoveCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListAddAllCodec.encodeRequest(    aString ,    datas   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListAddAllCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListCompareAndRemoveAllCodec.encodeRequest(    aString ,    datas   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListCompareAndRemoveAllCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListCompareAndRetainAllCodec.encodeRequest(    aString ,    datas   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListCompareAndRetainAllCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListClearCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListClearCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListGetAllCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListGetAllCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListAddListenerCodec.encodeRequest(    aString ,    aBoolean ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListAddListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = ListAddListenerCodec.encodeItemEvent( aData ,  aString ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = ListRemoveListenerCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListRemoveListenerCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListIsEmptyCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListIsEmptyCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListAddAllWithIndexCodec.encodeRequest(    aString ,    anInt ,    datas   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListAddAllWithIndexCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListGetCodec.encodeRequest(    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListGetCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListSetCodec.encodeRequest(    aString ,    anInt ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListSetCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListAddWithIndexCodec.encodeRequest(    aString ,    anInt ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListAddWithIndexCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListRemoveWithIndexCodec.encodeRequest(    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListRemoveWithIndexCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListLastIndexOfCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListLastIndexOfCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListIndexOfCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListIndexOfCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListSubCodec.encodeRequest(    aString ,    anInt ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListSubCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListIteratorCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListIteratorCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ListListIteratorCodec.encodeRequest(    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ListListIteratorCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SetSizeCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SetSizeCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SetContainsCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SetContainsCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SetContainsAllCodec.encodeRequest(    aString ,    datas   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SetContainsAllCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SetAddCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SetAddCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SetRemoveCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SetRemoveCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SetAddAllCodec.encodeRequest(    aString ,    datas   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SetAddAllCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SetCompareAndRemoveAllCodec.encodeRequest(    aString ,    datas   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SetCompareAndRemoveAllCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SetCompareAndRetainAllCodec.encodeRequest(    aString ,    datas   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SetCompareAndRetainAllCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SetClearCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SetClearCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SetGetAllCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SetGetAllCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SetAddListenerCodec.encodeRequest(    aString ,    aBoolean ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SetAddListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = SetAddListenerCodec.encodeItemEvent( aData ,  aString ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = SetRemoveListenerCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SetRemoveListenerCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SetIsEmptyCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SetIsEmptyCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = LockIsLockedCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = LockIsLockedCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = LockIsLockedByCurrentThreadCodec.encodeRequest(    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = LockIsLockedByCurrentThreadCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = LockGetLockCountCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = LockGetLockCountCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = LockGetRemainingLeaseTimeCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = LockGetRemainingLeaseTimeCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = LockLockCodec.encodeRequest(    aString ,    aLong ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = LockLockCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = LockUnlockCodec.encodeRequest(    aString ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = LockUnlockCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = LockForceUnlockCodec.encodeRequest(    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = LockForceUnlockCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = LockTryLockCodec.encodeRequest(    aString ,    aLong ,    aLong ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = LockTryLockCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ConditionAwaitCodec.encodeRequest(    aString ,    aLong ,    aLong ,    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ConditionAwaitCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ConditionBeforeAwaitCodec.encodeRequest(    aString ,    aLong ,    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ConditionBeforeAwaitCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ConditionSignalCodec.encodeRequest(    aString ,    aLong ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ConditionSignalCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ConditionSignalAllCodec.encodeRequest(    aString ,    aLong ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ConditionSignalAllCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ExecutorServiceShutdownCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ExecutorServiceShutdownCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ExecutorServiceIsShutdownCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ExecutorServiceIsShutdownCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ExecutorServiceCancelOnPartitionCodec.encodeRequest(    aString ,    anInt ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ExecutorServiceCancelOnPartitionCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ExecutorServiceCancelOnAddressCodec.encodeRequest(    aString ,    anAddress ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ExecutorServiceCancelOnAddressCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ExecutorServiceSubmitToPartitionCodec.encodeRequest(    aString ,    aString ,    aData ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ExecutorServiceSubmitToPartitionCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ExecutorServiceSubmitToAddressCodec.encodeRequest(    aString ,    aString ,    aData ,    anAddress   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ExecutorServiceSubmitToAddressCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicLongApplyCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicLongApplyCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicLongAlterCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicLongAlterCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicLongAlterAndGetCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicLongAlterAndGetCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicLongGetAndAlterCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicLongGetAndAlterCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicLongAddAndGetCodec.encodeRequest(    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicLongAddAndGetCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicLongCompareAndSetCodec.encodeRequest(    aString ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicLongCompareAndSetCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicLongDecrementAndGetCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicLongDecrementAndGetCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicLongGetCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicLongGetCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicLongGetAndAddCodec.encodeRequest(    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicLongGetAndAddCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicLongGetAndSetCodec.encodeRequest(    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicLongGetAndSetCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicLongIncrementAndGetCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicLongIncrementAndGetCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicLongGetAndIncrementCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicLongGetAndIncrementCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicLongSetCodec.encodeRequest(    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicLongSetCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicReferenceApplyCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicReferenceApplyCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicReferenceAlterCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicReferenceAlterCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicReferenceAlterAndGetCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicReferenceAlterAndGetCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicReferenceGetAndAlterCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicReferenceGetAndAlterCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicReferenceContainsCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicReferenceContainsCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicReferenceCompareAndSetCodec.encodeRequest(    aString ,    aData ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicReferenceCompareAndSetCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicReferenceGetCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicReferenceGetCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicReferenceSetCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicReferenceSetCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicReferenceClearCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicReferenceClearCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicReferenceGetAndSetCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicReferenceGetAndSetCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicReferenceSetAndGetCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicReferenceSetAndGetCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = AtomicReferenceIsNullCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = AtomicReferenceIsNullCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CountDownLatchAwaitCodec.encodeRequest(    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CountDownLatchAwaitCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CountDownLatchCountDownCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CountDownLatchCountDownCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CountDownLatchGetCountCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CountDownLatchGetCountCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CountDownLatchTrySetCountCodec.encodeRequest(    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CountDownLatchTrySetCountCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SemaphoreInitCodec.encodeRequest(    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SemaphoreInitCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SemaphoreAcquireCodec.encodeRequest(    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SemaphoreAcquireCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SemaphoreAvailablePermitsCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SemaphoreAvailablePermitsCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SemaphoreDrainPermitsCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SemaphoreDrainPermitsCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SemaphoreReducePermitsCodec.encodeRequest(    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SemaphoreReducePermitsCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SemaphoreReleaseCodec.encodeRequest(    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SemaphoreReleaseCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SemaphoreTryAcquireCodec.encodeRequest(    aString ,    anInt ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SemaphoreTryAcquireCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = SemaphoreIncreasePermitsCodec.encodeRequest(    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = SemaphoreIncreasePermitsCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ReplicatedMapPutCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapPutCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ReplicatedMapSizeCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapSizeCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ReplicatedMapIsEmptyCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapIsEmptyCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ReplicatedMapContainsKeyCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapContainsKeyCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ReplicatedMapContainsValueCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapContainsValueCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ReplicatedMapGetCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapGetCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ReplicatedMapRemoveCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapRemoveCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ReplicatedMapPutAllCodec.encodeRequest(    aString ,    aListOfEntry   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapPutAllCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ReplicatedMapClearCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapClearCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.encodeRequest(    aString ,    aData ,    aData ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = ReplicatedMapAddEntryListenerWithPredicateCodec.encodeRequest(    aString ,    aData ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapAddEntryListenerWithPredicateCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = ReplicatedMapAddEntryListenerWithPredicateCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = ReplicatedMapAddEntryListenerToKeyCodec.encodeRequest(    aString ,    aData ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapAddEntryListenerToKeyCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = ReplicatedMapAddEntryListenerToKeyCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = ReplicatedMapAddEntryListenerCodec.encodeRequest(    aString ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapAddEntryListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = ReplicatedMapAddEntryListenerCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = ReplicatedMapRemoveEntryListenerCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapRemoveEntryListenerCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ReplicatedMapKeySetCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapKeySetCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ReplicatedMapValuesCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapValuesCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ReplicatedMapEntrySetCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapEntrySetCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ReplicatedMapAddNearCacheEntryListenerCodec.encodeRequest(    aString ,    aBoolean ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ReplicatedMapAddNearCacheEntryListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = ReplicatedMapAddNearCacheEntryListenerCodec.encodeEntryEvent( aData ,  aData ,  aData ,  aData ,  anInt ,  aString ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = MapReduceCancelCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapReduceCancelCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapReduceJobProcessInformationCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapReduceJobProcessInformationCodec.encodeResponse(    jobPartitionStates ,    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapReduceForMapCodec.encodeRequest(    aString ,    aString ,    aData ,    aData ,    aData ,    aData ,    aString ,    anInt ,    datas ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapReduceForMapCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapReduceForListCodec.encodeRequest(    aString ,    aString ,    aData ,    aData ,    aData ,    aData ,    aString ,    anInt ,    datas ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapReduceForListCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapReduceForSetCodec.encodeRequest(    aString ,    aString ,    aData ,    aData ,    aData ,    aData ,    aString ,    anInt ,    datas ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapReduceForSetCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapReduceForMultiMapCodec.encodeRequest(    aString ,    aString ,    aData ,    aData ,    aData ,    aData ,    aString ,    anInt ,    datas ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapReduceForMultiMapCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = MapReduceForCustomCodec.encodeRequest(    aString ,    aString ,    aData ,    aData ,    aData ,    aData ,    aData ,    anInt ,    datas ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = MapReduceForCustomCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapContainsKeyCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapContainsKeyCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapGetCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapGetCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapGetForUpdateCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapGetForUpdateCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapSizeCodec.encodeRequest(    aString ,    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapSizeCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapIsEmptyCodec.encodeRequest(    aString ,    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapIsEmptyCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapPutCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapPutCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapSetCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapSetCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapPutIfAbsentCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapPutIfAbsentCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapReplaceCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapReplaceCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapReplaceIfSameCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapReplaceIfSameCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapRemoveCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapRemoveCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapDeleteCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapDeleteCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapRemoveIfSameCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapRemoveIfSameCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapKeySetCodec.encodeRequest(    aString ,    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapKeySetCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapKeySetWithPredicateCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapKeySetWithPredicateCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapValuesCodec.encodeRequest(    aString ,    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapValuesCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapValuesWithPredicateCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapValuesWithPredicateCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMapContainsValueCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMapContainsValueCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMultiMapPutCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMultiMapPutCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMultiMapGetCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMultiMapGetCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMultiMapRemoveCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMultiMapRemoveCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMultiMapRemoveEntryCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMultiMapRemoveEntryCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMultiMapValueCountCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMultiMapValueCountCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalMultiMapSizeCodec.encodeRequest(    aString ,    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalMultiMapSizeCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalSetAddCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalSetAddCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalSetRemoveCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalSetRemoveCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalSetSizeCodec.encodeRequest(    aString ,    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalSetSizeCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalListAddCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalListAddCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalListRemoveCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalListRemoveCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalListSizeCodec.encodeRequest(    aString ,    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalListSizeCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalQueueOfferCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalQueueOfferCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalQueueTakeCodec.encodeRequest(    aString ,    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalQueueTakeCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalQueuePollCodec.encodeRequest(    aString ,    aString ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalQueuePollCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalQueuePeekCodec.encodeRequest(    aString ,    aString ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalQueuePeekCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionalQueueSizeCodec.encodeRequest(    aString ,    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionalQueueSizeCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheAddEntryListenerCodec.encodeRequest(    aString ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheAddEntryListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = CacheAddEntryListenerCodec.encodeCacheEvent( anInt ,  cacheEventDatas ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = CacheAddInvalidationListenerCodec.encodeRequest(    aString ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheAddInvalidationListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = CacheAddInvalidationListenerCodec.encodeCacheInvalidationEvent( aString ,  aData ,  aString ,  aUUID ,  aLong   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
    {
        ClientMessage clientMessage = CacheAddInvalidationListenerCodec.encodeCacheBatchInvalidationEvent( aString ,  datas ,  strings ,  uuids ,  longs   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = CacheClearCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheClearCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheRemoveAllKeysCodec.encodeRequest(    aString ,    datas ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheRemoveAllKeysCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheRemoveAllCodec.encodeRequest(    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheRemoveAllCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheContainsKeyCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheContainsKeyCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheCreateConfigCodec.encodeRequest(    aData ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheCreateConfigCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheDestroyCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheDestroyCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheEntryProcessorCodec.encodeRequest(    aString ,    aData ,    aData ,    datas ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheEntryProcessorCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheGetAllCodec.encodeRequest(    aString ,    datas ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheGetAllCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheGetAndRemoveCodec.encodeRequest(    aString ,    aData ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheGetAndRemoveCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheGetAndReplaceCodec.encodeRequest(    aString ,    aData ,    aData ,    aData ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheGetAndReplaceCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheGetConfigCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheGetConfigCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheGetCodec.encodeRequest(    aString ,    aData ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheGetCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheIterateCodec.encodeRequest(    aString ,    anInt ,    anInt ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheIterateCodec.encodeResponse(    anInt ,    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheListenerRegistrationCodec.encodeRequest(    aString ,    aData ,    aBoolean ,    anAddress   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheListenerRegistrationCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheLoadAllCodec.encodeRequest(    aString ,    datas ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheLoadAllCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheManagementConfigCodec.encodeRequest(    aString ,    aBoolean ,    aBoolean ,    anAddress   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheManagementConfigCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CachePutIfAbsentCodec.encodeRequest(    aString ,    aData ,    aData ,    aData ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CachePutIfAbsentCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CachePutCodec.encodeRequest(    aString ,    aData ,    aData ,    aData ,    aBoolean ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CachePutCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheRemoveEntryListenerCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheRemoveEntryListenerCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheRemoveInvalidationListenerCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheRemoveInvalidationListenerCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheRemoveCodec.encodeRequest(    aString ,    aData ,    aData ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheRemoveCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheReplaceCodec.encodeRequest(    aString ,    aData ,    aData ,    aData ,    aData ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheReplaceCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheSizeCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheSizeCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheAddPartitionLostListenerCodec.encodeRequest(    aString ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheAddPartitionLostListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = CacheAddPartitionLostListenerCodec.encodeCachePartitionLostEvent( anInt ,  aString   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = CacheRemovePartitionLostListenerCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheRemovePartitionLostListenerCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CachePutAllCodec.encodeRequest(    aString ,    aListOfEntry ,    aData ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CachePutAllCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheIterateEntriesCodec.encodeRequest(    aString ,    anInt ,    anInt ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheIterateEntriesCodec.encodeResponse(    anInt ,    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheAddNearCacheInvalidationListenerCodec.encodeRequest(    aString ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheAddNearCacheInvalidationListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = CacheAddNearCacheInvalidationListenerCodec.encodeCacheInvalidationEvent( aString ,  aData ,  aString ,  aUUID ,  aLong   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
    {
        ClientMessage clientMessage = CacheAddNearCacheInvalidationListenerCodec.encodeCacheBatchInvalidationEvent( aString ,  datas ,  strings ,  uuids ,  longs   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = CacheFetchNearCacheInvalidationMetadataCodec.encodeRequest(    strings ,    anAddress   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheFetchNearCacheInvalidationMetadataCodec.encodeResponse(    aNamePartitionSequenceList ,    aPartitionUuidList   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheAssignAndGetUuidsCodec.encodeRequest( );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheAssignAndGetUuidsCodec.encodeResponse(    aPartitionUuidList   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheEventJournalSubscribeCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheEventJournalSubscribeCodec.encodeResponse(    aLong ,    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheEventJournalReadCodec.encodeRequest(    aString ,    aLong ,    anInt ,    anInt ,    aData ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheEventJournalReadCodec.encodeResponse(    anInt ,    datas ,    arrLongs ,    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CacheSetExpiryPolicyCodec.encodeRequest(    aString ,    datas ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CacheSetExpiryPolicyCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = XATransactionClearRemoteCodec.encodeRequest(    anXid   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = XATransactionClearRemoteCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = XATransactionCollectTransactionsCodec.encodeRequest( );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = XATransactionCollectTransactionsCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = XATransactionFinalizeCodec.encodeRequest(    anXid ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = XATransactionFinalizeCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = XATransactionCommitCodec.encodeRequest(    aString ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = XATransactionCommitCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = XATransactionCreateCodec.encodeRequest(    anXid ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = XATransactionCreateCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = XATransactionPrepareCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = XATransactionPrepareCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = XATransactionRollbackCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = XATransactionRollbackCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionCommitCodec.encodeRequest(    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionCommitCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionCreateCodec.encodeRequest(    aLong ,    anInt ,    anInt ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionCreateCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = TransactionRollbackCodec.encodeRequest(    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = TransactionRollbackCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ContinuousQueryPublisherCreateWithValueCodec.encodeRequest(    aString ,    aString ,    aData ,    anInt ,    anInt ,    aLong ,    aBoolean ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ContinuousQueryPublisherCreateWithValueCodec.encodeResponse(    aListOfEntry   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ContinuousQueryPublisherCreateCodec.encodeRequest(    aString ,    aString ,    aData ,    anInt ,    anInt ,    aLong ,    aBoolean ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ContinuousQueryPublisherCreateCodec.encodeResponse(    datas   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ContinuousQueryMadePublishableCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ContinuousQueryMadePublishableCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ContinuousQueryAddListenerCodec.encodeRequest(    aString ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ContinuousQueryAddListenerCodec.encodeResponse(    aString   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    {
        ClientMessage clientMessage = ContinuousQueryAddListenerCodec.encodeQueryCacheSingleEvent( aQueryCacheEventData   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
    {
        ClientMessage clientMessage = ContinuousQueryAddListenerCodec.encodeQueryCacheBatchEvent( queryCacheEventDatas ,  aString ,  anInt   );
        outputStream.writeInt(clientMessage.getFrameLength());
        outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
     }
}


{
    ClientMessage clientMessage = ContinuousQuerySetReadCursorCodec.encodeRequest(    aString ,    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ContinuousQuerySetReadCursorCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ContinuousQueryDestroyCacheCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ContinuousQueryDestroyCacheCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = RingbufferSizeCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = RingbufferSizeCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = RingbufferTailSequenceCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = RingbufferTailSequenceCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = RingbufferHeadSequenceCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = RingbufferHeadSequenceCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = RingbufferCapacityCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = RingbufferCapacityCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = RingbufferRemainingCapacityCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = RingbufferRemainingCapacityCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = RingbufferAddCodec.encodeRequest(    aString ,    anInt ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = RingbufferAddCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = RingbufferReadOneCodec.encodeRequest(    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = RingbufferReadOneCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = RingbufferAddAllCodec.encodeRequest(    aString ,    datas ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = RingbufferAddAllCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = RingbufferReadManyCodec.encodeRequest(    aString ,    aLong ,    anInt ,    anInt ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = RingbufferReadManyCodec.encodeResponse(    anInt ,    datas ,    arrLongs ,    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DurableExecutorShutdownCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DurableExecutorShutdownCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DurableExecutorIsShutdownCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DurableExecutorIsShutdownCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DurableExecutorSubmitToPartitionCodec.encodeRequest(    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DurableExecutorSubmitToPartitionCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DurableExecutorRetrieveResultCodec.encodeRequest(    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DurableExecutorRetrieveResultCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DurableExecutorDisposeResultCodec.encodeRequest(    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DurableExecutorDisposeResultCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DurableExecutorRetrieveAndDisposeResultCodec.encodeRequest(    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DurableExecutorRetrieveAndDisposeResultCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CardinalityEstimatorAddCodec.encodeRequest(    aString ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CardinalityEstimatorAddCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = CardinalityEstimatorEstimateCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = CardinalityEstimatorEstimateCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ScheduledExecutorShutdownCodec.encodeRequest(    aString ,    anAddress   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ScheduledExecutorShutdownCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ScheduledExecutorSubmitToPartitionCodec.encodeRequest(    aString ,    aByte ,    aString ,    aData ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ScheduledExecutorSubmitToPartitionCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ScheduledExecutorSubmitToAddressCodec.encodeRequest(    aString ,    anAddress ,    aByte ,    aString ,    aData ,    aLong ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ScheduledExecutorSubmitToAddressCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ScheduledExecutorGetAllScheduledFuturesCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ScheduledExecutorGetAllScheduledFuturesCodec.encodeResponse(    taskHandlers   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ScheduledExecutorGetStatsFromPartitionCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ScheduledExecutorGetStatsFromPartitionCodec.encodeResponse(    aLong ,    aLong ,    aLong ,    aLong ,    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ScheduledExecutorGetStatsFromAddressCodec.encodeRequest(    aString ,    aString ,    anAddress   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ScheduledExecutorGetStatsFromAddressCodec.encodeResponse(    aLong ,    aLong ,    aLong ,    aLong ,    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ScheduledExecutorGetDelayFromPartitionCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ScheduledExecutorGetDelayFromPartitionCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ScheduledExecutorGetDelayFromAddressCodec.encodeRequest(    aString ,    aString ,    anAddress   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ScheduledExecutorGetDelayFromAddressCodec.encodeResponse(    aLong   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ScheduledExecutorCancelFromPartitionCodec.encodeRequest(    aString ,    aString ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ScheduledExecutorCancelFromPartitionCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ScheduledExecutorCancelFromAddressCodec.encodeRequest(    aString ,    aString ,    anAddress ,    aBoolean   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ScheduledExecutorCancelFromAddressCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ScheduledExecutorIsCancelledFromPartitionCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ScheduledExecutorIsCancelledFromPartitionCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ScheduledExecutorIsCancelledFromAddressCodec.encodeRequest(    aString ,    aString ,    anAddress   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ScheduledExecutorIsCancelledFromAddressCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ScheduledExecutorIsDoneFromPartitionCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ScheduledExecutorIsDoneFromPartitionCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ScheduledExecutorIsDoneFromAddressCodec.encodeRequest(    aString ,    aString ,    anAddress   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ScheduledExecutorIsDoneFromAddressCodec.encodeResponse(    aBoolean   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ScheduledExecutorGetResultFromPartitionCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ScheduledExecutorGetResultFromPartitionCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ScheduledExecutorGetResultFromAddressCodec.encodeRequest(    aString ,    aString ,    anAddress   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ScheduledExecutorGetResultFromAddressCodec.encodeResponse(    aData   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ScheduledExecutorDisposeFromPartitionCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ScheduledExecutorDisposeFromPartitionCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = ScheduledExecutorDisposeFromAddressCodec.encodeRequest(    aString ,    aString ,    anAddress   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = ScheduledExecutorDisposeFromAddressCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DynamicConfigAddMultiMapConfigCodec.encodeRequest(    aString ,    aString ,    listenerConfigs ,    aBoolean ,    anInt ,    anInt ,    aBoolean ,    aString ,    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DynamicConfigAddMultiMapConfigCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DynamicConfigAddRingbufferConfigCodec.encodeRequest(    aString ,    anInt ,    anInt ,    anInt ,    anInt ,    aString ,    ringbufferStore ,    aString ,    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DynamicConfigAddRingbufferConfigCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DynamicConfigAddCardinalityEstimatorConfigCodec.encodeRequest(    aString ,    anInt ,    anInt ,    aString ,    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DynamicConfigAddCardinalityEstimatorConfigCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DynamicConfigAddLockConfigCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DynamicConfigAddLockConfigCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DynamicConfigAddListConfigCodec.encodeRequest(    aString ,    listenerConfigs ,    anInt ,    anInt ,    anInt ,    aBoolean ,    aString ,    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DynamicConfigAddListConfigCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DynamicConfigAddSetConfigCodec.encodeRequest(    aString ,    listenerConfigs ,    anInt ,    anInt ,    anInt ,    aBoolean ,    aString ,    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DynamicConfigAddSetConfigCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DynamicConfigAddReplicatedMapConfigCodec.encodeRequest(    aString ,    aString ,    aBoolean ,    aBoolean ,    aString ,    listenerConfigs ,    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DynamicConfigAddReplicatedMapConfigCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DynamicConfigAddTopicConfigCodec.encodeRequest(    aString ,    aBoolean ,    aBoolean ,    aBoolean ,    listenerConfigs   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DynamicConfigAddTopicConfigCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DynamicConfigAddExecutorConfigCodec.encodeRequest(    aString ,    anInt ,    anInt ,    aBoolean ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DynamicConfigAddExecutorConfigCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DynamicConfigAddDurableExecutorConfigCodec.encodeRequest(    aString ,    anInt ,    anInt ,    anInt ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DynamicConfigAddDurableExecutorConfigCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DynamicConfigAddScheduledExecutorConfigCodec.encodeRequest(    aString ,    anInt ,    anInt ,    anInt ,    aString ,    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DynamicConfigAddScheduledExecutorConfigCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DynamicConfigAddSemaphoreConfigCodec.encodeRequest(    aString ,    anInt ,    anInt ,    anInt ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DynamicConfigAddSemaphoreConfigCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DynamicConfigAddQueueConfigCodec.encodeRequest(    aString ,    listenerConfigs ,    anInt ,    anInt ,    anInt ,    anInt ,    aBoolean ,    aString ,    queueStoreConfig ,    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DynamicConfigAddQueueConfigCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DynamicConfigAddMapConfigCodec.encodeRequest(    aString ,    anInt ,    anInt ,    anInt ,    anInt ,    aString ,    aBoolean ,    aString ,    aString ,    aString ,    listenerConfigs ,    listenerConfigs ,    aBoolean ,    aString ,    aData ,    aString ,    anInt ,    mapStoreConfig ,    nearCacheConfig ,    wanReplicationRef ,    mapIndexConfigs ,    mapAttributeConfigs ,    queryCacheConfigs ,    aString ,    aData ,    hotRestartConfig ,    anInt ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DynamicConfigAddMapConfigCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DynamicConfigAddReliableTopicConfigCodec.encodeRequest(    aString ,    listenerConfigs ,    anInt ,    aBoolean ,    aString ,    aData   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DynamicConfigAddReliableTopicConfigCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DynamicConfigAddCacheConfigCodec.encodeRequest(    aString ,    aString ,    aString ,    aBoolean ,    aBoolean ,    aBoolean ,    aBoolean ,    aString ,    aString ,    aString ,    aString ,    anInt ,    anInt ,    aString ,    aString ,    aString ,    aBoolean ,    listenerConfigs ,    aString ,    timedExpiryPolicyFactoryConfig ,    cacheEntryListenerConfigs ,    evictionConfig ,    wanReplicationRef ,    hotRestartConfig   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DynamicConfigAddCacheConfigCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DynamicConfigAddEventJournalConfigCodec.encodeRequest(    aString ,    aString ,    aBoolean ,    anInt ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DynamicConfigAddEventJournalConfigCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DynamicConfigAddFlakeIdGeneratorConfigCodec.encodeRequest(    aString ,    anInt ,    aLong ,    aLong ,    aBoolean ,    aLong   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DynamicConfigAddFlakeIdGeneratorConfigCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DynamicConfigAddAtomicLongConfigCodec.encodeRequest(    aString ,    aString ,    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DynamicConfigAddAtomicLongConfigCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DynamicConfigAddAtomicReferenceConfigCodec.encodeRequest(    aString ,    aString ,    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DynamicConfigAddAtomicReferenceConfigCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DynamicConfigAddCountDownLatchConfigCodec.encodeRequest(    aString ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DynamicConfigAddCountDownLatchConfigCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DynamicConfigAddPNCounterConfigCodec.encodeRequest(    aString ,    anInt ,    aBoolean ,    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DynamicConfigAddPNCounterConfigCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = DynamicConfigAddMerkleTreeConfigCodec.encodeRequest(    aString ,    aBoolean ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = DynamicConfigAddMerkleTreeConfigCodec.encodeResponse( );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = FlakeIdGeneratorNewIdBatchCodec.encodeRequest(    aString ,    anInt   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = FlakeIdGeneratorNewIdBatchCodec.encodeResponse(    aLong ,    aLong ,    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = PNCounterGetCodec.encodeRequest(    aString ,    aListOfStringToLong ,    anAddress   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = PNCounterGetCodec.encodeResponse(    aLong ,    aListOfStringToLong ,    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = PNCounterAddCodec.encodeRequest(    aString ,    aLong ,    aBoolean ,    aListOfStringToLong ,    anAddress   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = PNCounterAddCodec.encodeResponse(    aLong ,    aListOfStringToLong ,    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}


{
    ClientMessage clientMessage = PNCounterGetConfiguredReplicaCountCodec.encodeRequest(    aString   );
     outputStream.writeInt(clientMessage.getFrameLength());
     outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}
{
    ClientMessage clientMessage = PNCounterGetConfiguredReplicaCountCodec.encodeResponse(    anInt   );
    outputStream.writeInt(clientMessage.getFrameLength());
    outputStream.write(clientMessage.buffer().byteArray(), 0 , clientMessage.getFrameLength());
}

         outputStream.close();
         out.close();

    }
}

