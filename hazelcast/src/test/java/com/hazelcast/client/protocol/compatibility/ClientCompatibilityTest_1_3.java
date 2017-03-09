/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.impl.CacheEventData;
import com.hazelcast.cache.impl.CacheEventDataImpl;
import com.hazelcast.cache.impl.CacheEventType;
import com.hazelcast.client.impl.MemberImpl;
import com.hazelcast.client.impl.client.DistributedObjectInfo;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.*;
import com.hazelcast.client.impl.protocol.util.SafeBuffer;
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
import java.io.IOException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.InputStream;
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
import static org.junit.Assert.assertFalse;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.*;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientCompatibilityTest_1_3 {
    private static final int FRAME_LEN_FIELD_SIZE = 4;

    @org.junit.Test
            public void test() throws IOException {
           InputStream input = getClass().getResourceAsStream("/1.3.protocol.compatibility.binary");
            DataInputStream inputStream = new DataInputStream(input);

{
    ClientMessage clientMessage = ClientAuthenticationCodec.encodeRequest(    aString ,    aString ,    aString ,    aString ,    aBoolean ,    aString ,    aByte ,    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ClientAuthenticationCodec.ResponseParameters params = ClientAuthenticationCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
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
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ClientAuthenticationCustomCodec.ResponseParameters params = ClientAuthenticationCustomCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
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
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ClientAddMembershipListenerCodec.ResponseParameters params = ClientAddMembershipListenerCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aString, params.response));
}
{
    class ClientAddMembershipListenerCodecHandler extends ClientAddMembershipListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  com.hazelcast.core.Member
 member ,   int
 eventType   ) {
                            assertTrue(isEqual(aMember, member));
                            assertTrue(isEqual(anInt, eventType));
        }
        @Override
        public void handle(  java.util.Collection<com.hazelcast.core.Member> members   ) {
                            assertTrue(isEqual(members, members));
        }
        @Override
        public void handle(  java.lang.String
 uuid ,   java.lang.String
 key ,   int
 operationType ,   java.lang.String
 value   ) {
                            assertTrue(isEqual(aString, uuid));
                            assertTrue(isEqual(aString, key));
                            assertTrue(isEqual(anInt, operationType));
                            assertTrue(isEqual(aString, value));
        }
    }
    ClientAddMembershipListenerCodecHandler handler = new ClientAddMembershipListenerCodecHandler();
    {
        int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
        handler.handle(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
     }
    {
        int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
        handler.handle(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
     }
    {
        int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
        handler.handle(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
     }
}


{
    ClientMessage clientMessage = ClientCreateProxyCodec.encodeRequest(    aString ,    aString ,    anAddress   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ClientCreateProxyCodec.ResponseParameters params = ClientCreateProxyCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = ClientDestroyProxyCodec.encodeRequest(    aString ,    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ClientDestroyProxyCodec.ResponseParameters params = ClientDestroyProxyCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = ClientGetPartitionsCodec.encodeRequest( );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ClientGetPartitionsCodec.ResponseParameters params = ClientGetPartitionsCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aPartitionTable, params.partitions));
}


{
    ClientMessage clientMessage = ClientRemoveAllListenersCodec.encodeRequest( );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ClientRemoveAllListenersCodec.ResponseParameters params = ClientRemoveAllListenersCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = ClientAddPartitionLostListenerCodec.encodeRequest(    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ClientAddPartitionLostListenerCodec.ResponseParameters params = ClientAddPartitionLostListenerCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aString, params.response));
}
{
    class ClientAddPartitionLostListenerCodecHandler extends ClientAddPartitionLostListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  int
 partitionId ,   int
 lostBackupCount ,   com.hazelcast.nio.Address
 source   ) {
                            assertTrue(isEqual(anInt, partitionId));
                            assertTrue(isEqual(anInt, lostBackupCount));
                            assertTrue(isEqual(anAddress, source));
        }
    }
    ClientAddPartitionLostListenerCodecHandler handler = new ClientAddPartitionLostListenerCodecHandler();
    {
        int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
        handler.handle(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
     }
}


{
    ClientMessage clientMessage = ClientRemovePartitionLostListenerCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ClientRemovePartitionLostListenerCodec.ResponseParameters params = ClientRemovePartitionLostListenerCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = ClientGetDistributedObjectsCodec.encodeRequest( );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ClientGetDistributedObjectsCodec.ResponseParameters params = ClientGetDistributedObjectsCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(distributedObjectInfos, params.response));
}


{
    ClientMessage clientMessage = ClientAddDistributedObjectListenerCodec.encodeRequest(    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ClientAddDistributedObjectListenerCodec.ResponseParameters params = ClientAddDistributedObjectListenerCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aString, params.response));
}
{
    class ClientAddDistributedObjectListenerCodecHandler extends ClientAddDistributedObjectListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  java.lang.String
 name ,   java.lang.String
 serviceName ,   java.lang.String
 eventType   ) {
                            assertTrue(isEqual(aString, name));
                            assertTrue(isEqual(aString, serviceName));
                            assertTrue(isEqual(aString, eventType));
        }
    }
    ClientAddDistributedObjectListenerCodecHandler handler = new ClientAddDistributedObjectListenerCodecHandler();
    {
        int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
        handler.handle(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
     }
}


{
    ClientMessage clientMessage = ClientRemoveDistributedObjectListenerCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ClientRemoveDistributedObjectListenerCodec.ResponseParameters params = ClientRemoveDistributedObjectListenerCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = ClientPingCodec.encodeRequest( );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ClientPingCodec.ResponseParameters params = ClientPingCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = MapPutCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapPutCodec.ResponseParameters params = MapPutCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = MapGetCodec.encodeRequest(    aString ,    aData ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapGetCodec.ResponseParameters params = MapGetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = MapRemoveCodec.encodeRequest(    aString ,    aData ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapRemoveCodec.ResponseParameters params = MapRemoveCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = MapReplaceCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapReplaceCodec.ResponseParameters params = MapReplaceCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = MapReplaceIfSameCodec.encodeRequest(    aString ,    aData ,    aData ,    aData ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapReplaceIfSameCodec.ResponseParameters params = MapReplaceIfSameCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = MapContainsKeyCodec.encodeRequest(    aString ,    aData ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapContainsKeyCodec.ResponseParameters params = MapContainsKeyCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = MapContainsValueCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapContainsValueCodec.ResponseParameters params = MapContainsValueCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = MapRemoveIfSameCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapRemoveIfSameCodec.ResponseParameters params = MapRemoveIfSameCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = MapDeleteCodec.encodeRequest(    aString ,    aData ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapDeleteCodec.ResponseParameters params = MapDeleteCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = MapFlushCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapFlushCodec.ResponseParameters params = MapFlushCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = MapTryRemoveCodec.encodeRequest(    aString ,    aData ,    aLong ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapTryRemoveCodec.ResponseParameters params = MapTryRemoveCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = MapTryPutCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapTryPutCodec.ResponseParameters params = MapTryPutCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = MapPutTransientCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapPutTransientCodec.ResponseParameters params = MapPutTransientCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = MapPutIfAbsentCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapPutIfAbsentCodec.ResponseParameters params = MapPutIfAbsentCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = MapSetCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapSetCodec.ResponseParameters params = MapSetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = MapLockCodec.encodeRequest(    aString ,    aData ,    aLong ,    aLong ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapLockCodec.ResponseParameters params = MapLockCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = MapTryLockCodec.encodeRequest(    aString ,    aData ,    aLong ,    aLong ,    aLong ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapTryLockCodec.ResponseParameters params = MapTryLockCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = MapIsLockedCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapIsLockedCodec.ResponseParameters params = MapIsLockedCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = MapUnlockCodec.encodeRequest(    aString ,    aData ,    aLong ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapUnlockCodec.ResponseParameters params = MapUnlockCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = MapAddInterceptorCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapAddInterceptorCodec.ResponseParameters params = MapAddInterceptorCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aString, params.response));
}


{
    ClientMessage clientMessage = MapRemoveInterceptorCodec.encodeRequest(    aString ,    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapRemoveInterceptorCodec.ResponseParameters params = MapRemoveInterceptorCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = MapAddEntryListenerToKeyWithPredicateCodec.encodeRequest(    aString ,    aData ,    aData ,    aBoolean ,    anInt ,    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapAddEntryListenerToKeyWithPredicateCodec.ResponseParameters params = MapAddEntryListenerToKeyWithPredicateCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aString, params.response));
}
{
    class MapAddEntryListenerToKeyWithPredicateCodecHandler extends MapAddEntryListenerToKeyWithPredicateCodec.AbstractEventHandler {
        @Override
        public void handle(  com.hazelcast.nio.serialization.Data
 key ,   com.hazelcast.nio.serialization.Data
 value ,   com.hazelcast.nio.serialization.Data
 oldValue ,   com.hazelcast.nio.serialization.Data
 mergingValue ,   int
 eventType ,   java.lang.String
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
        int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
        handler.handle(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
     }
}


{
    ClientMessage clientMessage = MapAddEntryListenerWithPredicateCodec.encodeRequest(    aString ,    aData ,    aBoolean ,    anInt ,    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapAddEntryListenerWithPredicateCodec.ResponseParameters params = MapAddEntryListenerWithPredicateCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aString, params.response));
}
{
    class MapAddEntryListenerWithPredicateCodecHandler extends MapAddEntryListenerWithPredicateCodec.AbstractEventHandler {
        @Override
        public void handle(  com.hazelcast.nio.serialization.Data
 key ,   com.hazelcast.nio.serialization.Data
 value ,   com.hazelcast.nio.serialization.Data
 oldValue ,   com.hazelcast.nio.serialization.Data
 mergingValue ,   int
 eventType ,   java.lang.String
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
        int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
        handler.handle(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
     }
}


{
    ClientMessage clientMessage = MapAddEntryListenerToKeyCodec.encodeRequest(    aString ,    aData ,    aBoolean ,    anInt ,    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapAddEntryListenerToKeyCodec.ResponseParameters params = MapAddEntryListenerToKeyCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aString, params.response));
}
{
    class MapAddEntryListenerToKeyCodecHandler extends MapAddEntryListenerToKeyCodec.AbstractEventHandler {
        @Override
        public void handle(  com.hazelcast.nio.serialization.Data
 key ,   com.hazelcast.nio.serialization.Data
 value ,   com.hazelcast.nio.serialization.Data
 oldValue ,   com.hazelcast.nio.serialization.Data
 mergingValue ,   int
 eventType ,   java.lang.String
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
        int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
        handler.handle(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
     }
}


{
    ClientMessage clientMessage = MapAddEntryListenerCodec.encodeRequest(    aString ,    aBoolean ,    anInt ,    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapAddEntryListenerCodec.ResponseParameters params = MapAddEntryListenerCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aString, params.response));
}
{
    class MapAddEntryListenerCodecHandler extends MapAddEntryListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  com.hazelcast.nio.serialization.Data
 key ,   com.hazelcast.nio.serialization.Data
 value ,   com.hazelcast.nio.serialization.Data
 oldValue ,   com.hazelcast.nio.serialization.Data
 mergingValue ,   int
 eventType ,   java.lang.String
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
        int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
        handler.handle(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
     }
}


{
    ClientMessage clientMessage = MapAddNearCacheEntryListenerCodec.encodeRequest(    aString ,    anInt ,    aBoolean   );
    int length = inputStream.readInt();
    // Since the test is generated for protocol version (1.3) which is earlier than latest change in the message
    // (version 1.4), only the bytes after frame length fields are compared
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
    MapAddNearCacheEntryListenerCodec.ResponseParameters params = MapAddNearCacheEntryListenerCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aString, params.response));
}
{
    class MapAddNearCacheEntryListenerCodecHandler extends MapAddNearCacheEntryListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  com.hazelcast.nio.serialization.Data
 key ,   java.lang.String
 sourceUuid ,   java.util.UUID
 partitionUuid ,   long
 sequence   ) {
                            assertTrue(isEqual(aData, key));
        }
        @Override
        public void handle(  java.util.Collection<com.hazelcast.nio.serialization.Data> keys ,   java.util.Collection<java.lang.String> sourceUuids ,   java.util.Collection<java.util.UUID> partitionUuids ,   java.util.Collection<java.lang.Long> sequences   ) {
                            assertTrue(isEqual(datas, keys));
        }
    }
    MapAddNearCacheEntryListenerCodecHandler handler = new MapAddNearCacheEntryListenerCodecHandler();
    {
        int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
        handler.handle(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
     }
    {
        int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
        handler.handle(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
     }
}


{
    ClientMessage clientMessage = MapRemoveEntryListenerCodec.encodeRequest(    aString ,    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapRemoveEntryListenerCodec.ResponseParameters params = MapRemoveEntryListenerCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = MapAddPartitionLostListenerCodec.encodeRequest(    aString ,    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapAddPartitionLostListenerCodec.ResponseParameters params = MapAddPartitionLostListenerCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aString, params.response));
}
{
    class MapAddPartitionLostListenerCodecHandler extends MapAddPartitionLostListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  int
 partitionId ,   java.lang.String
 uuid   ) {
                            assertTrue(isEqual(anInt, partitionId));
                            assertTrue(isEqual(aString, uuid));
        }
    }
    MapAddPartitionLostListenerCodecHandler handler = new MapAddPartitionLostListenerCodecHandler();
    {
        int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
        handler.handle(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
     }
}


{
    ClientMessage clientMessage = MapRemovePartitionLostListenerCodec.encodeRequest(    aString ,    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapRemovePartitionLostListenerCodec.ResponseParameters params = MapRemovePartitionLostListenerCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = MapGetEntryViewCodec.encodeRequest(    aString ,    aData ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapGetEntryViewCodec.ResponseParameters params = MapGetEntryViewCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(anEntryView, params.response));
}


{
    ClientMessage clientMessage = MapEvictCodec.encodeRequest(    aString ,    aData ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapEvictCodec.ResponseParameters params = MapEvictCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = MapEvictAllCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapEvictAllCodec.ResponseParameters params = MapEvictAllCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = MapLoadAllCodec.encodeRequest(    aString ,    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapLoadAllCodec.ResponseParameters params = MapLoadAllCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = MapLoadGivenKeysCodec.encodeRequest(    aString ,    datas ,    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapLoadGivenKeysCodec.ResponseParameters params = MapLoadGivenKeysCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = MapKeySetCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapKeySetCodec.ResponseParameters params = MapKeySetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(datas, params.response));
}


{
    ClientMessage clientMessage = MapGetAllCodec.encodeRequest(    aString ,    datas   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapGetAllCodec.ResponseParameters params = MapGetAllCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aListOfEntry, params.response));
}


{
    ClientMessage clientMessage = MapValuesCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapValuesCodec.ResponseParameters params = MapValuesCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(datas, params.response));
}


{
    ClientMessage clientMessage = MapEntrySetCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapEntrySetCodec.ResponseParameters params = MapEntrySetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aListOfEntry, params.response));
}


{
    ClientMessage clientMessage = MapKeySetWithPredicateCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapKeySetWithPredicateCodec.ResponseParameters params = MapKeySetWithPredicateCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(datas, params.response));
}


{
    ClientMessage clientMessage = MapValuesWithPredicateCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapValuesWithPredicateCodec.ResponseParameters params = MapValuesWithPredicateCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(datas, params.response));
}


{
    ClientMessage clientMessage = MapEntriesWithPredicateCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapEntriesWithPredicateCodec.ResponseParameters params = MapEntriesWithPredicateCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aListOfEntry, params.response));
}


{
    ClientMessage clientMessage = MapAddIndexCodec.encodeRequest(    aString ,    aString ,    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapAddIndexCodec.ResponseParameters params = MapAddIndexCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = MapSizeCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapSizeCodec.ResponseParameters params = MapSizeCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(anInt, params.response));
}


{
    ClientMessage clientMessage = MapIsEmptyCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapIsEmptyCodec.ResponseParameters params = MapIsEmptyCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = MapPutAllCodec.encodeRequest(    aString ,    aListOfEntry   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapPutAllCodec.ResponseParameters params = MapPutAllCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = MapClearCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapClearCodec.ResponseParameters params = MapClearCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = MapExecuteOnKeyCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapExecuteOnKeyCodec.ResponseParameters params = MapExecuteOnKeyCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = MapSubmitToKeyCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapSubmitToKeyCodec.ResponseParameters params = MapSubmitToKeyCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = MapExecuteOnAllKeysCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapExecuteOnAllKeysCodec.ResponseParameters params = MapExecuteOnAllKeysCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aListOfEntry, params.response));
}


{
    ClientMessage clientMessage = MapExecuteWithPredicateCodec.encodeRequest(    aString ,    aData ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapExecuteWithPredicateCodec.ResponseParameters params = MapExecuteWithPredicateCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aListOfEntry, params.response));
}


{
    ClientMessage clientMessage = MapExecuteOnKeysCodec.encodeRequest(    aString ,    aData ,    datas   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapExecuteOnKeysCodec.ResponseParameters params = MapExecuteOnKeysCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aListOfEntry, params.response));
}


{
    ClientMessage clientMessage = MapForceUnlockCodec.encodeRequest(    aString ,    aData ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapForceUnlockCodec.ResponseParameters params = MapForceUnlockCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = MapKeySetWithPagingPredicateCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapKeySetWithPagingPredicateCodec.ResponseParameters params = MapKeySetWithPagingPredicateCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(datas, params.response));
}


{
    ClientMessage clientMessage = MapValuesWithPagingPredicateCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapValuesWithPagingPredicateCodec.ResponseParameters params = MapValuesWithPagingPredicateCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aListOfEntry, params.response));
}


{
    ClientMessage clientMessage = MapEntriesWithPagingPredicateCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapEntriesWithPagingPredicateCodec.ResponseParameters params = MapEntriesWithPagingPredicateCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aListOfEntry, params.response));
}


{
    ClientMessage clientMessage = MapClearNearCacheCodec.encodeRequest(    aString ,    anAddress   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapClearNearCacheCodec.ResponseParameters params = MapClearNearCacheCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = MapFetchKeysCodec.encodeRequest(    aString ,    anInt ,    anInt ,    anInt   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapFetchKeysCodec.ResponseParameters params = MapFetchKeysCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(anInt, params.tableIndex));
                assertTrue(isEqual(datas, params.keys));
}


{
    ClientMessage clientMessage = MapFetchEntriesCodec.encodeRequest(    aString ,    anInt ,    anInt ,    anInt   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapFetchEntriesCodec.ResponseParameters params = MapFetchEntriesCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(anInt, params.tableIndex));
                assertTrue(isEqual(aListOfEntry, params.entries));
}


















{
    ClientMessage clientMessage = MultiMapPutCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MultiMapPutCodec.ResponseParameters params = MultiMapPutCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = MultiMapGetCodec.encodeRequest(    aString ,    aData ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MultiMapGetCodec.ResponseParameters params = MultiMapGetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(datas, params.response));
}


{
    ClientMessage clientMessage = MultiMapRemoveCodec.encodeRequest(    aString ,    aData ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MultiMapRemoveCodec.ResponseParameters params = MultiMapRemoveCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(datas, params.response));
}


{
    ClientMessage clientMessage = MultiMapKeySetCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MultiMapKeySetCodec.ResponseParameters params = MultiMapKeySetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(datas, params.response));
}


{
    ClientMessage clientMessage = MultiMapValuesCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MultiMapValuesCodec.ResponseParameters params = MultiMapValuesCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(datas, params.response));
}


{
    ClientMessage clientMessage = MultiMapEntrySetCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MultiMapEntrySetCodec.ResponseParameters params = MultiMapEntrySetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aListOfEntry, params.response));
}


{
    ClientMessage clientMessage = MultiMapContainsKeyCodec.encodeRequest(    aString ,    aData ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MultiMapContainsKeyCodec.ResponseParameters params = MultiMapContainsKeyCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = MultiMapContainsValueCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MultiMapContainsValueCodec.ResponseParameters params = MultiMapContainsValueCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = MultiMapContainsEntryCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MultiMapContainsEntryCodec.ResponseParameters params = MultiMapContainsEntryCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = MultiMapSizeCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MultiMapSizeCodec.ResponseParameters params = MultiMapSizeCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(anInt, params.response));
}


{
    ClientMessage clientMessage = MultiMapClearCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MultiMapClearCodec.ResponseParameters params = MultiMapClearCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = MultiMapValueCountCodec.encodeRequest(    aString ,    aData ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MultiMapValueCountCodec.ResponseParameters params = MultiMapValueCountCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(anInt, params.response));
}


{
    ClientMessage clientMessage = MultiMapAddEntryListenerToKeyCodec.encodeRequest(    aString ,    aData ,    aBoolean ,    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MultiMapAddEntryListenerToKeyCodec.ResponseParameters params = MultiMapAddEntryListenerToKeyCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aString, params.response));
}
{
    class MultiMapAddEntryListenerToKeyCodecHandler extends MultiMapAddEntryListenerToKeyCodec.AbstractEventHandler {
        @Override
        public void handle(  com.hazelcast.nio.serialization.Data
 key ,   com.hazelcast.nio.serialization.Data
 value ,   com.hazelcast.nio.serialization.Data
 oldValue ,   com.hazelcast.nio.serialization.Data
 mergingValue ,   int
 eventType ,   java.lang.String
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
        int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
        handler.handle(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
     }
}


{
    ClientMessage clientMessage = MultiMapAddEntryListenerCodec.encodeRequest(    aString ,    aBoolean ,    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MultiMapAddEntryListenerCodec.ResponseParameters params = MultiMapAddEntryListenerCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aString, params.response));
}
{
    class MultiMapAddEntryListenerCodecHandler extends MultiMapAddEntryListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  com.hazelcast.nio.serialization.Data
 key ,   com.hazelcast.nio.serialization.Data
 value ,   com.hazelcast.nio.serialization.Data
 oldValue ,   com.hazelcast.nio.serialization.Data
 mergingValue ,   int
 eventType ,   java.lang.String
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
        int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
        handler.handle(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
     }
}


{
    ClientMessage clientMessage = MultiMapRemoveEntryListenerCodec.encodeRequest(    aString ,    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MultiMapRemoveEntryListenerCodec.ResponseParameters params = MultiMapRemoveEntryListenerCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = MultiMapLockCodec.encodeRequest(    aString ,    aData ,    aLong ,    aLong ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MultiMapLockCodec.ResponseParameters params = MultiMapLockCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = MultiMapTryLockCodec.encodeRequest(    aString ,    aData ,    aLong ,    aLong ,    aLong ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MultiMapTryLockCodec.ResponseParameters params = MultiMapTryLockCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = MultiMapIsLockedCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MultiMapIsLockedCodec.ResponseParameters params = MultiMapIsLockedCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = MultiMapUnlockCodec.encodeRequest(    aString ,    aData ,    aLong ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MultiMapUnlockCodec.ResponseParameters params = MultiMapUnlockCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = MultiMapForceUnlockCodec.encodeRequest(    aString ,    aData ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MultiMapForceUnlockCodec.ResponseParameters params = MultiMapForceUnlockCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = MultiMapRemoveEntryCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MultiMapRemoveEntryCodec.ResponseParameters params = MultiMapRemoveEntryCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = QueueOfferCodec.encodeRequest(    aString ,    aData ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    QueueOfferCodec.ResponseParameters params = QueueOfferCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = QueuePutCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    QueuePutCodec.ResponseParameters params = QueuePutCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = QueueSizeCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    QueueSizeCodec.ResponseParameters params = QueueSizeCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(anInt, params.response));
}


{
    ClientMessage clientMessage = QueueRemoveCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    QueueRemoveCodec.ResponseParameters params = QueueRemoveCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = QueuePollCodec.encodeRequest(    aString ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    QueuePollCodec.ResponseParameters params = QueuePollCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = QueueTakeCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    QueueTakeCodec.ResponseParameters params = QueueTakeCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = QueuePeekCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    QueuePeekCodec.ResponseParameters params = QueuePeekCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = QueueIteratorCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    QueueIteratorCodec.ResponseParameters params = QueueIteratorCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(datas, params.response));
}


{
    ClientMessage clientMessage = QueueDrainToCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    QueueDrainToCodec.ResponseParameters params = QueueDrainToCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(datas, params.response));
}


{
    ClientMessage clientMessage = QueueDrainToMaxSizeCodec.encodeRequest(    aString ,    anInt   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    QueueDrainToMaxSizeCodec.ResponseParameters params = QueueDrainToMaxSizeCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(datas, params.response));
}


{
    ClientMessage clientMessage = QueueContainsCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    QueueContainsCodec.ResponseParameters params = QueueContainsCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = QueueContainsAllCodec.encodeRequest(    aString ,    datas   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    QueueContainsAllCodec.ResponseParameters params = QueueContainsAllCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = QueueCompareAndRemoveAllCodec.encodeRequest(    aString ,    datas   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    QueueCompareAndRemoveAllCodec.ResponseParameters params = QueueCompareAndRemoveAllCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = QueueCompareAndRetainAllCodec.encodeRequest(    aString ,    datas   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    QueueCompareAndRetainAllCodec.ResponseParameters params = QueueCompareAndRetainAllCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = QueueClearCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    QueueClearCodec.ResponseParameters params = QueueClearCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = QueueAddAllCodec.encodeRequest(    aString ,    datas   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    QueueAddAllCodec.ResponseParameters params = QueueAddAllCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = QueueAddListenerCodec.encodeRequest(    aString ,    aBoolean ,    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    QueueAddListenerCodec.ResponseParameters params = QueueAddListenerCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aString, params.response));
}
{
    class QueueAddListenerCodecHandler extends QueueAddListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  com.hazelcast.nio.serialization.Data
 item ,   java.lang.String
 uuid ,   int
 eventType   ) {
                            assertTrue(isEqual(aData, item));
                            assertTrue(isEqual(aString, uuid));
                            assertTrue(isEqual(anInt, eventType));
        }
    }
    QueueAddListenerCodecHandler handler = new QueueAddListenerCodecHandler();
    {
        int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
        handler.handle(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
     }
}


{
    ClientMessage clientMessage = QueueRemoveListenerCodec.encodeRequest(    aString ,    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    QueueRemoveListenerCodec.ResponseParameters params = QueueRemoveListenerCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = QueueRemainingCapacityCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    QueueRemainingCapacityCodec.ResponseParameters params = QueueRemainingCapacityCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(anInt, params.response));
}


{
    ClientMessage clientMessage = QueueIsEmptyCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    QueueIsEmptyCodec.ResponseParameters params = QueueIsEmptyCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = TopicPublishCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TopicPublishCodec.ResponseParameters params = TopicPublishCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = TopicAddMessageListenerCodec.encodeRequest(    aString ,    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TopicAddMessageListenerCodec.ResponseParameters params = TopicAddMessageListenerCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aString, params.response));
}
{
    class TopicAddMessageListenerCodecHandler extends TopicAddMessageListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  com.hazelcast.nio.serialization.Data
 item ,   long
 publishTime ,   java.lang.String
 uuid   ) {
                            assertTrue(isEqual(aData, item));
                            assertTrue(isEqual(aLong, publishTime));
                            assertTrue(isEqual(aString, uuid));
        }
    }
    TopicAddMessageListenerCodecHandler handler = new TopicAddMessageListenerCodecHandler();
    {
        int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
        handler.handle(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
     }
}


{
    ClientMessage clientMessage = TopicRemoveMessageListenerCodec.encodeRequest(    aString ,    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TopicRemoveMessageListenerCodec.ResponseParameters params = TopicRemoveMessageListenerCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = ListSizeCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ListSizeCodec.ResponseParameters params = ListSizeCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(anInt, params.response));
}


{
    ClientMessage clientMessage = ListContainsCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ListContainsCodec.ResponseParameters params = ListContainsCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = ListContainsAllCodec.encodeRequest(    aString ,    datas   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ListContainsAllCodec.ResponseParameters params = ListContainsAllCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = ListAddCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ListAddCodec.ResponseParameters params = ListAddCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = ListRemoveCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ListRemoveCodec.ResponseParameters params = ListRemoveCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = ListAddAllCodec.encodeRequest(    aString ,    datas   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ListAddAllCodec.ResponseParameters params = ListAddAllCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = ListCompareAndRemoveAllCodec.encodeRequest(    aString ,    datas   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ListCompareAndRemoveAllCodec.ResponseParameters params = ListCompareAndRemoveAllCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = ListCompareAndRetainAllCodec.encodeRequest(    aString ,    datas   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ListCompareAndRetainAllCodec.ResponseParameters params = ListCompareAndRetainAllCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = ListClearCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ListClearCodec.ResponseParameters params = ListClearCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = ListGetAllCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ListGetAllCodec.ResponseParameters params = ListGetAllCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(datas, params.response));
}


{
    ClientMessage clientMessage = ListAddListenerCodec.encodeRequest(    aString ,    aBoolean ,    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ListAddListenerCodec.ResponseParameters params = ListAddListenerCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aString, params.response));
}
{
    class ListAddListenerCodecHandler extends ListAddListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  com.hazelcast.nio.serialization.Data
 item ,   java.lang.String
 uuid ,   int
 eventType   ) {
                            assertTrue(isEqual(aData, item));
                            assertTrue(isEqual(aString, uuid));
                            assertTrue(isEqual(anInt, eventType));
        }
    }
    ListAddListenerCodecHandler handler = new ListAddListenerCodecHandler();
    {
        int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
        handler.handle(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
     }
}


{
    ClientMessage clientMessage = ListRemoveListenerCodec.encodeRequest(    aString ,    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ListRemoveListenerCodec.ResponseParameters params = ListRemoveListenerCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = ListIsEmptyCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ListIsEmptyCodec.ResponseParameters params = ListIsEmptyCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = ListAddAllWithIndexCodec.encodeRequest(    aString ,    anInt ,    datas   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ListAddAllWithIndexCodec.ResponseParameters params = ListAddAllWithIndexCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = ListGetCodec.encodeRequest(    aString ,    anInt   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ListGetCodec.ResponseParameters params = ListGetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = ListSetCodec.encodeRequest(    aString ,    anInt ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ListSetCodec.ResponseParameters params = ListSetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = ListAddWithIndexCodec.encodeRequest(    aString ,    anInt ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ListAddWithIndexCodec.ResponseParameters params = ListAddWithIndexCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = ListRemoveWithIndexCodec.encodeRequest(    aString ,    anInt   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ListRemoveWithIndexCodec.ResponseParameters params = ListRemoveWithIndexCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = ListLastIndexOfCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ListLastIndexOfCodec.ResponseParameters params = ListLastIndexOfCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(anInt, params.response));
}


{
    ClientMessage clientMessage = ListIndexOfCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ListIndexOfCodec.ResponseParameters params = ListIndexOfCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(anInt, params.response));
}


{
    ClientMessage clientMessage = ListSubCodec.encodeRequest(    aString ,    anInt ,    anInt   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ListSubCodec.ResponseParameters params = ListSubCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(datas, params.response));
}


{
    ClientMessage clientMessage = ListIteratorCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ListIteratorCodec.ResponseParameters params = ListIteratorCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(datas, params.response));
}


{
    ClientMessage clientMessage = ListListIteratorCodec.encodeRequest(    aString ,    anInt   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ListListIteratorCodec.ResponseParameters params = ListListIteratorCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(datas, params.response));
}


{
    ClientMessage clientMessage = SetSizeCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    SetSizeCodec.ResponseParameters params = SetSizeCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(anInt, params.response));
}


{
    ClientMessage clientMessage = SetContainsCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    SetContainsCodec.ResponseParameters params = SetContainsCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = SetContainsAllCodec.encodeRequest(    aString ,    datas   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    SetContainsAllCodec.ResponseParameters params = SetContainsAllCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = SetAddCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    SetAddCodec.ResponseParameters params = SetAddCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = SetRemoveCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    SetRemoveCodec.ResponseParameters params = SetRemoveCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = SetAddAllCodec.encodeRequest(    aString ,    datas   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    SetAddAllCodec.ResponseParameters params = SetAddAllCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = SetCompareAndRemoveAllCodec.encodeRequest(    aString ,    datas   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    SetCompareAndRemoveAllCodec.ResponseParameters params = SetCompareAndRemoveAllCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = SetCompareAndRetainAllCodec.encodeRequest(    aString ,    datas   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    SetCompareAndRetainAllCodec.ResponseParameters params = SetCompareAndRetainAllCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = SetClearCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    SetClearCodec.ResponseParameters params = SetClearCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = SetGetAllCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    SetGetAllCodec.ResponseParameters params = SetGetAllCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(datas, params.response));
}


{
    ClientMessage clientMessage = SetAddListenerCodec.encodeRequest(    aString ,    aBoolean ,    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    SetAddListenerCodec.ResponseParameters params = SetAddListenerCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aString, params.response));
}
{
    class SetAddListenerCodecHandler extends SetAddListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  com.hazelcast.nio.serialization.Data
 item ,   java.lang.String
 uuid ,   int
 eventType   ) {
                            assertTrue(isEqual(aData, item));
                            assertTrue(isEqual(aString, uuid));
                            assertTrue(isEqual(anInt, eventType));
        }
    }
    SetAddListenerCodecHandler handler = new SetAddListenerCodecHandler();
    {
        int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
        handler.handle(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
     }
}


{
    ClientMessage clientMessage = SetRemoveListenerCodec.encodeRequest(    aString ,    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    SetRemoveListenerCodec.ResponseParameters params = SetRemoveListenerCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = SetIsEmptyCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    SetIsEmptyCodec.ResponseParameters params = SetIsEmptyCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = LockIsLockedCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    LockIsLockedCodec.ResponseParameters params = LockIsLockedCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = LockIsLockedByCurrentThreadCodec.encodeRequest(    aString ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    LockIsLockedByCurrentThreadCodec.ResponseParameters params = LockIsLockedByCurrentThreadCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = LockGetLockCountCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    LockGetLockCountCodec.ResponseParameters params = LockGetLockCountCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(anInt, params.response));
}


{
    ClientMessage clientMessage = LockGetRemainingLeaseTimeCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    LockGetRemainingLeaseTimeCodec.ResponseParameters params = LockGetRemainingLeaseTimeCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aLong, params.response));
}


{
    ClientMessage clientMessage = LockLockCodec.encodeRequest(    aString ,    aLong ,    aLong ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    LockLockCodec.ResponseParameters params = LockLockCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = LockUnlockCodec.encodeRequest(    aString ,    aLong ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    LockUnlockCodec.ResponseParameters params = LockUnlockCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = LockForceUnlockCodec.encodeRequest(    aString ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    LockForceUnlockCodec.ResponseParameters params = LockForceUnlockCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = LockTryLockCodec.encodeRequest(    aString ,    aLong ,    aLong ,    aLong ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    LockTryLockCodec.ResponseParameters params = LockTryLockCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = ConditionAwaitCodec.encodeRequest(    aString ,    aLong ,    aLong ,    aString ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ConditionAwaitCodec.ResponseParameters params = ConditionAwaitCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = ConditionBeforeAwaitCodec.encodeRequest(    aString ,    aLong ,    aString ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ConditionBeforeAwaitCodec.ResponseParameters params = ConditionBeforeAwaitCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = ConditionSignalCodec.encodeRequest(    aString ,    aLong ,    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ConditionSignalCodec.ResponseParameters params = ConditionSignalCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = ConditionSignalAllCodec.encodeRequest(    aString ,    aLong ,    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ConditionSignalAllCodec.ResponseParameters params = ConditionSignalAllCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = ExecutorServiceShutdownCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ExecutorServiceShutdownCodec.ResponseParameters params = ExecutorServiceShutdownCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = ExecutorServiceIsShutdownCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ExecutorServiceIsShutdownCodec.ResponseParameters params = ExecutorServiceIsShutdownCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = ExecutorServiceCancelOnPartitionCodec.encodeRequest(    aString ,    anInt ,    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ExecutorServiceCancelOnPartitionCodec.ResponseParameters params = ExecutorServiceCancelOnPartitionCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = ExecutorServiceCancelOnAddressCodec.encodeRequest(    aString ,    anAddress ,    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ExecutorServiceCancelOnAddressCodec.ResponseParameters params = ExecutorServiceCancelOnAddressCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = ExecutorServiceSubmitToPartitionCodec.encodeRequest(    aString ,    aString ,    aData ,    anInt   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ExecutorServiceSubmitToPartitionCodec.ResponseParameters params = ExecutorServiceSubmitToPartitionCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = ExecutorServiceSubmitToAddressCodec.encodeRequest(    aString ,    aString ,    aData ,    anAddress   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ExecutorServiceSubmitToAddressCodec.ResponseParameters params = ExecutorServiceSubmitToAddressCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = AtomicLongApplyCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    AtomicLongApplyCodec.ResponseParameters params = AtomicLongApplyCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = AtomicLongAlterCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    AtomicLongAlterCodec.ResponseParameters params = AtomicLongAlterCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = AtomicLongAlterAndGetCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    AtomicLongAlterAndGetCodec.ResponseParameters params = AtomicLongAlterAndGetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aLong, params.response));
}


{
    ClientMessage clientMessage = AtomicLongGetAndAlterCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    AtomicLongGetAndAlterCodec.ResponseParameters params = AtomicLongGetAndAlterCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aLong, params.response));
}


{
    ClientMessage clientMessage = AtomicLongAddAndGetCodec.encodeRequest(    aString ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    AtomicLongAddAndGetCodec.ResponseParameters params = AtomicLongAddAndGetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aLong, params.response));
}


{
    ClientMessage clientMessage = AtomicLongCompareAndSetCodec.encodeRequest(    aString ,    aLong ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    AtomicLongCompareAndSetCodec.ResponseParameters params = AtomicLongCompareAndSetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = AtomicLongDecrementAndGetCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    AtomicLongDecrementAndGetCodec.ResponseParameters params = AtomicLongDecrementAndGetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aLong, params.response));
}


{
    ClientMessage clientMessage = AtomicLongGetCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    AtomicLongGetCodec.ResponseParameters params = AtomicLongGetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aLong, params.response));
}


{
    ClientMessage clientMessage = AtomicLongGetAndAddCodec.encodeRequest(    aString ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    AtomicLongGetAndAddCodec.ResponseParameters params = AtomicLongGetAndAddCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aLong, params.response));
}


{
    ClientMessage clientMessage = AtomicLongGetAndSetCodec.encodeRequest(    aString ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    AtomicLongGetAndSetCodec.ResponseParameters params = AtomicLongGetAndSetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aLong, params.response));
}


{
    ClientMessage clientMessage = AtomicLongIncrementAndGetCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    AtomicLongIncrementAndGetCodec.ResponseParameters params = AtomicLongIncrementAndGetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aLong, params.response));
}


{
    ClientMessage clientMessage = AtomicLongGetAndIncrementCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    AtomicLongGetAndIncrementCodec.ResponseParameters params = AtomicLongGetAndIncrementCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aLong, params.response));
}


{
    ClientMessage clientMessage = AtomicLongSetCodec.encodeRequest(    aString ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    AtomicLongSetCodec.ResponseParameters params = AtomicLongSetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = AtomicReferenceApplyCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    AtomicReferenceApplyCodec.ResponseParameters params = AtomicReferenceApplyCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = AtomicReferenceAlterCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    AtomicReferenceAlterCodec.ResponseParameters params = AtomicReferenceAlterCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = AtomicReferenceAlterAndGetCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    AtomicReferenceAlterAndGetCodec.ResponseParameters params = AtomicReferenceAlterAndGetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = AtomicReferenceGetAndAlterCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    AtomicReferenceGetAndAlterCodec.ResponseParameters params = AtomicReferenceGetAndAlterCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = AtomicReferenceContainsCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    AtomicReferenceContainsCodec.ResponseParameters params = AtomicReferenceContainsCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = AtomicReferenceCompareAndSetCodec.encodeRequest(    aString ,    aData ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    AtomicReferenceCompareAndSetCodec.ResponseParameters params = AtomicReferenceCompareAndSetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = AtomicReferenceGetCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    AtomicReferenceGetCodec.ResponseParameters params = AtomicReferenceGetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = AtomicReferenceSetCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    AtomicReferenceSetCodec.ResponseParameters params = AtomicReferenceSetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = AtomicReferenceClearCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    AtomicReferenceClearCodec.ResponseParameters params = AtomicReferenceClearCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = AtomicReferenceGetAndSetCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    AtomicReferenceGetAndSetCodec.ResponseParameters params = AtomicReferenceGetAndSetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = AtomicReferenceSetAndGetCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    AtomicReferenceSetAndGetCodec.ResponseParameters params = AtomicReferenceSetAndGetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = AtomicReferenceIsNullCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    AtomicReferenceIsNullCodec.ResponseParameters params = AtomicReferenceIsNullCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = CountDownLatchAwaitCodec.encodeRequest(    aString ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CountDownLatchAwaitCodec.ResponseParameters params = CountDownLatchAwaitCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = CountDownLatchCountDownCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CountDownLatchCountDownCodec.ResponseParameters params = CountDownLatchCountDownCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = CountDownLatchGetCountCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CountDownLatchGetCountCodec.ResponseParameters params = CountDownLatchGetCountCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(anInt, params.response));
}


{
    ClientMessage clientMessage = CountDownLatchTrySetCountCodec.encodeRequest(    aString ,    anInt   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CountDownLatchTrySetCountCodec.ResponseParameters params = CountDownLatchTrySetCountCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = SemaphoreInitCodec.encodeRequest(    aString ,    anInt   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    SemaphoreInitCodec.ResponseParameters params = SemaphoreInitCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = SemaphoreAcquireCodec.encodeRequest(    aString ,    anInt   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    SemaphoreAcquireCodec.ResponseParameters params = SemaphoreAcquireCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = SemaphoreAvailablePermitsCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    SemaphoreAvailablePermitsCodec.ResponseParameters params = SemaphoreAvailablePermitsCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(anInt, params.response));
}


{
    ClientMessage clientMessage = SemaphoreDrainPermitsCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    SemaphoreDrainPermitsCodec.ResponseParameters params = SemaphoreDrainPermitsCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(anInt, params.response));
}


{
    ClientMessage clientMessage = SemaphoreReducePermitsCodec.encodeRequest(    aString ,    anInt   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    SemaphoreReducePermitsCodec.ResponseParameters params = SemaphoreReducePermitsCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = SemaphoreReleaseCodec.encodeRequest(    aString ,    anInt   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    SemaphoreReleaseCodec.ResponseParameters params = SemaphoreReleaseCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = SemaphoreTryAcquireCodec.encodeRequest(    aString ,    anInt ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    SemaphoreTryAcquireCodec.ResponseParameters params = SemaphoreTryAcquireCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = ReplicatedMapPutCodec.encodeRequest(    aString ,    aData ,    aData ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ReplicatedMapPutCodec.ResponseParameters params = ReplicatedMapPutCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = ReplicatedMapSizeCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ReplicatedMapSizeCodec.ResponseParameters params = ReplicatedMapSizeCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(anInt, params.response));
}


{
    ClientMessage clientMessage = ReplicatedMapIsEmptyCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ReplicatedMapIsEmptyCodec.ResponseParameters params = ReplicatedMapIsEmptyCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = ReplicatedMapContainsKeyCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ReplicatedMapContainsKeyCodec.ResponseParameters params = ReplicatedMapContainsKeyCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = ReplicatedMapContainsValueCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ReplicatedMapContainsValueCodec.ResponseParameters params = ReplicatedMapContainsValueCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = ReplicatedMapGetCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ReplicatedMapGetCodec.ResponseParameters params = ReplicatedMapGetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = ReplicatedMapRemoveCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ReplicatedMapRemoveCodec.ResponseParameters params = ReplicatedMapRemoveCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = ReplicatedMapPutAllCodec.encodeRequest(    aString ,    aListOfEntry   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ReplicatedMapPutAllCodec.ResponseParameters params = ReplicatedMapPutAllCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = ReplicatedMapClearCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ReplicatedMapClearCodec.ResponseParameters params = ReplicatedMapClearCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.encodeRequest(    aString ,    aData ,    aData ,    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.ResponseParameters params = ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aString, params.response));
}
{
    class ReplicatedMapAddEntryListenerToKeyWithPredicateCodecHandler extends ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.AbstractEventHandler {
        @Override
        public void handle(  com.hazelcast.nio.serialization.Data
 key ,   com.hazelcast.nio.serialization.Data
 value ,   com.hazelcast.nio.serialization.Data
 oldValue ,   com.hazelcast.nio.serialization.Data
 mergingValue ,   int
 eventType ,   java.lang.String
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
        int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
        handler.handle(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
     }
}


{
    ClientMessage clientMessage = ReplicatedMapAddEntryListenerWithPredicateCodec.encodeRequest(    aString ,    aData ,    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ReplicatedMapAddEntryListenerWithPredicateCodec.ResponseParameters params = ReplicatedMapAddEntryListenerWithPredicateCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aString, params.response));
}
{
    class ReplicatedMapAddEntryListenerWithPredicateCodecHandler extends ReplicatedMapAddEntryListenerWithPredicateCodec.AbstractEventHandler {
        @Override
        public void handle(  com.hazelcast.nio.serialization.Data
 key ,   com.hazelcast.nio.serialization.Data
 value ,   com.hazelcast.nio.serialization.Data
 oldValue ,   com.hazelcast.nio.serialization.Data
 mergingValue ,   int
 eventType ,   java.lang.String
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
        int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
        handler.handle(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
     }
}


{
    ClientMessage clientMessage = ReplicatedMapAddEntryListenerToKeyCodec.encodeRequest(    aString ,    aData ,    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ReplicatedMapAddEntryListenerToKeyCodec.ResponseParameters params = ReplicatedMapAddEntryListenerToKeyCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aString, params.response));
}
{
    class ReplicatedMapAddEntryListenerToKeyCodecHandler extends ReplicatedMapAddEntryListenerToKeyCodec.AbstractEventHandler {
        @Override
        public void handle(  com.hazelcast.nio.serialization.Data
 key ,   com.hazelcast.nio.serialization.Data
 value ,   com.hazelcast.nio.serialization.Data
 oldValue ,   com.hazelcast.nio.serialization.Data
 mergingValue ,   int
 eventType ,   java.lang.String
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
        int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
        handler.handle(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
     }
}


{
    ClientMessage clientMessage = ReplicatedMapAddEntryListenerCodec.encodeRequest(    aString ,    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ReplicatedMapAddEntryListenerCodec.ResponseParameters params = ReplicatedMapAddEntryListenerCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aString, params.response));
}
{
    class ReplicatedMapAddEntryListenerCodecHandler extends ReplicatedMapAddEntryListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  com.hazelcast.nio.serialization.Data
 key ,   com.hazelcast.nio.serialization.Data
 value ,   com.hazelcast.nio.serialization.Data
 oldValue ,   com.hazelcast.nio.serialization.Data
 mergingValue ,   int
 eventType ,   java.lang.String
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
        int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
        handler.handle(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
     }
}


{
    ClientMessage clientMessage = ReplicatedMapRemoveEntryListenerCodec.encodeRequest(    aString ,    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ReplicatedMapRemoveEntryListenerCodec.ResponseParameters params = ReplicatedMapRemoveEntryListenerCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = ReplicatedMapKeySetCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ReplicatedMapKeySetCodec.ResponseParameters params = ReplicatedMapKeySetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(datas, params.response));
}


{
    ClientMessage clientMessage = ReplicatedMapValuesCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ReplicatedMapValuesCodec.ResponseParameters params = ReplicatedMapValuesCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(datas, params.response));
}


{
    ClientMessage clientMessage = ReplicatedMapEntrySetCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ReplicatedMapEntrySetCodec.ResponseParameters params = ReplicatedMapEntrySetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aListOfEntry, params.response));
}


{
    ClientMessage clientMessage = ReplicatedMapAddNearCacheEntryListenerCodec.encodeRequest(    aString ,    aBoolean ,    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ReplicatedMapAddNearCacheEntryListenerCodec.ResponseParameters params = ReplicatedMapAddNearCacheEntryListenerCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aString, params.response));
}
{
    class ReplicatedMapAddNearCacheEntryListenerCodecHandler extends ReplicatedMapAddNearCacheEntryListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  com.hazelcast.nio.serialization.Data
 key ,   com.hazelcast.nio.serialization.Data
 value ,   com.hazelcast.nio.serialization.Data
 oldValue ,   com.hazelcast.nio.serialization.Data
 mergingValue ,   int
 eventType ,   java.lang.String
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
        int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
        handler.handle(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
     }
}


{
    ClientMessage clientMessage = MapReduceCancelCodec.encodeRequest(    aString ,    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapReduceCancelCodec.ResponseParameters params = MapReduceCancelCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = MapReduceJobProcessInformationCodec.encodeRequest(    aString ,    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapReduceJobProcessInformationCodec.ResponseParameters params = MapReduceJobProcessInformationCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(jobPartitionStates, params.jobPartitionStates));
                assertTrue(isEqual(anInt, params.processRecords));
}


{
    ClientMessage clientMessage = MapReduceForMapCodec.encodeRequest(    aString ,    aString ,    aData ,    aData ,    aData ,    aData ,    aString ,    anInt ,    datas ,    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapReduceForMapCodec.ResponseParameters params = MapReduceForMapCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aListOfEntry, params.response));
}


{
    ClientMessage clientMessage = MapReduceForListCodec.encodeRequest(    aString ,    aString ,    aData ,    aData ,    aData ,    aData ,    aString ,    anInt ,    datas ,    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapReduceForListCodec.ResponseParameters params = MapReduceForListCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aListOfEntry, params.response));
}


{
    ClientMessage clientMessage = MapReduceForSetCodec.encodeRequest(    aString ,    aString ,    aData ,    aData ,    aData ,    aData ,    aString ,    anInt ,    datas ,    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapReduceForSetCodec.ResponseParameters params = MapReduceForSetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aListOfEntry, params.response));
}


{
    ClientMessage clientMessage = MapReduceForMultiMapCodec.encodeRequest(    aString ,    aString ,    aData ,    aData ,    aData ,    aData ,    aString ,    anInt ,    datas ,    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapReduceForMultiMapCodec.ResponseParameters params = MapReduceForMultiMapCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aListOfEntry, params.response));
}


{
    ClientMessage clientMessage = MapReduceForCustomCodec.encodeRequest(    aString ,    aString ,    aData ,    aData ,    aData ,    aData ,    aData ,    anInt ,    datas ,    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    MapReduceForCustomCodec.ResponseParameters params = MapReduceForCustomCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aListOfEntry, params.response));
}


{
    ClientMessage clientMessage = TransactionalMapContainsKeyCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalMapContainsKeyCodec.ResponseParameters params = TransactionalMapContainsKeyCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = TransactionalMapGetCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalMapGetCodec.ResponseParameters params = TransactionalMapGetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = TransactionalMapGetForUpdateCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalMapGetForUpdateCodec.ResponseParameters params = TransactionalMapGetForUpdateCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = TransactionalMapSizeCodec.encodeRequest(    aString ,    aString ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalMapSizeCodec.ResponseParameters params = TransactionalMapSizeCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(anInt, params.response));
}


{
    ClientMessage clientMessage = TransactionalMapIsEmptyCodec.encodeRequest(    aString ,    aString ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalMapIsEmptyCodec.ResponseParameters params = TransactionalMapIsEmptyCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = TransactionalMapPutCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalMapPutCodec.ResponseParameters params = TransactionalMapPutCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = TransactionalMapSetCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalMapSetCodec.ResponseParameters params = TransactionalMapSetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = TransactionalMapPutIfAbsentCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalMapPutIfAbsentCodec.ResponseParameters params = TransactionalMapPutIfAbsentCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = TransactionalMapReplaceCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalMapReplaceCodec.ResponseParameters params = TransactionalMapReplaceCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = TransactionalMapReplaceIfSameCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalMapReplaceIfSameCodec.ResponseParameters params = TransactionalMapReplaceIfSameCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = TransactionalMapRemoveCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalMapRemoveCodec.ResponseParameters params = TransactionalMapRemoveCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = TransactionalMapDeleteCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalMapDeleteCodec.ResponseParameters params = TransactionalMapDeleteCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = TransactionalMapRemoveIfSameCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalMapRemoveIfSameCodec.ResponseParameters params = TransactionalMapRemoveIfSameCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = TransactionalMapKeySetCodec.encodeRequest(    aString ,    aString ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalMapKeySetCodec.ResponseParameters params = TransactionalMapKeySetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(datas, params.response));
}


{
    ClientMessage clientMessage = TransactionalMapKeySetWithPredicateCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalMapKeySetWithPredicateCodec.ResponseParameters params = TransactionalMapKeySetWithPredicateCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(datas, params.response));
}


{
    ClientMessage clientMessage = TransactionalMapValuesCodec.encodeRequest(    aString ,    aString ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalMapValuesCodec.ResponseParameters params = TransactionalMapValuesCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(datas, params.response));
}


{
    ClientMessage clientMessage = TransactionalMapValuesWithPredicateCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalMapValuesWithPredicateCodec.ResponseParameters params = TransactionalMapValuesWithPredicateCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(datas, params.response));
}


{
    ClientMessage clientMessage = TransactionalMultiMapPutCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalMultiMapPutCodec.ResponseParameters params = TransactionalMultiMapPutCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = TransactionalMultiMapGetCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalMultiMapGetCodec.ResponseParameters params = TransactionalMultiMapGetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(datas, params.response));
}


{
    ClientMessage clientMessage = TransactionalMultiMapRemoveCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalMultiMapRemoveCodec.ResponseParameters params = TransactionalMultiMapRemoveCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(datas, params.response));
}


{
    ClientMessage clientMessage = TransactionalMultiMapRemoveEntryCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalMultiMapRemoveEntryCodec.ResponseParameters params = TransactionalMultiMapRemoveEntryCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = TransactionalMultiMapValueCountCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalMultiMapValueCountCodec.ResponseParameters params = TransactionalMultiMapValueCountCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(anInt, params.response));
}


{
    ClientMessage clientMessage = TransactionalMultiMapSizeCodec.encodeRequest(    aString ,    aString ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalMultiMapSizeCodec.ResponseParameters params = TransactionalMultiMapSizeCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(anInt, params.response));
}


{
    ClientMessage clientMessage = TransactionalSetAddCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalSetAddCodec.ResponseParameters params = TransactionalSetAddCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = TransactionalSetRemoveCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalSetRemoveCodec.ResponseParameters params = TransactionalSetRemoveCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = TransactionalSetSizeCodec.encodeRequest(    aString ,    aString ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalSetSizeCodec.ResponseParameters params = TransactionalSetSizeCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(anInt, params.response));
}


{
    ClientMessage clientMessage = TransactionalListAddCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalListAddCodec.ResponseParameters params = TransactionalListAddCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = TransactionalListRemoveCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalListRemoveCodec.ResponseParameters params = TransactionalListRemoveCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = TransactionalListSizeCodec.encodeRequest(    aString ,    aString ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalListSizeCodec.ResponseParameters params = TransactionalListSizeCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(anInt, params.response));
}


{
    ClientMessage clientMessage = TransactionalQueueOfferCodec.encodeRequest(    aString ,    aString ,    aLong ,    aData ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalQueueOfferCodec.ResponseParameters params = TransactionalQueueOfferCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = TransactionalQueueTakeCodec.encodeRequest(    aString ,    aString ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalQueueTakeCodec.ResponseParameters params = TransactionalQueueTakeCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = TransactionalQueuePollCodec.encodeRequest(    aString ,    aString ,    aLong ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalQueuePollCodec.ResponseParameters params = TransactionalQueuePollCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = TransactionalQueuePeekCodec.encodeRequest(    aString ,    aString ,    aLong ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalQueuePeekCodec.ResponseParameters params = TransactionalQueuePeekCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = TransactionalQueueSizeCodec.encodeRequest(    aString ,    aString ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionalQueueSizeCodec.ResponseParameters params = TransactionalQueueSizeCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(anInt, params.response));
}


{
    ClientMessage clientMessage = CacheAddEntryListenerCodec.encodeRequest(    aString ,    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CacheAddEntryListenerCodec.ResponseParameters params = CacheAddEntryListenerCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aString, params.response));
}
{
    class CacheAddEntryListenerCodecHandler extends CacheAddEntryListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  int
 type ,   java.util.Collection<com.hazelcast.cache.impl.CacheEventData> keys ,   int
 completionId   ) {
                            assertTrue(isEqual(anInt, type));
                            assertTrue(isEqual(cacheEventDatas, keys));
                            assertTrue(isEqual(anInt, completionId));
        }
    }
    CacheAddEntryListenerCodecHandler handler = new CacheAddEntryListenerCodecHandler();
    {
        int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
        handler.handle(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
     }
}


{
    ClientMessage clientMessage = CacheAddInvalidationListenerCodec.encodeRequest(    aString ,    aBoolean   );
    int length = inputStream.readInt();
    // Since the test is generated for protocol version (1.3) which is earlier than latest change in the message
    // (version 1.4), only the bytes after frame length fields are compared
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
    CacheAddInvalidationListenerCodec.ResponseParameters params = CacheAddInvalidationListenerCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aString, params.response));
}
{
    class CacheAddInvalidationListenerCodecHandler extends CacheAddInvalidationListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  java.lang.String
 name ,   com.hazelcast.nio.serialization.Data
 key ,   java.lang.String
 sourceUuid ,   java.util.UUID
 partitionUuid ,   long
 sequence   ) {
                            assertTrue(isEqual(aString, name));
                            assertTrue(isEqual(aData, key));
                            assertTrue(isEqual(aString, sourceUuid));
        }
        @Override
        public void handle(  java.lang.String
 name ,   java.util.Collection<com.hazelcast.nio.serialization.Data> keys ,   java.util.Collection<java.lang.String> sourceUuids ,   java.util.Collection<java.util.UUID> partitionUuids ,   java.util.Collection<java.lang.Long> sequences   ) {
                            assertTrue(isEqual(aString, name));
                            assertTrue(isEqual(datas, keys));
                            assertTrue(isEqual(strings, sourceUuids));
        }
    }
    CacheAddInvalidationListenerCodecHandler handler = new CacheAddInvalidationListenerCodecHandler();
    {
        int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
        handler.handle(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
     }
    {
        int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
        handler.handle(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
     }
}


{
    ClientMessage clientMessage = CacheClearCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CacheClearCodec.ResponseParameters params = CacheClearCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = CacheRemoveAllKeysCodec.encodeRequest(    aString ,    datas ,    anInt   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CacheRemoveAllKeysCodec.ResponseParameters params = CacheRemoveAllKeysCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = CacheRemoveAllCodec.encodeRequest(    aString ,    anInt   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CacheRemoveAllCodec.ResponseParameters params = CacheRemoveAllCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = CacheContainsKeyCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CacheContainsKeyCodec.ResponseParameters params = CacheContainsKeyCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = CacheCreateConfigCodec.encodeRequest(    aData ,    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CacheCreateConfigCodec.ResponseParameters params = CacheCreateConfigCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = CacheDestroyCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CacheDestroyCodec.ResponseParameters params = CacheDestroyCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = CacheEntryProcessorCodec.encodeRequest(    aString ,    aData ,    aData ,    datas ,    anInt   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CacheEntryProcessorCodec.ResponseParameters params = CacheEntryProcessorCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = CacheGetAllCodec.encodeRequest(    aString ,    datas ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CacheGetAllCodec.ResponseParameters params = CacheGetAllCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aListOfEntry, params.response));
}


{
    ClientMessage clientMessage = CacheGetAndRemoveCodec.encodeRequest(    aString ,    aData ,    anInt   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CacheGetAndRemoveCodec.ResponseParameters params = CacheGetAndRemoveCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = CacheGetAndReplaceCodec.encodeRequest(    aString ,    aData ,    aData ,    aData ,    anInt   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CacheGetAndReplaceCodec.ResponseParameters params = CacheGetAndReplaceCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = CacheGetConfigCodec.encodeRequest(    aString ,    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CacheGetConfigCodec.ResponseParameters params = CacheGetConfigCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = CacheGetCodec.encodeRequest(    aString ,    aData ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CacheGetCodec.ResponseParameters params = CacheGetCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = CacheIterateCodec.encodeRequest(    aString ,    anInt ,    anInt ,    anInt   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CacheIterateCodec.ResponseParameters params = CacheIterateCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(anInt, params.tableIndex));
                assertTrue(isEqual(datas, params.keys));
}


{
    ClientMessage clientMessage = CacheListenerRegistrationCodec.encodeRequest(    aString ,    aData ,    aBoolean ,    anAddress   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CacheListenerRegistrationCodec.ResponseParameters params = CacheListenerRegistrationCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = CacheLoadAllCodec.encodeRequest(    aString ,    datas ,    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CacheLoadAllCodec.ResponseParameters params = CacheLoadAllCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = CacheManagementConfigCodec.encodeRequest(    aString ,    aBoolean ,    aBoolean ,    anAddress   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CacheManagementConfigCodec.ResponseParameters params = CacheManagementConfigCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = CachePutIfAbsentCodec.encodeRequest(    aString ,    aData ,    aData ,    aData ,    anInt   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CachePutIfAbsentCodec.ResponseParameters params = CachePutIfAbsentCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = CachePutCodec.encodeRequest(    aString ,    aData ,    aData ,    aData ,    aBoolean ,    anInt   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CachePutCodec.ResponseParameters params = CachePutCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = CacheRemoveEntryListenerCodec.encodeRequest(    aString ,    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CacheRemoveEntryListenerCodec.ResponseParameters params = CacheRemoveEntryListenerCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = CacheRemoveInvalidationListenerCodec.encodeRequest(    aString ,    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CacheRemoveInvalidationListenerCodec.ResponseParameters params = CacheRemoveInvalidationListenerCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = CacheRemoveCodec.encodeRequest(    aString ,    aData ,    aData ,    anInt   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CacheRemoveCodec.ResponseParameters params = CacheRemoveCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = CacheReplaceCodec.encodeRequest(    aString ,    aData ,    aData ,    aData ,    aData ,    anInt   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CacheReplaceCodec.ResponseParameters params = CacheReplaceCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = CacheSizeCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CacheSizeCodec.ResponseParameters params = CacheSizeCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(anInt, params.response));
}


{
    ClientMessage clientMessage = CacheAddPartitionLostListenerCodec.encodeRequest(    aString ,    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CacheAddPartitionLostListenerCodec.ResponseParameters params = CacheAddPartitionLostListenerCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aString, params.response));
}
{
    class CacheAddPartitionLostListenerCodecHandler extends CacheAddPartitionLostListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  int
 partitionId ,   java.lang.String
 uuid   ) {
                            assertTrue(isEqual(anInt, partitionId));
                            assertTrue(isEqual(aString, uuid));
        }
    }
    CacheAddPartitionLostListenerCodecHandler handler = new CacheAddPartitionLostListenerCodecHandler();
    {
        int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
        handler.handle(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
     }
}


{
    ClientMessage clientMessage = CacheRemovePartitionLostListenerCodec.encodeRequest(    aString ,    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CacheRemovePartitionLostListenerCodec.ResponseParameters params = CacheRemovePartitionLostListenerCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = CachePutAllCodec.encodeRequest(    aString ,    aListOfEntry ,    aData ,    anInt   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CachePutAllCodec.ResponseParameters params = CachePutAllCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = CacheIterateEntriesCodec.encodeRequest(    aString ,    anInt ,    anInt ,    anInt   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CacheIterateEntriesCodec.ResponseParameters params = CacheIterateEntriesCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(anInt, params.tableIndex));
                assertTrue(isEqual(aListOfEntry, params.entries));
}








{
    ClientMessage clientMessage = XATransactionClearRemoteCodec.encodeRequest(    anXid   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    XATransactionClearRemoteCodec.ResponseParameters params = XATransactionClearRemoteCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = XATransactionCollectTransactionsCodec.encodeRequest( );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    XATransactionCollectTransactionsCodec.ResponseParameters params = XATransactionCollectTransactionsCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(datas, params.response));
}


{
    ClientMessage clientMessage = XATransactionFinalizeCodec.encodeRequest(    anXid ,    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    XATransactionFinalizeCodec.ResponseParameters params = XATransactionFinalizeCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = XATransactionCommitCodec.encodeRequest(    aString ,    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    XATransactionCommitCodec.ResponseParameters params = XATransactionCommitCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = XATransactionCreateCodec.encodeRequest(    anXid ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    XATransactionCreateCodec.ResponseParameters params = XATransactionCreateCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aString, params.response));
}


{
    ClientMessage clientMessage = XATransactionPrepareCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    XATransactionPrepareCodec.ResponseParameters params = XATransactionPrepareCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = XATransactionRollbackCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    XATransactionRollbackCodec.ResponseParameters params = XATransactionRollbackCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = TransactionCommitCodec.encodeRequest(    aString ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionCommitCodec.ResponseParameters params = TransactionCommitCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = TransactionCreateCodec.encodeRequest(    aLong ,    anInt ,    anInt ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionCreateCodec.ResponseParameters params = TransactionCreateCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aString, params.response));
}


{
    ClientMessage clientMessage = TransactionRollbackCodec.encodeRequest(    aString ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    TransactionRollbackCodec.ResponseParameters params = TransactionRollbackCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = ContinuousQueryPublisherCreateWithValueCodec.encodeRequest(    aString ,    aString ,    aData ,    anInt ,    anInt ,    aLong ,    aBoolean ,    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ContinuousQueryPublisherCreateWithValueCodec.ResponseParameters params = ContinuousQueryPublisherCreateWithValueCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aListOfEntry, params.response));
}


{
    ClientMessage clientMessage = ContinuousQueryPublisherCreateCodec.encodeRequest(    aString ,    aString ,    aData ,    anInt ,    anInt ,    aLong ,    aBoolean ,    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ContinuousQueryPublisherCreateCodec.ResponseParameters params = ContinuousQueryPublisherCreateCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(datas, params.response));
}


{
    ClientMessage clientMessage = ContinuousQueryMadePublishableCodec.encodeRequest(    aString ,    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ContinuousQueryMadePublishableCodec.ResponseParameters params = ContinuousQueryMadePublishableCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = ContinuousQueryAddListenerCodec.encodeRequest(    aString ,    aBoolean   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ContinuousQueryAddListenerCodec.ResponseParameters params = ContinuousQueryAddListenerCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aString, params.response));
}
{
    class ContinuousQueryAddListenerCodecHandler extends ContinuousQueryAddListenerCodec.AbstractEventHandler {
        @Override
        public void handle(  com.hazelcast.map.impl.querycache.event.QueryCacheEventData
 data   ) {
                            assertTrue(isEqual(aQueryCacheEventData, data));
        }
        @Override
        public void handle(  java.util.Collection<com.hazelcast.map.impl.querycache.event.QueryCacheEventData> events ,   java.lang.String
 source ,   int
 partitionId   ) {
                            assertTrue(isEqual(queryCacheEventDatas, events));
                            assertTrue(isEqual(aString, source));
                            assertTrue(isEqual(anInt, partitionId));
        }
    }
    ContinuousQueryAddListenerCodecHandler handler = new ContinuousQueryAddListenerCodecHandler();
    {
        int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
        handler.handle(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
     }
    {
        int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.read(bytes);
        handler.handle(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
     }
}


{
    ClientMessage clientMessage = ContinuousQuerySetReadCursorCodec.encodeRequest(    aString ,    aString ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ContinuousQuerySetReadCursorCodec.ResponseParameters params = ContinuousQuerySetReadCursorCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = ContinuousQueryDestroyCacheCodec.encodeRequest(    aString ,    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    ContinuousQueryDestroyCacheCodec.ResponseParameters params = ContinuousQueryDestroyCacheCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = RingbufferSizeCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    RingbufferSizeCodec.ResponseParameters params = RingbufferSizeCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aLong, params.response));
}


{
    ClientMessage clientMessage = RingbufferTailSequenceCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    RingbufferTailSequenceCodec.ResponseParameters params = RingbufferTailSequenceCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aLong, params.response));
}


{
    ClientMessage clientMessage = RingbufferHeadSequenceCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    RingbufferHeadSequenceCodec.ResponseParameters params = RingbufferHeadSequenceCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aLong, params.response));
}


{
    ClientMessage clientMessage = RingbufferCapacityCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    RingbufferCapacityCodec.ResponseParameters params = RingbufferCapacityCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aLong, params.response));
}


{
    ClientMessage clientMessage = RingbufferRemainingCapacityCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    RingbufferRemainingCapacityCodec.ResponseParameters params = RingbufferRemainingCapacityCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aLong, params.response));
}


{
    ClientMessage clientMessage = RingbufferAddCodec.encodeRequest(    aString ,    anInt ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    RingbufferAddCodec.ResponseParameters params = RingbufferAddCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aLong, params.response));
}


{
    ClientMessage clientMessage = RingbufferReadOneCodec.encodeRequest(    aString ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    RingbufferReadOneCodec.ResponseParameters params = RingbufferReadOneCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = RingbufferAddAllCodec.encodeRequest(    aString ,    datas ,    anInt   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    RingbufferAddAllCodec.ResponseParameters params = RingbufferAddAllCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aLong, params.response));
}


{
    ClientMessage clientMessage = RingbufferReadManyCodec.encodeRequest(    aString ,    aLong ,    anInt ,    anInt ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    RingbufferReadManyCodec.ResponseParameters params = RingbufferReadManyCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(anInt, params.readCount));
                assertTrue(isEqual(datas, params.items));
}


{
    ClientMessage clientMessage = DurableExecutorShutdownCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    DurableExecutorShutdownCodec.ResponseParameters params = DurableExecutorShutdownCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = DurableExecutorIsShutdownCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    DurableExecutorIsShutdownCodec.ResponseParameters params = DurableExecutorIsShutdownCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aBoolean, params.response));
}


{
    ClientMessage clientMessage = DurableExecutorSubmitToPartitionCodec.encodeRequest(    aString ,    aData   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    DurableExecutorSubmitToPartitionCodec.ResponseParameters params = DurableExecutorSubmitToPartitionCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(anInt, params.response));
}


{
    ClientMessage clientMessage = DurableExecutorRetrieveResultCodec.encodeRequest(    aString ,    anInt   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    DurableExecutorRetrieveResultCodec.ResponseParameters params = DurableExecutorRetrieveResultCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = DurableExecutorDisposeResultCodec.encodeRequest(    aString ,    anInt   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    DurableExecutorDisposeResultCodec.ResponseParameters params = DurableExecutorDisposeResultCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = DurableExecutorRetrieveAndDisposeResultCodec.encodeRequest(    aString ,    anInt   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    DurableExecutorRetrieveAndDisposeResultCodec.ResponseParameters params = DurableExecutorRetrieveAndDisposeResultCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aData, params.response));
}


{
    ClientMessage clientMessage = CardinalityEstimatorAddCodec.encodeRequest(    aString ,    aLong   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CardinalityEstimatorAddCodec.ResponseParameters params = CardinalityEstimatorAddCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
}


{
    ClientMessage clientMessage = CardinalityEstimatorEstimateCodec.encodeRequest(    aString   );
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    assertTrue(isEqual(Arrays.copyOf(clientMessage.buffer().byteArray(), clientMessage.getFrameLength()), bytes));

}
{
    int length = inputStream.readInt();
    byte[] bytes = new byte[length];
    inputStream.read(bytes);
    CardinalityEstimatorEstimateCodec.ResponseParameters params = CardinalityEstimatorEstimateCodec.decodeResponse(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
                assertTrue(isEqual(aLong, params.response));
}





































        inputStream.close();
        input.close();

    }
}


