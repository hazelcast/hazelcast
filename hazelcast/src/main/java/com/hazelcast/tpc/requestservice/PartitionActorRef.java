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

package com.hazelcast.tpc.requestservice;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.server.tcp.TcpServerConnection;
import com.hazelcast.tpc.engine.AsyncSocket;
import com.hazelcast.tpc.engine.Engine;
import com.hazelcast.tpc.engine.Eventloop;
import com.hazelcast.tpc.engine.actor.ActorRef;
import com.hazelcast.tpc.engine.frame.Frame;

import java.util.concurrent.CompletableFuture;

import static com.hazelcast.internal.util.HashUtil.hashToIndex;
import static com.hazelcast.tpc.engine.frame.Frame.OFFSET_REQ_CALL_ID;

/**
 * An {@link ActorRef} that routes messages to the {@link PartitionActor}.
 *
 * todo:
 * Should also handle redirect messages.
 */
public final class PartitionActorRef extends ActorRef<Frame> {

    private final int partitionId;
    private final InternalPartitionService partitionService;
    private final Address thisAddress;
    private final Requests requests;
    private final RequestService requestService;
    private final Eventloop eventloop;

    public PartitionActorRef(int partitionId,
                             InternalPartitionService partitionService,
                             Engine engine,
                             RequestService requestService,
                             Address thisAddress,
                             Requests requests) {
        this.partitionId = partitionId;
        this.partitionService = partitionService;
        this.thisAddress = thisAddress;
        this.requests = requests;
        this.requestService = requestService;
        this.eventloop = engine.eventloop(hashToIndex(partitionId, engine.eventloopCount()));
    }

    public CompletableFuture<Frame> submit(Frame request) {
        CompletableFuture<Frame> future = request.future;

        Address address = partitionService.getPartitionOwner(partitionId);
        if (address.equals(thisAddress)) {
             eventloop.execute(request);
        } else {
            // todo: this should in theory not be needed. We could use the last
            // address and only in case of a redirect, we update.
            TcpServerConnection connection = requestService.getConnection(address);

            AsyncSocket socket = connection.sockets[hashToIndex(partitionId, connection.sockets.length)];

            // we need to acquire the frame because storage will release it once written
            // and we need to keep the frame around for the response.
            request.acquire();
            long callId = requests.nextCallId();

            request.putLong(OFFSET_REQ_CALL_ID, callId);
            requests.map.put(callId, request);

            //todo: deal with return value.
            socket.writeAndFlush(request);
        }
        return future;
    }

    @Override
    public void send(Frame request) {
        submit(request);
    }
}
