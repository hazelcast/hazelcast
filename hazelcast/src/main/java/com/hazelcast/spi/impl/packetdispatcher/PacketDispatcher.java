/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.packetdispatcher;

import com.hazelcast.nio.Packet;

/**
 * The {@link PacketDispatcher} is responsible for dispatching a Packet to the appropriate service.
 *
 * When it receives a Packet (from the IO system) it dispatches to the appropriate service, e.g :
 * <ol>
 * <li>OperationService</li>
 * <li>EventService</li>
 * <li>WanReplicationService</li>
 * </ol>
 */
public interface PacketDispatcher {

    /**
     * Dispatches a packet to the appropriate service.
     *
     * This method is likely going to be called from the IO system when it takes a Packet of the wire and offers it to be
     * processed by the system.
     *
     * The implementation should not throw any exception; they should be handled by the PacketDispatcher. The IO thread should
     * not be bothered by such a task.
     *
     * Important:
     * Don't do anything time consuming on the thread that calls the dispatch method. It can quickly cause bad performance on the
     * io system.
     *
     * @param packet the Packet to dispatch.
     */
    void dispatch(Packet packet);
}
