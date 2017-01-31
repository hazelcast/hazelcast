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

package com.hazelcast.spi.impl;

import com.hazelcast.nio.Packet;

/**
 * The {@link PacketHandler} is responsible for handling packets.
 *
 * It provides an abstraction for different components that want to receive packets and handle them. For example an
 * OperationService that receive operation or operation-response packets.
 */
public interface PacketHandler {

    /**
     * Signals the PacketHandler that there is a packet to be handled.
     *
     * @param packet the response packet to handle
     */
    void handle(Packet packet) throws Exception;
}
