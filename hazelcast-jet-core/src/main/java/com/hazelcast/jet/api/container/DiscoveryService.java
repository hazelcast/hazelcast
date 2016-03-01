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

package com.hazelcast.jet.api.container;


import com.hazelcast.jet.api.data.io.SocketReader;
import com.hazelcast.jet.api.data.io.SocketWriter;
import com.hazelcast.nio.Address;

import java.util.Map;

/**
 * Abstract discovery-service interface;
 *
 * The goal is to find JET-nodes;
 *
 * After discovery it created corresponding writers and readers;
 */
public interface DiscoveryService {
    /**
     * Executes discovery process;
     */
    void executeDiscovery();

    /**
     * @return - discovered socket writers;
     */
    Map<Address, SocketWriter> getSocketWriters();

    /**
     * @return - discovered socket readers;
     */
    Map<Address, SocketReader> getSocketReaders();
}

