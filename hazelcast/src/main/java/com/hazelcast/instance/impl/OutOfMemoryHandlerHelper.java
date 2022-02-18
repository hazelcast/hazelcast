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

package com.hazelcast.instance.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.server.Server;

import static com.hazelcast.internal.util.EmptyStatement.ignore;

/**
 * Helper class for OutOfMemoryHandlers to close sockets, stop threads, release allocated resources
 * of an HazelcastInstanceImpl.
 *
 * @see com.hazelcast.core.OutOfMemoryHandler
 * @see DefaultOutOfMemoryHandler
*/
public final class OutOfMemoryHandlerHelper {

    private OutOfMemoryHandlerHelper() {
    }

    public static void tryCloseConnections(HazelcastInstance hazelcastInstance) {
        if (hazelcastInstance == null) {
            return;
        }
        HazelcastInstanceImpl factory = (HazelcastInstanceImpl) hazelcastInstance;
        closeSockets(factory);
    }

    private static void closeSockets(HazelcastInstanceImpl factory) {
        Server server = factory.node.getServer();
        if (server != null) {
            try {
                server.shutdown();
            } catch (Throwable ignored) {
                ignore(ignored);
            }
        }
    }

    public static void tryShutdown(HazelcastInstance hazelcastInstance) {
        if (hazelcastInstance == null) {
            return;
        }
        HazelcastInstanceImpl factory = (HazelcastInstanceImpl) hazelcastInstance;
        closeSockets(factory);
        try {
            factory.node.shutdown(true);
        } catch (Throwable ignored) {
            ignore(ignored);
        }
    }
}
