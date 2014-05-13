/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.OutOfMemoryHandler;

/**
 * To clear resources of the client upon OutOfMemory
 */
public class ClientOutOfMemoryHandler extends OutOfMemoryHandler {

    @Override
    public void onOutOfMemory(OutOfMemoryError oom, HazelcastInstance[] hazelcastInstances) {
        System.err.println(oom);
        for (HazelcastInstance instance : hazelcastInstances) {
            if (instance instanceof HazelcastClient) {
                ClientHelper.cleanResources((HazelcastClient) instance);
            }
        }
    }

    public static final class ClientHelper {

        private ClientHelper() {
        }

        public static void cleanResources(HazelcastClient client) {
            closeSockets(client);
            tryStopThreads(client);
            tryShutdown(client);
        }

        private static void closeSockets(HazelcastClient client) {
            final ClientConnectionManager connectionManager = client.getConnectionManager();
            if (connectionManager != null) {
                try {
                    connectionManager.shutdown();
                } catch (Throwable ignored) {
                }
            }
        }

        private static void tryShutdown(HazelcastClient client) {
            if (client == null) {
                return;
            }
            try {
                client.doShutdown();
            } catch (Throwable ignored) {
            }
        }

        public static void tryStopThreads(HazelcastClient client) {
            if (client == null) {
                return;
            }
            try {
                client.getThreadGroup().interrupt();
            } catch (Throwable ignored) {
            }
        }

    }
}
