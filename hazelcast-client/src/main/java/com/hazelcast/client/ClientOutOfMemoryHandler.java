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

package com.hazelcast.client;

import com.hazelcast.client.connection.nio.ClientConnectionManagerImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.DefaultOutOfMemoryHandler;

import static com.hazelcast.util.EmptyStatement.ignore;

/**
 * To clear resources of the client upon OutOfMemory
 */
public class ClientOutOfMemoryHandler extends DefaultOutOfMemoryHandler {

    @Override
    public void onOutOfMemory(OutOfMemoryError oome, HazelcastInstance[] hazelcastInstances) {
        for (HazelcastInstance instance : hazelcastInstances) {
            if (instance instanceof HazelcastClientInstanceImpl) {
                ClientHelper.cleanResources((HazelcastClientInstanceImpl) instance);
            }
        }
        try {
            oome.printStackTrace(System.err);
        } catch (Throwable ignored) {
            ignore(ignored);
        }
    }

    private static final class ClientHelper {

        private ClientHelper() {
        }

        static void cleanResources(HazelcastClientInstanceImpl client) {
            closeSockets(client);
            tryShutdown(client);
        }

        private static void closeSockets(HazelcastClientInstanceImpl client) {
            ClientConnectionManagerImpl connectionManager = (ClientConnectionManagerImpl) client.getConnectionManager();
            if (connectionManager != null) {
                try {
                    connectionManager.shutdown();
                } catch (Throwable ignored) {
                    ignore(ignored);
                }
            }
        }

        private static void tryShutdown(HazelcastClientInstanceImpl client) {
            try {
                client.doShutdown();
            } catch (Throwable ignored) {
                ignore(ignored);
            }
        }
    }
}
