/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client;

import com.hazelcast.client.cluster.Bind;
import com.hazelcast.impl.ClusterOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.logging.Level;

public class DefaultClientBinder implements ClientBinder {
    private HazelcastClient client;
    private final ILogger logger = Logger.getLogger(getClass().getName());

    public DefaultClientBinder(HazelcastClient client) {
        this.client = client;
    }

    public void bind(Connection connection) throws IOException {
        Bind b = null;
        try {
            b = new Bind(new Address(connection.getAddress().getHostName(), connection.getSocket().getLocalPort()));
        } catch (UnknownHostException e) {
            logger.log(Level.WARNING, e.getMessage() + " while creating the bind package.");
        }
        Packet bind = new Packet();
        bind.set("remotelyProcess", ClusterOperation.REMOTELY_PROCESS, Serializer.toByte(null), Serializer.toByte(b));
        client.getOutRunnable().writer.write(connection, bind);
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            return;
        }
    }
}
