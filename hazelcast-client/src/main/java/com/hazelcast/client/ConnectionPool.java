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

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ConnectionPool {
    private final ConnectionManager connectionManager;

    static private final int POOL_SIZE = 1;

    BlockingQueue<Connection> pool = new LinkedBlockingQueue<Connection>(POOL_SIZE);

    public ConnectionPool(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
        while (pool.size() < POOL_SIZE) {
            try {
                Connection connection = connectionManager.createNextConnection();
                connectionManager.bindConnection(connection);
                pool.offer(connection);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public Connection takeConnection() throws InterruptedException {
        return pool.take();
    }

    public void releaseConnection(Connection connection) {
        pool.offer(connection);
    }
}
