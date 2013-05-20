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

package com.hazelcast.client.connection;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;

import java.io.Closeable;
import java.io.IOException;

/**
 * @mdogan 5/15/13
 */
public interface Connection extends Closeable {

    Address getEndpoint();

    boolean write(Data data) throws IOException;

    Data read() throws IOException;

    int getId();

    long getLastReadTime();

    /**
     * May close socket or return connection to a pool depending on connection's type.
     *
     * @throws IOException
     */
    void release() throws IOException;

    /**
     * Closes connection's socket, regardless of connection's type.
     *
     * @throws IOException
     */
    void close() throws IOException;

}
