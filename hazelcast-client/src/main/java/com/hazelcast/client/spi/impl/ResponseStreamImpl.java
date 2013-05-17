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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.connection.Connection;
import com.hazelcast.client.spi.ResponseStream;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

import java.io.IOException;

/**
 * @mdogan 5/17/13
 */
final class ResponseStreamImpl implements ResponseStream {

    private final SerializationService serializationService;
    private final Connection connection;

    ResponseStreamImpl(SerializationService serializationService, Connection connection) {
        this.serializationService = serializationService;
        this.connection = connection;
    }

    public Object read() throws Exception {
        final Data data = connection.read();
        return serializationService.toObject(data);
    }

    public void end() throws IOException {
        connection.close();
    }
}
