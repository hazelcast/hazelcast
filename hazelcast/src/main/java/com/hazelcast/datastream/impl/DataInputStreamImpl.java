/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datastream.impl;

import com.hazelcast.datastream.DataInputStream;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.function.Consumer;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static java.util.concurrent.TimeUnit.MILLISECONDS;


class DataInputStreamImpl<R> implements DataInputStream<R>, Consumer<Data> {

    private final InternalSerializationService serializationService;
    private final DSService service;
    private final String dataStreamName;
    private final BlockingQueue<Data> queue = new LinkedBlockingQueue<>();

    DataInputStreamImpl(InternalSerializationService serializationService,
                        DSService service,
                        String name,
                        List<Integer> partitionIds,
                        List<Long> offsets) {
        this.serializationService = serializationService;
        this.service = service;
        this.dataStreamName = name;
        service.startListening(dataStreamName, this, partitionIds, offsets);
    }

    @Override
    public void accept(Data packet) {
        queue.offer(packet);
    }

    @Override
    public R read() throws InterruptedException {
        return serializationService.toObject(queue.take());
    }

    @Override
    public R read(long timeoutMs) throws InterruptedException {
        return serializationService.toObject(queue.poll(timeoutMs, MILLISECONDS));
    }

    @Override
    public R poll() {
        return serializationService.toObject(queue.poll());
    }
}
