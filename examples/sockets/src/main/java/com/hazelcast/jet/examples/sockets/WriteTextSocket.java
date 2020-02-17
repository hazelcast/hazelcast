/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.examples.sockets;

import com.hazelcast.function.ConsumerEx;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.IMap;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A Pipeline which writes the contents of an IMap
 * to a server using the socket writer.
 */
public class WriteTextSocket {

    private static final String HOST = "localhost";
    private static final int PORT = 5252;

    private static final int SOURCE_ITEM_COUNT = 100_000;
    private static final String SOURCE_NAME = "map";

    private static final AtomicInteger COUNTER = new AtomicInteger();

    public static void main(String[] args) throws Exception {
        NettyServer nettyServer = new NettyServer(PORT, ConsumerEx.noop(), msg -> COUNTER.incrementAndGet());
        nettyServer.start();

        JetInstance jet = Jet.bootstrappedInstance();

        try {
            System.out.println("Populating map...");
            IMap<Integer, Integer> map = jet.getMap(SOURCE_NAME);
            IntStream.range(0, SOURCE_ITEM_COUNT).parallel().forEach(i -> map.put(i, i));

            Pipeline p = Pipeline.create();
            p.readFrom(Sources.map(SOURCE_NAME))
             .writeTo(Sinks.socket(HOST, PORT, e -> e.getValue().toString(), UTF_8));

            System.out.println("Executing job...");
            jet.newJob(p).join();
        } finally {
            nettyServer.stop();
            Jet.shutdownAll();
        }

        System.out.println("Server read " + COUNTER.get() + " items from the socket.");
    }
}
