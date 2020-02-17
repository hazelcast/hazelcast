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

import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A Pipeline which connects to a server and writes
 * the received lines to an IList.
 */
public class StreamTextSocket {

    private static final String HOST = "localhost";
    private static final int PORT = 5252;

    private static final String SINK_NAME = "list";

    private static final AtomicInteger COUNTER = new AtomicInteger(100_000);

    public static void main(String[] args) throws Exception {
        NettyServer nettyServer = new NettyServer(PORT, channel -> {
            for (int i; (i = COUNTER.getAndDecrement()) > 0; ) {
                channel.writeAndFlush(i + "\n");
            }
            channel.close();
        }, ConsumerEx.noop());
        nettyServer.start();

        JetInstance jet = Jet.bootstrappedInstance();

        try {
            Pipeline p = Pipeline.create();
            p.readFrom(Sources.socket(HOST, PORT, UTF_8))
             .withoutTimestamps()
             .writeTo(Sinks.list(SINK_NAME));

            jet.newJob(p).join();

            System.out.println("Jet received " + jet.getList(SINK_NAME).size() + " items from the socket");
        } finally {
            nettyServer.stop();
            Jet.shutdownAll();
        }

    }
}
