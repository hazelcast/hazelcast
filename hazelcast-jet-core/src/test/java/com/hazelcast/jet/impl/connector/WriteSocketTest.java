/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestSupport;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.stream.IStreamMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Processors.readMap;
import static com.hazelcast.jet.Processors.writeSocket;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.util.stream.IntStream.range;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class WriteSocketTest extends JetTestSupport {

    private final static String HOST = "localhost";
    private final static int PORT = 8787;
    private final static int ITEM_COUNT = 1000;

    @Test
    public void test() throws Exception {

        ServerSocket socket = new ServerSocket(PORT);
        CountDownLatch latch = new CountDownLatch(ITEM_COUNT);
        new Thread(() -> uncheckRun(() -> {
            while (!socket.isClosed()) {
                Socket accept = socket.accept();
                new Thread(() -> uncheckRun(() -> {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(accept.getInputStream()));
                    String line = reader.readLine();
                    while (line != null) {
                        line = reader.readLine();
                        latch.countDown();
                    }
                })).start();
            }
        })).start();

        JetInstance jetInstance = createJetMember();
        createJetMember();
        IStreamMap<Integer, String> map = jetInstance.getMap("map");
        range(0, 1000).forEach(i -> map.put(i, i + "\n"));

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", readMap("map"));
        Vertex sink = dag.newVertex("sink", writeSocket(HOST, PORT));

        dag.edge(between(source, sink));

        jetInstance.newJob(dag).execute().get();
        assertOpenEventually(latch);
        socket.close();
    }

}
