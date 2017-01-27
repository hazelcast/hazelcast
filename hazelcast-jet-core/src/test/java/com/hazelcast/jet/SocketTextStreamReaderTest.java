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

package com.hazelcast.jet;

import com.hazelcast.core.IList;
import com.hazelcast.jet.impl.connector.IListWriter;
import com.hazelcast.jet.impl.connector.SocketTextStreamReader;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class SocketTextStreamReaderTest extends JetTestSupport {

    private JetTestInstanceFactory factory;
    private JetInstance instance;

    private final static String HOST = "localhost";
    private final static int PORT = 8888;

    @Before
    public void setupEngine() {
        factory = new JetTestInstanceFactory();
        instance = factory.newMember();
    }

    @After
    public void shutdown() {
        factory.shutdownAll();
    }


    @Test
    public void when_dataWrittenToSocket_then_dataReadFromReader() throws Exception {
        // Given
        ServerSocket socket = new ServerSocket(PORT);
        new Thread(() -> uncheckRun(() -> {
            Socket accept = socket.accept();
            PrintWriter writer = new PrintWriter(accept.getOutputStream());
            writer.write("hello \n");
            writer.write("world \n");
            writer.write("jet \n");
            writer.flush();
            accept.close();
            socket.close();
        })).start();

        DAG dag = new DAG();
        Vertex producer = dag.newVertex("producer", SocketTextStreamReader.supplier(HOST, PORT)).localParallelism(1);
        Vertex consumer = dag.newVertex("consumer", IListWriter.supplier("consumer")).localParallelism(1);
        dag.edge(between(producer, consumer));

        // When
        instance.newJob(dag).execute().get();

        IList<Object> list = instance.getList("consumer");
        assertTrueEventually(() -> assertEquals(3, list.size()));
    }

    @Test
    @Ignore // This test currently fails, since BufferedReader can't be interrupted
    public void when_jobCancelled_then_readerClosed() throws Exception {
        ServerSocket socket = new ServerSocket(PORT);
        Socket[] accept = new Socket[1];
        CountDownLatch acceptionLatch = new CountDownLatch(1);
        new Thread(() -> uncheckRun(() -> {
            accept[0] = socket.accept();
            acceptionLatch.countDown();
            PrintWriter writer = new PrintWriter(accept[0].getOutputStream());
            writer.write("hello \n");
            writer.write("world \n");
            writer.write("jet \n");
            writer.flush();
        })).start();

        DAG dag = new DAG().vertex(
                new Vertex("producer", SocketTextStreamReader.supplier(HOST, PORT)).localParallelism(1)
        );

        Future<Void> job = instance.newJob(dag).execute();
        acceptionLatch.await();
        job.cancel(true);

        assertTrueEventually(() -> {
            assertTrue("Socket not closed", accept[0].isClosed());
        });
    }
}
