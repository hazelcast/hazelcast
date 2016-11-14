/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet2;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.jet2.impl.IListWriter;
import com.hazelcast.jet2.impl.SocketTextStreamReader;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class SocketTextStreamReaderTest extends HazelcastTestSupport {

    private static TestHazelcastInstanceFactory factory;
    private JetEngine jetEngine;
    private HazelcastInstance instance;

    @BeforeClass
    public static void setupFactory() {
        factory = new TestHazelcastInstanceFactory();
    }

    @AfterClass
    public static void shutdownFactory() {
        factory.shutdownAll();
    }

    @Before
    public void setupEngine() {
        instance = factory.newHazelcastInstance();
        jetEngine = JetEngine.get(instance, "jetEngine");
    }

    @After
    public void shutdown() {
        instance.shutdown();
    }


    @Test
    public void testSocketReader() {
        DAG dag = new DAG();
        final String host = "localhost";
        final int port = 8888;
        Runnable server = () -> {
            try {
                ServerSocket socket = new ServerSocket(port);
                Socket accept = socket.accept();
                PrintWriter writer = new PrintWriter(accept.getOutputStream());
                writer.write("hello \n");
                writer.write("world \n");
                writer.write("jet \n");
                writer.flush();
                accept.close();
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        };
        new Thread(server).start();
        Vertex producer = new Vertex("producer", SocketTextStreamReader.supplier(host, port))
                .parallelism(1);

        Vertex consumer = new Vertex("consumer", IListWriter.supplier("consumer"))
                .parallelism(1);

        dag.addVertex(producer)
                .addVertex(consumer)
                .addEdge(new Edge(producer, consumer));

        jetEngine.newJob(dag).execute();

        IList<Object> list = instance.getList("consumer");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(3, list.size());
            }
        });
    }

}
