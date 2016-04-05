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
package com.hazelcast.jet;

import com.hazelcast.jet.api.application.Application;
import com.hazelcast.jet.base.JetBaseTest;
import com.hazelcast.jet.impl.dag.EdgeImpl;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.processors.CombinerProcessor;
import com.hazelcast.jet.processors.CountProcessor;
import com.hazelcast.jet.spi.container.ContainerDescriptor;
import com.hazelcast.jet.spi.dag.DAG;
import com.hazelcast.jet.spi.dag.Edge;
import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.jet.spi.strategy.ShufflingStrategy;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class FileToFileTest extends JetBaseTest {

    private static final int CNT = 10000;

    @Test
    public void simpleFileToFileTest() throws Exception {
        JetBaseTest.initCluster(2);
        Application application = createApplication("fileToFileTest");

        DAG dag = createDAG();

        File input = createInputFile();
        System.out.println(input.getAbsolutePath());
        File output = File.createTempFile("output", ".txt");

        Vertex counter = createVertex("counter", CountProcessor.Factory.class, 1);
        Vertex combiner = createVertex("combiner", CombinerProcessor.Factory.class, 1);
        addVertices(dag, counter, combiner);

        Edge edge = new EdgeImpl.EdgeBuilder("edge", counter, combiner)
                .shuffling(true)
                .shufflingStrategy(new SingleNodeShufflingStrategy(SERVER.getCluster().getLocalMember().getAddress()))
                .build();

        addEdges(dag, edge);

        counter.addSourceFile(input.getPath());
        combiner.addSinkFile(output.getPath());
        try {
            executeApplication(dag, application).get(TIME_TO_AWAIT, TimeUnit.SECONDS);

            List<String> strings = Files.readAllLines(output.toPath());

            assertEquals(1, strings.size());

            String[] tuple = strings.get(0).split("\\s+");

            assertEquals(0, Integer.parseInt(tuple[0]));
            assertEquals(CNT * 2, Integer.parseInt(tuple[1]));
        }
        finally {
            application.finalizeApplication().get(TIME_TO_AWAIT, TimeUnit.SECONDS);
        }
    }

    private File createInputFile() throws IOException {
        File input = File.createTempFile("input", ".txt");
        try (FileWriter writer = new FileWriter(input)) {
            for (int i = 0; i < CNT; i++) {
                writer.write(Integer.toString(i));
                writer.write("\n");
            }
        }
        return input;
    }

    private static class SingleNodeShufflingStrategy implements ShufflingStrategy {


        private final int port;
        private final String host;

        public SingleNodeShufflingStrategy(Address address) {
            this.host = address.getHost();
            this.port = address.getPort();

        }

        @Override
        public Address[] getShufflingAddress(ContainerDescriptor containerDescriptor) {
            try {
                return new Address[] { new Address(host, port) };
            } catch (UnknownHostException e) {
                throw JetUtil.reThrow(e);
            }
        }
    }
}
