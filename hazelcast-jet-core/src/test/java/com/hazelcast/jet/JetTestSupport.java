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

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.application.Application;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.processor.ContainerProcessor;
import com.hazelcast.jet.processor.ProcessorDescriptor;
import com.hazelcast.test.HazelcastTestSupport;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class JetTestSupport extends HazelcastTestSupport {

    protected static TestHazelcastFactory hazelcastInstanceFactory;
    protected static final int TASK_COUNT = 4;

    @BeforeClass
    public static void setUpFactory() {
        setLogLevel(Level.INFO);
        hazelcastInstanceFactory = new TestHazelcastFactory();
    }

    protected static HazelcastInstance createCluster(int nodeCount) {
        HazelcastInstance instance = null;
        for (int i = 0; i < nodeCount; i++) {
            instance = hazelcastInstanceFactory.newHazelcastInstance();
        }
        return instance;
    }

    protected static <K, V> IMap<K, V> getMap(HazelcastInstance instance) {
        return instance.getMap(randomName());
    }

    protected static void fillMapWithInts(IMap<Integer, Integer> map, int count) {
        Map<Integer, Integer> vals = IntStream.range(0, count).boxed().collect(Collectors.toMap(m -> m, m -> m));
        map.putAll(vals);
    }

    protected static void fillListWithInts(IList<Integer> list, int count) {
        for (int i = 0; i < count; i++) {
            list.add(i);
        }
    }

    protected static <E> IList<E> getList(HazelcastInstance instance) {
        return instance.getList(randomName());
    }

    public static Vertex createVertex(String name, Class<? extends ContainerProcessor> processorClass,
                                      int taskCount) {
        return new Vertex(
                name,
                ProcessorDescriptor.
                        builder(processorClass).
                        withTaskCount(taskCount).
                        build()
        );
    }

    public static Vertex createVertex(String name, Class<? extends ContainerProcessor> processorClass) {
        return createVertex(name, processorClass, TASK_COUNT);
    }

    public static void execute(Application application) throws ExecutionException, InterruptedException {
        try {
            application.execute().get();
        } finally {
            application.finalizeApplication();
        }
    }

    @AfterClass
    public static void tearDownFactory() {
        hazelcastInstanceFactory.shutdownAll();
    }
}
