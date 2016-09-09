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
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.hazelcast.util.UuidUtil;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class JetTestSupport extends HazelcastTestSupport {

    protected static TestHazelcastFactory hazelcastInstanceFactory;
    protected static final int PARALLELISM = 8;

    @BeforeClass
    public static void setUpFactory() {
        setLogLevel(Level.INFO);
        hazelcastInstanceFactory = new TestHazelcastFactory();
    }

    @AfterClass
    public static void tearDownFactory() {
        hazelcastInstanceFactory.shutdownAll();
    }

    protected static HazelcastInstance createCluster(TestHazelcastInstanceFactory factory, int nodeCount) {
        HazelcastInstance instance = null;
        for (int i = 0; i < nodeCount; i++) {
            instance = factory.newHazelcastInstance();
        }
        return instance;
    }

    protected static HazelcastInstance createCluster(int nodeCount) {
        return createCluster(hazelcastInstanceFactory, nodeCount);
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

    public static Vertex createVertex(String name, Class<? extends Processor> processorClass, int parallelism) {
        return new Vertex(name, processorClass).parallelism(parallelism);
    }

    public static Vertex createVertex(String name, Class<? extends Processor> processorClass) {
        return createVertex(name, processorClass, PARALLELISM);
    }

    public static void execute(Job job) throws ExecutionException, InterruptedException {
        try {
            job.execute().get();
        } finally {
            job.destroy();
        }
    }

    protected static String randomJobName() {
        return UuidUtil.newUnsecureUuidString().replaceAll("-", "");
    }
}
