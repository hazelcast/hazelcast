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

package com.hazelcast.jet.stream;

import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetConfig;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.JetTestSupport;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@Category({QuickTest.class, ParallelTest.class})
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
public abstract class AbstractStreamTest extends JetTestSupport {

    public static final int COUNT = 10000;
    public static final int NODE_COUNT = 2;

    protected static JetInstance client;
    protected static JetInstance instance;
    private static final TestMode MEMBER_TEST_MODE = new TestMode("member", () -> instance);
    private static final TestMode CLIENT_TEST_MODE = new TestMode("client", () -> client);

    private static JetTestInstanceFactory factory = new JetTestInstanceFactory();
    private static final ExecutorService executor = Executors.newCachedThreadPool();

    @Parameter
    public TestMode testMode;

    // each entry is Object[] with
    // {URI, ClassLoader, expected prefix, expected prefixed cache name, expected distributed object name}
    @Parameters(name = "{index}: mode={0}")
    public static Iterable<? extends Object> parameters() throws URISyntaxException {
        return Arrays.asList(MEMBER_TEST_MODE, CLIENT_TEST_MODE);
    }

    @BeforeClass
    public static void setupCluster() throws InterruptedException, ExecutionException {
        setLogLevel(Level.INFO);
        // configure the engine to have a sane thread count
        int parallelism = Runtime.getRuntime().availableProcessors() / NODE_COUNT / 2;
        JetConfig config = new JetConfig()
                .setExecutionThreadCount(parallelism <= 2 ? 2 : parallelism);
        instance = createCluster(NODE_COUNT, config);
        client = factory.newClient();
    }

    private static JetInstance createCluster(int nodeCount, JetConfig config) throws ExecutionException, InterruptedException {
        factory = new JetTestInstanceFactory();
        List<Future<JetInstance>> futures = new ArrayList<>();
        for (int i = 0; i < nodeCount; i++) {
            futures.add(executor.submit(() -> factory.newMember(config)));
        }
        JetInstance instance = null;
        for (Future<JetInstance> future : futures) {
            instance = future.get();
        }
        return instance;
    }

    @AfterClass
    public static void shutdown() {
        factory.shutdownAll();
    }

    protected <K, V> IStreamMap<K, V> getStreamMap() {
        return IStreamMap.streamMap(getMap(testMode.getInstance()));
    }

    protected <E> IStreamList<E> getStreamList() {
        return IStreamList.streamList(getList(testMode.getInstance()));
    }

    protected static int fillMap(IMap<String, Integer> map) {
        return fillMap(map, COUNT);
    }

    protected static int fillMap(IMap<String, Integer> map, int count) {
        for (int i = 0; i < count; i++) {
            map.put("key-" + i, i);
        }
        return count;
    }

    protected static void fillList(IList<Integer> list) {
        fillListWithInts(list, COUNT);
    }

    protected static <R> void fillList(IList<R> list, Function<Integer, R> mapper) {
        for (int i = 0; i < COUNT; i++) {
            list.add(mapper.apply(i));
        }
    }

    protected static void fillList(IList<Integer> list, Iterator<Integer> iterator) {
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
    }

    protected static <T> List<T> sortedList(IList<T> list) {
        @SuppressWarnings("unchecked") T[] array = (T[]) new Object[list.size()];
        list.toArray(array);
        Arrays.sort(array);
        return Arrays.asList(array);
    }

    protected <T extends Comparable<T>> void assertNotSorted(T[] array) {
        for (int i = 1; i < array.length; i++) {
            if (array[i - 1].compareTo(array[i]) > 0) {
                return;
            }
        }
        fail("Items in array " + Arrays.toString(array) + " were sorted.");
    }

    protected static final class TestMode {

        private final String name;
        private final Supplier<JetInstance> supplier;

        protected TestMode(String name, Supplier<JetInstance> func) {
            this.name = name;
            this.supplier = func;
        }

        protected JetInstance getInstance() {
            return supplier.get();
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
