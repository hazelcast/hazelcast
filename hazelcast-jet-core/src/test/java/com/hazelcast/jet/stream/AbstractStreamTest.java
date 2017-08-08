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

package com.hazelcast.jet.stream;

import com.hazelcast.cache.ICache;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.JetTestSupport;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@Category({QuickTest.class, ParallelTest.class})
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
public abstract class AbstractStreamTest extends JetTestSupport {

    public static final int COUNT = 10000;

    protected static JetInstance instance;

    private static final int NODE_COUNT = 2;

    private static JetInstance client;
    private static JetInstance[] instances;
    private static JetTestInstanceFactory factory = new JetTestInstanceFactory();

    private static final TestMode MEMBER_TEST_MODE = new TestMode("member", () -> instance);
    private static final TestMode CLIENT_TEST_MODE = new TestMode("client", () -> client);

    @Parameter
    public TestMode testMode;

    // each entry is Object[] with
    // {URI, ClassLoader, expected prefix, expected prefixed cache name, expected distributed object name}
    @Parameters(name = "{index}: mode={0}")
    public static Iterable<?> parameters() {
        return Arrays.asList(MEMBER_TEST_MODE, CLIENT_TEST_MODE);
    }

    @BeforeClass
    public static void setupCluster() throws Exception {
        // configure the engine to have a sane thread count
        int parallelism = Runtime.getRuntime().availableProcessors() / NODE_COUNT / 2;
        JetConfig config = new JetConfig();
        config.getInstanceConfig().setCooperativeThreadCount(parallelism <= 2 ? 2 : parallelism);
        //Necessary for ICache
        config.getHazelcastConfig().addCacheConfig(new CacheSimpleConfig().setName("*"));
        instance = createCluster(NODE_COUNT, config);
        client = factory.newClient();
    }

    @AfterClass
    public static void tearDown() {
        factory.terminateAll();
        factory = null;
        instances = null;
        instance = null;
        client = null;
    }

    protected DistributedStream<Map.Entry<String, Integer>> streamMap() {
        IStreamMap<String, Integer> map = getMap(testMode.getInstance());
        fillMap(map);
        return map.stream();
    }

    protected DistributedStream<Map.Entry<String, Integer>> streamCache() {
        IStreamCache<String, Integer> cache = getCache();
        fillCache(cache);
        return cache.stream();
    }

    protected DistributedStream<Integer> streamList() {
        IStreamList<Integer> list = getList(testMode.getInstance());
        fillList(list);
        return list.stream();
    }

    protected <K, V> IStreamMap<K, V> getMap() {
        return getMap(testMode.getInstance());
    }

    protected <K, V> IStreamCache<K, V> getCache() {
        String cacheName = randomName();
        IStreamCache<K, V> cache = null;
        for (JetInstance jetInstance : instances) {
            cache = jetInstance.getCacheManager().getCache(cacheName);
        }
        return cache;
    }

    protected <E> IStreamList<E> getList() {
        return getList(testMode.getInstance());
    }

    private static JetInstance createCluster(int nodeCount, JetConfig config) throws Exception {
        factory = new JetTestInstanceFactory();
        instances = new JetInstance[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            instances[i] = factory.newMember(config);
        }
        return instances[0];
    }

    protected static int fillMap(IMap<String, Integer> map) {
        return fillMap(map, COUNT);
    }

    protected static int fillCache(ICache<String, Integer> cache) {
        return fillCache(cache, COUNT);
    }

    protected static int fillMap(IMap<String, Integer> map, int count) {
        for (int i = 0; i < count; i++) {
            map.put("key-" + i, i);
        }
        return count;
    }

    protected static int fillCache(ICache<String, Integer> cache, int count) {
        for (int i = 0; i < count; i++) {
            cache.put("key-" + i, i);
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

    protected static <T extends Comparable<T>> void assertNotSorted(T[] array) {
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
