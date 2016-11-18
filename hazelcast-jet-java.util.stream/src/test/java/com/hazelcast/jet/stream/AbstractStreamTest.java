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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.stream.AbstractStreamTest.Options;
import com.hazelcast.jet.stream.impl.StreamUtil;
import com.hazelcast.jet2.JetEngine;
import com.hazelcast.jet2.JetEngineConfig;
import com.hazelcast.jet2.JetTestSupport;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.ParallelRunnerOptions;
import com.hazelcast.test.annotation.ConfigureParallelRunnerWith;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@Category({QuickTest.class, ParallelTest.class})
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@ConfigureParallelRunnerWith(Options.class)
public abstract class AbstractStreamTest extends JetTestSupport {

    public static final int COUNT = 10000;
    public static final int NODE_COUNT = 2;

    public static final TestMode MEMBER_TEST_MODE = new TestMode("member", t -> t.instance);
    public static final TestMode CLIENT_TEST_MODE = new TestMode("client", t -> t.client);

    protected HazelcastInstance instance;
    protected HazelcastInstance client;

    @Parameter
    public TestMode testMode;

    // each entry is Object[] with
    // {URI, ClassLoader, expected prefix, expected prefixed cache name, expected distributed object name}
    @Parameters(name = "{index}: mode={0}")
    public static Iterable<? extends Object> parameters() throws URISyntaxException {
        return Arrays.asList(MEMBER_TEST_MODE, CLIENT_TEST_MODE);
    }

    @Before
    public void setupCluster() throws InterruptedException, ExecutionException {
        setLogLevel(Level.INFO);
        String engineName = "stream-test";
        System.setProperty(StreamUtil.ENGINE_NAME_PROPERTY.getName(), engineName);
        instance = createCluster(NODE_COUNT);

        // configure the engine to have a sane thread count
        JetEngineConfig config = new JetEngineConfig()
                .setParallelism(Runtime.getRuntime().availableProcessors() / NODE_COUNT);
        JetEngine.get(instance, engineName, config);
        if (testMode == CLIENT_TEST_MODE) {
            client = factory.newHazelcastClient();
        }
    }

    protected <K, V> IStreamMap<K, V> getStreamMap() {
        return IStreamMap.streamMap(getMap(testMode.getInstance(this)));
    }

    protected <E> IStreamList<E> getStreamList() {
        return IStreamList.streamList(getList(testMode.getInstance(this)));
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
        private final Function<AbstractStreamTest, HazelcastInstance> func;

        protected TestMode(String name, Function<AbstractStreamTest, HazelcastInstance> func) {
            this.name = name;
            this.func = func;
        }

        protected HazelcastInstance getInstance(AbstractStreamTest test) {
            return func.apply(test);
        }

        @Override
        public String toString() {
            return name;
        }
    }

    public static class Options implements ParallelRunnerOptions {

        @Override
        public int maxParallelTests() {
            return Runtime.getRuntime().availableProcessors() / 2;
        }
    }
}
