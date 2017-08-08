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
import com.hazelcast.core.IMap;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.stream.IStreamCache;
import com.hazelcast.jet.stream.IStreamList;
import com.hazelcast.jet.stream.IStreamMap;
import com.hazelcast.nio.Address;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class JetTestSupport extends HazelcastTestSupport {

    private JetTestInstanceFactory instanceFactory;

    @After
    public void shutdownFactory() {
        if (instanceFactory != null) {
            instanceFactory.shutdownAll();
        }
    }

    protected JetInstance createJetClient() {
        return instanceFactory.newClient();
    }

    protected JetInstance createJetMember() {
        return this.createJetMember(new JetConfig());
    }

    protected JetInstance createJetMember(JetConfig config) {
        if (instanceFactory == null) {
            instanceFactory = new JetTestInstanceFactory();
        }
        return instanceFactory.newMember(config);
    }

    protected JetInstance createJetMember(JetConfig config, Address[] blockedAddress) {
        if (instanceFactory == null) {
            instanceFactory = new JetTestInstanceFactory();
        }
        return instanceFactory.newMember(config, blockedAddress);
    }

    protected static <K, V> IStreamMap<K, V> getMap(JetInstance instance) {
        return instance.getMap(randomName());
    }

    protected static <K, V> IStreamCache<K, V> getCache(JetInstance instance) {
        return instance.getCacheManager().getCache(randomName());
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

    protected static <E> IStreamList<E> getList(JetInstance instance) {
        return instance.getList(randomName());
    }

    public static void assertTrueEventually(UncheckedRunnable runnable) {
        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                runnable.run();
            }
        });
    }

    public static void assertTrueEventually(UncheckedRunnable runnable, long timeoutSeconds) {
        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                runnable.run();
            }
        }, timeoutSeconds);
    }

    public static void assertTrueAllTheTime(UncheckedRunnable runnable, long durationSeconds) {
        HazelcastTestSupport.assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                runnable.run();
            }
        }, durationSeconds);
    }

    public Address nextAddress() {
        return instanceFactory.nextAddress();
    }

    @FunctionalInterface
    public interface UncheckedRunnable {
        void run() throws Exception;
    }
}
