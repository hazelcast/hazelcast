/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline;

import com.hazelcast.cache.ICache;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.TestInClusterSupport;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.nio.Address;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.IntStream;
import javax.cache.Cache;
import org.junit.Before;

import static com.hazelcast.query.TruePredicate.truePredicate;
import static java.util.stream.Collectors.toList;

public abstract class PipelineTestSupport extends TestInClusterSupport {

    static final char ITEM_COUNT = 10;
    final String srcName = randomName();
    final String sinkName = randomName();

    Pipeline p;
    BatchStage<Integer> srcStage;
    Sink<Object> sink;

    IMap<String, Integer> srcMap;
    ICache<String, Integer> srcCache;
    IList<Object> srcList;
    IList<Object> sinkList;

    @Before
    public void beforePipelineTestSupport() {
        p = Pipeline.create();
        srcMap = jet().getMap(srcName);
        srcCache = jet().getCacheManager().getCache(srcName);
        srcList = jet().getList(srcName);
        sink = Sinks.list(sinkName);
        sinkList = jet().getList(sinkName);
    }

    void addToSrcList(List<Integer> data) {
        srcList.addAll(data);
    }

    void addToList(List<Integer> dest, List<Integer> data) {
        dest.addAll(data);
    }

    void putToSrcMap(List<Integer> data) {
        putToMap(srcMap, data);
    }

    void putToSrcCache(List<Integer> data) {
        putToCache(srcCache, data);
    }

    static void putToMap(Map<String, Integer> dest, List<Integer> data) {
        int[] key = {0};
        data.forEach(i -> dest.put(String.valueOf(key[0]++), i));
    }

    static void putToCache(Cache<String, Integer> dest, List<Integer> data) {
        int[] key = {0};
        data.forEach(i -> dest.put(String.valueOf(key[0]++), i));
    }

    JetInstance jet() {
        return testMode.getJet();
    }

    void execute() {
        jet().newJob(p).join();
    }

    Map<Object, Integer> sinkToBag() {
        return toBag(this.sinkList);
    }

    static BatchSource<Integer> mapValuesSource(String srcName) {
        return Sources.map(srcName, truePredicate(),
                (DistributedFunction<Entry<String, Integer>, Integer>) Entry::getValue);
    }

    static <T> Map<T, Integer> toBag(Collection<T> coll) {
        Map<T, Integer> bag = new HashMap<>();
        for (T t : coll) {
            bag.merge(t, 1, (count, x) -> count + 1);
        }
        return bag;
    }

    static List<Integer> sequence(int itemCount) {
        return IntStream.range(0, itemCount).boxed().collect(toList());
    }

    static List<HazelcastInstance> createRemoteCluster(Config config, int size) {
        ArrayList<HazelcastInstance> instances = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            instances.add(Hazelcast.newHazelcastInstance(config));
        }
        return instances;
    }

    static List<HazelcastInstance> createRemoteCluster(int size) {
        return createRemoteCluster(new Config(), size);
    }

    static ClientConfig getClientConfigForRemoteCluster(HazelcastInstance instance) {
        ClientConfig clientConfig = new ClientConfig();
        Address address = instance.getCluster().getLocalMember().getAddress();
        clientConfig.getNetworkConfig().addAddress(address.getHost() + ':' + address.getPort());
        clientConfig.getGroupConfig().setName(instance.getConfig().getGroupConfig().getName());
        return clientConfig;
    }

}
