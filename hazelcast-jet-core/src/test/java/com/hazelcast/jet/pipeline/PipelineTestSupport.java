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
import com.hazelcast.jet.Job;
import com.hazelcast.jet.TestInClusterSupport;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.nio.Address;
import org.junit.Before;

import javax.cache.Cache;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.IntStream;

import static com.hazelcast.query.TruePredicate.truePredicate;
import static java.util.stream.Collectors.toList;

public abstract class PipelineTestSupport extends TestInClusterSupport {

    protected int itemCount = 1_000;
    protected final String srcName = journaledMapName();
    protected final String sinkName = randomName();

    protected Pipeline p;
    protected Sink<Object> sink;
    protected BatchSource<Object> source;

    protected IMap<String, Integer> srcMap;
    protected IList<Object> srcList;
    protected IList<Object> sinkList;

    private ICache<String, Integer> srcCache;

    @Before
    public void beforePipelineTestSupport() {
        p = Pipeline.create();
        srcMap = jet().getMap(srcName);
        srcCache = jet().getCacheManager().getCache(srcName);
        srcList = jet().getList(srcName);
        source = Sources.list(srcName);

        sink = Sinks.list(sinkName);
        sinkList = jet().getList(sinkName);
    }

    protected JetInstance jet() {
        return testMode.getJet();
    }

    protected void execute() {
        jet().newJob(p).join();
    }

    protected Job start() {
        return jet().newJob(p);
    }

    static String journaledMapName() {
        return randomMapName(JOURNALED_MAP_PREFIX);
    }

    protected void addToSrcList(Collection<Integer> data) {
        srcList.addAll(data);
    }

    void putToBatchSrcMap(Collection<Integer> data) {
        putToMap(srcMap, data);
    }

    void putToBatchSrcCache(Collection<Integer> data) {
        putToCache(srcCache, data);
    }

    static void putToMap(Map<String, Integer> dest, Collection<Integer> data) {
        int[] key = {0};
        data.forEach(i -> dest.put(String.valueOf(key[0]++), i));
    }

    static void putToCache(Cache<String, Integer> dest, Collection<Integer> data) {
        int[] key = {0};
        data.forEach(i -> dest.put(String.valueOf(key[0]++), i));
    }

    <T> Sink<T> sinkList() {
        return Sinks.list(sinkName);
    }

    @SuppressWarnings("unchecked")
    <T> Map<T, Integer> sinkToBag() {
        return toBag((List<T>) this.sinkList);
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

    protected static List<Integer> sequence(int itemCount) {
        return IntStream.range(0, itemCount).boxed().collect(toList());
    }

    static List<HazelcastInstance> createRemoteCluster(Config config, int size) {
        ArrayList<HazelcastInstance> instances = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            instances.add(Hazelcast.newHazelcastInstance(config));
        }
        return instances;
    }

    static ClientConfig getClientConfigForRemoteCluster(HazelcastInstance instance) {
        ClientConfig clientConfig = new ClientConfig();
        Address address = instance.getCluster().getLocalMember().getAddress();
        clientConfig.getNetworkConfig().addAddress(address.getHost() + ':' + address.getPort());
        clientConfig.getGroupConfig().setName(instance.getConfig().getGroupConfig().getName());
        return clientConfig;
    }

}
