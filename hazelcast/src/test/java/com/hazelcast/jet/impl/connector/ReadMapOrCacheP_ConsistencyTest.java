/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static com.hazelcast.jet.pipeline.Sources.map;
import static com.hazelcast.jet.pipeline.Sources.remoteMap;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({NightlyTest.class})
public class ReadMapOrCacheP_ConsistencyTest extends JetTestSupport {

    private static final String MAP_NAME = "map";
    private static final int NUM_ITEMS = 100_000;

    private static AtomicInteger processedCount;
    private static CountDownLatch startLatch;
    private static CountDownLatch proceedLatch;

    private final List<HazelcastInstance> remoteInstances = new ArrayList<>();
    private HazelcastInstance hz;

    @Before
    public void setup() {
        hz = createHazelcastInstance();

        processedCount = new AtomicInteger();
        startLatch = new CountDownLatch(1);
        proceedLatch = new CountDownLatch(1);
    }

    @After
    public void after() {
        for (HazelcastInstance instance : remoteInstances) {
            instance.getLifecycleService().terminate();
        }
    }

    @Test
    public void test_addingItems_local() {
        test_addingItems(hz.getMap(MAP_NAME), null);
    }

    @Test
    public void test_addingItems_remote() {
        Config config = new Config().setClusterName(UuidUtil.newUnsecureUuidString());
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        remoteInstances.add(hz);


        test_addingItems(hz.getMap(MAP_NAME), clientConfig(hz));
    }

    @Test
    public void test_removingItems_local() {
        test_removingItems(hz.getMap(MAP_NAME), null);
    }

    @Test
    public void test_removingItems_remote() {
        Config config = new Config().setClusterName(UuidUtil.newUnsecureUuidString());
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        remoteInstances.add(hz);

        test_removingItems(hz.getMap(MAP_NAME), clientConfig(hz));
    }

    @Test
    public void test_migration_local() throws Exception {
        test_migration(hz.getMap(MAP_NAME), null, () -> createHazelcastInstance());
    }

    @Test
    public void test_migration_remote() throws Exception {
        Config config = smallInstanceConfig().setClusterName(UuidUtil.newUnsecureUuidString());
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        remoteInstances.add(hz);

        test_migration(hz.getMap(MAP_NAME), clientConfig(hz),
                () -> remoteInstances.add(Hazelcast.newHazelcastInstance(config)));
    }

    private void test_removingItems(IMap<Integer, Integer> map, ClientConfig clientConfig) {
        Map<Integer, Integer> tmpMap = new HashMap<>();
        for (int i = 0; i < NUM_ITEMS; i++) {
            tmpMap.put(i, i);
        }
        map.putAll(tmpMap);

        int remainingItemCount = 1024;

        Pipeline p = Pipeline.create();
        p.readFrom(mapSource(clientConfig))
         .map(o -> {
             proceedLatch.await();
             // apply some gentle backpressure
             if (processedCount.incrementAndGet() % 128 == 0) {
                 Thread.sleep(10);
             }
             return o.getKey();
         })
         .setLocalParallelism(1)
         .writeTo(AssertionSinks.assertCollected(list -> {
             // check no duplicates
             Set<Integer> collected = new HashSet<>(list);
             assertEquals("there were duplicates", list.size(), collected.size());

             // we should still have the items we didn't remove
             for (int i = 0; i < remainingItemCount; i++) {
                 assertTrue("key " + i + " was missing", collected.contains(i));
             }
         }));

        Job job = hz.getJet().newJob(p);

        proceedLatch.countDown();

        // delete some items
        for (int i = remainingItemCount; i < NUM_ITEMS; i++) {
            map.delete(i);
        }

        job.join();
    }

    private void test_addingItems(IMap<Integer, Integer> map, ClientConfig clientConfig) {
        int initialItemCount = 1024; // use low initial item count to force resizing
        for (int i = 0; i < initialItemCount; i++) {
            map.put(i, i);
        }

        Pipeline p = Pipeline.create();
        p.readFrom(mapSource(clientConfig))
         .map(o -> {
             proceedLatch.await();
             // apply some gentle backpressure
             if (processedCount.incrementAndGet() % 128 == 0) {
                 Thread.sleep(10);
             }
             return o.getKey();
         })
         .setLocalParallelism(1)
         .writeTo(AssertionSinks.assertCollected(list -> {
             // check no duplicates
             Set<Integer> collected = new HashSet<>(list);
             assertEquals("there were duplicates", list.size(), collected.size());

             // check all initial items before iteration started
             for (int i = 0; i < initialItemCount; i++) {
                 assertTrue("key " + i + " was missing", collected.contains(i));
             }
         }));

        Job job = hz.getJet().newJob(p);

        proceedLatch.countDown();

        // put some additional items
        for (int i = initialItemCount; i < initialItemCount + NUM_ITEMS; i++) {
            map.put(i, i);
        }

        job.join();
    }

    private void test_migration(IMap<Integer, Integer> map, ClientConfig clientConfig, Runnable newInstanceAction)
            throws InterruptedException {
        // populate the map
        Map<Integer, Integer> tmpMap = new HashMap<>();
        for (int i = 0; i < NUM_ITEMS; i++) {
            tmpMap.put(i, i);
        }
        map.putAll(tmpMap);

        int initialProcessingLimit = 1024;
        Pipeline p = Pipeline.create();
        p.readFrom(mapSource(clientConfig))
         .map(o -> {
             // process first 1024 items, then wait for migration
             int count = processedCount.incrementAndGet();
             if (count == initialProcessingLimit) {
                 // signal to start new node
                 startLatch.countDown();
             } else if (count > initialProcessingLimit) {
                 // wait for migration to complete
                 proceedLatch.await();
             }
             return o.getKey();
         })
         .setLocalParallelism(1)
         .writeTo(AssertionSinks.assertAnyOrder(IntStream.range(0, NUM_ITEMS).boxed().collect(toList())));

        Job job = hz.getJet().newJob(p, new JobConfig().setAutoScaling(false));

        // start the job. The map reader will be blocked thanks to the backpressure from the mapping stage
        startLatch.await();
        // create new member, migration will take place
        newInstanceAction.run();
        // release the latch, the rest of the items will be processed after the migration
        proceedLatch.countDown();

        job.join();
    }

    private BatchSource<Entry<Integer, Integer>> mapSource(ClientConfig clientConfig) {
        return clientConfig == null ? map(MAP_NAME) : remoteMap(MAP_NAME, clientConfig);
    }

    private ClientConfig clientConfig(HazelcastInstance instance) {
        Member member = instance.getCluster().getLocalMember();
        Address address = member.getAddress();
        String hostPort = address.getHost() + ":" + address.getPort();

        ClientConfig clientConfig = new ClientConfig().setClusterName(instance.getConfig().getClusterName());
        clientConfig.getNetworkConfig().addAddress(hostPort);

        return clientConfig;
    }
}
