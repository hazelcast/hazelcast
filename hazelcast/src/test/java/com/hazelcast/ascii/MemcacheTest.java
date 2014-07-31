/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.ascii;

import com.hazelcast.ascii.memcache.MemcacheEntry;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

/**
 * User: sancar
 * Date: 3/7/13
 * Time: 2:48 PM
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class MemcacheTest extends HazelcastTestSupport {

    final static Config config = new XmlConfigBuilder().build();

    @BeforeClass
    @AfterClass
    public static void killAllHazelcastInstances() throws IOException {
        Hazelcast.shutdownAll();
    }

    public MemcachedClient getMemcacheClient(HazelcastInstance instance) throws IOException {
        final LinkedList<InetSocketAddress> addresses = new LinkedList<InetSocketAddress>();
        addresses.add(instance.getCluster().getLocalMember().getInetSocketAddress());
        final ConnectionFactory factory = new ConnectionFactoryBuilder().setOpTimeout(60 * 60 * 60).setDaemon(true).setFailureMode(FailureMode.Retry).build();
        return new MemcachedClient(factory, addresses);
    }

    @Test
    public void testMemcacheSimple() throws IOException, ExecutionException, InterruptedException {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        MemcachedClient client = getMemcacheClient(instance);
        try {
            for (int i = 0; i < 100; i++) {
                final OperationFuture<Boolean> future = client.set(String.valueOf(i), 0, i);
                assertEquals(Boolean.TRUE, future.get());
            }
            for (int i = 0; i < 100; i++) {
                assertEquals(i, client.get(String.valueOf(i)));
            }
            for (int i = 0; i < 100; i++) {
                final OperationFuture<Boolean> future = client.add(String.valueOf(i), 0, i * 100);
                assertEquals(Boolean.FALSE, future.get());
            }
            for (int i = 0; i < 100; i++) {
                assertEquals(i, client.get(String.valueOf(i)));
            }
            for (int i = 100; i < 200; i++) {
                final OperationFuture<Boolean> future = client.add(String.valueOf(i), 0, i);
                assertEquals(Boolean.TRUE, future.get());
            }
            for (int i = 0; i < 200; i++) {
                assertEquals(i, client.get(String.valueOf(i)));
            }
            for (int i = 0; i < 200; i++) {
                final OperationFuture<Boolean> future = client.replace(String.valueOf(i), 0, i * 10);
                assertEquals(Boolean.TRUE, future.get());
            }
            for (int i = 0; i < 200; i++) {
                assertEquals(i * 10, client.get(String.valueOf(i)));
            }
            for (int i = 200; i < 400; i++) {
                final OperationFuture<Boolean> future = client.replace(String.valueOf(i), 0, i);
                assertEquals(Boolean.FALSE, future.get());
            }
            for (int i = 200; i < 400; i++) {
                assertEquals(null, client.get(String.valueOf(i)));
            }
            for (int i = 100; i < 200; i++) {
                final OperationFuture<Boolean> future = client.delete(String.valueOf(i));
                assertEquals(Boolean.TRUE, future.get());
            }
            for (int i = 100; i < 200; i++) {
                assertEquals(null, client.get(String.valueOf(100)));
            }
            for (int i = 100; i < 200; i++) {
                final OperationFuture<Boolean> future = client.delete(String.valueOf(i));
                assertEquals(Boolean.FALSE, future.get());
            }

            final LinkedList<String> keys = new LinkedList<String>();
            for (int i = 0; i < 100; i++) {
                keys.add(String.valueOf(i));
            }
            final Map<String, Object> bulk = client.getBulk(keys);
            for (int i = 0; i < 100; i++) {
                assertEquals(i * 10, bulk.get(String.valueOf(i)));
            }
            // STATS
            final Map<String, String> stats = client.getStats().get(instance.getCluster().getLocalMember().getInetSocketAddress());
            assertEquals("700", stats.get("cmd_set"));
            assertEquals("1000", stats.get("cmd_get"));
            assertEquals("700", stats.get("get_hits"));
            assertEquals("300", stats.get("get_misses"));
            assertEquals("100", stats.get("delete_hits"));
            assertEquals("100", stats.get("delete_misses"));
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void testMemcacheWithIMap() throws IOException, InterruptedException, ExecutionException {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        MemcachedClient client = getMemcacheClient(instance);
        final String prefix = "testMemcacheWithIMap:";
        try {
            final IMap<String, Object> map = instance.getMap("hz_memcache_testMemcacheWithIMap");
            for (int i = 0; i < 100; i++) {
                map.put(String.valueOf(i), String.valueOf(i));
            }
            for (int i = 0; i < 100; i++) {
                assertEquals(String.valueOf(i), client.get(prefix + String.valueOf(i)));
                final OperationFuture<Boolean> future = client.set(prefix + String.valueOf(i), 0, String.valueOf(i * 10));
                future.get();
            }
            for (int i = 0; i < 100; i++) {
                final MemcacheEntry memcacheEntry = (MemcacheEntry) map.get(String.valueOf(i));
                final MemcacheEntry expected = new MemcacheEntry(prefix + String.valueOf(i), String.valueOf(i * 10).getBytes(), 0);
                assertEquals(expected, memcacheEntry);
            }
            final OperationFuture<Boolean> future = client.delete(prefix);
            future.get();
            for (int i = 0; i < 100; i++) {
                assertEquals(null, client.get(prefix + String.valueOf(i)));
            }
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void testIncrementAndDecrement() throws IOException, ExecutionException, InterruptedException {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        MemcachedClient client = getMemcacheClient(instance);
        try {
            for (int i = 0; i < 100; i++) {
                final OperationFuture<Boolean> future = client.set(String.valueOf(i), 0, i);
                future.get();
            }
            for (int i = 0; i < 100; i++) {
                assertEquals(i * 2, client.incr(String.valueOf(i), i));
            }
            for (int i = 100; i < 120; i++) {
                assertEquals(-1, client.incr(String.valueOf(i), i));
            }
            for (int i = 0; i < 100; i++) {
                assertEquals(i, client.decr(String.valueOf(i), i));
            }
            for (int i = 100; i < 130; i++) {
                assertEquals(-1, client.decr(String.valueOf(i), i));
            }
            for (int i = 0; i < 100; i++) {
                assertEquals(i, client.get(String.valueOf(i)));
            }
            final Map<String, String> stats = client.getStats().get(instance.getCluster().getLocalMember().getInetSocketAddress());
            assertEquals("100", stats.get("cmd_set"));
            assertEquals("100", stats.get("cmd_get"));
            assertEquals("100", stats.get("incr_hits"));
            assertEquals("20", stats.get("incr_misses"));
            assertEquals("100", stats.get("decr_hits"));
            assertEquals("30", stats.get("decr_misses"));
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void testMemcacheAppendPrepend() throws IOException, ExecutionException, InterruptedException {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        MemcachedClient client = getMemcacheClient(instance);
        try {
            for (int i = 0; i < 100; i++) {
                final OperationFuture<Boolean> future = client.set(String.valueOf(i), 0, String.valueOf(i));
                future.get();
            }
            for (int i = 0; i < 100; i++) {
                final OperationFuture<Boolean> future = client.append(0, String.valueOf(i), "append");
                assertEquals(Boolean.TRUE, future.get());
            }
            for (int i = 0; i < 100; i++) {
                final OperationFuture<Boolean> future = client.prepend(0, String.valueOf(i), "prepend");
                assertEquals(Boolean.TRUE, future.get());
            }
            for (int i = 1; i < 100; i++) {
                assertEquals("prepend" + String.valueOf(i) + "append", client.get(String.valueOf(i)));
            }
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void testQuit() throws IOException {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        MemcachedClient client = getMemcacheClient(instance);
        client.shutdown();
    }

    @Test
    public void testMemcacheTTL() throws IOException, ExecutionException, InterruptedException {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        final MemcachedClient client = getMemcacheClient(instance);
        try {
            client.set(String.valueOf(0), 5, 10).get();
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    assertEquals(null, client.get(String.valueOf(0)));
                }
            });
        } finally {
            client.shutdown();
        }
    }
}