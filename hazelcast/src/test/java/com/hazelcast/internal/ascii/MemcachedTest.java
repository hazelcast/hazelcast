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

package com.hazelcast.internal.ascii;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.internal.ascii.memcache.MemcacheCommandProcessor;
import com.hazelcast.internal.ascii.memcache.MemcacheEntry;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ConcurrentModificationException;

import static com.hazelcast.instance.EndpointQualifier.MEMCACHE;
import static com.hazelcast.test.MemcacheTestUtil.shutdownQuietly;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MemcachedTest extends HazelcastTestSupport {

    protected HazelcastInstance instance;
    protected MemcachedClient client;

    protected Config createConfig() {
        Config config = smallInstanceConfig();
        config.getNetworkConfig().getMemcacheProtocolConfig().setEnabled(true);
        // Join is disabled intentionally. will start standalone HazelcastInstances.
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getAutoDetectionConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        return config;
    }

    @Before
    public void setup() throws Exception {
        instance = Hazelcast.newHazelcastInstance(createConfig());
        client = getMemcachedClient(instance);
    }

    @After
    public void tearDown() {
        try {
            shutdownQuietly(client);
        } catch (ConcurrentModificationException e) {
            // See https://github.com/hazelcast/hazelcast/issues/14204
            // Due to a MemcachedClient bug, we swallow this exception
        } finally {
            if (instance != null) {
                instance.getLifecycleService().terminate();
            }
        }
    }

    @Test
    public void testSetAndGet() throws Exception {
        String key = "key";
        String value = "value";

        OperationFuture<Boolean> future = client.set(key, 0, value);
        assertEquals(Boolean.TRUE, future.get());

        assertEquals(value, client.get(key));

        checkStats(1, 1, 1, 0, 0, 0, 0, 0, 0, 0);
    }

    @Test
    public void testAddAndGet() throws Exception {
        String key = "key";
        String value = "value";
        String key2 = "key2";
        String value2 = "value2";

        OperationFuture<Boolean> future = client.set(key, 0, value);
        assertEquals(Boolean.TRUE, future.get());

        future = client.add(key, 0, value2);
        assertEquals(Boolean.FALSE, future.get());
        assertEquals(value, client.get(key));

        future = client.add(key2, 0, value2);
        assertEquals(Boolean.TRUE, future.get());
        assertEquals(value2, client.get(key2));

        checkStats(3, 2, 2, 0, 0, 0, 0, 0, 0, 0);
    }

    @Test
    public void testReplace() throws Exception {
        String key = "key";
        String value = "value";
        String value2 = "value2";

        OperationFuture<Boolean> future = client.replace(key, 0, value2);
        assertEquals(Boolean.FALSE, future.get());
        assertNull(client.get(key));

        future = client.set(key, 0, value);
        assertEquals(Boolean.TRUE, future.get());

        future = client.replace(key, 0, value2);
        assertEquals(Boolean.TRUE, future.get());
        assertEquals(value2, client.get(key));

        checkStats(3, 2, 1, 1, 0, 0, 0, 0, 0, 0);
    }

    @Test
    public void testDelete() throws Exception {
        String key = "key";
        String value = "value";

        OperationFuture<Boolean> future = client.delete(key);
        assertEquals(Boolean.FALSE, future.get());

        future = client.set(key, 0, value);
        assertEquals(Boolean.TRUE, future.get());

        future = client.delete(key);
        assertEquals(Boolean.TRUE, future.get());
        assertNull(client.get(key));

        checkStats(1, 1, 0, 1, 1, 1, 0, 0, 0, 0);
    }

    @Test
    public void testBulkGet() throws Exception {
        List<String> keys = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            keys.add("key" + i);
        }

        Map<String, Object> result = client.getBulk(keys);
        assertEquals(0, result.size());

        String value = "value";
        for (String key : keys) {
            OperationFuture<Boolean> future = client.set(key, 0, value);
            future.get();
        }

        result = client.getBulk(keys);
        assertEquals(keys.size(), result.size());
        for (String key : keys) {
            assertEquals(value, result.get(key));
        }

        checkStats(keys.size(), keys.size() * 2, keys.size(), keys.size(), 0, 0, 0, 0, 0, 0);
    }

    @Test
    public void testSetGetDelete_WithDefaultIMap() throws Exception {
        testSetGetDelete_WithIMap(MemcacheCommandProcessor.DEFAULT_MAP_NAME, "");
    }

    @Test
    public void testSetGetDelete_WithCustomIMap() throws Exception {
        String mapName = randomMapName();
        testSetGetDelete_WithIMap(MemcacheCommandProcessor.MAP_NAME_PREFIX + mapName, mapName + ":");
    }

    private void testSetGetDelete_WithIMap(String mapName, String prefix) throws Exception {
        String key = "key";
        String value = "value";
        String value2 = "value2";

        IMap<String, Object> map = instance.getMap(mapName);
        map.put(key, value);
        assertEquals(value, client.get(prefix + key));

        client.set(prefix + key, 0, value2).get();

        MemcacheEntry memcacheEntry = (MemcacheEntry) map.get(key);
        MemcacheEntry expectedEntry = new MemcacheEntry(prefix + key, value2.getBytes(), 0);
        assertEquals(expectedEntry, memcacheEntry);
        assertEquals(prefix + key, memcacheEntry.getKey());

        client.delete(prefix + key).get();
        assertNull(client.get(prefix + key));
        assertNull(map.get(key));
    }

    @Test
    public void testDeleteAll_withIMapPrefix() throws Exception {
        String mapName = randomMapName();
        String prefix = mapName + ":";
        IMap<String, Object> map = instance.getMap(MemcacheCommandProcessor.MAP_NAME_PREFIX + mapName);

        for (int i = 0; i < 100; i++) {
            map.put(String.valueOf(i), i);
        }

        OperationFuture<Boolean> future = client.delete(prefix);
        future.get();

        for (int i = 0; i < 100; i++) {
            assertNull(client.get(prefix + String.valueOf(i)));
        }
        assertTrue(map.isEmpty());
    }

    @Test
    public void testIncrement() throws Exception {
        String key = "key";

        long value = client.incr(key, 1);
        assertEquals(-1, value);
        assertNull(client.get(key));

        OperationFuture<Boolean> future = client.set(key, 0, "1");
        future.get();

        value = client.incr(key, 10);
        assertEquals(11, value);

        checkStats(1, 1, 0, 1, 0, 0, 1, 1, 0, 0);
    }

    @Test
    public void testDecrement() throws Exception {
        String key = "key";

        long value = client.decr(key, 1);
        assertEquals(-1, value);
        assertNull(client.get(key));

        OperationFuture<Boolean> future = client.set(key, 0, "5");
        future.get();

        value = client.decr(key, 2);
        assertEquals(3, value);

        checkStats(1, 1, 0, 1, 0, 0, 0, 0, 1, 1);
    }

    @Test
    public void testAppend() throws Exception {
        String key = "key";
        String value = "value";
        String append = "123";

        OperationFuture<Boolean> future = client.append(key, append);
        assertEquals(Boolean.FALSE, future.get());

        future = client.set(key, 0, value);
        future.get();

        future = client.append(key, append);
        assertEquals(Boolean.TRUE, future.get());

        assertEquals(value + append, client.get(key));
    }

    @Test
    public void testPrepend() throws Exception {
        String key = "key";
        String value = "value";
        String prepend = "123";

        OperationFuture<Boolean> future = client.prepend(key, prepend);
        assertEquals(Boolean.FALSE, future.get());

        future = client.set(key, 0, value);
        future.get();

        future = client.prepend(key, prepend);
        assertEquals(Boolean.TRUE, future.get());

        assertEquals(prepend + value, client.get(key));
    }

    @Test
    public void testExpiration() throws Exception {
        final String key = "key";
        client.set(key, 3, "value").get();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNull(client.get(key));
            }
        });
    }

    @Test
    public void testSetGet_withLargeValue() throws Exception {
        String key = "key";
        int capacity = 10000;
        StringBuilder value = new StringBuilder(capacity);
        while (value.length() < capacity) {
            value.append(randomString());
        }

        OperationFuture<Boolean> future = client.set(key, 0, value.toString());
        future.get();

        Object result = client.get(key);
        assertEquals(value.toString(), result);
    }

    @Test
    public void testBulkSetGet_withManyKeys() throws Exception {
        int numberOfKeys = 1000;
        Collection<String> keys = new HashSet<String>(numberOfKeys);

        for (int i = 0; i < numberOfKeys; i++) {
            String key = "key" + i;
            OperationFuture<Boolean> future = client.set(key, 0, key);
            future.get();
            keys.add(key);
        }

        Map<String, Object> result = client.getBulk(keys);
        for (String key : keys) {
            assertEquals(key, result.get(key));
        }
    }

    @SuppressWarnings("checkstyle:parameternumber")
    private void checkStats(int sets, int gets, int getHits, int getMisses, int deleteHits, int deleteMisses,
                            int incHits, int incMisses, int decHits, int decMisses) {
        InetSocketAddress address = getMemcachedAddr(instance);
        Map<String, String> stats = client.getStats().get(address);

        assertEquals(String.valueOf(sets), stats.get("cmd_set"));
        assertEquals(String.valueOf(gets), stats.get("cmd_get"));
        assertEquals(String.valueOf(getHits), stats.get("get_hits"));
        assertEquals(String.valueOf(getMisses), stats.get("get_misses"));
        assertEquals(String.valueOf(deleteHits), stats.get("delete_hits"));
        assertEquals(String.valueOf(deleteMisses), stats.get("delete_misses"));
        assertEquals(String.valueOf(incHits), stats.get("incr_hits"));
        assertEquals(String.valueOf(incMisses), stats.get("incr_misses"));
        assertEquals(String.valueOf(decHits), stats.get("decr_hits"));
        assertEquals(String.valueOf(decMisses), stats.get("decr_misses"));
    }

    protected InetSocketAddress getMemcachedAddr(HazelcastInstance instance) {
        return instance.getCluster().getLocalMember().getSocketAddress(MEMCACHE);
    }

    protected MemcachedClient getMemcachedClient(HazelcastInstance instance) throws Exception {
        ConnectionFactory factory = new ConnectionFactoryBuilder()
                .setOpTimeout(60 * 60 * 60)
                .setDaemon(true)
                .setFailureMode(FailureMode.Retry)
                .build();
        return new MemcachedClient(factory, Collections.singletonList(getMemcachedAddr(instance)));
    }
}
