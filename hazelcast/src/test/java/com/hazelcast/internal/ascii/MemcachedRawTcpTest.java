/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hazelcast.instance.EndpointQualifier.MEMCACHE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MemcachedRawTcpTest extends HazelcastTestSupport {
    protected HazelcastInstance instance;

    private Socket clientSocket;
    private PrintWriter writer;
    private Scanner scanner;

    private final Pattern valueHeaderPattern = Pattern.compile("^VALUE (\\S+) (\\d+) (\\d+)$");

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

        clientSocket = new Socket();
        clientSocket.connect(getMemcachedAddress());

        writer = new PrintWriter(clientSocket.getOutputStream());
        scanner = new Scanner(clientSocket.getInputStream());
        scanner.useDelimiter("\r\n");
    }

    @After
    public void tearDown() throws Exception {
        writer.close();
        scanner.close();
        clientSocket.close();

        if (instance != null) {
            instance.getLifecycleService().terminate();
        }
    }

    public void writeLine(String line) {
        writer.write(line + "\r\n");
        writer.flush();
    }

    public String readLine() throws Exception {
        CompletableFuture<String> lineFuture = new CompletableFuture<>();

        Thread t = new Thread(() -> {
            if (scanner.hasNextLine()) {
                lineFuture.complete(scanner.nextLine());
            } else {
                lineFuture.complete(null);
            }
        });
        t.start();

        String line = null;
        try {
            line = lineFuture.get(1000, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            t.interrupt();
        }

        if (line == null) {
            fail("readLine() failed: no line found");
        }

        return line;
    }

    @Test
    public void testSetAndGet() throws Exception {
        writeLine("set key 123 0 5");
        writeLine("value");
        assertEquals("STORED", readLine());

        writeLine("get key");
        assertEquals("VALUE key 123 5", readLine());
        assertEquals("value", readLine());
        assertEquals("END", readLine());

        checkStats(1, 1, 1, 0, 0, 0, 0, 0, 0, 0);
    }

    @Test
    public void testAddAndGet() throws Exception {
        writeLine("set key 123 0 3");
        writeLine("foo");
        assertEquals("STORED", readLine());

        writeLine("add key 45 0 3");
        writeLine("bar");
        assertEquals("NOT_STORED", readLine());

        writeLine("get key");
        assertEquals("VALUE key 123 3", readLine());
        assertEquals("foo", readLine());
        assertEquals("END", readLine());

        writeLine("set key2 45 0 3");
        writeLine("bar");
        assertEquals("STORED", readLine());

        writeLine("get key2");
        assertEquals("VALUE key2 45 3", readLine());
        assertEquals("bar", readLine());
        assertEquals("END", readLine());

        checkStats(3, 2, 2, 0, 0, 0, 0, 0, 0, 0);
    }

    @Test
    public void testReplace() throws Exception {
        writeLine("replace key 42 0 3");
        writeLine("bar");
        assertEquals("NOT_STORED", readLine());

        writeLine("get key");
        assertEquals("END", readLine());

        writeLine("set key 0 0 3");
        writeLine("foo");
        assertEquals("STORED", readLine());

        writeLine("replace key 42 0 3");
        writeLine("bar");
        assertEquals("STORED", readLine());

        writeLine("get key");
        assertEquals("VALUE key 42 3", readLine());
        assertEquals("bar", readLine());
        assertEquals("END", readLine());

        checkStats(3, 2, 1, 1, 0, 0, 0, 0, 0, 0);
    }

    @Test
    public void testDelete() throws Exception {
        writeLine("delete key");
        assertEquals("NOT_FOUND", readLine());

        writeLine("set key 0 0 3");
        writeLine("foo");
        assertEquals("STORED", readLine());

        writeLine("delete key");
        assertEquals("DELETED", readLine());

        writeLine("get key");
        assertEquals("END", readLine());

        checkStats(1, 1, 0, 1, 1, 1, 0, 0, 0, 0);
    }

    @Test
    public void testBulkGet() throws Exception {
        writeLine("get k0 k1 k2 k3 k4 k5 k6 k7 k8 k9");
        assertEquals("END", readLine());

        Map<String, String> entries = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            writeLine("set k" + i + " " + i + " 0 6");
            writeLine("value" + i);
            assertEquals("STORED", readLine());

            entries.put("k" + i, "value" + i);
        }

        writeLine("get k0 k1 k2 k3 k4 k5 k6 k7 k8 k9");
        for (int i = 0; i < 10; i++) {
            Matcher matcher = valueHeaderPattern.matcher(readLine());
            assertTrue(matcher.matches());

            String key = matcher.group(1);
            int flags = Integer.parseInt(matcher.group(2));
            int valueLength = Integer.parseInt(matcher.group(3));

            String value = readLine();

            assertEquals(key.charAt(1) - '0', flags);
            assertEquals(valueLength, value.length());

            assertEquals(entries.get(key), value);
            entries.remove(key); // remove key to not count again
        }
        assertEquals("END", readLine());

        checkStats(10, 20, 10, 10, 0, 0, 0, 0, 0, 0);
    }
//
//    @Test
//    public void testSetGetDelete_WithDefaultIMap() throws Exception {
//        testSetGetDelete_WithIMap(MemcacheCommandProcessor.DEFAULT_MAP_NAME, "");
//    }
//
//    @Test
//    public void testSetGetDelete_WithCustomIMap() throws Exception {
//        String mapName = randomMapName();
//        testSetGetDelete_WithIMap(MemcacheCommandProcessor.MAP_NAME_PREFIX + mapName, mapName + ":");
//    }
//
//    private void testSetGetDelete_WithIMap(String mapName, String prefix) throws Exception {
//        String key = "key";
//        String value = "value";
//        String value2 = "value2";
//
//        IMap<String, Object> map = instance.getMap(mapName);
//        map.put(key, value);
//        assertEquals(value, client.get(prefix + key));
//
//        client.set(prefix + key, 0, value2).get();
//
//        MemcacheEntry memcacheEntry = (MemcacheEntry) map.get(key);
//        MemcacheEntry expectedEntry = new MemcacheEntry(prefix + key, value2.getBytes(), 0);
//        assertEquals(expectedEntry, memcacheEntry);
//        assertEquals(prefix + key, memcacheEntry.getKey());
//
//        client.delete(prefix + key).get();
//        assertNull(client.get(prefix + key));
//        assertNull(map.get(key));
//    }
//
//    @Test
//    public void testDeleteAll_withIMapPrefix() throws Exception {
//        String mapName = randomMapName();
//        String prefix = mapName + ":";
//        IMap<String, Object> map = instance.getMap(MemcacheCommandProcessor.MAP_NAME_PREFIX + mapName);
//
//        for (int i = 0; i < 100; i++) {
//            map.put(String.valueOf(i), i);
//        }
//
//        OperationFuture<Boolean> future = client.delete(prefix);
//        future.get();
//
//        for (int i = 0; i < 100; i++) {
//            assertNull(client.get(prefix + String.valueOf(i)));
//        }
//        assertTrue(map.isEmpty());
//    }
//
    @Test
    public void testIncrement() throws Exception {
        writeLine("incr key 1");
        assertEquals("NOT_FOUND", readLine());

        writeLine("get key");
        assertEquals("END", readLine());

        writeLine("set key 0 0 2");
        writeLine("42");
        assertEquals("STORED", readLine());

        writeLine("incr key 153");
        assertEquals("195", readLine());

        writeLine("get key");
        assertEquals("VALUE key 0 3", readLine());
        assertEquals("195", readLine());
        assertEquals("END", readLine());

        checkStats(1, 2, 1, 1, 0, 0, 1, 1, 0, 0);
    }

    @Test
    public void testIncrement_byNegativeNumber_shouldFail() throws Exception {
        writeLine("set key 0 0 2");
        writeLine("42");
        assertEquals("STORED", readLine());

        writeLine("incr key -10");
        assertTrue(readLine().startsWith("CLIENT_ERROR"));
    }

    @Test
    public void testIncrement_onNegativeNumber_shouldFail() throws Exception {
        writeLine("set key 0 0 3");
        writeLine("-42");
        assertEquals("STORED", readLine());

        writeLine("incr key 1");
        assertTrue(readLine().startsWith("CLIENT_ERROR"));
    }

    @Test
    public void testIncrement_onNonDecimal_shouldFail() throws Exception {
        writeLine("set key 0 0 3");
        writeLine("foo");
        assertEquals("STORED", readLine());

        writeLine("incr key 1");
        assertTrue(readLine().startsWith("CLIENT_ERROR"));
    }

    @Test
    public void testIncrement_onLargerThanLongMax() throws Exception {
        writeLine("set key 0 0 20");
        writeLine("18446744073709551614"); // 2^64 - 2
        assertEquals("STORED", readLine());

        writeLine("incr key 1");
        assertEquals("18446744073709551615", readLine());

        writeLine("get key");
        assertEquals("VALUE key 0 20", readLine());
        assertEquals("18446744073709551615", readLine());
        assertEquals("END", readLine());
    }

    @Test
    public void testIncrement_withOverFlow() throws Exception {
        writeLine("set key 0 0 20");
        writeLine("18446744073709551615"); // 2^64 - 1
        assertEquals("STORED", readLine());

        writeLine("incr key 1");
        assertEquals("0", readLine()); // should wrap back to 0
    }


    @Test
    public void testDecrement() throws Exception {
        writeLine("decr key 1");
        assertEquals("NOT_FOUND", readLine());

        writeLine("get key");
        assertEquals("END", readLine());

        writeLine("set key 0 0 2");
        writeLine("42");
        assertEquals("STORED", readLine());

        writeLine("decr key 33");
        assertEquals("9", readLine());

        writeLine("get key");
        // decr operations that shorten a number's length are not guaranteed to update the length
        assertTrue(readLine().startsWith("VALUE key 0 "));
        assertTrue(readLine().matches("9 *"));
        assertEquals("END", readLine());

        checkStats(1, 2, 1, 1, 0, 0, 0, 0, 1, 1);
    }

    @Test
    public void testDecrement_byNegativeNumber_shouldFail() throws Exception {
        writeLine("set key 0 0 2");
        writeLine("42");
        assertEquals("STORED", readLine());

        writeLine("decr key -10");
        assertTrue(readLine().startsWith("CLIENT_ERROR"));
    }

    @Test
    public void testDecrement_onNegativeNumber_shouldFail() throws Exception {
        writeLine("set key 0 0 3");
        writeLine("-42");
        assertEquals("STORED", readLine());

        writeLine("decr key 1");
        assertTrue(readLine().startsWith("CLIENT_ERROR"));
    }

    @Test
    public void testDecrement_onNonDecimal_shouldFail() throws Exception {
        writeLine("set key 0 0 3");
        writeLine("foo");
        assertEquals("STORED", readLine());

        writeLine("decr key 1");
        assertTrue(readLine().startsWith("CLIENT_ERROR"));
    }

    @Test
    public void testDecrement_withUnderFlow() throws Exception {
        writeLine("set key 0 0 2");
        writeLine("42");
        assertEquals("STORED", readLine());

        writeLine("decr key 123");
        assertEquals("0", readLine());

        writeLine("get key");
        assertTrue(readLine().startsWith("VALUE key 0 "));
        assertTrue(readLine().matches("0 *"));
        assertEquals("END", readLine());
    }

    @Test
    public void testAppend() throws Exception {
        writeLine("append key 0 0 3");
        writeLine("bar");
        assertEquals("NOT_STORED", readLine());

        writeLine("set key 0 0 3");
        writeLine("foo");
        assertEquals("STORED", readLine());

        writeLine("append key 0 0 3");
        writeLine("bar");
        assertEquals("STORED", readLine());

        writeLine("get key 0 0 3");
        assertEquals("VALUE key 0 6", readLine());
        assertEquals("foobar", readLine());
        assertEquals("END", readLine());
    }

    @Test
    public void testPrepend() throws Exception {
        writeLine("prepend key 0 0 3");
        writeLine("bar");
        assertEquals("NOT_STORED", readLine());

        writeLine("set key 0 0 3");
        writeLine("foo");
        assertEquals("STORED", readLine());

        writeLine("prepend key 0 0 3");
        writeLine("bar");
        assertEquals("STORED", readLine());

        writeLine("get key 0 0 3");
        assertEquals("VALUE key 0 6", readLine());
        assertEquals("barfoo", readLine());
        assertEquals("END", readLine());
    }

    @Test
    public void testExpiration() throws Exception {
        writeLine("set key 0 3 3");
        writeLine("foo");
        assertEquals("STORED", readLine());

        assertTrueEventually(() -> {
            writeLine("get key");
            if (!readLine().equals("END")) {
                readLine(); // foo
                readLine(); // END
                fail();
            }
        });
    }

    @Test
    public void testSetGet_withLargeValue() throws Exception {
        int capacity = 10000;
        StringBuilder value = new StringBuilder(capacity);
        while (value.length() < capacity) {
            value.append(randomString());
        }

        writeLine("set key 0 0 " + value.length());
        writeLine(value.toString());
        assertEquals("STORED", readLine());

        writeLine("get key");
        assertEquals("VALUE key 0 " + value.length(), readLine());
        assertEquals(value.toString(), readLine());
    }

    @Test
    public void testBulkSetGet_withManyKeys() throws Exception {
        int numberOfKeys = 1000;

        Map<String, String> entries = new HashMap<>();
        for (int i = 0; i < numberOfKeys; i++) {
            String key = "key" + i;
            String value = "value" + i;
            entries.put(key, value);

            writeLine(String.format("set %s 0 0 %d", key, value.length()));
            writeLine(value);
            assertEquals("STORED", readLine());
        }

        writeLine("get " + String.join(" ", entries.keySet()));

        for (int i = 0; i < numberOfKeys; i++) {
            Matcher matcher = valueHeaderPattern.matcher(readLine());
            assertTrue(matcher.matches());

            String key = matcher.group(1);
            int flags = Integer.parseInt(matcher.group(2));
            int length = Integer.parseInt(matcher.group(3));

            assertEquals(0, flags);

            String value = readLine();
            assertEquals(length, value.length());
            assertEquals(entries.get(key), value);

            entries.remove(key); // remove key to not count it again
        }
        assertEquals("END", readLine());
    }

    @SuppressWarnings("checkstyle:parameternumber")
    private void checkStats(int sets, int gets, int getHits, int getMisses, int deleteHits, int deleteMisses,
                            int incHits, int incMisses, int decHits, int decMisses) throws Exception {
        writeLine("stats");

        Map<String, Integer> expectedStats = new HashMap<>();
        expectedStats.put("cmd_set", sets);
        expectedStats.put("cmd_get", gets);
        expectedStats.put("get_hits", getHits);
        expectedStats.put("get_misses", getMisses);
        expectedStats.put("delete_hits", deleteHits);
        expectedStats.put("delete_misses", deleteMisses);
        expectedStats.put("incr_hits", incHits);
        expectedStats.put("incr_misses", incMisses);
        expectedStats.put("decr_hits", decHits);
        expectedStats.put("decr_misses", decMisses);

        String line;
        while (!(line = readLine()).equals("END")) {
            String[] tokens = line.split(" ");
            String statName = tokens[1];
            String actualStatValue = tokens[2];
            if (expectedStats.containsKey(statName)) {
                assertEquals("statName: " + statName, expectedStats.get(statName).toString(), actualStatValue);
            }
        }
    }

    protected InetSocketAddress getMemcachedAddress() {
        return instance.getCluster().getLocalMember().getSocketAddress(MEMCACHE);
    }

}
