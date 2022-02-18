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
import com.hazelcast.internal.ascii.memcache.MemcacheCommandProcessor;
import com.hazelcast.internal.ascii.memcache.MemcacheEntry;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hazelcast.instance.EndpointQualifier.MEMCACHE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MemcachedRawTcpTest extends HazelcastTestSupport {
    private final Pattern valueHeaderPattern = Pattern.compile("^VALUE (\\S+) (\\d+) (\\d+)$");
    private static final String CRLF = "\r\n";

    private HazelcastInstance instance;
    private Socket clientSocket;
    private PrintWriter writer;
    private Scanner scanner;

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
        scanner.useDelimiter(CRLF);
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

    @Test
    public void testSetAndGet() throws Exception {
        sendLine("set key 123 0 5");
        sendLine("value");
        assertEquals("STORED", receiveLine());

        sendLine("get key");
        assertEquals("VALUE key 123 5", receiveLine());
        assertEquals("value", receiveLine());
        assertEquals("END", receiveLine());

        checkStats(1, 1, 1, 0, 0, 0, 0, 0, 0, 0);
    }

    @Test
    public void testAddAndGet() throws Exception {
        sendLine("set key 123 0 3");
        sendLine("foo");
        assertEquals("STORED", receiveLine());

        sendLine("add key 45 0 3");
        sendLine("bar");
        assertEquals("NOT_STORED", receiveLine());

        sendLine("get key");
        assertEquals("VALUE key 123 3", receiveLine());
        assertEquals("foo", receiveLine());
        assertEquals("END", receiveLine());

        sendLine("set key2 45 0 3");
        sendLine("bar");
        assertEquals("STORED", receiveLine());

        sendLine("get key2");
        assertEquals("VALUE key2 45 3", receiveLine());
        assertEquals("bar", receiveLine());
        assertEquals("END", receiveLine());

        checkStats(3, 2, 2, 0, 0, 0, 0, 0, 0, 0);
    }

    @Test
    public void testReplace() throws Exception {
        sendLine("replace key 42 0 3");
        sendLine("bar");
        assertEquals("NOT_STORED", receiveLine());

        sendLine("get key");
        assertEquals("END", receiveLine());

        sendLine("set key 0 0 3");
        sendLine("foo");
        assertEquals("STORED", receiveLine());

        sendLine("replace key 42 0 3");
        sendLine("bar");
        assertEquals("STORED", receiveLine());

        sendLine("get key");
        assertEquals("VALUE key 42 3", receiveLine());
        assertEquals("bar", receiveLine());
        assertEquals("END", receiveLine());

        checkStats(3, 2, 1, 1, 0, 0, 0, 0, 0, 0);
    }

    @Test
    public void testDelete() throws Exception {
        sendLine("delete key");
        assertEquals("NOT_FOUND", receiveLine());

        sendLine("set key 0 0 3");
        sendLine("foo");
        assertEquals("STORED", receiveLine());

        sendLine("delete key");
        assertEquals("DELETED", receiveLine());

        sendLine("get key");
        assertEquals("END", receiveLine());

        checkStats(1, 1, 0, 1, 1, 1, 0, 0, 0, 0);
    }

    @Test
    public void testBulkGet() throws Exception {
        sendLine("get k0 k1 k2 k3 k4 k5 k6 k7 k8 k9");
        assertEquals("END", receiveLine());

        Map<String, String> entries = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            sendLine("set k" + i + " " + i + " 0 6");
            sendLine("value" + i);
            assertEquals("STORED", receiveLine());

            entries.put("k" + i, "value" + i);
        }

        sendLine("get k0 k1 k2 k3 k4 k5 k6 k7 k8 k9");
        for (int i = 0; i < 10; i++) {
            Matcher matcher = valueHeaderPattern.matcher(receiveLine());
            assertTrue(matcher.matches());

            String key = matcher.group(1);
            int flags = Integer.parseInt(matcher.group(2));
            int valueLength = Integer.parseInt(matcher.group(3));

            String value = receiveLine();

            assertEquals(key.charAt(1) - '0', flags);
            assertEquals(valueLength, value.length());

            assertEquals(entries.get(key), value);

            // remove the entry to not count again
            entries.remove(key);
        }
        assertEquals("END", receiveLine());

        checkStats(10, 20, 10, 10, 0, 0, 0, 0, 0, 0);
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

        sendLine(String.format("get %s", prefix + key));
        assertEquals(String.format("VALUE %s 0 %d", prefix + key, value.length()), receiveLine());
        assertEquals(value, receiveLine());
        assertEquals("END", receiveLine());

        sendLine(String.format("set %s 123 0 %d", prefix + key, value2.length()));
        sendLine(value2);
        assertEquals("STORED", receiveLine());

        MemcacheEntry memcacheEntry = (MemcacheEntry) map.get(key);
        MemcacheEntry expectedEntry = new MemcacheEntry(prefix + key, value2.getBytes(), 123);
        assertEquals(expectedEntry, memcacheEntry);
        assertEquals(prefix + key, memcacheEntry.getKey());

        sendLine(String.format("delete %s", prefix + key));
        assertEquals("DELETED", receiveLine());
        assertNull(map.get(key));
    }

    @Test
    public void testIncrement() throws Exception {
        sendLine("incr key 1");
        assertEquals("NOT_FOUND", receiveLine());

        sendLine("get key");
        assertEquals("END", receiveLine());

        sendLine("set key 0 0 2");
        sendLine("42");
        assertEquals("STORED", receiveLine());

        sendLine("incr key 153");
        assertEquals("195", receiveLine());

        sendLine("get key");
        assertEquals("VALUE key 0 3", receiveLine());
        assertEquals("195", receiveLine());
        assertEquals("END", receiveLine());

        checkStats(1, 2, 1, 1, 0, 0, 1, 1, 0, 0);
    }

    @Test
    public void testIncrement_byLargerThanLongMax() throws Exception {
        sendLine("set key 0 0 2");
        sendLine("42");
        assertEquals("STORED", receiveLine());

        sendLine("incr key 18446744073709551573"); // 2^64 - 1 - 42
        assertEquals("18446744073709551615", receiveLine()); // 2^64 - 1
    }

    @Test
    public void testIncrement_byNegativeNumber_shouldFail() throws Exception {
        sendLine("set key 0 0 2");
        sendLine("42");
        assertEquals("STORED", receiveLine());

        sendLine("incr key -10");
        assertTrue(receiveLine().startsWith("CLIENT_ERROR"));
    }

    @Test
    public void testIncrement_onNegativeNumber_shouldFail() throws Exception {
        sendLine("set key 0 0 3");
        sendLine("-42");
        assertEquals("STORED", receiveLine());

        sendLine("incr key 1");
        assertTrue(receiveLine().startsWith("CLIENT_ERROR"));
    }

    @Test
    public void testIncrement_onNonDecimal_shouldFail() throws Exception {
        sendLine("set key 0 0 3");
        sendLine("foo");
        assertEquals("STORED", receiveLine());

        sendLine("incr key 1");
        assertTrue(receiveLine().startsWith("CLIENT_ERROR"));
    }

    @Test
    public void testIncrement_onEmptyValue_shouldFail() throws Exception {
        sendLine("set key 0 0 0");
        sendLine("");
        assertEquals("STORED", receiveLine());

        sendLine("incr key 1");
        assertTrue(receiveLine().startsWith("CLIENT_ERROR"));
    }

    @Test
    public void testIncrement_onLargerThanLongMax() throws Exception {
        sendLine("set key 0 0 20");
        sendLine("18446744073709551614"); // 2^64 - 2
        assertEquals("STORED", receiveLine());

        sendLine("incr key 1");
        assertEquals("18446744073709551615", receiveLine());

        sendLine("get key");
        assertEquals("VALUE key 0 20", receiveLine());
        assertEquals("18446744073709551615", receiveLine());
        assertEquals("END", receiveLine());
    }

    @Test
    public void testIncrement_withOverFlow() throws Exception {
        sendLine("set key 0 0 20");
        sendLine("18446744073709551615"); // 2^64 - 1
        assertEquals("STORED", receiveLine());

        sendLine("incr key 1");
        assertEquals("0", receiveLine()); // should wrap back to 0
    }


    @Test
    public void testDecrement() throws Exception {
        sendLine("decr key 1");
        assertEquals("NOT_FOUND", receiveLine());

        sendLine("get key");
        assertEquals("END", receiveLine());

        sendLine("set key 0 0 2");
        sendLine("42");
        assertEquals("STORED", receiveLine());

        sendLine("decr key 33");
        assertEquals("9", receiveLine());

        sendLine("get key");
        // decr operations that shorten a number's length are not guaranteed to update the length
        assertTrue(receiveLine().startsWith("VALUE key 0 "));
        assertTrue(receiveLine().matches("9 *"));
        assertEquals("END", receiveLine());

        checkStats(1, 2, 1, 1, 0, 0, 0, 0, 1, 1);
    }

    @Test
    public void testDecrement_byNegativeNumber_shouldFail() throws Exception {
        sendLine("set key 0 0 2");
        sendLine("42");
        assertEquals("STORED", receiveLine());

        sendLine("decr key -10");
        assertTrue(receiveLine().startsWith("CLIENT_ERROR"));
    }

    @Test
    public void testDecrement_onNegativeNumber_shouldFail() throws Exception {
        sendLine("set key 0 0 3");
        sendLine("-42");
        assertEquals("STORED", receiveLine());

        sendLine("decr key 1");
        assertTrue(receiveLine().startsWith("CLIENT_ERROR"));
    }

    @Test
    public void testDecrement_onNonDecimal_shouldFail() throws Exception {
        sendLine("set key 0 0 3");
        sendLine("foo");
        assertEquals("STORED", receiveLine());

        sendLine("decr key 1");
        assertTrue(receiveLine().startsWith("CLIENT_ERROR"));
    }

    @Test
    public void testDecrement_onLargerThanLongMax() throws Exception {
        sendLine("set key 0 0 20");
        sendLine("18446744073709551615"); // 2^64 - 1
        assertEquals("STORED", receiveLine());

        sendLine("decr key 1");
        assertEquals("18446744073709551614", receiveLine());

        sendLine("get key");
        assertEquals("VALUE key 0 20", receiveLine());
        assertEquals("18446744073709551614", receiveLine());
        assertEquals("END", receiveLine());
    }

    @Test
    public void testDecrement_withUnderFlow() throws Exception {
        sendLine("set key 0 0 2");
        sendLine("42");
        assertEquals("STORED", receiveLine());

        sendLine("decr key 123");
        assertEquals("0", receiveLine());

        sendLine("get key");
        assertTrue(receiveLine().startsWith("VALUE key 0 "));
        assertTrue(receiveLine().matches("0 *"));
        assertEquals("END", receiveLine());
    }

    @Test
    public void testAppend() throws Exception {
        sendLine("append key 0 0 3");
        sendLine("bar");
        assertEquals("NOT_STORED", receiveLine());

        sendLine("set key 0 0 3");
        sendLine("foo");
        assertEquals("STORED", receiveLine());

        sendLine("append key 0 0 3");
        sendLine("bar");
        assertEquals("STORED", receiveLine());

        sendLine("get key 0 0 3");
        assertEquals("VALUE key 0 6", receiveLine());
        assertEquals("foobar", receiveLine());
        assertEquals("END", receiveLine());
    }

    @Test
    public void testPrepend() throws Exception {
        sendLine("prepend key 0 0 3");
        sendLine("bar");
        assertEquals("NOT_STORED", receiveLine());

        sendLine("set key 0 0 3");
        sendLine("foo");
        assertEquals("STORED", receiveLine());

        sendLine("prepend key 0 0 3");
        sendLine("bar");
        assertEquals("STORED", receiveLine());

        sendLine("get key 0 0 3");
        assertEquals("VALUE key 0 6", receiveLine());
        assertEquals("barfoo", receiveLine());
        assertEquals("END", receiveLine());
    }

    @Test
    public void testExpiration() throws Exception {
        sendLine("set key 0 3 3");
        sendLine("foo");
        assertEquals("STORED", receiveLine());

        assertTrueEventually(() -> {
            sendLine("get key");
            if (!receiveLine().equals("END")) {
                receiveLine(); // foo
                receiveLine(); // END
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

        sendLine("set key 0 0 " + value.length());
        sendLine(value.toString());
        assertEquals("STORED", receiveLine());

        sendLine("get key");
        assertEquals("VALUE key 0 " + value.length(), receiveLine());
        assertEquals(value.toString(), receiveLine());
    }

    @Test
    public void testBulkSetGet_withManyKeys() throws Exception {
        int numberOfKeys = 1000;

        Map<String, String> entries = new HashMap<>();
        for (int i = 0; i < numberOfKeys; i++) {
            String key = "key" + i;
            String value = "value" + i;
            entries.put(key, value);

            sendLine(String.format("set %s 0 0 %d", key, value.length()));
            sendLine(value);
            assertEquals("STORED", receiveLine());
        }

        sendLine("get " + String.join(" ", entries.keySet()));

        for (int i = 0; i < numberOfKeys; i++) {
            Matcher matcher = valueHeaderPattern.matcher(receiveLine());
            assertTrue(matcher.matches());

            String key = matcher.group(1);
            int flags = Integer.parseInt(matcher.group(2));
            int length = Integer.parseInt(matcher.group(3));

            assertEquals(0, flags);

            String value = receiveLine();
            assertEquals(length, value.length());
            assertEquals(entries.get(key), value);

            // remove the entry to not count it again
            entries.remove(key);
        }
        assertEquals("END", receiveLine());
    }

    /**
     * Sends a single request line to the server.
     * Adds CRLF at the end and flushes.
     * @param line line to send
     */
    private void sendLine(String line) {
        writer.write(line + CRLF);
        writer.flush();
    }

    /**
     * Receives a response line from the server
     */
    private String receiveLine() throws Exception {
        if (scanner.hasNextLine()) {
            return scanner.nextLine();
        } else {
            return null;
        }
    }

    @SuppressWarnings("checkstyle:parameternumber")
    private void checkStats(int sets, int gets, int getHits, int getMisses, int deleteHits, int deleteMisses,
                            int incHits, int incMisses, int decHits, int decMisses) throws Exception {
        sendLine("stats");

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
        while ((line = receiveLine()) != null && !line.equals("END")) {
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
