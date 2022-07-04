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

package com.hazelcast.console;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for demo console application.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class ConsoleAppTest extends HazelcastTestSupport {

    private static ByteArrayOutputStream baos;
    private static PrintStream printStream;
    private ConsoleApp consoleApp;
    private HazelcastInstance hz;

    @BeforeClass
    public static void beforeClass() {
        baos = new ByteArrayOutputStream();
        try {
            printStream = new PrintStream(baos, true, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            // Should never happen for the UTF-8
        }
    }

    @Before
    public void before() {
        hz = createHazelcastInstance();
        consoleApp = new ConsoleApp(hz, printStream);
        resetSystemOut();
    }

    @Test
    public void executeOnKey() {
        for (int i = 0; i < 100; i++) {
            consoleApp.handleCommand(String.format("executeOnKey message%d key%d", i, i));
            assertTextInSystemOut("message" + i);
        }
    }

    /**
     * Tests m.put operation.
     */
    @Test
    public void mapPut() {
        IMap<String, String> map = hz.getMap("default");
        assertEquals("Unexpected map size", 0, map.size());

        consoleApp.handleCommand("m.put putTestKey testValue");
        assertTextInSystemOut("null"); // original value for the key
        assertEquals("Unexpected map size", 1, map.size());
        assertThat(map.get("putTestKey"), CoreMatchers.containsString("testValue"));

        consoleApp.handleCommand("m.put putTestKey testXValue");
        assertTextInSystemOut("testValue"); // original value for the key
        assertThat(map.get("putTestKey"), CoreMatchers.containsString("testXValue"));
        consoleApp.handleCommand("m.put putTestKey2 testValue");
        assertEquals("Unexpected map size", 2, map.size());
    }

    /**
     * Tests m.remove operation.
     */
    @Test
    public void mapRemove() {
        IMap<String, String> map = hz.getMap("default");
        map.put("a", "valueOfA");
        map.put("b", "valueOfB");
        resetSystemOut();
        consoleApp.handleCommand("m.remove b");
        assertTextInSystemOut("valueOfB"); // original value for the key
        assertEquals("Unexpected map size", 1, map.size());
        assertFalse("Unexpected entry in the map", map.containsKey("b"));
    }

    /**
     * Tests m.delete operation.
     */
    @Test
    public void mapDelete() {
        IMap<String, String> map = hz.getMap("default");
        map.put("a", "valueOfA");
        map.put("b", "valueOfB");
        resetSystemOut();
        consoleApp.handleCommand("m.delete b");
        assertTextInSystemOut("true"); // result of successful operation
        assertEquals("Unexpected map size", 1, map.size());
        assertFalse("Unexpected entry in the map", map.containsKey("b"));
    }

    /**
     * Tests m.get operation.
     */
    @Test
    public void mapGet() {
        hz.<String, String>getMap("default").put("testGetKey", "testGetValue");
        consoleApp.handleCommand("m.get testGetKey");
        assertTextInSystemOut("testGetValue");
    }

    /**
     * Tests m.putmany operation.
     */
    @Test
    public void mapPutMany() {
        IMap<String, ?> map = hz.getMap("default");
        consoleApp.handleCommand("m.putmany 100 8 1000");
        assertEquals("Unexpected map size", 100, map.size());
        assertFalse(map.containsKey("key999"));
        assertTrue(map.containsKey("key1000"));
        assertTrue(map.containsKey("key1099"));
        assertFalse(map.containsKey("key1100"));
        assertEquals(8, ((byte[]) map.get("key1050")).length);
    }

    /**
     * Asserts that given substring in standard output buffer. Calling this method resets the buffer.
     *
     * @param substring a substring that asserted to be in stdout buffer
     */
    private void assertTextInSystemOut(String substring) {
        assertThat(resetSystemOut(), CoreMatchers.containsString(substring));
    }

    /**
     * Gets content of standard output buffer.
     */
    private static String getSystemOut() {
        return new String(baos.toByteArray(), StandardCharsets.UTF_8);
    }

    /**
     * Clears standard output buffer.
     *
     * @return original content of the standard output
     */
    private static String resetSystemOut() {
        final String result = getSystemOut();
        baos.reset();
        return result;
    }
}
