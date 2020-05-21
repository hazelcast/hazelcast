/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for demo console application.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class ConsoleAppTest extends HazelcastTestSupport {
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    private HazelcastInstance hazelcast;
    private ByteArrayOutputStream applicationOutput;
    private ConsoleApp consoleApp;

    @Before
    public void before() {
        applicationOutput = new ByteArrayOutputStream();
        PrintStream systemOutput = createAppConsole();
        hazelcast = createHazelcastInstance();
        consoleApp = new ConsoleApp(hazelcast, systemOutput);
    }

    private PrintStream createAppConsole() {
        try {
            return new PrintStream(applicationOutput, true, "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Test
    public void namespaceWithDoubledUnderscoreAtTheBeginning() {
        consoleApp.handleCommand("__test_ns__m.putmany 10000");
        assertThat(hazelcast.getMap("__test_ns").size(), is(not(0)));
    }

    @Test
    public void namespaceWithDoubledUnderscoreAnTheEnd() {
        consoleApp.handleCommand("__test_ns____m.putmany 10000");
        assertThat(hazelcast.getMap("__test_ns__").size(), is(not(0)));
    }

    @Test
    public void settingEchoMode() {
        consoleApp.handleCommand("echo true");
        assertTextInApplicationSystemOut("echo: true");
    }

    @Test
    public void mistypedValueWhenSettingEcho_setItToFalse() {
        consoleApp.handleCommand("echo trye");
        assertTextInApplicationSystemOut("echo: false");
    }

    @Test
    public void printingHelp() {
        consoleApp.handleCommand("help");
        assertTextInApplicationSystemOut(HELP_MESSAGE);
    }

    @Test
    public void emptyFileReference() {
        consoleApp.handleCommand("@");
        assertTextInApplicationSystemOut("usage: @<file-name>");
    }

    @Test
    public void scriptFile() throws Exception {
        String fileName = "script";
        File script = folder.newFile(fileName);
        new FileWriter(script).append("echo true").close();
        consoleApp.handleCommand("@"+script.getAbsolutePath());
        assertTextInApplicationSystemOut("echo: true");
    }

    @Test
    public void executeOnKey() {
        for (int i = 0; i < 100; i++) {
            consoleApp.handleCommand(String.format("executeOnKey message%d key%d", i, i));
            assertTextInApplicationSystemOut("message" + i);
        }
    }

    /**
     * Tests m.put operation.
     */
    @Test
    public void mapPut() {
        IMap<String, String> map = hazelcast.getMap("default");

        assertEquals("Unexpected map size", 0, map.size());

        consoleApp.handleCommand("m.put putTestKey testValue");
        assertTextInApplicationSystemOut("null"); // original value for the key
        assertEquals("Unexpected map size", 1, map.size());
        assertThat(map.get("putTestKey"), CoreMatchers.containsString("testValue"));

        consoleApp.handleCommand("m.put putTestKey testXValue");
        assertTextInApplicationSystemOut("testValue"); // original value for the key
        assertThat(map.get("putTestKey"), CoreMatchers.containsString("testXValue"));
        consoleApp.handleCommand("m.put putTestKey2 testValue");
        assertEquals("Unexpected map size", 2, map.size());
    }

    /**
     * Tests m.remove operation.
     */
    @Test
    public void mapRemove() {
        IMap<String, String> map = hazelcast.getMap("default");
        map.put("a", "valueOfA");
        map.put("b", "valueOfB");
        resetSystemOut();
        consoleApp.handleCommand("m.remove b");
        assertTextInApplicationSystemOut("valueOfB"); // original value for the key
        assertEquals("Unexpected map size", 1, map.size());
        assertFalse("Unexpected entry in the map", map.containsKey("b"));
    }

    /**
     * Tests m.delete operation.
     */
    @Test
    public void mapDelete() {
        IMap<String, String> map = hazelcast.getMap("default");
        map.put("a", "valueOfA");
        map.put("b", "valueOfB");
        resetSystemOut();
        consoleApp.handleCommand("m.delete b");
        assertTextInApplicationSystemOut("true"); // result of successful operation
        assertEquals("Unexpected map size", 1, map.size());
        assertFalse("Unexpected entry in the map", map.containsKey("b"));
    }

    /**
     * Tests m.get operation.
     */
    @Test
    public void mapGet() {
        hazelcast.<String, String>getMap("default").put("testGetKey", "testGetValue");
        consoleApp.handleCommand("m.get testGetKey");
        assertTextInApplicationSystemOut("testGetValue");
    }

    /**
     * Tests m.putmany operation.
     */
    @Test
    public void mapPutMany() {
        IMap<String, ?> map = hazelcast.getMap("default");
        consoleApp.handleCommand("m.putmany 100 8 1000");
        assertEquals("Unexpected map size", 100, map.size());
        assertFalse(map.containsKey("key999"));
        assertTrue(map.containsKey("key1000"));
        assertTrue(map.containsKey("key1099"));
        assertFalse(map.containsKey("key1100"));
        assertEquals(8, ((byte[]) map.get("key1050")).length);
    }

    private void assertTextInApplicationSystemOut(String substring) {
        assertThat(resetSystemOut(), CoreMatchers.containsString(substring));
    }

    private String getApplicationOutput() {
        return new String(applicationOutput.toByteArray(), StandardCharsets.UTF_8);
    }

    private String resetSystemOut() {
        final String result = getApplicationOutput();
        applicationOutput.reset();
        return result;
    }

    // @formatter:off
    private static final String HELP_MESSAGE =
            "Commands:\n" +
            "-- General commands\n" +
            "echo true|false                      //turns on/off echo of commands (default false)\n" +
            "silent true|false                    //turns on/off silent of command output (default false)\n" +
            "#<number> <command>                  //repeats <number> time <command>, replace $i in <command> with current iteration (0..<number-1>)\n" +
            "&<number> <command>                  //forks <number> threads to execute <command>, replace $t in <command> with current thread number (0..<number-1>\n" +
            "     When using #x or &x, is is advised to use silent true as well.\n" +
            "     When using &x with m.putmany and m.removemany, each thread will get a different share of keys unless a start key index is specified\n" +
            "jvm                                  //displays info about the runtime\n" +
            "who                                  //displays info about the cluster\n" +
            "whoami                               //displays info about this cluster member\n" +
            "ns <string>                          //switch the namespace for using the distributed data structure name  <string> (e.g. queue/map/set/list name; defaults to \"default\")\n" +
            "@<file>                              //executes the given <file> script. Use '//' for comments in the script\n" +
            "\n" +
            "-- Queue commands\n" +
            "q.offer <string>                     //adds a string object to the queue\n" +
            "q.poll                               //takes an object from the queue\n" +
            "q.offermany <number> [<size>]        //adds indicated number of string objects to the queue ('obj<i>' or byte[<size>]) \n" +
            "q.pollmany <number>                  //takes indicated number of objects from the queue\n" +
            "q.iterator [remove]                  //iterates the queue, remove if specified\n" +
            "q.size                               //size of the queue\n" +
            "q.clear                              //clears the queue\n" +
            "\n" +
            "-- Set commands\n" +
            "s.add <string>                       //adds a string object to the set\n" +
            "s.remove <string>                    //removes the string object from the set\n" +
            "s.addmany <number>                   //adds indicated number of string objects to the set ('obj<i>')\n" +
            "s.removemany <number>                //takes indicated number of objects from the set\n" +
            "s.iterator [remove]                  //iterates the set, removes if specified\n" +
            "s.size                               //size of the set\n" +
            "s.clear                              //clears the set\n" +
            "\n" +
            "-- Lock commands\n" +
            "lock <key>                           //same as Hazelcast.getCPSubsystem().getLock(key).lock()\n" +
            "tryLock <key>                        //same as Hazelcast.getCPSubsystem().getLock(key).tryLock()\n" +
            "tryLock <key> <time>                 //same as tryLock <key> with timeout in seconds\n" +
            "unlock <key>                         //same as Hazelcast.getCPSubsystem().getLock(key).unlock()\n" +
            "\n" +
            "-- Map commands\n" +
            "m.put <key> <value>                  //puts an entry to the map\n" +
            "m.remove <key>                       //removes the entry of given key from the map\n" +
            "m.get <key>                          //returns the value of given key from the map\n" +
            "m.putmany <number> [<size>] [<index>]//puts indicated number of entries to the map ('key<i>':byte[<size>], <index>+(0..<number>)\n" +
            "m.removemany <number> [<index>]      //removes indicated number of entries from the map ('key<i>', <index>+(0..<number>)\n" +
            "     When using &x with m.putmany and m.removemany, each thread will get a different share of keys unless a start key <index> is specified\n" +
            "m.keys                               //iterates the keys of the map\n" +
            "m.values                             //iterates the values of the map\n" +
            "m.entries                            //iterates the entries of the map\n" +
            "m.iterator [remove]                  //iterates the keys of the map, remove if specified\n" +
            "m.size                               //size of the map\n" +
            "m.localSize                          //local size of the map\n" +
            "m.clear                              //clears the map\n" +
            "m.destroy                            //destroys the map\n" +
            "m.lock <key>                         //locks the key\n" +
            "m.tryLock <key>                      //tries to lock the key and returns immediately\n" +
            "m.tryLock <key> <time>               //tries to lock the key within given seconds\n" +
            "m.unlock <key>                       //unlocks the key\n" +
            "m.stats                              //shows the local stats of the map\n" +
            "\n" +
            "-- MultiMap commands\n" +
            "mm.put <key> <value>                  //puts an entry to the multimap\n" +
            "mm.get <key>                          //returns the value of given key from the multimap\n" +
            "mm.remove <key>                       //removes the entry of given key from the multimap\n" +
            "mm.size                               //size of the multimap\n" +
            "mm.clear                              //clears the multimap\n" +
            "mm.destroy                            //destroys the multimap\n" +
            "mm.iterator [remove]                  //iterates the keys of the multimap, remove if specified\n" +
            "mm.keys                               //iterates the keys of the multimap\n" +
            "mm.values                             //iterates the values of the multimap\n" +
            "mm.entries                            //iterates the entries of the multimap\n" +
            "mm.lock <key>                         //locks the key\n" +
            "mm.tryLock <key>                      //tries to lock the key and returns immediately\n" +
            "mm.tryLock <key> <time>               //tries to lock the key within given seconds\n" +
            "mm.unlock <key>                       //unlocks the key\n" +
            "mm.stats                              //shows the local stats of the multimap\n" +
            "\n" +
            "-- List commands:\n" +
            "l.add <string>                        //adds a string object to the list\n" +
            "l.add <index> <string>                //adds a string object as an item with given index in the list\n" +
            "l.contains <string>                   //checks if the list contains a string object\n" +
            "l.remove <string>                     //removes a string object from the list\n" +
            "l.remove <index>                      //removes the item with given index from the list\n" +
            "l.set <index> <string>                //sets a string object to the item with given index in the list\n" +
            "l.iterator [remove]                   //iterates the list, remove if specified\n" +
            "l.size                                //size of the list\n" +
            "l.clear                               //clears the list\n" +
            "\n" +
            "-- IAtomicLong commands:\n" +
            "a.get                                 //returns the value of the atomic long\n" +
            "a.set <long>                          //sets a value to the atomic long\n" +
            "a.inc                                 //increments the value of the atomic long by one\n" +
            "a.dec                                 //decrements the value of the atomic long by one\n" +
            "\n" +
            "-- Executor Service commands:\n" +
            "execute <echo-input>                                     //executes an echo task on random member\n" +
            "executeOnKey <echo-input> <key>                          //executes an echo task on the member that owns the given key\n" +
            "executeOnMember <echo-input> <memberIndex>               //executes an echo task on the member with given index\n" +
            "executeOnMembers <echo-input>                            //executes an echo task on all of the members\n" +
            "e<threadcount>.simulateLoad <task-count> <delaySeconds>  //simulates load on executor with given number of thread (e1..e16)\n" +
            "\n";
    // @formatter:on
}
