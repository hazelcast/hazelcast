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

package com.hazelcast.client.atomicreference;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.IFunction;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientAtomicReferenceTest {

    static final String name = "test1";
    static HazelcastInstance client;
    static HazelcastInstance server;
    static IAtomicReference<String> clientReference;
    static IAtomicReference<String> serverReference;

    @BeforeClass
    public static void init() {
        server = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
        clientReference = client.getAtomicReference(name);
        serverReference = server.getAtomicReference(name);
    }

    @AfterClass
    public static void destroy() {
        client.shutdown();
        Hazelcast.shutdownAll();
    }

    @Before
    @After
    public void after() {
        serverReference.set(null);
    }

    @Test
    public void get() throws Exception {
        assertNull(clientReference.get());

        serverReference.set("foo");

        assertEquals("foo", clientReference.get());
    }

    @Test
    public void isNull() throws Exception {
        assertTrue(clientReference.isNull());

        serverReference.set("foo");
        assertFalse(clientReference.isNull());
    }

    @Test
    public void contains() {
        assertTrue(clientReference.contains(null));
        assertFalse(clientReference.contains("foo"));

        serverReference.set("foo");

        assertFalse(clientReference.contains(null));
        assertTrue(clientReference.contains("foo"));
        assertFalse(clientReference.contains("bar"));
    }

    @Test
    public void set() throws Exception {
        clientReference.set(null);
        assertTrue(serverReference.isNull());

        clientReference.set("foo");
        assertEquals("foo", serverReference.get());

        clientReference.set("foo");
        assertEquals("foo", serverReference.get());

        clientReference.set("bar");
        assertEquals("bar", serverReference.get());

        clientReference.set(null);
        assertTrue(serverReference.isNull());
    }

    @Test
    public void clear() throws Exception {
        clientReference.clear();
        assertTrue(serverReference.isNull());

        serverReference.set("foo");
        clientReference.clear();
        assertTrue(serverReference.isNull());
    }

    @Test
    public void getAndSet() throws Exception {
        assertNull(clientReference.getAndSet(null));
        assertTrue(serverReference.isNull());

        assertNull(clientReference.getAndSet("foo"));
        assertEquals("foo", serverReference.get());

        assertEquals("foo", clientReference.getAndSet("foo"));
        assertEquals("foo", serverReference.get());

        assertEquals("foo", clientReference.getAndSet("bar"));
        assertEquals("bar", serverReference.get());

        assertEquals("bar", clientReference.getAndSet(null));
        assertTrue(serverReference.isNull());
    }


    @Test
    public void setAndGet() throws Exception {
        assertNull(clientReference.setAndGet(null));
        assertTrue(serverReference.isNull());

        assertEquals("foo", clientReference.setAndGet("foo"));
        assertEquals("foo", serverReference.get());

        assertEquals("foo", clientReference.setAndGet("foo"));
        assertEquals("foo", serverReference.get());

        assertEquals("bar", clientReference.setAndGet("bar"));
        assertEquals("bar", serverReference.get());

        assertNull(clientReference.setAndGet(null));
        assertTrue(serverReference.isNull());
    }

    @Test
    public void compareAndSet() throws Exception {
        assertTrue(clientReference.compareAndSet(null, null));
        assertTrue(serverReference.isNull());

        assertFalse(clientReference.compareAndSet("foo", null));
        assertTrue(serverReference.isNull());

        assertTrue(clientReference.compareAndSet(null, "foo"));
        assertEquals("foo", serverReference.get());

        assertTrue(clientReference.compareAndSet("foo", "foo"));
        assertEquals("foo", serverReference.get());

        assertFalse(clientReference.compareAndSet("bar", "foo"));
        assertEquals("foo", serverReference.get());

        assertTrue(clientReference.compareAndSet("foo", "bar"));
        assertEquals("bar", serverReference.get());

        assertTrue(clientReference.compareAndSet("bar", null));
        assertNull(serverReference.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void apply_whenCalledWithNullFunction() {
        clientReference.apply(null);
    }

    @Test
    public void apply() {
        assertEquals("null",clientReference.apply(new AppendFunction("")));
        assertEquals(null,clientReference.get());

        clientReference.set("foo");
        assertEquals("foobar", clientReference.apply(new AppendFunction("bar")));
        assertEquals("foo",clientReference.get());

        assertEquals(null, clientReference.apply(new NullFunction()));
        assertEquals("foo",clientReference.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void alter_whenCalledWithNullFunction() {
        clientReference.alter(null);
    }

    @Test
    public void alter() {
        clientReference.alter(new NullFunction());
        assertEquals(null,clientReference.get());

        clientReference.set("foo");
        clientReference.alter(new AppendFunction("bar"));
        assertEquals("foobar",clientReference.get());

        clientReference.alter(new NullFunction());
        assertEquals(null,clientReference.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void alterAndGet_whenCalledWithNullFunction() {
        clientReference.alterAndGet(null);
    }

    @Test
    public void alterAndGet() {
        assertNull(clientReference.alterAndGet(new NullFunction()));
        assertEquals(null,clientReference.get());

        clientReference.set("foo");
        assertEquals("foobar",clientReference.alterAndGet(new AppendFunction("bar")));
        assertEquals("foobar",clientReference.get());

        assertEquals(null,clientReference.alterAndGet(new NullFunction()));
        assertEquals(null,clientReference.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void getAndAlter_whenCalledWithNullFunction() {
        clientReference.alterAndGet(null);
    }

    @Test
    public void getAndAlter() {
        assertNull(clientReference.getAndAlter(new NullFunction()));
        assertEquals(null,clientReference.get());

        clientReference.set("foo");
        assertEquals("foo",clientReference.getAndAlter(new AppendFunction("bar")));
        assertEquals("foobar",clientReference.get());

        assertEquals("foobar",clientReference.getAndAlter(new NullFunction()));
        assertEquals(null,clientReference.get());
    }

    private static class AppendFunction implements IFunction<String,String> {
        private String add;

        private AppendFunction(String add) {
            this.add = add;
        }

        @Override
        public String apply(String input) {
            return input+add;
        }
    }

    private static class NullFunction implements IFunction<String,String> {
        @Override
        public String apply(String input) {
            return null;
        }
    }
}
