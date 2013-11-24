package com.hazelcast.client.atomicreference;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.annotation.SerialTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.*;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(SerialTest.class)
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
        client.getLifecycleService().shutdown();
        Hazelcast.shutdownAll();
    }

    @Before
    @After
    public void clear() {
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
    public void set() throws Exception {
        clientReference.set(null);
        assertTrue(serverReference.isNull());

        clientReference.set("foo");
        assertEquals("foo", serverReference.get());

        clientReference.set("foo");
        assertEquals("foo", serverReference.get());

        clientReference.set("bar");
        assertEquals("bar",serverReference.get());

        clientReference.set(null);
        assertTrue(serverReference.isNull());
    }

    @Test
    public void getAndSet() throws Exception {
        assertNull(clientReference.getAndSet(null));
        assertTrue(serverReference.isNull());

        assertNull(clientReference.getAndSet("foo"));
        assertEquals("foo", serverReference.get());

        assertEquals("foo",clientReference.getAndSet("foo"));
        assertEquals("foo", serverReference.get());

        assertEquals("foo", clientReference.getAndSet("bar"));
        assertEquals("bar",serverReference.get());

        assertEquals("bar",clientReference.getAndSet(null));
        assertTrue(serverReference.isNull());
    }


    @Test
    public void setAndGet() throws Exception {
        assertNull(clientReference.setAndGet(null));
        assertTrue(serverReference.isNull());

        assertEquals("foo",clientReference.setAndGet("foo"));
        assertEquals("foo", serverReference.get());

        assertEquals("foo",clientReference.setAndGet("foo"));
        assertEquals("foo", serverReference.get());

        assertEquals("bar", clientReference.setAndGet("bar"));
        assertEquals("bar",serverReference.get());

        assertNull(clientReference.setAndGet(null));
        assertTrue(serverReference.isNull());
    }

    @Test
    public void compareAndSet() throws Exception {
        assertTrue(clientReference.compareAndSet(null,null));
        assertTrue(serverReference.isNull());

        assertFalse(clientReference.compareAndSet("foo",null));
        assertTrue(serverReference.isNull());

        assertTrue(clientReference.compareAndSet(null,"foo"));
        assertEquals("foo",serverReference.get());

        assertTrue(clientReference.compareAndSet("foo","foo"));
        assertEquals("foo",serverReference.get());

        assertFalse(clientReference.compareAndSet("bar","foo"));
        assertEquals("foo",serverReference.get());

        assertTrue(clientReference.compareAndSet("foo","bar"));
        assertEquals("bar",serverReference.get());

        assertTrue(clientReference.compareAndSet("bar",null));
        assertNull(serverReference.get());
    }
}
