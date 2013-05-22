package com.hazelcast.map;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.GroupProperties;
import org.junit.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

/**
 * @ali 5/22/13
 */
public class ClientMapTest {

    static final String name = "test";
    static HazelcastInstance hz;
    static HazelcastInstance server;
    static IMap map;

    @BeforeClass
    public static void init(){
        Config config = new Config();
        server = Hazelcast.newHazelcastInstance(config);
        hz = HazelcastClient.newHazelcastClient(null);
        map = hz.getMap(name);
    }

    @AfterClass
    public static void destroy() {
        hz.getLifecycleService().shutdown();
        Hazelcast.shutdownAll();
    }

    @Before
    @After
    public void clear() throws IOException {
        map.clear();
    }

    @Test
    public void testContains() throws Exception {

        fillMap();

        assertFalse(map.containsKey("key10"));
        assertTrue(map.containsKey("key1"));

        assertFalse(map.containsValue("value10"));
        assertTrue(map.containsValue("value1"));

    }

    @Test
    public void testGet(){
        fillMap();
        for (int i=0; i<10; i++){
            Object o = map.get("key"+i);
            assertEquals("value"+i,o);
        }
    }

    @Test
    public void testRemoveAndDelete(){
        fillMap();
        assertNull(map.remove("key10"));
        map.delete("key9");
        assertEquals(9, map.size());
        for (int i=0; i<9; i++){
            Object o = map.remove("key" + i);
            assertEquals("value"+i,o);
        }
        assertEquals(0, map.size());
    }

    @Test
    public void testRemoveIfSame(){
        fillMap();
        assertFalse(map.remove("key2", "value"));
        assertEquals(10, map.size());

        assertTrue(map.remove("key2", "value2"));
        assertEquals(9, map.size());
    }

    @Test
    public void flush(){
        //TODO map store
    }

    @Test
    public void getAll(){
        //TODO
    }

    @Test
    public void testAsyncGet() throws Exception{
        fillMap();
        Future f = map.getAsync("key1");
        Object o = null;
        try {
            o = f.get(0, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e){
        }
        assertNull(o);
        o = f.get();
        assertEquals("value1",o);
    }

    @Test
    public void testAsyncPut() throws Exception{
        fillMap();
        Future f = map.putAsync("key3", "value");

        Object o = null;
        try {
            o = f.get(0, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e){
        }
        assertNull(o);
        o = f.get();
        assertEquals("value3",o);
        assertEquals("value", map.get("key3"));
    }

    @Test
    public void testAsyncRemove() throws Exception {
        fillMap();
        Future f = map.removeAsync("key4");
        Object o = null;
        try {
            o = f.get(0, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e){
        }
        assertNull(o);
        o = f.get();
        assertEquals("value4",o);
        assertEquals(9, map.size());
    }

    @Test
    public void testTtyPutGetRemove() throws Exception {
        //TODO test after locks
    }

    @Test
    public void testPutTtl() throws Exception {
        map.put("key1","value1", 1, TimeUnit.SECONDS);
        assertNotNull(map.get("key1"));
        Thread.sleep(2000);
        assertNull(map.get("key1"));
    }

    @Test
    public void testPutIfAbsent() throws Exception {
        assertNull(map.putIfAbsent("key1","value1"));
        assertEquals("value1", map.putIfAbsent("key1","value3"));
    }

    @Test
    public void testPutIfAbsentTtl() throws Exception {
        assertNull(map.putIfAbsent("key1", "value1", 1, TimeUnit.SECONDS));
        assertEquals("value1", map.putIfAbsent("key1", "value3", 1, TimeUnit.SECONDS));
        Thread.sleep(2000);
        assertNull(map.putIfAbsent("key1", "value3", 1, TimeUnit.SECONDS));
        assertEquals("value3", map.putIfAbsent("key1", "value4", 1, TimeUnit.SECONDS));
    }

    @Test
    public void testSet() throws Exception {
        map.set("key1","value1");
        assertEquals("value1", map.get("key1"));

        map.set("key1","value2");
        assertEquals("value2", map.get("key1"));

        map.set("key1", "value3", 1, TimeUnit.SECONDS);
        assertEquals("value3", map.get("key1"));

        Thread.sleep(2000);
        assertNull(map.get("key1"));

    }

    @Test
    public void testPutTransient() throws Exception {
        //TODO mapstore
    }

    @Test
    public void testLock() throws Exception {
        map.put("key1", "value1");
        assertEquals("value1", map.get("key1"));
        map.lock("key1");
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(){
            public void run() {
                map.tryPut("key1","value2", 1, TimeUnit.SECONDS);
                latch.countDown();
            }
        }.start();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals("value1", map.get("key1"));
        map.forceUnlock("key1");
    }

    @Test
    public void testLockTtl() throws Exception {
        map.put("key1", "value1");
        assertEquals("value1", map.get("key1"));
        map.lock("key1", 2, TimeUnit.SECONDS);
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(){
            public void run() {
                map.tryPut("key1","value2", 5, TimeUnit.SECONDS);
                latch.countDown();
            }
        }.start();
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertFalse(map.isLocked("key1"));
        assertEquals("value2", map.get("key1"));
        map.forceUnlock("key1");
    }

    @Test
    public void testTryLock() throws Exception {
//        final IMap tempMap = server.getMap(name);
        final IMap tempMap = map;

        assertTrue(tempMap.tryLock("key1", 2, TimeUnit.SECONDS));
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(){
            public void run() {
                try {
                    if(!tempMap.tryLock("key1", 2, TimeUnit.SECONDS)){
                        latch.countDown();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        assertTrue(latch.await(100, TimeUnit.SECONDS));

        assertTrue(tempMap.isLocked("key1"));

        final CountDownLatch latch2 = new CountDownLatch(1);
        new Thread(){
            public void run() {
                try {
                    if(tempMap.tryLock("key1", 20, TimeUnit.SECONDS)){
                        latch2.countDown();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        Thread.sleep(1000);
        tempMap.unlock("key1");
        assertTrue(latch2.await(100, TimeUnit.SECONDS));
        assertTrue(tempMap.isLocked("key1"));
        tempMap.forceUnlock("key1");
    }

    @Test
    public void testForceUnlock() throws Exception {
        map.lock("key1");
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(){
            public void run() {
                map.forceUnlock("key1");
                latch.countDown();
            }
        }.start();
        assertTrue(latch.await(100, TimeUnit.SECONDS));
        assertFalse(map.isLocked("key1"));
    }

    @Test
    public void testReplace() throws Exception {
        assertNull(map.replace("key1","value1"));
        map.put("key1","value1");
        assertEquals("value1", map.replace("key1","value2"));
        assertEquals("value2", map.get("key1"));

        assertFalse(map.replace("key1", "value1", "value3"));
        assertEquals("value2", map.get("key1"));
        assertTrue(map.replace("key1", "value2", "value3"));
        assertEquals("value3", map.get("key1"));
    }

    private void fillMap(){
        for (int i=0; i<10; i++){
            map.put("key" + i, "value" + i);
        }
    }

    static {
        System.setProperty("hazelcast.version.check.enabled", "false");
        System.setProperty("hazelcast.socket.bind.any", "false");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
//        System.setProperty("java.net.preferIPv6Addresses", "true");
//        System.setProperty("hazelcast.prefer.ipv4.stack", "false");
    }

}
