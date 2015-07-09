package com.hazelcast.client.ringbuffer;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.impl.client.PortableReadResultSet;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Future;

import static com.hazelcast.ringbuffer.OverflowPolicy.OVERWRITE;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class RingbufferTest extends HazelcastTestSupport{

    public static int CAPACITY = 10;

    static HazelcastInstance client;
    static HazelcastInstance server;
    private Ringbuffer<String> clientRingbuffer;
    private Ringbuffer<String> serverRingbuffer;

    @BeforeClass
    public static void init() {
        Config config = new Config();
        config.addRingBufferConfig(new RingbufferConfig("rb*").setCapacity(CAPACITY));

        server = Hazelcast.newHazelcastInstance(config);
        client = HazelcastClient.newHazelcastClient();
    }

    @Before
    public void setup() {
        String name = "rb-" + HazelcastTestSupport.randomString();
        clientRingbuffer = client.getRingbuffer(name);
        serverRingbuffer = server.getRingbuffer(name);
    }

    @AfterClass
    public static void destroy() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void headSequence() {
        for (int k = 0; k < 2 * CAPACITY; k++) {
            serverRingbuffer.add("foo");
        }

        assertEquals(serverRingbuffer.headSequence(), clientRingbuffer.headSequence());
    }

    @Test
    public void tailSequence() {
        for (int k = 0; k < 2 * CAPACITY; k++) {
            serverRingbuffer.add("foo");
        }

        assertEquals(serverRingbuffer.tailSequence(), clientRingbuffer.tailSequence());
    }

    @Test
    public void size() {
        serverRingbuffer.add("foo");

        assertEquals(serverRingbuffer.size(), clientRingbuffer.size());
    }

    @Test
    public void capacity() {
        assertEquals(serverRingbuffer.capacity(), clientRingbuffer.capacity());
    }

    @Test
    public void remainingCapacity() {
        serverRingbuffer.add("foo");

        assertEquals(serverRingbuffer.remainingCapacity(), clientRingbuffer.remainingCapacity());
    }

    @Test
    public void add() throws Exception {
        clientRingbuffer.add("foo");
        assertEquals("foo", serverRingbuffer.readOne(0));
    }

    @Test
    public void addAsync() throws Exception {
        Future<Long> f = clientRingbuffer.addAsync("foo", OVERWRITE);
        Long result = f.get();

        assertEquals(new Long(serverRingbuffer.headSequence()), result);
        assertEquals("foo", serverRingbuffer.readOne(0));
        assertEquals(0, serverRingbuffer.headSequence());
        assertEquals(0, serverRingbuffer.tailSequence());
    }

    @Test
    public void addAll() throws Exception {
        Future<Long> f = clientRingbuffer.addAllAsync(asList("foo", "bar"), OVERWRITE);
        Long result = f.get();

        assertEquals(new Long(serverRingbuffer.tailSequence()), result);
        assertEquals("foo", serverRingbuffer.readOne(0));
        assertEquals("bar", serverRingbuffer.readOne(1));
        assertEquals(0, serverRingbuffer.headSequence());
        assertEquals(1, serverRingbuffer.tailSequence());
    }

    @Test
    public void readOne() throws Exception {
        serverRingbuffer.add("foo");
        assertEquals("foo", clientRingbuffer.readOne(0));
    }

    @Test
    public void readManyAsync() throws Exception {
        serverRingbuffer.add("1");
        serverRingbuffer.add("2");
        serverRingbuffer.add("3");

        ICompletableFuture<ReadResultSet<String>> f = clientRingbuffer.readManyAsync(0, 3, 3, null);

        ReadResultSet rs = f.get();
        assertInstanceOf(PortableReadResultSet.class, rs);

        assertEquals(3, rs.readCount());
        assertEquals("1", rs.get(0));
        assertEquals("2", rs.get(1));
        assertEquals("3", rs.get(2));
    }
}
