package com.hazelcast.client.ringbuffer;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IFunction;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.impl.client.PortableReadResultSet;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Future;

import static com.hazelcast.ringbuffer.OverflowPolicy.OVERWRITE;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class RingbufferTest extends HazelcastTestSupport {

    public static int CAPACITY = 10;

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private Ringbuffer<String> clientRingbuffer;
    private Ringbuffer<String> serverRingbuffer;
    private HazelcastInstance server;
    private HazelcastInstance client;

    @Before
    public void setup() {
        Config config = new Config();
        config.addRingBufferConfig(new RingbufferConfig("rb*").setCapacity(CAPACITY));

        server = hazelcastFactory.newHazelcastInstance(config);
        client = hazelcastFactory.newHazelcastClient();

        String name = "rb-" + HazelcastTestSupport.randomString();
        clientRingbuffer = client.getRingbuffer(name);
        serverRingbuffer = server.getRingbuffer(name);
    }

    @After
    public void destroy() {
        hazelcastFactory.shutdownAll();
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
    public void readManyAsync_noFilter() throws Exception {
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

    // checks if the max count works. So if more results are available than needed, the surplus results should not be read.
    @Test
    public void readManyAsync_maxCount() throws Exception {
        serverRingbuffer.add("1");
        serverRingbuffer.add("2");
        serverRingbuffer.add("3");
        serverRingbuffer.add("4");
        serverRingbuffer.add("5");
        serverRingbuffer.add("6");

        ICompletableFuture<ReadResultSet<String>> f = clientRingbuffer.readManyAsync(0, 3, 3, null);

        ReadResultSet rs = f.get();
        assertInstanceOf(PortableReadResultSet.class, rs);

        assertEquals(3, rs.readCount());
        assertEquals("1", rs.get(0));
        assertEquals("2", rs.get(1));
        assertEquals("3", rs.get(2));
    }

    @Test
    public void readManyAsync_withFilter() throws Exception {
        serverRingbuffer.add("good1");
        serverRingbuffer.add("bad1");
        serverRingbuffer.add("good2");
        serverRingbuffer.add("bad2");
        serverRingbuffer.add("good3");
        serverRingbuffer.add("bad3");

        ICompletableFuture<ReadResultSet<String>> f = clientRingbuffer.readManyAsync(0, 3, 3, new Filter());

        ReadResultSet rs = f.get();
        assertInstanceOf(PortableReadResultSet.class, rs);

        assertEquals(5, rs.readCount());
        assertEquals("good1", rs.get(0));
        assertEquals("good2", rs.get(1));
        assertEquals("good3", rs.get(2));
    }

    // checks if the max count works in combination with a filter.
    // So if more results are available than needed, the surplus results should not be read.
    @Test
    public void readManyAsync_withFilter_andMaxCount() throws Exception {
        serverRingbuffer.add("good1");
        serverRingbuffer.add("bad1");
        serverRingbuffer.add("good2");
        serverRingbuffer.add("bad2");
        serverRingbuffer.add("good3");
        serverRingbuffer.add("bad3");
        serverRingbuffer.add("good4");
        serverRingbuffer.add("bad4");

        ICompletableFuture<ReadResultSet<String>> f = clientRingbuffer.readManyAsync(0, 3, 3, new Filter());

        ReadResultSet rs = f.get();
        assertInstanceOf(PortableReadResultSet.class, rs);

        assertEquals(5, rs.readCount());
        assertEquals("good1", rs.get(0));
        assertEquals("good2", rs.get(1));
        assertEquals("good3", rs.get(2));
    }

    static class Filter implements IFunction<String, Boolean> {
        @Override
        public Boolean apply(String input) {
            return input.startsWith("good");
        }
    }
}

