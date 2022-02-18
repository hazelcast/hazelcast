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

package com.hazelcast.client.ringbuffer;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.IdentifiedDataSerializableFactory;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.client.test.ringbuffer.filter.StartsWithStringFilter;
import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.ringbuffer.impl.ReadResultSetImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.ringbuffer.OverflowPolicy.OVERWRITE;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class RingbufferTest extends HazelcastTestSupport {

    public static final int CAPACITY = 10;

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private HazelcastInstance client;
    private HazelcastInstance server;
    private Ringbuffer<String> clientRingbuffer;
    private Ringbuffer<String> serverRingbuffer;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void init() {
        Config config = new Config();
        config.addRingBufferConfig(new RingbufferConfig("rb*").setCapacity(CAPACITY));
        // Set operation timeout to larger than test timeout. So the tests do not pass accidentally because of retries.
        // The tests should depend on notifier system, not retrying.
        config.setProperty("hazelcast.operation.call.timeout.millis", "305000");
        config.getSerializationConfig()
              .addDataSerializableFactory(IdentifiedDataSerializableFactory.FACTORY_ID, new IdentifiedDataSerializableFactory());

        server = hazelcastFactory.newHazelcastInstance(config);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSerializationConfig().addDataSerializableFactory(IdentifiedDataSerializableFactory.FACTORY_ID,
                new IdentifiedDataSerializableFactory());
        client = hazelcastFactory.newHazelcastClient(clientConfig);

        String name = "rb-" + randomString();
        serverRingbuffer = server.getRingbuffer(name);
        clientRingbuffer = client.getRingbuffer(name);
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test(expected = IllegalArgumentException.class)
    public void readManyAsync_whenStartSequenceIsNegative() {
        clientRingbuffer.readManyAsync(-1, 1, 10, null);
    }

    @Test
    public void readManyAsync_whenStartSequenceIsNoLongerAvailable_getsClamped() throws Exception {
        serverRingbuffer.addAllAsync(asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"), OVERWRITE);
        CompletionStage<ReadResultSet<String>> f = clientRingbuffer.readManyAsync(0, 1, 10, null);

        ReadResultSet rs = f.toCompletableFuture().get();
        assertEquals(10, rs.readCount());
        assertEquals("1", rs.get(0));
        assertEquals("10", rs.get(9));
    }

    @Test
    public void readManyAsync_whenStartSequenceIsEqualToTailSequence() throws Exception {
        serverRingbuffer.addAllAsync(asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"), OVERWRITE);
        CompletionStage<ReadResultSet<String>> f = clientRingbuffer.readManyAsync(10, 1, 10, null);

        ReadResultSet rs = f.toCompletableFuture().get();
        assertEquals(1, rs.readCount());
        assertEquals("10", rs.get(0));
    }

    @Test(expected = TimeoutException.class)
    public void readManyAsync_whenStartSequenceIsJustBeyondTailSequence() throws Exception {
        serverRingbuffer.addAllAsync(asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"), OVERWRITE);
        CompletionStage<ReadResultSet<String>> f = clientRingbuffer.readManyAsync(11, 1, 10, null);

        //test that the read blocks (should at least fail some of the time, if not the case)
        f.toCompletableFuture().get(250, TimeUnit.MILLISECONDS);
    }

    @Test(expected = TimeoutException.class)
    public void readManyAsync_whenStartSequenceIsWellBeyondTailSequence() throws Exception {
        serverRingbuffer.addAllAsync(asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"), OVERWRITE);
        CompletionStage<ReadResultSet<String>> f = clientRingbuffer.readManyAsync(19, 1, 10, null);

        //test that the read blocks (should at least fail some of the time, if not the case)
        f.toCompletableFuture().get(250, TimeUnit.MILLISECONDS);
    }

    @Test
    public void readOne_whenHitsStale_shouldNotBeBlocked() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        Thread consumer = new Thread(() -> {
            try {
                clientRingbuffer.readOne(0);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (StaleSequenceException e) {
                latch.countDown();
            }
        });
        consumer.start();
        serverRingbuffer.addAllAsync(asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"), OVERWRITE);
        assertOpenEventually(latch);
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
        Future<Long> f = clientRingbuffer.addAsync("foo", OVERWRITE).toCompletableFuture();
        Long result = f.get();

        assertEquals(new Long(serverRingbuffer.headSequence()), result);
        assertEquals("foo", serverRingbuffer.readOne(0));
        assertEquals(0, serverRingbuffer.headSequence());
        assertEquals(0, serverRingbuffer.tailSequence());
    }

    @Test
    public void addAll() throws Exception {
        Future<Long> f = clientRingbuffer.addAllAsync(asList("foo", "bar"), OVERWRITE).toCompletableFuture();
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

        Future<ReadResultSet<String>> f = clientRingbuffer.readManyAsync(0, 3, 3, null).toCompletableFuture();

        ReadResultSet rs = f.get();
        assertInstanceOf(ReadResultSetImpl.class, rs);

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

        Future<ReadResultSet<String>> f = clientRingbuffer.readManyAsync(0, 3, 3, null).toCompletableFuture();

        ReadResultSet rs = f.get();
        assertInstanceOf(ReadResultSetImpl.class, rs);

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

        Future<ReadResultSet<String>> f = clientRingbuffer
                .readManyAsync(0, 3, 3, new StartsWithStringFilter("good")).toCompletableFuture();

        ReadResultSet rs = f.get();
        assertInstanceOf(ReadResultSetImpl.class, rs);

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

        Future<ReadResultSet<String>> f = clientRingbuffer
                .readManyAsync(0, 3, 3, new StartsWithStringFilter("good")).toCompletableFuture();

        ReadResultSet rs = f.get();
        assertInstanceOf(ReadResultSetImpl.class, rs);

        assertEquals(5, rs.readCount());
        assertEquals("good1", rs.get(0));
        assertEquals("good2", rs.get(1));
        assertEquals("good3", rs.get(2));
    }
}
