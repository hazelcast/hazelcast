package com.hazelcast.ringbuffer.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IFunction;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.ringbuffer.OverflowPolicy.FAIL;
import static com.hazelcast.ringbuffer.OverflowPolicy.OVERWRITE;
import static com.hazelcast.test.AbstractHazelcastClassRunner.getTestMethodName;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public abstract class RingbufferAbstractTest extends HazelcastTestSupport {

    protected HazelcastInstance[] instances;
    protected IAtomicLong atomicLong;
    private Ringbuffer<String> ringbuffer;
    private Config config;
    private HazelcastInstance local;
    private String name;

    @Before
    public void setup() {

        config = new Config();
        config.addRingBufferConfig(new RingbufferConfig("add_overwritingOldData*")
                .setCapacity(5));
        config.addRingBufferConfig(new RingbufferConfig("add_manyTimesRoundTheRing*")
                .setCapacity(5));
        config.addRingBufferConfig(new RingbufferConfig("readOne_whenBeforeHead*")
                .setCapacity(5));
        config.addRingBufferConfig(new RingbufferConfig("readOne_whenObjectInMemoryFormat*")
                .setCapacity(5)
                .setInMemoryFormat(OBJECT));
        config.addRingBufferConfig(new RingbufferConfig("readManyAsync_whenEnoughItems_andObjectInMemoryFormat*")
                .setCapacity(5)
                .setInMemoryFormat(OBJECT));
        config.addRingBufferConfig(new RingbufferConfig("addAsync_whenOverwrite_andNoTTL*")
                .setCapacity(300)
                .setTimeToLiveSeconds(0));
        config.addRingBufferConfig(new RingbufferConfig("addAllAsync_whenCollectionExceedsCapacity*")
                .setCapacity(5));
        config.addRingBufferConfig(new RingbufferConfig("addAllAsync_manyTimesRoundTheRing*")
                .setCapacity(50));
        config.addRingBufferConfig(new RingbufferConfig("addAllAsync_whenObjectInMemoryFormat*")
                .setCapacity(50)
                .setInMemoryFormat(OBJECT));
        config.addRingBufferConfig(new RingbufferConfig("addAsync_fail_whenNoSpace*")
                .setCapacity(300)
                .setTimeToLiveSeconds(10));
        config.addRingBufferConfig(new RingbufferConfig("addAsync_whenOverwrite_andTTL*")
                .setCapacity(300));
        config.addRingBufferConfig(new RingbufferConfig("addAsync_whenOverwrite_andNoTTL*")
                .setCapacity(300)
                .setTimeToLiveSeconds(0));
        config.addRingBufferConfig(new RingbufferConfig("remainingCapacity*")
                .setCapacity(300)
                .setTimeToLiveSeconds(10));

        instances = newInstances(config);
        local = instances[0];
        HazelcastInstance target = instances[instances.length - 1];

        name = HazelcastTestSupport.randomNameOwnedBy(target, getTestMethodName());
        ringbuffer = local.getRingbuffer(name);
    }

    protected abstract HazelcastInstance[] newInstances(Config config);

    private static List<String> randomList(int size) {
        List<String> items = new ArrayList<String>(size);
        for (int k = 0; k < size; k++) {
            items.add("" + k);
        }
        return items;
    }

    // ================ tailSequence ==========================================

    @Test
    public void tailSequence_whenNothingAdded() {
        long found = ringbuffer.tailSequence();

        assertEquals(-1, found);
    }

    // ================ remainingCapacity ==========================================

    // the actual logic of remaining capacity is tested directly in the RingbufferContainerTest. So we just need
    // to do some basic tests to see if the operation itself is set up correctly
    @Test
    public void remainingCapacity() {
        assertEquals(ringbuffer.capacity(), ringbuffer.remainingCapacity());

        ringbuffer.add("first");

        assertEquals(ringbuffer.capacity() - 1, ringbuffer.remainingCapacity());
        ringbuffer.add("second");

        assertEquals(ringbuffer.capacity() - 2, ringbuffer.remainingCapacity());
    }

    // ================ headSequence ==========================================

    @Test
    public void headSequence_whenNothingAdded() {
        long found = ringbuffer.headSequence();

        assertEquals(0, found);
    }

    // ================ capacity =========================================

    @Test
    public void capacity() {
        assertEquals(RingbufferConfig.DEFAULT_CAPACITY, ringbuffer.capacity());
    }

    // ================ size =========================================

    @Test
    public void size_whenUnused() {
        long found = ringbuffer.size();
        assertEquals(0, found);
    }

    @Test
    public void size_whenSomeItemsAdded() throws Exception {
        // we are adding some items, but we don't go beyond the capacity
        // most of the testing already is done in the RingbufferContainerTest

        for (int k = 1; k <= 100; k++) {
            ringbuffer.add("foo");
            assertEquals(k, ringbuffer.size());
        }
    }

    // ================ add ==========================================

    @Test(expected = NullPointerException.class)
    public void addAsync_whenNullItem() {
        ringbuffer.addAsync(null, FAIL);
    }

    @Test(expected = NullPointerException.class)
    public void addAsync_whenNullOverflowPolicy() {
        ringbuffer.addAsync("foo", null);
    }

    @Test
    public void addAsync_fail_whenSpace() throws Exception {
        long sequence = ringbuffer.addAsync("item", FAIL).get();

        assertEquals(0, sequence);
        assertEquals(0, ringbuffer.headSequence());
        assertEquals(0, ringbuffer.tailSequence());
        assertEquals("item", ringbuffer.readOne(sequence));
    }

    @Test
    public void addAsync_fail_whenNoSpace() throws Exception {
        // fill the buffer with data
        for (int k = 0; k < ringbuffer.capacity(); k++) {
            ringbuffer.add("old");
        }

        long oldHead = ringbuffer.headSequence();
        long oldTail = ringbuffer.tailSequence();

        long result = ringbuffer.addAsync("item", FAIL).get();

        assertEquals(-1, result);
        assertEquals(oldHead, ringbuffer.headSequence());
        assertEquals(oldTail, ringbuffer.tailSequence());

        // verify that all the old data is still present.
        for (long seq = oldHead; seq <= oldTail; seq++) {
            assertEquals("old", ringbuffer.readOne(seq));
        }
    }

    @Test
    public void addAsync_overwrite_whenSpace() throws Exception {
        long sequence = ringbuffer.addAsync("item", OVERWRITE).get();

        assertEquals(0, sequence);
        assertEquals(0, ringbuffer.headSequence());
        assertEquals(0, ringbuffer.tailSequence());
        assertEquals("item", ringbuffer.readOne(sequence));
    }

    @Test
    public void addAsync_whenOverwrite_andTTL() throws ExecutionException, InterruptedException {
        addAsync_whenOverwrite();
    }

    @Test
    public void addAsync_whenOverwrite_andNoTTL() throws ExecutionException, InterruptedException {
        addAsync_whenOverwrite();
    }

    private void addAsync_whenOverwrite() throws InterruptedException, ExecutionException {
        RingbufferConfig c = config.getRingbufferConfig(ringbuffer.getName());

        // fill the buffer with data
        for (int k = 0; k < ringbuffer.capacity(); k++) {
            ringbuffer.add("old");
        }

        for (int iteration = 0; iteration < c.getCapacity() * 100; iteration++) {
            long oldTail = ringbuffer.tailSequence();

            String item = "" + iteration;
            long sequence = ringbuffer.addAsync(item, OVERWRITE).get();
            long expectedSequence = oldTail + 1;

            assertEquals(expectedSequence, sequence);
            assertEquals(expectedSequence, ringbuffer.tailSequence());

            if (ringbuffer.tailSequence() < c.getCapacity()) {
                assertEquals(0, ringbuffer.headSequence());
            } else {
                assertEquals(ringbuffer.tailSequence() - c.getCapacity() + 1, ringbuffer.headSequence());
            }

            assertEquals(item, ringbuffer.readOne(expectedSequence));
        }
    }


    // ================ add ==========================================


    @Test(expected = NullPointerException.class)
    public void add_whenNullItem() throws Exception {
        ringbuffer.add(null);
    }

    /**
     * In this test we are going to overwrite old data a few times (so we are going round
     * the ring a few times). At the end we expect to see the latest items; everything
     * that is overwritten should not be shown anymore.
     */
    @Test
    public void add_overwritingOldData() throws Exception {
        RingbufferConfig c = config.getRingbufferConfig(ringbuffer.getName());

        long lastSequence = ringbuffer.tailSequence();
        long expectedTailSeq = 0;

        for (long k = 0; k < 2 * c.getCapacity(); k++) {
            long sequence = ringbuffer.add("item-" + k);
            lastSequence = ringbuffer.tailSequence();
            assertEquals(expectedTailSeq, sequence);
            assertEquals(expectedTailSeq, lastSequence);
            assertEquals(expectedTailSeq, ringbuffer.tailSequence());
            expectedTailSeq++;
        }

        assertEquals(lastSequence - c.getCapacity() + 1, ringbuffer.headSequence());

        // verifying the content
        for (long sequence = ringbuffer.headSequence(); sequence <= ringbuffer.tailSequence(); sequence++) {
            assertEquals("bad content at sequence:" + sequence, "item-" + sequence, ringbuffer.readOne(sequence));
        }
    }

    // this test verifies that the add works correctly if we go round the ringbuffer many times.
    @Test
    public void add_manyTimesRoundTheRing() throws Exception {
        RingbufferConfig c = config.getRingbufferConfig(ringbuffer.getName());

        for (int iteration = 0; iteration < c.getCapacity() * 100; iteration++) {
            long oldTail = ringbuffer.tailSequence();

            String item = "" + iteration;
            long sequence = ringbuffer.add(item);
            long expectedSequence = oldTail + 1;

            assertEquals(expectedSequence, sequence);
            assertEquals(expectedSequence, ringbuffer.tailSequence());

            if (ringbuffer.tailSequence() < c.getCapacity()) {
                assertEquals(0, ringbuffer.headSequence());
            } else {
                assertEquals(ringbuffer.tailSequence() - c.getCapacity() + 1, ringbuffer.headSequence());
            }

            assertEquals(item, ringbuffer.readOne(expectedSequence));
        }
    }

    // ================== addAll ========================================


    @Test(expected = IllegalArgumentException.class)
    public void addAllAsync_whenCollectionTooLarge() {
        ringbuffer.addAllAsync(randomList(RingbufferProxy.MAX_BATCH_SIZE + 1), OVERWRITE);
    }

    @Test(expected = NullPointerException.class)
    public void addAllAsync_whenNullCollection() {
        ringbuffer.addAllAsync(null, OVERWRITE);
    }

    @Test(expected = NullPointerException.class)
    public void addAllAsync_whenCollectionContainsNullElement() {
        List<String> list = new LinkedList<String>();
        list.add(null);
        ringbuffer.addAllAsync(list, OVERWRITE);
    }

    @Test(expected = NullPointerException.class)
    public void addAllAsync_whenNullOverflowPolicy() {
        ringbuffer.addAllAsync(new LinkedList<String>(), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void addAllAsync_whenEmpty() throws Exception {
        ringbuffer.addAllAsync(new LinkedList<String>(), OVERWRITE);
    }

    @Test
    public void addAllAsync_whenCollectionExceedsCapacity() throws Exception {
        RingbufferConfig c = config.getRingbufferConfig(ringbuffer.getName());

        long oldTailSeq = ringbuffer.tailSequence();

        List<String> items = randomList(c.getCapacity() + 20);
        long result = ringbuffer.addAllAsync(items, OVERWRITE).get();

        assertEquals(oldTailSeq + items.size(), ringbuffer.tailSequence());
        assertEquals(ringbuffer.tailSequence() - c.getCapacity() + 1, ringbuffer.headSequence());
        assertEquals(ringbuffer.tailSequence(), result);
    }

    @Test
    public void addAllAsync_whenObjectInMemoryFormat() throws Exception {
        List<String> items = randomList(10);

        long oldTailSeq = ringbuffer.tailSequence();

        long result = ringbuffer.addAllAsync(items, OVERWRITE).get();

        assertEquals(0, ringbuffer.headSequence());
        assertEquals(oldTailSeq + items.size(), ringbuffer.tailSequence());
        assertEquals(ringbuffer.tailSequence(), result);

        long startSequence = ringbuffer.headSequence();
        for (int k = 0; k < items.size(); k++) {
            assertEquals(items.get(k), ringbuffer.readOne(startSequence + k));
        }
    }

    @Test
    public void addAllAsync() throws Exception {
        List<String> items = randomList(10);

        long oldTailSeq = ringbuffer.tailSequence();

        long result = ringbuffer.addAllAsync(items, OVERWRITE).get();

        assertEquals(0, ringbuffer.headSequence());
        assertEquals(oldTailSeq + items.size(), ringbuffer.tailSequence());
        assertEquals(ringbuffer.tailSequence(), result);

        long startSequence = ringbuffer.headSequence();
        for (int k = 0; k < items.size(); k++) {
            assertEquals(items.get(k), ringbuffer.readOne(startSequence + k));
        }
    }

    // this test verifies that the addAllAsync works correctly if we go round the ringbuffer many times.
    @Test
    public void addAllAsync_manyTimesRoundTheRing() throws Exception {
        RingbufferConfig c = config.getRingbufferConfig(ringbuffer.getName());
        Random random = new Random();

        for (int iteration = 0; iteration < 1000; iteration++) {
            List<String> items = randomList(max(1, random.nextInt(c.getCapacity())));

            long previousTailSeq = ringbuffer.tailSequence();

            long result = ringbuffer.addAllAsync(items, OVERWRITE).get();

            assertEquals(previousTailSeq + items.size(), ringbuffer.tailSequence());

            if (ringbuffer.tailSequence() < c.getCapacity()) {
                assertEquals(0, ringbuffer.headSequence());
            } else {
                assertEquals(ringbuffer.tailSequence() - c.getCapacity() + 1, ringbuffer.headSequence());
            }
            assertEquals(ringbuffer.tailSequence(), result);

            long startSequence = previousTailSeq + 1;
            for (int k = 0; k < items.size(); k++) {
                assertEquals(items.get(k), ringbuffer.readOne(startSequence + k));
            }
        }
    }

    // ================== read ========================================

    @Test
    public void readOne() throws Exception {
        ringbuffer.add("first");
        ringbuffer.add("second");

        long oldHead = ringbuffer.headSequence();
        long oldTail = ringbuffer.tailSequence();

        assertEquals("first", ringbuffer.readOne(0));
        assertEquals("second", ringbuffer.readOne(1));
        // make sure that the head/tail of the ringbuffer have not changed.
        assertEquals(oldHead, ringbuffer.headSequence());
        assertEquals(oldTail, ringbuffer.tailSequence());
    }

    @Test
    public void readOne_whenObjectInMemoryFormat() throws Exception {
        ringbuffer.add("first");
        ringbuffer.add("second");

        long oldHead = ringbuffer.headSequence();
        long oldTail = ringbuffer.tailSequence();

        assertEquals("first", ringbuffer.readOne(0));
        assertEquals("second", ringbuffer.readOne(1));
        // make sure that the head/tail of the ringbuffer have not changed.
        assertEquals(oldHead, ringbuffer.headSequence());
        assertEquals(oldTail, ringbuffer.tailSequence());
    }

    @Test
    public void readOne_whenOneAfterTail_thenBlock() throws Exception {
        ringbuffer.add("1");
        ringbuffer.add("2");

        final long tail = ringbuffer.tailSequence();

        // first we do the invocation. This invocation is going to block
        final Future f = spawn(new Callable<String>() {
            @Override
            public String call() throws Exception {
                return ringbuffer.readOne(tail + 1);
            }
        });

        // then we check if the future is not going to complete.
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertFalse(f.isDone());
            }
        }, 2);

        // then we add the item
        ringbuffer.add("3");

        // and eventually the future should complete
        assertCompletesEventually(f);

        assertEquals("3", f.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void readOne_whenTooFarAfterTail_thenIllegalArgumentException() throws Exception {
        ringbuffer.add("1");
        ringbuffer.add("2");
        long tailSeq = ringbuffer.tailSequence();

        ringbuffer.readOne(tailSeq + 2);
    }

    @Test
    public void readOne_whenBeforeHead_thenStaleSequenceException() throws Exception {
        RingbufferConfig ringbufferConfig = config.getRingbufferConfig(ringbuffer.getName());

        for (int k = 0; k < ringbufferConfig.getCapacity() * 2; k++) {
            ringbuffer.add("foo");
        }

        long headSeq = ringbuffer.headSequence();
        try {
            ringbuffer.readOne(headSeq - 1);
            fail();
        } catch (StaleSequenceException expected) {
            assertEquals(headSeq, expected.getHeadSeq());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void read_whenNegativeSequence_thenIllegalArgumentException() throws Exception {
        ringbuffer.readOne(-1);
    }

    // ==================== asyncReadOrWait ==============================

    @Test
    public void readManyAsync_whenReadingBeyondTail() throws ExecutionException, InterruptedException {
        ringbuffer.add("1");
        ringbuffer.add("2");

        long seq = ringbuffer.tailSequence() + 2;
        ICompletableFuture f = ringbuffer.readManyAsync(seq, 1, 1, null);
        try {
            f.get();
            fail();
        } catch (ExecutionException e) {
            assertInstanceOf(IllegalArgumentException.class, e.getCause());
        }
    }

    @Test
    public void readyManyAsync_whenSomeWaitingForSingleItemNeeded() throws ExecutionException, InterruptedException {
        final ICompletableFuture<ReadResultSet<String>> f = ringbuffer.readManyAsync(0, 1, 10, null);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertFalse(f.isDone());
            }
        }, 5);

        ringbuffer.add("1");

        assertCompletesEventually(f);

        ReadResultSet<String> resultSet = f.get();

        assertEquals(singletonList("1"), resultSet);
        assertEquals(1, resultSet.readCount());
    }

    @Test
    public void readManyAsync_whenSomeWaitingNeeded() throws ExecutionException, InterruptedException {
        final ICompletableFuture<ReadResultSet<String>> f = ringbuffer.readManyAsync(0, 2, 10, null);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertFalse(f.isDone());
            }
        }, 5);

        ringbuffer.add("1");

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertFalse(f.isDone());
            }
        }, 5);

        ringbuffer.add("2");

        assertCompletesEventually(f);
        ReadResultSet<String> resultSet = f.get();
        assertNotNull(resultSet);

        assertEquals(asList("1", "2"), f.get());
        assertEquals(2, resultSet.readCount());
    }

    @Test
    public void readManyAsync_whenMinZeroAndItemAvailable() throws ExecutionException, InterruptedException {
        ringbuffer.add("1");

        ICompletableFuture<ReadResultSet<String>> f = ringbuffer.readManyAsync(0, 0, 10, null);

        assertCompletesEventually(f);
        assertEquals(singletonList("1"), f.get());
    }

    @Test
    public void readManyAsync_whenMinZeroAndNoItemAvailable() throws ExecutionException, InterruptedException {
        ICompletableFuture<ReadResultSet<String>> f = ringbuffer.readManyAsync(0, 0, 10, null);

        assertCompletesEventually(f);
        assertEquals(EMPTY_LIST, f.get());
    }

    @Test
    public void readManyAsync_whenEnoughItems() throws ExecutionException, InterruptedException {
        ringbuffer.add("item1");
        ringbuffer.add("item2");
        ringbuffer.add("item3");
        ringbuffer.add("item4");

        ICompletableFuture<ReadResultSet<String>> f = ringbuffer.readManyAsync(1, 1, 2, null);
        assertCompletesEventually(f);

        ReadResultSet<String> resultSet = f.get();

        assertNotNull(resultSet);
        assertEquals(asList("item2", "item3"), resultSet);
        assertEquals(2, resultSet.readCount());
    }

    @Test
    public void readManyAsync_whenEnoughItems_andObjectInMemoryFormat() throws ExecutionException, InterruptedException {
        ringbuffer.add("item1");
        ringbuffer.add("item2");
        ringbuffer.add("item3");
        ringbuffer.add("item4");

        ICompletableFuture<ReadResultSet<String>> f = ringbuffer.readManyAsync(1, 1, 2, null);
        assertCompletesEventually(f);

        ReadResultSet<String> resultSet = f.get();

        assertNotNull(resultSet);
        assertEquals(asList("item2", "item3"), resultSet);
        assertEquals(2, resultSet.readCount());
    }

    public static class GoodStringFunction implements IFunction<String, Boolean>, Serializable {
        @Override
        public Boolean apply(String input) {
            return input.startsWith("good");
        }
    }

    @Test
    public void readManyAsync_withFilter() throws ExecutionException, InterruptedException {
        ringbuffer.add("good1");
        ringbuffer.add("bad1");
        ringbuffer.add("good2");
        ringbuffer.add("bad");
        ringbuffer.add("good3");
        ringbuffer.add("bad1");

        ICompletableFuture<ReadResultSet<String>> f = ringbuffer.readManyAsync(0, 2, 10, new GoodStringFunction());

        assertCompletesEventually(f);

        ReadResultSet<String> resultSet = f.get();

        assertNotNull(resultSet);
        assertEquals(asList("good1", "good2", "good3"), resultSet);
        assertEquals(6, resultSet.readCount());
    }

    @Test
    public void readManyAsync_emptyBatchAndNoItems() throws Exception {
        ICompletableFuture<ReadResultSet<String>> f = ringbuffer.readManyAsync(0, 0, 10, null);

        assertCompletesEventually(f);

        ReadResultSet<String> resultSet = f.get();

        assertEquals(new LinkedList<String>(), resultSet);
        assertEquals(0, resultSet.readCount());
    }

    @Test
    public void readManyAsync_whenMoreAvailableThanMinimumLessThanMaximum() throws ExecutionException, InterruptedException {
        ringbuffer.add("1");
        ringbuffer.add("2");
        ringbuffer.add("3");

        ICompletableFuture<ReadResultSet<String>> f = ringbuffer.readManyAsync(ringbuffer.headSequence(), 2, 10, null);

        assertCompletesEventually(f);

        ReadResultSet<String> resultSet = f.get();

        assertEquals(asList("1", "2", "3"), resultSet);
        assertEquals(3, resultSet.readCount());
    }

    @Test
    public void readManyAsync_whenMoreAvailableThanMaximum() throws ExecutionException, InterruptedException {
        ringbuffer.add("1");
        ringbuffer.add("2");
        ringbuffer.add("3");

        ICompletableFuture<ReadResultSet<String>> f = ringbuffer.readManyAsync(ringbuffer.headSequence(), 1, 2, null);
        assertCompletesEventually(f);

        ReadResultSet<String> resultSet = f.get();

        assertNotNull(resultSet);
        assertEquals(asList("1", "2"), resultSet);
        assertEquals(2, resultSet.readCount());
    }

    @Test(expected = IllegalArgumentException.class)
    public void readManyAsync_whenMinCountLargerThanCapacity() {
        ringbuffer.readManyAsync(0, RingbufferConfig.DEFAULT_CAPACITY + 1, RingbufferConfig.DEFAULT_CAPACITY + 1, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void readManyAsync_whenStartSequenceNegative() {
        ringbuffer.readManyAsync(-1, 1, 1, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void readManyAsync_whenMinCountNegative() {
        ringbuffer.readManyAsync(0, -1, 1, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void readManyAsync_whenMaxSmallerThanMinCount() {
        ringbuffer.readManyAsync(0, 5, 4, null);
    }

    @Test
    public void readManyAsync_whenMaxCountTooHigh() throws Exception {
        ringbuffer.readManyAsync(0, 1, RingbufferProxy.MAX_BATCH_SIZE, null);
    }

    // ===================== destroy ==========================

    @Test
    public void destroy() throws Exception {
        ringbuffer.add("1");
        ringbuffer.destroy();

        ringbuffer = local.getRingbuffer(name);
        assertEquals(0, ringbuffer.headSequence());
        assertEquals(-1, ringbuffer.tailSequence());
    }

    @Test(expected = DistributedObjectDestroyedException.class)
    public void destroy_whenBlockedThreads_thenDistributedObjectDestroyedException() throws Exception {
        spawn(new Runnable() {
            @Override
            public void run() {
                sleepSeconds(2);
                ringbuffer.destroy();
            }
        });

        InternalCompletableFuture f = (InternalCompletableFuture) ringbuffer.readManyAsync(0, 1, 1, null);
        f.join();
    }
    // ===================== misc ==========================

    @Test
    public void test_toString() {
        String actual = ringbuffer.toString();
        String expected = format("Ringbuffer{name='%s'}", ringbuffer.getName());
        assertEquals(expected, actual);
    }
}
