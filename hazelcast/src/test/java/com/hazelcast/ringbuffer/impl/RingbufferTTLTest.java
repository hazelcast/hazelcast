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

package com.hazelcast.ringbuffer.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RingbufferTTLTest extends HazelcastTestSupport {

    private HazelcastInstance hz;
    private Ringbuffer<String> ringbuffer;
    private RingbufferContainer ringbufferContainer;
    private ArrayRingbuffer arrayRingbuffer;

    public void setup(RingbufferConfig ringbufferConfig) {
        Config config = new Config();
        config.addRingBufferConfig(ringbufferConfig);
        hz = createHazelcastInstance(config);
        final String name = ringbufferConfig.getName();
        ringbuffer = hz.getRingbuffer(name);

        RingbufferService ringbufferService = getNodeEngineImpl(hz).getService(RingbufferService.SERVICE_NAME);
        ringbufferContainer = ringbufferService.getOrCreateContainer(
                ringbufferService.getRingbufferPartitionId(name),
                RingbufferService.getRingbufferNamespace(name),
                ringbufferConfig);
        arrayRingbuffer = (ArrayRingbuffer) ringbufferContainer.getRingbuffer();
    }

    // when ttl is set, then eventually all the items needs to get expired.
    // In this particular test we fill the ringbuffer and wait for it to expire.
    // Expiration is given a 2 second martin of error. So we have a ttl of
    // 10 seconds, then we expect that within 12 seconds the buffer is empty.
    @Test
    public void whenTTLEnabled_thenEventuallyRingbufferEmpties() throws Exception {
        int ttl = 10;
        int maximumVisibleTTL = ttl + 2;

        setup(new RingbufferConfig("foo").setTimeToLiveSeconds(ttl));

        for (int k = 0; k < ringbuffer.capacity(); k++) {
            ringbuffer.add("item" + k);
        }

        final long tail = ringbuffer.tailSequence();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(tail, ringbuffer.tailSequence());
                assertEquals(tail + 1, ringbuffer.headSequence());
                assertEquals(0, ringbuffer.size());
                assertEquals(ringbuffer.capacity(), ringbuffer.remainingCapacity());
            }
        }, maximumVisibleTTL);

        // and we verify that the slots are nulled so we don't have a memory leak on our hands.
        for (int k = 0; k < ringbuffer.capacity(); k++) {
            assertNull(arrayRingbuffer.getItems()[k]);
        }
    }

    // if ttl is set, we need to make sure that during that period all items are accessible.
    // In this particular test we set the ttl period to 10 second. The minimumVisibilitySeconds to
    // give us an error margin of 2 seconds (due to threading etc... we don't want to have another
    // spurious failing test.
    @Test
    public void whenTTLEnabled_thenVisibilityIsGuaranteed() {
        int ttl = 10;
        int minimumVisibleTTL = ttl - 2;

        setup(new RingbufferConfig("foo").setTimeToLiveSeconds(ttl).setCapacity(100));

        for (int k = 0; k < ringbuffer.capacity(); k++) {
            ringbuffer.add("item" + k);
        }

        final long head = ringbuffer.headSequence();
        final long tail = ringbuffer.tailSequence();
        final long size = ringbuffer.size();

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(head, ringbuffer.headSequence());
                assertEquals(tail, ringbuffer.tailSequence());
                assertEquals(size, ringbuffer.size());

                for (long seq = head; seq <= tail; seq++) {
                    assertEquals("item" + seq, ringbuffer.readOne(seq));
                }
            }
        }, minimumVisibleTTL);
    }

    @Test
    public void whenTTLDisabled_thenNothingRetires() {
        setup(new RingbufferConfig("foo").setTimeToLiveSeconds(0).setCapacity(100));

        for (int k = 0; k < ringbuffer.capacity(); k++) {
            ringbuffer.add("item" + k);
        }

        final long head = ringbuffer.headSequence();
        final long tail = ringbuffer.tailSequence();
        final long size = ringbuffer.size();

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(head, ringbuffer.headSequence());
                assertEquals(tail, ringbuffer.tailSequence());
                assertEquals(size, ringbuffer.size());

                for (long seq = head; seq <= tail; seq++) {
                    assertEquals("item" + seq, ringbuffer.readOne(seq));
                }
            }
        }, 5);
    }

    @Test
    public void whenTTLEnabled_thenReadManyShouldSkipExpiredItems() throws Exception {
        setup(new RingbufferConfig("foo").setTimeToLiveSeconds(1).setCapacity(100));

        long head = ringbuffer.headSequence();
        ringbuffer.add("a");

        ReadResultSet<String> result = ringbuffer.readManyAsync(head, 0, 10, null).toCompletableFuture().get();
        assertThat(result).containsOnly("a");

        sleepMillis(1100);

        result = ringbuffer.readManyAsync(head, 0, 10, null).toCompletableFuture().get();
        assertThat(result).isEmpty();
    }
}
