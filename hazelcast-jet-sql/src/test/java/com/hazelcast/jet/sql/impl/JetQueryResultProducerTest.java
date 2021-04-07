/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.util.ArrayDequeInbox;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.ResultIterator;
import com.hazelcast.sql.impl.row.Row;
import org.junit.Test;

import java.util.NoSuchElementException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

import static com.hazelcast.sql.impl.ResultIterator.HasNextResult.TIMEOUT;
import static com.hazelcast.sql.impl.ResultIterator.HasNextResult.YES;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class JetQueryResultProducerTest extends JetTestSupport {

    private final JetQueryResultProducer producer = new JetQueryResultProducer();
    private final ResultIterator<Row> iterator = producer.iterator();
    private final ArrayDequeInbox inbox = new ArrayDequeInbox(new ProgressTracker());

    @Test
    public void smokeTest() throws Exception {
        Semaphore semaphore = new Semaphore(0);
        Future<?> future = spawn(() -> {
            try {
                assertThat(iterator.hasNext(0, SECONDS)).isEqualTo(TIMEOUT);
                semaphore.release();
                assertThat(iterator.hasNext()).isTrue();
                assertInstanceOf(Row.class, iterator.next());
                semaphore.release();
                assertThat(iterator.hasNext()).isFalse();
                assertThatThrownBy(iterator::next)
                        .isInstanceOf(NoSuchElementException.class);
                semaphore.release();
            } catch (Throwable t) {
                logger.info("", t);
                throw t;
            }
        });

        semaphore.acquire();
        // now we're after the hasNextImmediately call in the thread

        // check that the thread is blocked in `hasNext` - that it did not release the 2nd permit
        sleepMillis(50);
        assertThat(semaphore.availablePermits()).isZero();

        inbox.queue().add(new Object[0]);
        producer.consume(inbox);

        // 2nd permit - the row returned from the iterator
        semaphore.acquire();

        // check that the thread is blocked in `hasNext` - that it did not release the 2nd permit
        sleepMillis(50);
        assertThat(semaphore.availablePermits()).isZero();

        producer.done();

        assertTrueEventually(future::isDone, 5);
        semaphore.acquire();

        // called for the side-effect of throwing the exception if it happened in the thread
        future.get();
    }

    @Test
    public void when_done_then_remainingItemsIterated() {
        inbox.queue().add(new Object[]{1});
        inbox.queue().add(new Object[]{2});
        producer.consume(inbox);
        producer.done();

        assertThat(iterator.hasNext()).isTrue();
        assertThat((int) iterator.next().get(0)).isEqualTo(1);
        assertThat(iterator.hasNext()).isTrue();
        assertThat((int) iterator.next().get(0)).isEqualTo(2);
        assertThat(iterator).isExhausted();
    }

    @Test
    public void when_doneWithErrorWhileWaiting_then_throw_async() {
        assertThat(iterator.hasNext(0, SECONDS)).isEqualTo(TIMEOUT);
        producer.onError(QueryException.error("mock error"));
        assertThatThrownBy(() -> iterator.hasNext(0, SECONDS))
                .hasMessageContaining("mock error");
    }

    @Test
    public void when_doneWithErrorWhileWaiting_then_throw_sync() throws Exception {
        Future<?> future = spawn(() -> {
            assertThatThrownBy(() -> iterator.hasNext(1, DAYS))
                    .hasMessageContaining("mock error");
        });
        sleepMillis(50); // sleep so that the thread starts blocking in `hasNext`
        producer.onError(QueryException.error("mock error"));
        future.get();
    }

    @Test
    public void when_nextItemWhileWaiting_then_hasNextReturns() throws Exception {
        Future<?> future = spawn(() -> {
            assertThat(iterator.hasNext(1, DAYS)).isEqualTo(YES);
            assertThat((int) iterator.next().get(0)).isEqualTo(42);
        });
        sleepMillis(50); // sleep so that the thread starts blocking in `hasNext`

        inbox.queue().add(new Object[]{42});
        producer.consume(inbox);
        assertThat(inbox).isEmpty();
        future.get();
    }

    @Test
    public void when_noNextItem_then_timeoutElapses() {
        long start = System.nanoTime();
        iterator.hasNext(500, MILLISECONDS);
        long elapsed = MILLISECONDS.toNanos(System.nanoTime() - start);
        assertThat(elapsed >= 500).isTrue();
    }

    @Test
    public void when_iteratorRequestedTheSecondTime_then_fail() {
        assertThatThrownBy(producer::iterator)
                .hasMessageContaining("can be requested only once");
    }

    @Test
    public void when_onErrorAfterDone_then_ignored() {
        producer.done();
        producer.onError(QueryException.error("error"));

        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void when_onErrorCalledTwice_then_secondIgnored() {
        producer.onError(QueryException.error("error1"));
        producer.onError(QueryException.error("error2"));

        assertThatThrownBy(iterator::hasNext)
                .hasMessageContaining("error1");
    }

    @Test
    public void when_doneCalledTwice_then_secondIgnored() {
        producer.done();
        producer.done();

        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    public void when_queueCapacityExceeded_then_inboxNotConsumed() {
        int numExcessItems = 2;
        for (int i = 0; i < JetQueryResultProducer.QUEUE_CAPACITY + numExcessItems; i++) {
            inbox.queue().add(new Object[0]);
        }
        producer.consume(inbox);
        assertThat(inbox).hasSize(2);
    }
}
