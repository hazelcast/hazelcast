/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl;

import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.util.ArrayDequeInbox;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.ResultIterator;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.NoSuchElementException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

import static com.hazelcast.jet.sql.SqlTestSupport.jetRow;
import static com.hazelcast.sql.impl.ResultIterator.HasNextResult.DONE;
import static com.hazelcast.sql.impl.ResultIterator.HasNextResult.TIMEOUT;
import static com.hazelcast.sql.impl.ResultIterator.HasNextResult.YES;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryResultProducerImplTest extends JetTestSupport {

    private QueryResultProducerImpl producer;
    private ResultIterator<JetSqlRow> iterator;
    private final ArrayDequeInbox inbox = new ArrayDequeInbox(new ProgressTracker());

    private void initProducer(boolean blockForNextItem) {
        producer = new QueryResultProducerImpl(blockForNextItem);
        iterator = producer.iterator();
    }

    @Test
    public void smokeTest_streaming() throws Exception {
        initProducer(false);
        Semaphore semaphore = new Semaphore(0);
        Future<?> future = spawn(() -> {
            try {
                assertThat(iterator.hasNext(0, SECONDS)).isEqualTo(TIMEOUT);
                semaphore.release();
                assertThat(iterator.hasNext()).isTrue();
                assertInstanceOf(JetSqlRow.class, iterator.next());
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

        inbox.queue().add(jetRow());
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
    public void smokeTest_blocking() throws Exception {
        initProducer(true);
        Semaphore semaphore = new Semaphore(0);
        Future<?> future = spawn(() -> {
            try {
                semaphore.release();
                assertThat(iterator.hasNext(0, SECONDS)).isEqualTo(YES);
                assertThat(iterator.next().getFieldCount()).isEqualTo(0);
                semaphore.release();
                assertThat(iterator.hasNext(0, SECONDS)).isEqualTo(DONE);
                semaphore.release();
            } catch (Throwable t) {
                logger.info("", t);
                throw t;
            }
        });

        semaphore.acquire();
        // now the spawned thread is blocked in hasNext()

        // check that the thread is blocked in `hasNext` - that it did not release the 2nd permit
        sleepMillis(50);
        assertThat(semaphore.availablePermits()).isZero();

        inbox.queue().add(jetRow());
        producer.consume(inbox);

        // 2nd permit - the row returned from the iterator
        semaphore.acquire();

        // check that the thread is blocked in the 2nd `hasNext` - that it did not release the 3rd permit
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
        initProducer(false);
        inbox.queue().add(jetRow(1));
        inbox.queue().add(jetRow(2));
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
        initProducer(false);
        assertThat(iterator.hasNext(0, SECONDS)).isEqualTo(TIMEOUT);
        producer.onError(QueryException.error("mock error"));
        assertThatThrownBy(() -> iterator.hasNext(0, SECONDS))
                .hasMessageContaining("mock error");
    }

    @Test
    public void when_doneWithErrorWhileWaiting_then_throw_sync() throws Exception {
        initProducer(false);
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
        initProducer(false);
        Future<?> future = spawn(() -> {
            assertThat(iterator.hasNext(1, DAYS)).isEqualTo(YES);
            assertThat((int) iterator.next().get(0)).isEqualTo(42);
        });
        sleepMillis(50); // sleep so that the thread starts blocking in `hasNext`

        inbox.queue().add(jetRow(42));
        producer.consume(inbox);
        assertThat(inbox).isEmpty();
        future.get();
    }

    @Test
    public void when_noNextItem_then_timeoutElapses() {
        initProducer(false);
        long start = System.nanoTime();
        iterator.hasNext(500, MILLISECONDS);
        long elapsed = MILLISECONDS.toNanos(System.nanoTime() - start);
        assertThat(elapsed >= 500).isTrue();
    }

    @Test
    public void when_iteratorRequestedTheSecondTime_then_fail() {
        initProducer(false);
        assertThatThrownBy(producer::iterator)
                .hasMessageContaining("can be requested only once");
    }

    @Test
    public void when_onErrorAfterDone_then_ignored() {
        initProducer(false);
        producer.done();
        producer.onError(QueryException.error("error"));

        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void when_onErrorCalledTwice_then_secondIgnored() {
        initProducer(false);
        producer.onError(QueryException.error("error1"));
        producer.onError(QueryException.error("error2"));

        assertThatThrownBy(iterator::hasNext)
                .hasMessageContaining("error1");
    }

    @Test
    public void when_doneCalledTwice_then_secondIgnored() {
        initProducer(false);
        producer.done();
        producer.done();

        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    public void when_queueCapacityExceeded_then_inboxNotConsumed() {
        initProducer(false);
        int numExcessItems = 2;
        for (int i = 0; i < QueryResultProducerImpl.QUEUE_CAPACITY + numExcessItems; i++) {
            inbox.queue().add(jetRow());
        }
        producer.consume(inbox);
        assertThat(inbox).hasSize(2);
    }
}
