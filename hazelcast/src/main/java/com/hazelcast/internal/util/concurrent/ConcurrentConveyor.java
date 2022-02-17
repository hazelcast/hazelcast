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

package com.hazelcast.internal.util.concurrent;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Collection;
import java.util.Queue;
import java.util.function.Predicate;

import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.locks.LockSupport.unpark;

/**
 * A many-to-one conveyor of interthread messages. Allows a setup where
 * communication from N submitter threads to 1 drainer thread happens over N
 * one-to-one concurrent queues. Queues are numbered from 0 to N-1 and the queue
 * at index 0 is the <i>default</i> queue. There are some convenience methods
 * which assume the usage of the default queue.
 * <p>
 * Allows the drainer thread to signal completion and failure to the submitters
 * and make their blocking {@code submit()} calls fail with an exception. This
 * mechanism supports building an implementation which is both starvation-safe
 * and uses bounded queues with blocking queue submission.
 * <p>
 * There is a further option for the drainer to apply immediate backpressure to
 * the submitter by invoking {@link #backpressureOn()}. This will make the
 * {@code submit()} invocations block after having successfully submitted their
 * item, until the drainer calls {@link #backpressureOff()} or fails. This
 * mechanism allows the drainer to apply backpressure and keep draining the queue,
 * thus letting all submitters progress until after submitting their item. Such an
 * arrangement eliminates a class of deadlock patterns where the submitter blocks
 * to submit the item that would have made the drainer remove backpressure.
 * <p>
 * Does not manage drainer threads. There should be only one drainer thread at a
 * time.
 * <p>
 * <h3>Usage example</h3>
 * <pre>
 * // 1. Set up the concurrent conveyor
 *
 * final int queueCapacity = 128;
 * final Runnable doneItem = new Runnable() { public void run() {} };
 * final QueuedPipe&lt;Runnable&gt;[] qs = new QueuedPipe[2];
 * qs[0] = new OneToOneConcurrentArrayQueue&lt;Runnable&gt;(queueCapacity);
 * qs[1] = new OneToOneConcurrentArrayQueue&lt;Runnable&gt;(queueCapacity);
 * final ConcurrentConveyor&lt;Runnable&gt; conveyor = concurrentConveyor(doneItem, qs);
 *
 * // 2. Set up the drainer thread
 *
 * final Thread drainer = new Thread(new Runnable() {
 *     private int submitterGoneCount;
 *
 *     &#64;Override
 *     public void run() {
 *         conveyor.drainerArrived();
 *         try {
 *             final List&lt;Runnable&gt; batch = new ArrayList&lt;Runnable&gt;(queueCapacity);
 *             while (submitterGoneCount &lt; conveyor.queueCount()) {
 *                 for (int i = 0; i &lt; conveyor.queueCount(); i++) {
 *                     batch.clear();
 *                     conveyor.drainTo(i, batch);
 *                     // process(batch) should increment submitterGoneCount
 *                     // each time it encounters the conveyor.submitterGoneItem()
 *                     process(batch);
 *                 }
 *             }
 *             conveyor.drainerDone();
 *         } catch (Throwable t) {
 *             conveyor.drainerFailed(t);
 *         }
 *     }
 * });
 * drainer.start();
 *
 * // 3. Set up the submitter threads
 *
 * for (final int submitterIndex : new int[] { 0, 1, 2, 3 }) {
 *     new Thread(new Runnable() {
 *         &#64;Override
 *         public void run() {
 *             final QueuedPipe&lt;Runnable&gt; q = conveyor.queue(submitterIndex);
 *             try {
 *                 while (!askedToStop) {
 *                     conveyor.submit(q, new Item());
 *                 }
 *             } finally {
 *                 try {
 *                     conveyor.submit(q, conveyor.submitterGoneItem());
 *                 } catch (ConcurrentConveyorException e) {
 *                     // logger.warning() || rethrow || ...
 *                 }
 *             }
 *         }
 *     }).start();
 * }
 * </pre>
 */
@SuppressWarnings("checkstyle:interfaceistype")
public class ConcurrentConveyor<E> {
    /**
     * How many times to busy-spin while waiting to submit to the work queue.
     */
    public static final int SUBMIT_SPIN_COUNT = 1000;
    /**
     * How many times to yield while waiting to submit to the work queue.
     */
    public static final int SUBMIT_YIELD_COUNT = 200;
    /**
     * Max park microseconds while waiting to submit to the work queue.
     */
    public static final long SUBMIT_MAX_PARK_MICROS = 200;
    /**
     * Idling strategy used by the {@code submit()} methods.
     */
    public static final IdleStrategy SUBMIT_IDLER = new BackoffIdleStrategy(
            SUBMIT_SPIN_COUNT, SUBMIT_YIELD_COUNT, 1, MICROSECONDS.toNanos(SUBMIT_MAX_PARK_MICROS));

    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private static final Throwable REGULAR_DEPARTURE = regularDeparture();
    private final QueuedPipe<E>[] queues;
    private final E submitterGoneItem;

    private volatile boolean backpressure;
    private volatile Thread drainer;
    private volatile Throwable drainerDepartureCause;
    private volatile int liveQueueCount;

    ConcurrentConveyor(E submitterGoneItem, QueuedPipe<E>... queues) {
        if (queues.length == 0) {
            throw new IllegalArgumentException("No concurrent queues supplied");
        }
        this.submitterGoneItem = submitterGoneItem;
        this.queues = validateAndCopy(queues);
        this.liveQueueCount = queues.length;
    }

    private QueuedPipe<E>[] validateAndCopy(QueuedPipe<E>[] queues) {
        final QueuedPipe<E>[] safeCopy = new QueuedPipe[queues.length];
        for (int i = 0; i < queues.length; i++) {
            if (queues[i] == null) {
                throw new IllegalArgumentException("Queue at index " + i + " is null");
            }
            safeCopy[i] = queues[i];
        }
        return safeCopy;
    }

    /**
     * Creates a new concurrent conveyor.
     *
     * @param submitterGoneItem the object that a submitter thread can use to
     *                          signal it's done submitting
     * @param queues            the concurrent queues the conveyor will manage
     */
    public static <E1> ConcurrentConveyor<E1> concurrentConveyor(
            E1 submitterGoneItem, QueuedPipe<E1>... queues
    ) {
        return new ConcurrentConveyor<E1>(submitterGoneItem, queues);
    }

    /**
     * @return the last item that the submitter thread submits to the conveyor
     */
    public final E submitterGoneItem() {
        return submitterGoneItem;
    }

    /**
     * Returns the size of the array holding the concurrent queues. Initially
     * (when the conveyor is constructed) each array slot should point to a
     * distinct concurrent queue; therefore the size of the array matches the
     * number of queues. Some array slots may be nulled out later by calling
     * {@link #removeQueue(int)}, but this method will keep reporting the same
     * number. The intended use case for this method is giving the upper bound
     * for a loop that iterates over all queues. Since queue indices never
     * change, this number must stay the same.
     */
    public final int queueCount() {
        return queues.length;
    }

    /**
     * Returns the number of remaining live queues, i.e., {@link #queueCount()}
     * minus the number of queues nulled out by calling {@link #removeQueue(int)}.
     */
    public final int liveQueueCount() {
        return liveQueueCount;
    }

    /**
     * @return the concurrent queue at the given index
     */
    public final QueuedPipe<E> queue(int index) {
        return queues[index];
    }

    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT",
            justification = "liveQueueCount is updated only by the drainer thread")
    public final boolean removeQueue(int index) {
        final boolean didRemove = queues[index] != null;
        queues[index] = null;
        liveQueueCount--;
        return didRemove;
    }

    /**
     * Offers an item to the queue at the given index.
     *
     * @return whether the item was accepted by the queue
     */
    public final boolean offer(int queueIndex, E item) {
        return offer(queues[queueIndex], item);
    }

    /**
     * Offers an item to the given queue. No check is performed that the queue
     * actually belongs to this conveyor.
     *
     * @return whether the item was accepted by the queue
     * @throws ConcurrentConveyorException if the draining thread has already left
     */
    public final boolean offer(Queue<E> queue, E item) throws ConcurrentConveyorException {
        if (queue.offer(item)) {
            return true;
        } else {
            checkDrainerGone();
            unparkDrainer();
            return false;
        }
    }

    /**
     * Blocks until successfully inserting the given item to the given queue.
     * No check is performed that the queue actually belongs to this conveyor.
     * If the {@code #backpressure} flag is raised on this conveyor at the time
     * the item has been submitted, further blocks until the flag is lowered.
     *
     * @throws ConcurrentConveyorException if the current thread is interrupted
     * or the draining thread has already left
     */
    public final void submit(Queue<E> queue, E item) throws ConcurrentConveyorException {
        for (long idleCount = 0; !queue.offer(item); idleCount++) {
            SUBMIT_IDLER.idle(idleCount);
            checkDrainerGone();
            unparkDrainer();
            checkInterrupted();
        }
        for (long idleCount = 0; backpressure; idleCount++) {
            SUBMIT_IDLER.idle(idleCount);
            checkInterrupted();
        }
    }

    /**
     * Drains a batch of items from the default queue into the supplied collection.
     *
     * @return the number of items drained
     */
    public final int drainTo(Collection<? super E> drain) {
        return drain(queues[0], drain, Integer.MAX_VALUE);
    }

    /**
     * Drains a batch of items from the queue at the supplied index into the
     * supplied collection.
     *
     * @return the number of items drained
     */
    public final int drainTo(int queueIndex, Collection<? super E> drain) {
        return drain(queues[queueIndex], drain, Integer.MAX_VALUE);
    }

    /**
     * Drains a batch of items from the queue at the supplied index to the
     * supplied {@code itemHandler}. Stops draining, after the {@code itemHandler}
     * returns false;
     *
     * @return the number of items drained
     */
    public final int drain(int queueIndex, Predicate<? super E> itemHandler) {
        return queues[queueIndex].drain(itemHandler);
    }

    /**
     * Drains no more than {@code limit} items from the default queue into the
     * supplied collection.
     *
     * @return the number of items drained
     */
    public final int drainTo(Collection<? super E> drain, int limit) {
        return drain(queues[0], drain, limit);
    }

    /**
     * Drains no more than {@code limit} items from the queue at the supplied
     * index into the supplied collection.
     *
     * @return the number of items drained
     */
    public final int drainTo(int queueIndex, Collection<? super E> drain, int limit) {
        return drain(queues[queueIndex], drain, limit);
    }

    /**
     * Called by the drainer thread to signal that it has started draining the
     * queue.
     */
    public final void drainerArrived() {
        drainerDepartureCause = null;
        drainer = currentThread();
    }

    /**
     * Called by the drainer thread to signal that it has failed and will drain
     * no more items from the queue.
     *
     * @param t the drainer's failure
     */
    public final void drainerFailed(Throwable t) {
        if (t == null) {
            throw new NullPointerException("ConcurrentConveyor.drainerFailed(null)");
        }
        drainer = null;
        drainerDepartureCause = t;
    }


    /**
     * Called by the drainer thread to signal that it is done draining the queue.
     */
    public final void drainerDone() {
        drainer = null;
        drainerDepartureCause = REGULAR_DEPARTURE;
    }

    /**
     * @return whether the drainer thread has left
     */
    public final boolean isDrainerGone() {
        return drainerDepartureCause != null;
    }

    /**
     * Checks whether the drainer thread has left and throws an exception if it
     * has. If the drainer thread has failed, its failure will be the cause of
     * the exception thrown.
     */
    public final void checkDrainerGone() {
        final Throwable cause = drainerDepartureCause;
        if (cause == REGULAR_DEPARTURE) {
            throw new ConcurrentConveyorException("Queue drainer has already left");
        }
        propagateDrainerFailure(cause);
    }

    /**
     * Blocks until the drainer thread leaves.
     */
    public final void awaitDrainerGone() {
        for (long i = 0; !isDrainerGone(); i++) {
            SUBMIT_IDLER.idle(i);
        }
        propagateDrainerFailure(drainerDepartureCause);
    }

    /**
     * Raises the {@code backpressure} flag, which will make the caller of
     * {@code submit} to block until the flag is lowered.
     */
    public final void backpressureOn() {
        backpressure = true;
    }

    /**
     * Lowers the {@code backpressure} flag.
     */
    public final void backpressureOff() {
        backpressure = false;
    }

    private int drain(QueuedPipe<E> q, Collection<? super E> drain, int limit) {
        return q.drainTo(drain, limit);
    }

    private void unparkDrainer() {
        final Thread drainer = this.drainer;
        if (drainer != null) {
            unpark(drainer);
        }
    }

    private static void propagateDrainerFailure(Throwable cause) {
        if (cause != null && cause != REGULAR_DEPARTURE) {
            throw new ConcurrentConveyorException("Queue drainer failed", cause);
        }
    }

    private static void checkInterrupted() throws ConcurrentConveyorException {
        if (currentThread().isInterrupted()) {
            throw new ConcurrentConveyorException("Thread interrupted");
        }
    }

    private static ConcurrentConveyorException regularDeparture() {
        final ConcurrentConveyorException e = new ConcurrentConveyorException("Regular departure");
        e.setStackTrace(new StackTraceElement[0]);
        return e;
    }
}
