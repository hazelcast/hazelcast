/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.internal.tpcengine.util.ReflectionUtil;
import com.hazelcast.map.impl.operation.MapOperation;

import javax.annotation.Nullable;
import java.lang.invoke.VarHandle;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.util.ThreadUtil.isRunningOnPartitionThread;

/**
 * Per record-store state of the map-store offloaded-operations
 * machinery (see {@link
 * com.hazelcast.map.impl.operation.steps.engine.StepRunner StepRunner}).
 * <p>
 * It maintains two invariants:
 * <ul>
 *     <li>At most one step-chain is executing against this record
 *     store at any time, even though a chain hops between the
 *     partition thread and offloaded executors. This is enforced
 *     by the {@link #executionInProgress} flag which is acquired
 *     when a chain starts and released only when the queue is
 *     drained (or the chain dies).</li>
 *     <li>A single operation instance is never executed by two
 *     chains. Re-submissions of an already queued or running
 *     instance (e.g. by invocation retry while the operation is
 *     parked in an offloaded step) are rejected by
 *     {@link #offer}.</li>
 * </ul>
 * Thread-safety: {@link #offer}, {@link #peekNext}, {@link #complete}
 * and {@link #tryAcquire} must only be called from the partition
 * thread, while {@link #release} may be called from a non-partition
 * thread only when a chain dies unexpectedly.
 */
public final class OffloadedOperations {

    private static final VarHandle SIZE = ReflectionUtil.findVarHandle("size", int.class);

    /**
     * Queued and currently-running operations, in submission order.
     * <p>
     * An operation stays here from {@link #offer} until
     * {@link #complete}, so the first element is the operation the
     * active chain is currently executing (or about to execute), and
     * set-membership rejects duplicate submissions of an in-flight
     * instance.
     * <p>
     * {@link MapOperation}s do not override {@code equals}/{@code
     * hashCode}, so this behaves as an insertion-ordered
     * <em>identity</em> set; the duplicate detection relies on that.
     */
    private final Set<MapOperation> operations = new LinkedHashSet<>();

    /**
     * {@code true} while the queued operations are being executed.
     * Execution spans all steps of all queued operations, across
     * partition and offloaded threads; the flag is only released
     * when the queue is drained. It is an {@link AtomicBoolean}
     * because release can happen from a non-partition thread when
     * execution dies unexpectedly.
     */
    private final AtomicBoolean executionInProgress = new AtomicBoolean();

    /**
     * Number of queued + running operations. Kept as a single-writer
     * counter so it can be read safely from non-partition threads
     * (stats, tests).
     */
    private volatile int size;

    /**
     * Queues the given operation unless this exact instance is
     * already queued or running.
     *
     * @return {@code true} if the operation was queued, {@code false}
     * if it is a duplicate submission of an in-flight instance
     */
    public boolean offer(MapOperation operation) {
        assert isRunningOnPartitionThread();

        if (!operations.add(operation)) {
            return false;
        }
        SIZE.setOpaque(this, operations.size());
        return true;
    }

    /**
     * @return the next operation to execute, or {@code null} if the
     * queue is drained. The operation is <em>not</em> removed: it
     * stays in this set — guarding against duplicate submissions —
     * until {@link #complete} is called for it. Callers must
     * therefore complete the previous operation before peeking the
     * next one.
     */
    @Nullable
    public MapOperation peekNext() {
        assert isRunningOnPartitionThread();

        Iterator<MapOperation> iterator = operations.iterator();
        return iterator.hasNext() ? iterator.next() : null;
    }

    /**
     * Marks the given operation's step-chain as finished (its
     * response has been sent), allowing future re-submissions
     * of the same instance to be queued again.
     */
    public void complete(MapOperation operation) {
        assert isRunningOnPartitionThread();

        if (operations.remove(operation)) {
            SIZE.setOpaque(this, operations.size());
        }
    }

    /**
     * Tries to take chain-ownership of this record store.
     * <p>
     * Callers must start executing steps only after this
     * returns {@code true}, and must eventually call
     * {@link #release} exactly once.
     */
    public boolean tryAcquire() {
        assert isRunningOnPartitionThread();

        return executionInProgress.compareAndSet(false, true);
    }

    /**
     * Releases chain-ownership. Called on the partition thread when
     * the queue is drained; may also be called from other threads as
     * a safety valve when a chain dies unexpectedly.
     */
    public void release() {
        boolean released = executionInProgress.compareAndSet(true, false);
        assert released : "release() was called while no execution was in progress";
    }

    /**
     * @return number of queued + running offloaded operations. Safe
     * to call from non-partition threads.
     */
    public int size() {
        return size;
    }
}
