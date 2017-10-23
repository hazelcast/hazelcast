/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.core;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;

/**
 * When Jet executes a DAG, it creates one or more instances of {@code
 * Processor} on each cluster member to do the work of a given vertex. The
 * vertex's {@code localParallelism} property controls the number of
 * processors per member.
 * <p>
 * The processor is a single-threaded processing unit that performs the
 * computation needed to transform zero or more input data streams into
 * zero or more output streams. Each input/output stream corresponds to
 * an edge on the vertex. The correspondence between a stream and an
 * edge is established via the edge's <em>ordinal</em>.
 * <p>
 * The special case of zero input streams applies to a <em>source</em>
 * vertex, which gets its data from the environment. The special case of
 * zero output streams applies to a <em>sink</em> vertex, which pushes its
 * data to the environment.
 * <p>
 * The processor accepts input from instances of {@link Inbox} and pushes
 * its output to an instance of {@link Outbox}.
 * <p>
 * See the {@link #isCooperative()} for important restrictions to how the
 * processor should work.
 */
public interface Processor {

    /**
     * Tells whether this processor is able to participate in cooperative
     * multithreading. This means that each invocation of a processing method
     * will take a reasonably small amount of time (up to a millisecond).
     * Violations will manifest themselves as increased latency due to slower
     * switching of processors.
     * <p>
     * A cooperative processor should also not attempt any blocking operations,
     * such as I/O operations, waiting for locks/semaphores or sleep
     * operations. Violations to this rule will manifest themselves as less
     * than 100% CPU usage under maximum load. The processor should also return
     * as soon as an item is rejected by the outbox (that is when the {@link
     * Outbox#offer(Object) offer()} method returns {@code false}).
     * <p>
     * If this processor declares itself cooperative, it will share a thread
     * with other cooperative processors. Otherwise it will run in a dedicated
     * Java thread.
     * <p>
     * Jet prefers cooperative processors because they result in greater overall
     * throughput. A processor should be non-cooperative only if it involves
     * blocking operations, which would cause all other processors on the same
     * shared thread to starve.
     * <p>
     * Processor instances on single vertex are allowed to return different
     * value, but single processor instance must return constant value.
     * <p>
     * The default implementation returns {@code true}.
     */
    default boolean isCooperative() {
        return true;
    }

    /**
     * Initializes this processor with the outbox that the processing methods
     * must use to deposit their output items. This method will be called
     * exactly once and strictly before any calls to processing methods ({@link
     * #process(int, Inbox)} and {@link #complete()}).
     * <p>
     * The default implementation does nothing.
     */
    default void init(@Nonnull Outbox outbox, @Nonnull Context context) {
    }

    /**
     * Called with a batch of items retrieved from an inbound edge's stream. The
     * items are in the inbox and this method may process zero or more of them,
     * removing each item after it is processed. Does not remove an item until it
     * is done with it.
     * <p>
     * If the method returns with items still present in the inbox, it will be
     * called again before proceeding to call any other methods. There is at
     * least one item in the inbox when this method is called.
     * <p>
     * The default implementation does nothing.
     *
     * @param ordinal ordinal of the inbound edge
     * @param inbox   the inbox containing the pending items
     */
    default void process(int ordinal, @Nonnull Inbox inbox) {
    }

    /**
     * Called when there is no pending data in the inbox. Allows the processor
     * to produce output in the absence of input. If it returns {@code false},
     * it will be called again before proceeding to call any other method.
     */
    default boolean tryProcess() {
        return true;
    }

    /**
     * Called after the edge input with the supplied {@code ordinal} is
     * exhausted. If it returns {@code false}, it will be called again before
     * proceeding to call any other method.
     *
     * @return {@code true} if the processor is now done completing the edge,
     *         {@code false} otherwise.
     */
    default boolean completeEdge(int ordinal) {
        return true;
    }

    /**
     * Called after all the inbound edges' streams are exhausted. If it returns
     * {@code false}, it will be invoked again until it returns {@code true}.
     * After this method is called, no other processing methods will be called on
     * this processor, except for {@link #saveToSnapshot()}.
     * <p>
     * Non-cooperative processors are required to return from this method from
     * time to time to give the system a chance to check for snapshot requests
     * and job cancellation. The time the processor spends in this method affects
     * the latency of snapshots and job cancellations.
     *
     * @return {@code true} if the completing step is now done, {@code false}
     *         otherwise.
     */
    default boolean complete() {
        return true;
    }

    /**
     * Stores its snapshotted state by adding items to the outbox's {@link
     * Outbox#offerToSnapshot(Object, Object) snapshot bucket}. If it returns
     * {@code false}, it will be called again before proceeding to call any
     * other method.
     * <p>
     * This method will only be called after a call to {@link #process(int,
     * Inbox) process()} returns and the inbox is empty. After all the input is
     * exhausted, it may also be called between {@link #complete()} calls. Once
     * {@code complete()} returns {@code true}, this method won't be called
     * anymore.
     * <p>
     * The default implementation takes no action and returns {@code true}.
     */
    default boolean saveToSnapshot() {
        return true;
    }

    /**
     * Called when a batch of items is received during the "restore from
     * snapshot" operation. The type of items in the inbox is {@code
     * Map.Entry}. May emit items to the outbox.
     * <p>
     * If it returns with items still present in the inbox, it will be
     * called again before proceeding to call any other methods. It is
     * never called with an empty inbox.
     * <p>
     * The default implementation throws an exception.
     */
    default void restoreFromSnapshot(@Nonnull Inbox inbox) {
        throw new JetException("Processor " + getClass().getName()
                + " does not override the restoreFromSnapshot() method");
    }

    /**
     * Called after a job was restarted from a snapshot and the processor
     * has consumed all the snapshot data.
     * <p>
     * If it returns {@code false}, it will be called again before proceeding
     * to call any other methods.
     * <p>
     * The default implementation takes no action and returns {@code true}.
     */
    default boolean finishSnapshotRestore() {
        return true;
    }

    /**
     * Context passed to the processor in the
     * {@link #init(Outbox, Context) init()} call.
     */
    interface Context {

        /**
         * Returns the current Jet instance
         */
        @Nonnull
        JetInstance jetInstance();

        /**
         *  Return a logger for the processor
         */
        @Nonnull
        ILogger logger();

        /**
         * Returns the index of the processor among all the processors created for
         * this vertex on all nodes: its unique cluster-wide index.
         */
        int globalProcessorIndex();

        /***
         * Returns the name of the vertex associated with this processor.
         */
        @Nonnull
        String vertexName();

        /**
         * Returns true, if snapshots will be saved for this job.
         */
        default boolean snapshottingEnabled() {
            return processingGuarantee() != ProcessingGuarantee.NONE;
        }

        /**
         * Returns the guarantee for current job.
         */
        ProcessingGuarantee processingGuarantee();
    }
}
