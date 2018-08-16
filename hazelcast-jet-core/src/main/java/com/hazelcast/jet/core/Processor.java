/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
     * operations. Violations of this rule will manifest as less than 100% CPU
     * usage under maximum load. The processor must also return as soon as the
     * outbox rejects an item (that is when the {@link Outbox#offer(Object)
     * offer()} method returns {@code false}).
     * <p>
     * If this processor declares itself cooperative, it will share a thread
     * with other cooperative processors. Otherwise it will run in a dedicated
     * Java thread.
     * <p>
     * Jet prefers cooperative processors because they result in a greater
     * overall throughput. A processor should be non-cooperative only if it
     * involves blocking operations, which would cause all other processors on
     * the same shared thread to starve.
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
     * Tries to process the supplied watermark. The value is always greater
     * than in the previous call. The watermark is delivered for processing
     * after it has been received from all the edges or the {@link
     * com.hazelcast.jet.config.JobConfig#setMaxWatermarkRetainMillis(int)
     * maximum retention time} has elapsed.
     * <p>
     * The implementation may choose to process only partially and return
     * {@code false}, in which case it will be called again later with the same
     * {@code timestamp} before any other processing method is called. When the
     * method returns {@code true}, the watermark is forwarded to the
     * downstream processors.
     * <p>
     * The default implementation just returns {@code true}.
     *
     * <h3>Caution for Jobs With the At-Least-Once Guarantee</h3>
     * Jet propagates the value of the watermark by sending <em>watermark
     * items</em> interleaved with the regular stream items. If a job
     * configured with the <i>at-least-once</i> processing guarantee gets
     * restarted, the same watermark, like any other stream item, can be
     * delivered again. Therefore the processor may be asked to process
     * a watermark older than the one it had already processed before the
     * restart.
     *
     * @param watermark watermark to be processed
     * @return {@code true} if this watermark has now been processed,
     *         {@code false} otherwise.
     */
    default boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return true;
    }

    /**
     * This method will be called periodically and only when the current batch
     * of items in the inbox has been exhausted. It can be used to produce
     * output in the absence of input or to do general maintenance work.
     * <p>
     * If the call returns {@code false}, it will be called again before proceeding
     * to call any other method. Default implementation returns {@code true}.
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
     * For example, a streaming source processor will return {@code false}
     * forever. Unlike other methods which guarantee that no other method is
     * called until they return {@code true}, {@link #saveToSnapshot()} can be
     * called even though this method returned {@code false}. If you returned
     * because {@code Outbox.offer()} returned {@code false}, make sure to
     * first offer the pending item to the outbox in {@link #saveToSnapshot()}
     * before continuing to {@linkplain Outbox#offerToSnapshot offer to
     * snapshot}.
     *
     *<p>
     * After this method is called, no other processing methods are called on
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
     * Stores its snapshotted state by adding items to the outbox's {@linkplain
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
     * <b>Note:</b> if you returned from {@link #complete()} because some of
     * the {@code Outbox.offer()} method returned false, you need to make sure
     * to re-offer the pending item in this method before offering any items to
     * {@link Outbox#offerToSnapshot}.
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
     * Called as the last method in the processor lifecycle. It is called
     * whether the job was successful or not, and strictly before {@link
     * ProcessorSupplier#close} is called on this member. The method might get
     * called even if {@link #init} method was not yet called.
     * <p>
     * The method will be called right after {@link #complete()} returns {@code
     * true}, that is before the job is finished. The job might still be
     * running other processors.
     * <p>
     * If this method throws an exception, it is logged but it won't be
     * reported as a job failure or cause the job to fail.
     */
    default void close() throws Exception {
    }

    /**
     * Context passed to the processor in the
     * {@link #init(Outbox, Context) init()} call.
     */
    interface Context extends ProcessorSupplier.Context {

        /**
         *  Return a logger for the processor
         */
        @Nonnull
        ILogger logger();

        /**
         * Returns the index of the processor among all the processors created for
         * this vertex on a single node: it's a unique node-wide index.
         * <p>
         * The value is in the range {@code [0...localParallelism-1]}.
         */
        int localProcessorIndex();

        /**
         * Returns the index of the processor among all the processors created for
         * this vertex on all nodes: it's a unique cluster-wide index.
         * <p>
         * The value is in the range {@code [0...totalParallelism-1]}.
         */
        int globalProcessorIndex();

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
