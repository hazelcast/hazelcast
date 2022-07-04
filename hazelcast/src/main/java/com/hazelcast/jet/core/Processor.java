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

package com.hazelcast.jet.core;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.RestartableException;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.ClusterProperty;

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
 *
 * <h3>Processing methods</h3>
 *
 * When the documentation in this class refers to <em>processing methods,</em>
 * we mean all methods except for these:
 * <ul>
 *     <li>{@link #isCooperative()}
 *     <li>{@link #init(Outbox, Context)}
 *     <li>{@link #close()}
 * </ul>
 *
 * <h3>Transactional processors</h3>
 *
 * If this processor communicates with an external transactional store, after
 * the snapshot is restored and before it executes any code in a <em>processing
 * method</em>, it should rollback all transactions that this processor
 * created. It should only rollback transactions created by this vertex and
 * this job; it can use the vertex name and job ID passed to the {@link #init}
 * method in the context to filter.
 * <p>
 * <b>Determining the list of transactions to rollback</b><br/>
 * You can't store the IDs of the created transactions to the snapshot, as one
 * might intuitively think. The job might run for a while after creating a
 * snapshot and start a new transaction and we need to roll that one too. The
 * job might even fail before it creates the first snapshot.
 * <p>
 * There are multiple ways to tackle this:
 * <ul>
 *     <li>enumerate all pending transactions in the external system and
 *     rollback those that were created by this processor. For example, a file
 *     sink can list files in the directory it is writing to
 *
 *     <li>if the remote system doesn't allow us to enumerate transactions,
 *     we can use deterministic scheme for transaction ID and probe all IDs
 *     that could be used by this processor. For example: {@code jobId +
 *     vertexId + globalProcessorIndex + sequence}
 * </ul>
 *
 * <h3>How the methods are called</h3>
 *
 * Besides {@link #init}, {@link #close} and {@link #isCooperative} the methods
 * are called in a tight loop with a possibly short back-off if the method does
 * no work. "No work" is defined as adding nothing to outbox and taking nothing
 * from inbox. If you do heavy work on each call (such as querying a remote
 * service), you can do additional back-off: use {@code sleep} in a
 * non-cooperative processor or do nothing if sufficient time didn't elapse.
 *
 * @since Jet 3.0
 */
public interface Processor {

    /**
     * Tells whether this processor is able to participate in cooperative
     * multithreading. If this processor declares itself cooperative, it will
     * share a thread with other cooperative processors. Otherwise it will run
     * in a dedicated Java thread.
     * <p>
     * There are specific requirements that all <em>processing methods</em> of
     * a cooperative processor must follow:
     * <ul>
     *     <li>each call must take a reasonably small amount of time (up to a
     *     millisecond). Violations will manifest as increased latency due to
     *     slower switching of processors.
     *
     *     <li>should also not attempt any blocking operations, such as I/O
     *     operations, waiting for locks/semaphores or sleep operations.
     *     Violations of this rule will manifest as less than 100% CPU usage
     *     under maximum load (note that this is possible for other reasons
     *     too, for example if the network is the bottleneck or if {@linkplain
     *     ClusterProperty#JET_IDLE_COOPERATIVE_MAX_MICROSECONDS parking time} is
     *     too high). The processor must also return as soon as the outbox
     *     rejects an item (that is when the {@link Outbox#offer(Object)
     *     offer()} method returns {@code false}).
     * </ul>
     *
     * Non-cooperative processors are allowed to block, but still must return
     * at least once per second (that is, they should not block
     * indeterminately). If they block longer, snapshots will take longer to
     * complete and job will respond more slowly to termination: Jet doesn't
     * interrupt the dedicated threads if it wants them to cancel, it waits for
     * them to return.
     * <p>
     * Jet prefers cooperative processors because they result in a greater
     * overall throughput. A processor should be non-cooperative only if it
     * involves blocking operations, which would cause all other processors on
     * the same shared thread to starve.
     * <p>
     * Processor instances of a single vertex are allowed to return different
     * values, but a single processor instance must always return the same value.
     * <p>
     * The default implementation returns {@code true}.
     */
    default boolean isCooperative() {
        return true;
    }

    /**
     * Initializes this processor with the outbox that the <em>processing
     * methods</em> must use to deposit their output items. This method will be
     * called exactly once and strictly before any calls to other methods
     * (except for the {@link #isCooperative()} method.
     * <p>
     * Even if this processor is cooperative, this method is allowed to do
     * blocking operations.
     * <p>
     * The default implementation does nothing.
     *
     * @param context useful environment information
     */
    default void init(@Nonnull Outbox outbox, @Nonnull Context context) throws Exception {
    }

    /**
     * Called with a batch of items retrieved from an inbound edge's stream. The
     * items are in the inbox and this method may process zero or more of them,
     * removing each item after it is processed. Does not remove an item until it
     * is done with it.
     * <p>
     * If the method returns with items still present in the inbox, it will be
     * called again before proceeding to call any other method (except for
     * {@link #snapshotCommitFinish}), with the same items. In other words, no
     * more items are added to the inbox if the previous call didn't return an
     * empty inbox.
     * <p>
     * There is at least one item in the inbox when this method is called.
     * <p>
     * The default implementation throws an exception, it is suitable for source
     * processors.
     *
     * @param ordinal ordinal of the inbound edge
     * @param inbox   the inbox containing the pending items
     */
    default void process(int ordinal, @Nonnull Inbox inbox) {
        throw new UnsupportedOperationException("Missing implementation in " + getClass());
    }

    /**
     * Tries to process the supplied watermark. The value is always greater
     * than in the previous call. The watermark is delivered for processing
     * after it has been received from all the edges.
     * <p>
     * The implementation may choose to process only partially and return
     * {@code false}, in which case it will be called again later with the same
     * {@code timestamp} before any other <em>processing method</em> is called.
     * Before the method returns {@code true}, it should emit the watermark to
     * the downstream processors. Sink processors can ignore the watermark and
     * simply return {@code true}.
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
     *         {@code false} to call this method again with the same watermark
     */
    boolean tryProcessWatermark(@Nonnull Watermark watermark);

    /**
     * This method will be called periodically and only when the current batch
     * of items in the inbox has been exhausted. It can be used to produce
     * output in the absence of input or to do general maintenance work. If the
     * job restores state from a snapshot, this method is called for the first
     * time after {@link #finishSnapshotRestore()}.
     * <p>
     * If the call returns {@code false}, it will be called again before
     * proceeding to call any other <em>processing method</em>. Default
     * implementation returns {@code true}.
     */
    default boolean tryProcess() {
        return true;
    }

    /**
     * Called after the edge input with the supplied {@code ordinal} is
     * exhausted. If it returns {@code false}, it will be called again before
     * proceeding to call any other method.
     * <p>
     * If this method tried to offer to the outbox and the offer call returned
     * false, this method must also return false and retry the offer in the
     * next call.
     *
     * @return {@code true} if the processor is now done completing the edge,
     *         {@code false} to call this method again
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
     * called even though this method returned {@code false}.
     * <p>
     * After this method is called, no other processing methods are called on
     * this processor, except for {@link #snapshotCommitFinish}.
     * <p>
     * Non-cooperative processors are required to return from this method from
     * time to time to give the system a chance to check for snapshot requests
     * and job cancellation. The time the processor spends in this method affects
     * the latency of snapshots and job cancellations.
     *
     * @return {@code true} if the completing step is now done, {@code false}
     *         to call this method again
     */
    default boolean complete() {
        return true;
    }

    /**
     * Stores the processor's state to a state snapshot by adding items to the
     * outbox's {@linkplain Outbox#offerToSnapshot(Object, Object) snapshot
     * bucket}. If this method returns {@code false}, it will be called again
     * before proceeding to call any other method.
     * <p>
     * This method will only be called after a call to {@link #process(int,
     * Inbox) process()} returns with an empty inbox. After all the input is
     * exhausted, it is also called between {@link #complete()} calls. Once
     * {@code complete()} returns {@code true}, this method won't be called
     * anymore.
     * <p>
     * The default implementation does nothing and returns {@code true}.
     *
     * @return {@code true} if this step is done, {@code false} to call this
     *      method again
     */
    default boolean saveToSnapshot() {
        return true;
    }

    /**
     * Prepares the transactions for commit after the snapshot is completed. If
     * the processor doesn't use transactions, it can just return {@code true}
     * or rely on the no-op default implementation. This is the first phase of
     * a two-phase commit.
     * <p>
     * This method is called right after {@link #saveToSnapshot()}. After
     * this method returns {@code true}, Jet will return to call the processing
     * methods again. Some time later, {@link #snapshotCommitFinish} will be
     * called.
     * <p>
     * When this processor communicates with an external transactional
     * store, it should do the following:
     *
     * <ul>
     *     <li>mark the current active transaction with the external system as
     *     <em>prepared</em> and stop using it. The prepared transaction will
     *     be committed when {@link #snapshotCommitFinish} with {@code
     *     commitTransactions == true} is called
     *
     *     <li>store IDs of the pending transaction(s) to the snapshot. Note
     *     that there can be multiple prepared transactions if the previous
     *     snapshot completed with {@code commitTransactions == false}
     *
     *     <li>optionally, start a new active transaction that will be used to
     *     handle input or produce output until {@code onSnapshotCompleted()}
     *     is called. If the implementation doesn't start a new active
     *     transaction, it can opt to not process more input or emit any output
     * </ul>
     *
     * This method is skipped if the snapshot was initiated using {@link
     * Job#exportSnapshot}. If this method is skipped, {@link
     * #snapshotCommitFinish} will be skipped too.
     *
     * @return {@code true} if this step is done, {@code false} to call this
     *      method again
     * @since Jet 4.0
     */
    default boolean snapshotCommitPrepare() {
        return true;
    }

    /**
     * This is the second phase of a two-phase commit. Jet calls it after the
     * snapshot was successfully saved on all other processors in the job on
     * all cluster members.
     * <p>
     * This method can be called even when the {@link #process(int, Inbox)
     * process()} method didn't process the items in the inbox. For this reason
     * this method must not add any items to the outbox. It is also called
     * between {@link #complete()} calls. Once {@code complete()} returns
     * {@code true}, this method can still be called to finish the snapshot
     * that was started before this processor completed.
     * <p>
     * The processor should do the following:
     *
     * <ul>
     *     <li>if {@code success == true}, it should commit the prepared
     *     transactions. It must not continue to use the just-committed
     *     transaction ID - we stored it in the latest snapshot and after
     *     restart we commit the transactions with IDs found in the snapshot -
     *     we would commit the items written after the snapshot.
     *
     *     <li>if {@code success == false}, it should do nothing to the
     *     prepared transactions. If it didn't create a new active transaction
     *     in {@link #saveToSnapshot}, it can continue to use the last active
     *     transaction as active.
     * </ul>
     * <p>
     * The method is called repeatedly until it eventually returns {@code
     * true}. No other method on this processor will be called before it
     * returns {@code true}.
     *
     * <h4>Error handling</h4>
     *
     * The two-phase commit protocol requires that the second phase must
     * eventually succeed. If you're not able to commit your transactions now,
     * you should either return {@code false} and try again later, or you can
     * throw a {@link RestartableException} to cause a job restart; the
     * processor is required to commit the transactions with IDs stored in the
     * state snapshot after the restart in {@link #restoreFromSnapshot}. This
     * is necessary to ensure exactly-once processing of transactional
     * processors.
     * <p>
     * The default implementation takes no action and returns {@code true}.
     *
     * @param success true, if the first snapshot phase completed successfully
     * @return {@code true} if this step is done, {@code false} to call this
     *      method again
     * @since Jet 4.0
     */
    default boolean snapshotCommitFinish(boolean success) {
        return true;
    }

    /**
     * Called when a batch of items is received during the "restore from
     * snapshot" operation. The type of items in the inbox is {@code
     * Map.Entry}, key and value types are exactly as they were saved in {@link
     * #saveToSnapshot()}. This method may emit items to the outbox.
     * <p>
     * If this method returns with items still present in the inbox, it will
     * be called again before proceeding to call any other methods. No more
     * items are added to the inbox if the method didn't return with an empty
     * inbox. It is never called with an empty inbox. After all items are
     * processed, {@link #finishSnapshotRestore()} is called.
     * <p>
     * If a transaction ID saved in {@link #snapshotCommitPrepare()} is
     * restored, this method should commit that transaction. If the processor
     * is unable to commit those transactions, data loss or duplication might
     * occur. The processor must be ready to restore a transaction ID that no
     * longer exists in the remote system: either because the transaction was
     * already committed (this is the most common case) or because the
     * transaction timed out in the remote system. Also the job ID, if it's
     * part of the transaction ID, can be different from the current job ID, if
     * the job was {@linkplain JobConfig#setInitialSnapshotName started from an
     * exported state}. These cases should be handled gracefully.
     * <p>
     * The default implementation throws an exception - if you emit
     * something in {@link #saveToSnapshot()}, you must be able to handle it
     * here. If you don't override {@link #saveToSnapshot()}, throwing an
     * exception here will never happen.
     */
    default void restoreFromSnapshot(@Nonnull Inbox inbox) {
        throw new JetException("Processor " + getClass().getName()
                + " does not override the restoreFromSnapshot() method");
    }

    /**
     * Called after a job was restarted from a snapshot and the processor has
     * consumed all the snapshot data in {@link #restoreFromSnapshot}.
     * <p>
     * If this method returns {@code false}, it will be called again before
     * proceeding to call any other methods.
     * <p>
     * If this method tried to offer to the outbox and the offer call returned
     * false, this method must also return false and retry the offer in the
     * next call.
     * <p>
     * The default implementation takes no action and returns {@code true}.
     *
     * @return {@code true} if this step is done, {@code false} to call this
     *      method again
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
     * See {@link #closeIsCooperative()} regarding the cooperative behavior of
     * this method.
     * <p>
     * If this method throws an exception, it is logged but it won't be
     * reported as a job failure or cause the job to fail.
     * <p>
     * The default implementation does nothing.
     */
    default void close() throws Exception {
    }

    /**
     * Returns {@code true} if the {@link #close()} method of this processor is
     * cooperative. If it's not, the call to the {@code close()} method is
     * off-loaded to another thread.
     * <p>
     * This flag is ignored for non-cooperative processors.
     */
    default boolean closeIsCooperative() {
        return false;
    }

    /**
     * Context passed to the processor in the
     * {@link #init(Outbox, Context) init()} call.
     *
     * @since Jet 3.0
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
         * The value is in the range {@code [0...totalParallelism-1]}. For
         * example if there are 2 members in the cluster and {@linkplain
         * #localParallelism() local parallelism} is 4, the processors on the
         * 1st cluster member will have indexes 0..3 and on the second member
         * they will have indexes 4..7.
         */
        int globalProcessorIndex();

        /**
         * Returns the slice of partitions for this processor. It distributes
         * {@link #memberPartitions()} according to the {@link #localParallelism()}
         * and {@link #localProcessorIndex()}.
         */
        default int[] processorPartitions() {
            int[] memberPartitions = memberPartitions();
            int[] res = new int[memberPartitions.length / localParallelism()
                    + (memberPartitions.length % localParallelism() > localProcessorIndex() ? 1 : 0)];
            for (int i = localProcessorIndex(), j = 0; i < memberPartitions.length; i += localParallelism(), j++) {
                res[j] = memberPartitions[i];
            }
            return res;
        }
    }
}
