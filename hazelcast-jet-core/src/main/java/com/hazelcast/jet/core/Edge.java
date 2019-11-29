/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.impl.MasterJobContext;
import com.hazelcast.jet.impl.execution.init.CustomClassLoadedObject;
import com.hazelcast.jet.impl.util.ConstantFunctionEx;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.core.Partitioner.defaultPartitioner;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;

/**
 * Represents an edge between two {@link Vertex vertices} in a {@link DAG}.
 * Conceptually, data travels over the edge from the source vertex to the
 * destination vertex. Practically, since the vertex is distributed across
 * the cluster and across threads in each cluster member, the edge is
 * implemented by a number of concurrent queues and network sender/receiver
 * pairs.
 * <p>
 * It is often desirable to arrange that all items belonging to the same
 * collation key are received by the same processing unit (instance of
 * {@link Processor}). This is achieved by configuring an appropriate
 * {@link Partitioner} on the edge. The partitioner will determine the
 * partition ID of each item and all items with the same partition ID will
 * be routed to the same {@code Processor} instance. Depending on the value
 * of edge's <em>distributed</em> property, the processor will be unique
 * cluster-wide, or only within each member.
 * <p>
 * A newly instantiated Edge is non-distributed with a {@link
 * RoutingPolicy#UNICAST UNICAST} routing policy.
 *
 * @since 3.0
 */
public class Edge implements IdentifiedDataSerializable {

    private Vertex source; // transient field, restored during DAG deserialization
    private String sourceName;
    private int sourceOrdinal;

    private Vertex destination; // transient field, restored during DAG deserialization
    private String destName;
    private int destOrdinal;

    private int priority;
    private boolean isDistributed;
    private Partitioner<?> partitioner;
    private RoutingPolicy routingPolicy = RoutingPolicy.UNICAST;

    private EdgeConfig config;

    protected Edge() {
    }

    protected Edge(@Nonnull Vertex source, int sourceOrdinal, Vertex destination, int destOrdinal) {
        this.source = source;
        this.sourceName = source.getName();
        this.sourceOrdinal = sourceOrdinal;

        this.destination = destination;
        this.destName = destination != null ? destination.getName() : null;
        this.destOrdinal = destOrdinal;
    }

    /**
     * Returns an edge between two vertices. The ordinal of the edge
     * is 0 at both ends. Equivalent to {@code from(source).to(destination)}.
     *
     * @param source        the source vertex
     * @param destination   the destination vertex
     */
    @Nonnull
    public static Edge between(@Nonnull Vertex source, @Nonnull Vertex destination) {
        return new Edge(source, 0, destination, 0);
    }

    /**
     * Returns an edge with the given source vertex and no destination vertex.
     * The ordinal of the edge is 0. Typically followed by one of the
     * {@code to()} method calls.
     */
    @Nonnull
    public static Edge from(@Nonnull Vertex source) {
        return from(source, 0);
    }

    /**
     * Returns an edge with the given source vertex at the given ordinal
     * and no destination vertex. Typically follewed by a call to one of
     * the {@code to()} methods.
     */
    @Nonnull
    public static Edge from(@Nonnull Vertex source, int ordinal) {
        return new Edge(source, ordinal, null, 0);
    }

    /**
     * Sets the destination vertex of this edge, with ordinal 0.
     */
    @Nonnull
    public Edge to(@Nonnull Vertex destination) {
        this.destination = destination;
        this.destName = destination.getName();
        return this;
    }

    /**
     * Sets the destination vertex and ordinal of this edge.
     */
    @Nonnull
    public Edge to(@Nonnull Vertex destination, int ordinal) {
        this.destination = destination;
        this.destName = destination.getName();
        this.destOrdinal = ordinal;
        return this;
    }

    /**
     * Returns this edge's source vertex.
     */
    @Nonnull
    public Vertex getSource() {
        return source;
    }

    /**
     * Returns this edge's destination vertex.
     */
    public Vertex getDestination() {
        return destination;
    }

    /**
     * Returns the name of the source vertex.
     */
    @Nonnull
    public String getSourceName() {
        return sourceName;
    }

    /**
     * Returns the ordinal of the edge at the source vertex.
     */
    public int getSourceOrdinal() {
        return sourceOrdinal;
    }

    /**
     * Returns the name of the destination vertex.
     */
    public String getDestName() {
        return destName;
    }

    /**
     * Returns the ordinal of the edge at the destination vertex.
     */
    public int getDestOrdinal() {
        return destOrdinal;
    }

    /**
     * Sets the priority of the edge. A lower number means higher priority
     * and the default is 0.
     * <p>
     * Example: there two incoming edges on a vertex, with priorities 1 and 2.
     * The data from the edge with priority 1 will be processed in full before
     * accepting any data from the edge with priority 2.
     *
     * <h4>Possible deadlock</h4>
     *
     * If you split the output of one source vertex and later join the streams
     * with different priorities, you're very likely to run into a deadlock. Consider this DAG:
     * <pre>
     * S --+---- V1 ----+--- J
     *      \          /
     *       +-- V2 --+
     * </pre>

     * The vertex {@code J} joins the streams, that were originally split from
     * source {@code S}. Let's say the input from {@code V1} has higher
     * priority than the input from {@code V2}. In this case, no item from
     * {@code V2} will be processed by {@code J} before {@code V1} completes,
     * which presupposes that {@code S} also completes. But {@code S} cannot
     * complete, because it can't emit all items to {@code V2} because {@code
     * V2} is blocked by {@code J}, which is not processing its items. This is
     * a deadlock.
     * <p>
     * This DAG can work only if {@code S} emits as few items into both paths
     * as can fit into the queues (see {@linkplain EdgeConfig#setQueueSize
     * queue size configuration}.
     *
     * <h4>Note</h4>

     * Having different priority edges will cause postponing of
     * the first snapshot until after upstream vertices of higher priority
     * edges are completed.
     * Reason: after receiving a {@link
     * com.hazelcast.jet.impl.execution.SnapshotBarrier barrier} we stop
     * processing items on that edge until the barrier is received from all
     * other edges. However, we also don't process lower priority edges until
     * higher priority edges are done, which prevents receiving the barrier on
     * them, which in the end stalls the job indefinitely. Technically this
     * applies only to {@link
     * com.hazelcast.jet.config.ProcessingGuarantee#EXACTLY_ONCE EXACTLY_ONCE}
     * snapshot mode, but the snapshot is also postponed for {@link
     * com.hazelcast.jet.config.ProcessingGuarantee#AT_LEAST_ONCE
     * AT_LEAST_ONCE} jobs, because the snapshot won't complete until after all
     * higher priority edges are completed and will increase the number of
     * duplicately processed items.
     */
    @Nonnull
    public Edge priority(int priority) {
        if (priority == MasterJobContext.SNAPSHOT_RESTORE_EDGE_PRIORITY) {
            throw new IllegalArgumentException("priority must not be Integer.MIN_VALUE ("
                    + MasterJobContext.SNAPSHOT_RESTORE_EDGE_PRIORITY + ')');
        }
        this.priority = priority;
        return this;
    }

    /**
     * Returns the value of edge's <em>priority</em>, as explained on
     * {@link #priority(int)}.
     */
    public int getPriority() {
        return priority;
    }

    /**
     * Chooses the {@link RoutingPolicy#UNICAST UNICAST} routing policy for
     * this edge.
     */
    @Nonnull
    public Edge unicast() {
        routingPolicy = RoutingPolicy.UNICAST;
        return this;
    }

    /**
     * Activates the {@link RoutingPolicy#PARTITIONED PARTITIONED} routing
     * policy and applies the {@link Partitioner#defaultPartitioner() default}
     * Hazelcast partitioning strategy. The strategy is applied to the result of
     * the {@code extractKeyFn} function.
     */
    @Nonnull
    public <T> Edge partitioned(@Nonnull FunctionEx<T, ?> extractKeyFn) {
        // optimization for ConstantFunctionEx
        if (extractKeyFn instanceof ConstantFunctionEx) {
            return allToOne(extractKeyFn.apply(null));
        }
        return partitioned(extractKeyFn, defaultPartitioner());
    }

    /**
     * Activates the {@link RoutingPolicy#PARTITIONED PARTITIONED} routing
     * policy and applies the provided partitioning strategy. The strategy
     * is applied to the result of the {@code extractKeyFn} function.
     */
    @Nonnull
    public <T, K> Edge partitioned(
            @Nonnull FunctionEx<T, K> extractKeyFn,
            @Nonnull Partitioner<? super K> partitioner
    ) {
        checkSerializable(extractKeyFn, "extractKeyFn");
        checkSerializable(partitioner, "partitioner");
        this.routingPolicy = RoutingPolicy.PARTITIONED;
        this.partitioner = new KeyPartitioner<>(extractKeyFn, partitioner, toDebugString());
        return this;
    }

    /**
     * Activates a special-cased {@link RoutingPolicy#PARTITIONED PARTITIONED}
     * routing policy where all items will be routed to the same partition ID,
     * determined from the given {@code key}. It means that all items will be
     * directed to the same processor and other processors will be idle.
     * <p>
     * It is equivalent to using {@code partitioned(t -> key)}, but it a
     * has small optimization that the partition ID is not recalculated
     * for each stream item.
     */
    @Nonnull
    public Edge allToOne(Object key) {
        return partitioned(wholeItem(), new Single(key));
    }

    /**
     * Activates the {@link RoutingPolicy#BROADCAST BROADCAST} routing policy.
     */
    @Nonnull
    public Edge broadcast() {
        routingPolicy = RoutingPolicy.BROADCAST;
        return this;
    }

    /**
     * Activates the {@link RoutingPolicy#ISOLATED ISOLATED} routing policy
     * which establishes isolated paths from upstream to downstream processors.
     * Each downstream processor is assigned exactly one upstream processor and
     * each upstream processor is assigned a disjoint subset of downstream
     * processors. This allows the selective application of backpressure to
     * just one source processor that feeds a given downstream processor.
     * <p>
     * These restrictions imply that the downstream's local parallelism
     * cannot be less than upstream's. Since all traffic will be local, this
     * policy is not allowed on a distributed edge.
     */
    @Nonnull
    public Edge isolated() {
        routingPolicy = RoutingPolicy.ISOLATED;
        return this;
    }

    /**
     * Returns the instance encapsulating the partitioning strategy in effect
     * on this edge.
     */
    public Partitioner<?> getPartitioner() {
        return partitioner;
    }

    /**
     * Returns the {@link RoutingPolicy} in effect on the edge.
     */
    @Nonnull
    public RoutingPolicy getRoutingPolicy() {
        return routingPolicy;
    }

    /**
     * Declares that the edge is distributed. A non-distributed edge only
     * transfers data within the same member. If the data source running on
     * local member is distributed (produces only a slice of all the data on
     * any given member), the local processors will not observe all the data.
     * The same holds true when the data originates from an upstream
     * distributed edge.
     * <p>
     * A <em>distributed</em> edge allows all the data to be observed by all
     * the processors (using the {@link RoutingPolicy#BROADCAST BROADCAST}
     * routing policy) and, more attractively, all the data with a given
     * partition ID to be observed by the same unique processor, regardless of
     * whether it is running on the local or a remote member (using the {@link
     * RoutingPolicy#PARTITIONED PARTITIONED} routing policy).
     */
    public Edge distributed() {
        isDistributed = true;
        return this;
    }

    /**
     * Says whether this edge is <em>distributed</em>. The effects of this
     * property are discussed in {@link #distributed()}.
     */
    public boolean isDistributed() {
        return isDistributed;
    }

    /**
     * Returns the {@code EdgeConfig} instance associated with this edge.
     * Default value is {@code null}.
     */
    @Nullable
    public EdgeConfig getConfig() {
        return config;
    }

    /**
     * Assigns an {@code EdgeConfig} to this edge. If {@code null} is supplied,
     * the edge will use {@link JetConfig#getDefaultEdgeConfig()}.
     */
    public Edge setConfig(@Nullable EdgeConfig config) {
        this.config = config;
        return this;
    }

    @Nonnull @Override
    public String toString() {
        return toDebugString();
    }

    private String toDebugString() {
        final StringBuilder b = new StringBuilder();
        if (sourceOrdinal == 0 && destOrdinal == 0) {
            b.append("between(\"").append(sourceName).append("\", \"").append(destName).append("\")");
        } else {
            b.append("from(\"").append(sourceName).append('"');
            if (sourceOrdinal != 0) {
                b.append(", ").append(sourceOrdinal);
            }
            b.append(").to(\"").append(destName).append('"');
            if (destOrdinal != 0) {
                b.append(", ").append(destOrdinal);
            }
            b.append(')');
        }
        switch (getRoutingPolicy()) {
            case UNICAST:
                break;
            case ISOLATED:
                b.append(".isolated()");
                break;
            case PARTITIONED:
                b.append(getPartitioner() instanceof Single ? ".allToOne()" : ".partitioned(?)");
                break;
            case BROADCAST:
                b.append(".broadcast()");
                break;
            default:
        }
        if (isDistributed()) {
            b.append(".distributed()");
        }
        if (getPriority() != 0) {
            b.append(".priority(").append(getPriority()).append(')');
        }
        return b.toString();
    }

    @Override
    public boolean equals(Object obj) {
        final Edge that;
        return this == obj
                || obj instanceof Edge
                    && this.sourceName.equals((that = (Edge) obj).sourceName)
                    && this.destName.equals(that.destName)
                    && this.sourceOrdinal == that.sourceOrdinal
                    && this.destOrdinal == that.destOrdinal;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceName, destName, sourceOrdinal, destOrdinal);
    }

    void restoreSourceAndDest(Map<String, Vertex> nameToVertex) {
        source = nameToVertex.get(sourceName);
        destination = nameToVertex.get(destName);
        assert source != null : "Couldn't restore source vertex " + sourceName + " from map " + nameToVertex;
        assert destination != null : "Couldn't restore destination vertex " + destName + " from map " + nameToVertex;
    }

    // Implementation of IdentifiedDataSerializable

    @Override
    public void writeData(@Nonnull ObjectDataOutput out) throws IOException {
        out.writeUTF(getSourceName());
        out.writeInt(getSourceOrdinal());
        out.writeUTF(getDestName());
        out.writeInt(getDestOrdinal());
        out.writeInt(getPriority());
        out.writeBoolean(isDistributed());
        out.writeObject(getRoutingPolicy());
        CustomClassLoadedObject.write(out, getPartitioner());
        out.writeObject(getConfig());
    }

    @Override
    public void readData(@Nonnull ObjectDataInput in) throws IOException {
        sourceName = in.readUTF();
        sourceOrdinal = in.readInt();
        destName = in.readUTF();
        destOrdinal = in.readInt();
        priority = in.readInt();
        isDistributed = in.readBoolean();
        routingPolicy = in.readObject();
        try {
            partitioner = CustomClassLoadedObject.read(in);
        } catch (HazelcastSerializationException e) {
            throw new HazelcastSerializationException("Error deserializing edge '" + sourceName + "' -> '"
                    + destName + "': " + e, e);
        }
        config = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return JetDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetDataSerializerHook.EDGE;
    }

    // END Implementation of IdentifiedDataSerializable


    /**
     * An edge describes a connection from many upstream processors to many
     * downstream processors. The routing policy decides where exactly to route
     * each particular item emitted from an upstream processor. To simplify
     * the reasoning we introduce the concept of the <em>set of candidate
     * downstream processors</em>, or the <em>candidate set</em> for short. On
     * a local edge the candidate set contains only local processors and on a
     * distributed edge it contain all the processors.
     */
    public enum RoutingPolicy implements Serializable {
        /**
         * For each item a single destination processor is chosen from the
         * candidate set, with no restriction on the choice.
         */
        UNICAST,
        /**
         * Like {@link #UNICAST}, but guarantees that any given downstream
         * processor receives data from exactly one upstream processor. This is
         * needed in some DAG setups to apply selective backpressure to individual
         * upstream source processors.
         * <p>
         * The downstream's local parallelism must not be less than the upstream's.
         * This policy is only available on a local edge.
         */
        ISOLATED,
        /**
         * Each item is sent to the one processor responsible for the item's
         * partition ID. On a distributed edge the processor is unique across the
         * cluster; on a non-distributed edge the processor is unique only within a
         * member.
         */
        PARTITIONED,
        /**
         * Each item is sent to all candidate processors.
         */
        BROADCAST
    }

    private static class Single implements Partitioner<Object> {

        private static final long serialVersionUID = 1L;

        private final Object key;
        private int partition;

        Single(Object key) {
            this.key = key;
        }

        @Override
        public void init(@Nonnull DefaultPartitionStrategy strategy) {
            partition = strategy.getPartition(key);
        }

        @Override
        public int getPartition(@Nonnull Object item, int partitionCount) {
            return partition;
        }
    }

    private static final class KeyPartitioner<T, K> implements Partitioner<T> {

        private static final long serialVersionUID = 1L;

        private final FunctionEx<T, K> keyExtractor;
        private final Partitioner<? super K> partitioner;
        private final String edgeDebugName;

        KeyPartitioner(@Nonnull FunctionEx<T, K> keyExtractor, @Nonnull Partitioner<? super K> partitioner,
                       String edgeDebugName) {
            this.keyExtractor = keyExtractor;
            this.partitioner = partitioner;
            this.edgeDebugName = edgeDebugName;
        }

        @Override
        public void init(DefaultPartitionStrategy strategy) {
            partitioner.init(strategy);
        }

        @Override
        public int getPartition(T item, int partitionCount) {
            K key = keyExtractor.apply(item);
            if (key == null) {
                throw new JetException("Null key from key extractor, edge: " + edgeDebugName);
            }
            return partitioner.getPartition(key, partitionCount);
        }
    }
}
