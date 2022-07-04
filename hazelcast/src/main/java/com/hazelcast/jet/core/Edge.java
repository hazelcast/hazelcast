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

import com.hazelcast.cluster.Address;
import com.hazelcast.function.ComparatorEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;
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
import com.hazelcast.spi.annotation.PrivateApi;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Objects;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.core.Partitioner.defaultPartitioner;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static java.util.Objects.requireNonNull;

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
 * @since Jet 3.0
 */
public class Edge implements IdentifiedDataSerializable {
    /**
     * An address returned by {@link #getDistributedTo()} denoting an edge that
     * distributes the items among all members.
     *
     * @since Jet 4.3
     */
    public static final Address DISTRIBUTE_TO_ALL;

    static {
        try {
            DISTRIBUTE_TO_ALL = new Address("255.255.255.255", 0);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private transient boolean locked;
    private Vertex source; // transient field, restored during DAG deserialization
    private String sourceName;
    private int sourceOrdinal;

    private Vertex destination; // transient field, restored during DAG deserialization
    private String destName;
    private int destOrdinal;

    private int priority;
    private Address distributedTo;
    private Partitioner<?> partitioner;
    private RoutingPolicy routingPolicy = RoutingPolicy.UNICAST;
    private ComparatorEx<?> comparator;
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
     * and no destination vertex. Typically followed by a call to one of
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
        throwIfLocked();
        return to(destination, 0);
    }

    /**
     * Sets the destination vertex and ordinal of this edge.
     */
    @Nonnull
    public Edge to(@Nonnull Vertex destination, int ordinal) {
        throwIfLocked();
        if (this.destination != null) {
            throw new IllegalStateException("destination already set");
        }
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
    @Nullable
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
    @Nullable
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
        throwIfLocked();
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
     * this edge. This policy is the default.
     */
    @Nonnull
    public Edge unicast() {
        throwIfLocked();
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
        throwIfLocked();
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
        throwIfLocked();
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
     * It is equivalent to using {@code partitioned(t -> key)}, but it has
     * a small optimization that the partition ID is not recalculated
     * for each stream item.
     */
    @Nonnull
    public Edge allToOne(Object key) {
        throwIfLocked();
        return partitioned(wholeItem(), new Single(key));
    }

    /**
     * Activates the {@link RoutingPolicy#BROADCAST BROADCAST} routing policy.
     */
    @Nonnull
    public Edge broadcast() {
        throwIfLocked();
        routingPolicy = RoutingPolicy.BROADCAST;
        return this;
    }

    /**
     * Activates the {@link RoutingPolicy#ISOLATED ISOLATED} routing policy
     * which establishes isolated paths from upstream to downstream processors.
     * <p>
     * Since all traffic will be local, this policy is not allowed on a
     * distributed edge.
     */
    @Nonnull
    public Edge isolated() {
        throwIfLocked();
        routingPolicy = RoutingPolicy.ISOLATED;
        return this;
    }

    /**
     * Specifies that the data traveling on this edge is ordered according to
     * the provided comparator. The edge maintains this property when merging
     * the data coming from different upstream processors, so that the
     * receiving processor observes them in the proper order. Every upstream
     * processor must emit the data in the same order because the edge doesn't
     * sort, it only prevents reordering while receiving.
     * <p>
     * The implementation currently doesn't handle watermarks or barriers: if
     * the source processors emit watermarks or you add a processing guarantee,
     * the job will fail at runtime.
     *
     * @since Jet 4.3
     */
    public Edge ordered(@Nonnull ComparatorEx<?> comparator) {
        throwIfLocked();
        this.comparator = comparator;
        return this;
    }

    /**
     * Activates the {@link RoutingPolicy#FANOUT FANOUT} routing policy.
     *
     * @since Jet 4.4
     */
    @Nonnull
    public Edge fanout() {
        throwIfLocked();
        routingPolicy = RoutingPolicy.FANOUT;
        return this;
    }

    /**
     * Returns the instance encapsulating the partitioning strategy in effect
     * on this edge.
     */
    @Nullable
    public Partitioner<?> getPartitioner() {
        return partitioner;
    }

    /**
     * Returns the comparator defined on this edge using {@link
     * #ordered(ComparatorEx)}.
     *
     * @since Jet 4.3
     **/
    @Nullable
    public ComparatorEx<?> getOrderComparator() {
        return comparator;
    }

    /**
     * Returns the {@link RoutingPolicy} in effect on the edge.
     */
    @Nonnull
    public RoutingPolicy getRoutingPolicy() {
        return routingPolicy;
    }

    /**
     * Declares that the edge is local. A local edge only transfers data within
     * the same member, network is not involved. This setting is the default.
     *
     * @see #distributed()
     * @see #distributeTo(Address)
     * @see #getDistributedTo()
     *
     * @since Jet 4.3
     */
    @Nonnull
    public Edge local() {
        throwIfLocked();
        distributedTo = null;
        return this;
    }

    /**
     * Returning if the edge is local.
     */
    public boolean isLocal() {
        return distributedTo == null;
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
     *
     * @see #distributeTo(Address)
     * @see #local()
     * @see #getDistributedTo()
     */
    @Nonnull
    public Edge distributed() {
        throwIfLocked();
        distributedTo = DISTRIBUTE_TO_ALL;
        return this;
    }

    /**
     * Declares that all items sent over this edge will be delivered to the
     * specified member. Processors on other members will not receive any data.
     * <p>
     * This option is most useful for sinks if we want to ensure that the
     * results are written (or sent from) only that member.
     * <p>
     * It's not suitable for fault-tolerant jobs. If the {@code targetMember}
     * is not a member, the job can't be executed and will fail.
     *
     * @param targetMember the member to deliver the items to
     *
     * @see #distributed()
     * @see #local()
     * @see #getDistributedTo()
     *
     * @since Jet 4.3
     */
    @Nonnull
    public Edge distributeTo(@Nonnull Address targetMember) {
        throwIfLocked();
        if (requireNonNull(targetMember).equals(DISTRIBUTE_TO_ALL)) {
            throw new IllegalArgumentException();
        }
        distributedTo = targetMember;
        return this;
    }

    /**
     * Possible return values:<ul>
     *     <li>null - route only to local members (after a {@link #local()}
     *          call)
     *     <li>"255.255.255.255:0 - route to all members (after a {@link
     *          #distributed()} call)
     *     <li>else - route to specific member (after a {@link #distributeTo}
     *          call)
     * </ul>
     *
     * @since Jet 4.3
     */
    @Nullable
    public Address getDistributedTo() {
        return distributedTo;
    }

    /**
     * Says whether this edge distributes items among all members, as requested
     * by the {@link #distributed()} method.
     */
    public boolean isDistributed() {
        return DISTRIBUTE_TO_ALL.equals(distributedTo);
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
    @Nonnull
    public Edge setConfig(@Nullable EdgeConfig config) {
        throwIfLocked();
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
            case FANOUT:
                b.append(".fanout()");
                break;
            default:
        }
        if (DISTRIBUTE_TO_ALL.equals(distributedTo)) {
            b.append(".distributed()");
        } else if (distributedTo != null) {
            b.append(".distributeTo(").append(distributedTo).append(')');
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
        out.writeObject(getDistributedTo());
        out.writeString(getRoutingPolicy().name());
        out.writeObject(getConfig());
        CustomClassLoadedObject.write(out, getPartitioner());
        CustomClassLoadedObject.write(out, getOrderComparator());
    }

    @Override
    public void readData(@Nonnull ObjectDataInput in) throws IOException {
        sourceName = in.readUTF();
        sourceOrdinal = in.readInt();
        destName = in.readUTF();
        destOrdinal = in.readInt();
        priority = in.readInt();
        distributedTo = in.readObject();
        routingPolicy = RoutingPolicy.valueOf(in.readString());
        config = in.readObject();
        try {
            partitioner = CustomClassLoadedObject.read(in);
            comparator = CustomClassLoadedObject.read(in);
        } catch (HazelcastSerializationException e) {
            throw new HazelcastSerializationException("Error deserializing edge '" + sourceName + "' -> '"
                    + destName + "': " + e, e);
        }
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
     * distributed edge it contains all the processors.
     */
    public enum RoutingPolicy implements Serializable {
        /**
         * This policy chooses for each item a single destination processor
         * from the candidate set, with no restriction on the choice.
         */
        UNICAST,
        /**
         * This policy sets up isolated parallel data paths between two vertices
         * as much as it can, given the level of mismatch between the local
         * parallelism (LP) of the upstream vs. the downstream vertices.
         * Specifically:
         * <ul><li>
         *     If LP_upstream <= LP_downstream, every downstream processor receives
         *     data from only one upstream processor
         * </li><li>
         *     If LP_upstream >= LP_downstream, every upstream processor sends data to
         *     only one downstream processor
         * </li></ul>
         * If LP_upstream = LP_downstream, both of the above are true and there are
         * isolated pairs of upstream and downstream processors.
         * <p>
         * This policy is only available on a local edge.
         */
        ISOLATED,
        /**
         * This policy sends every item to the one processor responsible for the
         * item's partition ID. On a distributed edge, this processor is unique
         * across the cluster; on a non-distributed edge, the processor is unique
         * only within a member.
         */
        PARTITIONED,
        /**
         * This policy sends each item to all candidate processors.
         */
        BROADCAST,
        /**
         * This policy sends an item to all members, but only to one processor on
         * each member. It's a combination of {@link #BROADCAST} and {@link
         * #UNICAST}: an item is first <em>broadcast</em> to all members, and then,
         * on each member, it is <em>unicast</em> to one processor.
         * <p>
         * If the destination local parallelism is 1, the behavior is equal to
         * {@link #BROADCAST}. If the member count in the cluster is 1, the
         * behavior is equal to {@link #UNICAST}.
         * <p>
         * To work as expected, the edge must be also {@link #distributed()}.
         * Otherwise it will work just like {@link #UNICAST}.
         *
         * @since Jet 4.4
         */
        FANOUT
    }

    static class Single implements Partitioner<Object>, IdentifiedDataSerializable {

        private static final long serialVersionUID = 1L;

        private Object key;
        private int partition;

        Single() {
        }

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

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(key);
            out.writeInt(partition);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            key = in.readObject();
            partition = in.readInt();
        }

        @Override
        public int getFactoryId() {
            return JetDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return JetDataSerializerHook.EDGE_SINGLE_PARTITIONER;
        }
    }

    static final class KeyPartitioner<T, K> implements Partitioner<T>, SerializationServiceAware,
            IdentifiedDataSerializable {

        private static final long serialVersionUID = 1L;

        private FunctionEx<T, K> keyExtractor;
        private Partitioner<? super K> partitioner;
        private String edgeDebugName;
        private SerializationService serializationService;

        KeyPartitioner() {
        }

        KeyPartitioner(@Nonnull FunctionEx<T, K> keyExtractor, @Nonnull Partitioner<? super K> partitioner,
                       String edgeDebugName) {
            this.keyExtractor = keyExtractor;
            this.partitioner = partitioner;
            this.edgeDebugName = edgeDebugName;
        }

        @Override
        public void init(@Nonnull DefaultPartitionStrategy strategy) {
            partitioner.init(strategy);
            if (keyExtractor instanceof SerializationServiceAware) {
                ((SerializationServiceAware) keyExtractor).setSerializationService(serializationService);
            }
        }

        @Override
        public int getPartition(@Nonnull T item, int partitionCount) {
            K key = keyExtractor.apply(item);
            if (key == null) {
                throw new JetException("Null key from key extractor, edge: " + edgeDebugName);
            }
            return partitioner.getPartition(key, partitionCount);
        }

        @Override
        public void setSerializationService(SerializationService serializationService) {
            this.serializationService = serializationService;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(keyExtractor);
            out.writeObject(partitioner);
            out.writeString(edgeDebugName);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            keyExtractor = in.readObject();
            partitioner = in.readObject();
            edgeDebugName = in.readString();
        }

        @Override
        public int getFactoryId() {
            return JetDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return JetDataSerializerHook.EDGE_KEY_PARTITIONER;
        }
    }

    private void throwIfLocked() {
        if (locked) {
            throw new IllegalStateException("Edge is already locked");
        }
    }

    /**
     * Used to prevent further mutations this instance after submitting it for execution.
     * <p>
     * It's not a public API, can be removed in the future.
     */
    @PrivateApi
    void lock() {
        locked = true;
    }
}
