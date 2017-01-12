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

package com.hazelcast.jet;

import com.hazelcast.jet.impl.SerializationConstants;
import com.hazelcast.jet.impl.execution.init.CustomClassLoadedObject;
import com.hazelcast.jet.stream.Distributed;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.UuidUtil;

import java.io.IOException;
import java.io.Serializable;

/**
 * Represents an edge between two {@link Vertex vertices} in a {@link DAG}.
 * Conceptually, data travels over the edge from the source vertex to the
 * destination vertex. Practically, since the vertex is distributed across
 * the cluster and across threads in each cluster member, the edge is
 * implemented by a number of concurrent queues and network sender/receiver pairs.
 * <p>
 * It is often desirable to arrange that all items belonging to the same collation
 * key are received by the same processing unit (instance of {@link Processor}).
 * This is achieved by configuring an appropriate {@link Partitioner} on the edge.
 * The partitioner will determine the partition ID of each item and all items with
 * the same partition ID will be routed to the same {@code Processor} instance.
 * Depending on the value of edge's <em>distributed</em> property, the processor
 * will be unique cluster-wide, or only within each member.
 * <p>
 * A newly instantiated Edge is non-distributed with a
 * {@link ForwardingPattern#VARIABLE_UNICAST VARIABLE_UNICAST} forwarding pattern.
 */
public class Edge implements IdentifiedDataSerializable {

    private String source;
    private int sourceOrdinal;
    private String destination;
    private int destOrdinal;

    private int priority;
    private boolean isBuffered;

    private boolean isDistributed;
    private Partitioner partitioner;
    private ForwardingPattern forwardingPattern = ForwardingPattern.VARIABLE_UNICAST;

    private EdgeConfig config;

    Edge() {
    }

    private Edge(String source, int sourceOrdinal, String destination, int destOrdinal) {
        this.source = source;
        this.sourceOrdinal = sourceOrdinal;
        this.destination = destination;
        this.destOrdinal = destOrdinal;
    }

    /**
     * Returns an edge between two vertices. The ordinal of the edge
     * is 0 at both ends. Equivalent to {@code from(source).to(destination)}.
     *
     * @param source        the source vertex
     * @param destination   the destination vertex
     */
    public static Edge between(Vertex source, Vertex destination) {
        return new Edge(source.getName(), 0, destination.getName(), 0);
    }

    /**
     * Returns an edge with the given source vertex and no destination vertex.
     * The ordinal of the edge is 0. Typically followed by a call to one of
     * the {@code to()} methods.
     */
    public static Edge from(Vertex source) {
        return from(source, 0);
    }

    /**
     * Returns an edge with the given source vertex at the given ordinal
     * and no destination vertex. Typically follewed by a call to one of
     * the {@code to()} methods.
     */
    public static Edge from(Vertex source, int ordinal) {
        return new Edge(source.getName(), ordinal, null, 0);
    }

    /**
     * Sets the destination vertex of this edge, with ordinal 0.
     */
    public Edge to(Vertex dest) {
        this.destination = dest.getName();
        return this;
    }

    /**
     * Sets the destination vertex and ordinal of this edge.
     */
    public Edge to(Vertex dest, int ordinal) {
        this.destination = dest.getName();
        this.destOrdinal = ordinal;
        return this;
    }

    /**
     * Returns the name of the source vertex.
     */
    public String getSource() {
        return source;
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
    public String getDestination() {
        return destination;
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
     * Example: there two incoming edges on a vertex, with priorities 1 and 2. The
     * data from the edge with priority 1 will be processed in full before accepting
     * any data from the edge with priority 2.
     */
    public Edge priority(int priority) {
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
     * Activates unbounded buffering on this edge. Normally this should be avoided,
     * but at some points the logic of the DAG requires it. This is one scenario:
     * a vertex sends output to two edges, creating a fork in the DAG. The branches
     * later rejoin at a downstream vertex which assigns different priorities to its
     * two inbound edges. The one with the lower priority won't be consumed until the
     * higher-priority one is consumed in full. However, since the data for both edges
     * is generated simultaneously, and since the lower-priority input will apply
     * backpressure while waiting for the higher-priority input to be consumed, this
     * will result in a deadlock. The deadlock is resolved by activating unbounded
     * buffering on the lower-priority edge.
     * <p>
     * <strong>NOTE:</strong> when this feature is activated, the
     * {@link EdgeConfig#setHighWaterMark(int) high water mark} property of
     * {@code EdgeConfig} is ignored and the maximum value is used.
     */
    public Edge buffered() {
        isBuffered = true;
        return this;
    }

    /**
     * Returns whether {@link #buffered() unbounded buffering} is activated for this edge.
     */
    public boolean isBuffered() {
        return isBuffered;
    }

    /**
     * Activates the {@link ForwardingPattern#PARTITIONED PARTITIONED} forwarding
     * pattern and applies the default Hazelcast partitioning strategy.
     */
    public Edge partitioned() {
        this.forwardingPattern = ForwardingPattern.PARTITIONED;
        this.partitioner = new Default();
        return this;
    }

    /**
     * Activates the {@link ForwardingPattern#PARTITIONED PARTITIONED} forwarding
     * pattern and applies the default Hazelcast partitioning strategy. The strategy
     * is not applied directly to the item, but to the result of the supplied
     * {@code keyExtractor} function.
     */
    public <T, R> Edge partitionedByKey(Distributed.Function<T, R> keyExtractor) {
        this.forwardingPattern = ForwardingPattern.PARTITIONED;
        this.partitioner = new Keyed<>(keyExtractor);
        return this;
    }

    /**
     * Activates the {@link ForwardingPattern#PARTITIONED PARTITIONED} forwarding
     * pattern and applies the provided custom partitioning strategy.
     */
    public Edge partitionedByCustom(Partitioner partitioner) {
        this.forwardingPattern = ForwardingPattern.PARTITIONED;
        this.partitioner = partitioner;
        return this;
    }

    /**
     * Activates a special-cased {@link ForwardingPattern#PARTITIONED PARTITIONED}
     * forwarding pattern where all items will be assigned the same, randomly chosen
     * partition ID. Therefore all items will be directed to the same processor.
     */
    public Edge allToOne() {
        return partitionedByCustom(new Single());
    }

    /**
     * Activates the {@link ForwardingPattern#BROADCAST BROADCAST} forwarding
     * pattern.
     */
    public Edge broadcast() {
        forwardingPattern = ForwardingPattern.BROADCAST;
        return this;
    }

    /**
     * Returns the instance encapsulating the partitioning strategy in effect
     * on this edge.
     */
    public Partitioner getPartitioner() {
        return partitioner;
    }

    /**
     * Returns the {@link ForwardingPattern} in effect on the edge.
     */
    public ForwardingPattern getForwardingPattern() {
        return forwardingPattern;
    }

    /**
     * Declares that the edge is distributed. A non-distributed edge only transfers
     * data within the same member. If the data source running on local member is
     * distributed (produces only a slice of all the data on any given member), the
     * local processors will not observe all the data. The same holds true when the data
     * originates from an upstream distributed edge.
     * <p>
     * A <em>distributed</em> edge allows all the data to be observed by all the processors
     * (using the {@link ForwardingPattern#BROADCAST BROADCAST} forwarding pattern) and,
     * more attractively, all the data with a given partition ID to be observed by the same
     * unique processor, regardless of whether it is running on the local or a remote member
     * (using the {@link ForwardingPattern#PARTITIONED PARTITIONED} forwarding pattern).
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
     */
    public EdgeConfig getConfig() {
        return config;
    }

    /**
     * Assigns an {@code EdgeConfig} to this edge.
     */
    public Edge setConfig(EdgeConfig config) {
        this.config = config;
        return this;
    }

    @Override
    public String toString() {
        return '(' + source + ", " + sourceOrdinal + ") -> (" + destination + ", " + destOrdinal + ')';
    }

    @Override
    public boolean equals(Object obj) {
        final Edge that;
        return this == obj
                || obj instanceof Edge
                    && this.source.equals((that = (Edge) obj).source)
                    && this.destination.equals(that.destination);
    }

    @Override
    public int hashCode() {
        return 37 * source.hashCode() + destination.hashCode();
    }


    // Implementation of IdentifiedDataSerializable

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(source);
        out.writeInt(sourceOrdinal);
        out.writeUTF(destination);
        out.writeInt(destOrdinal);
        out.writeInt(priority);
        out.writeBoolean(isBuffered);
        out.writeBoolean(isDistributed);
        out.writeObject(forwardingPattern);
        CustomClassLoadedObject.write(out, partitioner);
        out.writeObject(config);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        source = in.readUTF();
        sourceOrdinal = in.readInt();
        destination = in.readUTF();
        destOrdinal = in.readInt();
        priority = in.readInt();
        isBuffered = in.readBoolean();
        isDistributed = in.readBoolean();
        forwardingPattern = in.readObject();
        partitioner = CustomClassLoadedObject.read(in);
        config = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return SerializationConstants.FACTORY_ID;
    }

    @Override
    public int getId() {
        return SerializationConstants.EDGE;
    }

    // END Implementation of IdentifiedDataSerializable


    /**
     * Enumerates the supported patterns of forwarding data items along an edge. Since there
     * are many {@code Processor} instances doing the work of the same destination vertex, a
     * choice can be made which processor(s) to send the item to.
     * <p>
     * If the edge is not distributed, candidate processors are only those running within
     * the same cluster member.
     */
    public enum ForwardingPattern implements Serializable {
        /**
         * For each item a single destination processor is chosen, with no restriction on the choice.
         */
        VARIABLE_UNICAST,
        /**
         * Each item is sent to the one processor responsible for the item's partition ID. On
         * a distributed edge, the processor is unique across the cluster; on a non-distributed
         * edge the processor is unique only within a member.
         */
        PARTITIONED,
        /**
         * Each item is sent to all candidate processors.
         */
        BROADCAST
    }

    private static class Default implements Partitioner {
        private static final long serialVersionUID = 1L;

        protected transient DefaultPartitionStrategy defaultPartitioning;

        @Override
        public void init(DefaultPartitionStrategy defaultPartitioning) {
            this.defaultPartitioning = defaultPartitioning;
        }

        @Override
        public int getPartition(Object item, int partitionCount) {
            return defaultPartitioning.getPartition(item);
        }
    }

    private static class Keyed<T, R> extends Default {
        private Distributed.Function<T, R> keyExtractor;

        Keyed(Distributed.Function<T, R> keyExtractor) {
            this.keyExtractor = keyExtractor;
        }

        @Override
        public int getPartition(Object item, int partitionCount) {
            return defaultPartitioning.getPartition(keyExtractor.apply((T) item));
        }
    }

    private static class Single implements Partitioner {

        private static final long serialVersionUID = 1L;

        private final String key;
        private int partition;

        Single() {
            key = UuidUtil.newUnsecureUuidString();
        }

        @Override
        public void init(DefaultPartitionStrategy service) {
            partition = service.getPartition(key);
        }

        @Override
        public int getPartition(Object item, int partitionCount) {
            return partition;
        }
    }
}
