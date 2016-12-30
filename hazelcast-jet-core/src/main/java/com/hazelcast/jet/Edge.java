/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.impl.execution.init.CustomClassLoadedObject;
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

    private int priority = Integer.MAX_VALUE;

    private ForwardingPattern forwardingPattern = ForwardingPattern.VARIABLE_UNICAST;
    private Partitioner partitioner;
    private boolean isDistributed;

    private EdgeConfig config;

    Edge() {
    }

    /**
     * Creates an edge between two vertices.
     *
     * @param source      the source vertex
     * @param destination the destination vertex
     */
    public Edge(Vertex source, Vertex destination) {
        this(source, 0, destination, 0);
    }

    /**
     * Creates an edge between two vertices.
     *
     * @param source        the source vertex
     * @param sourceOrdinal ordinal at the source
     * @param destination   the destination vertex
     * @param destOrdinal   ordinal at the destination
     */
    public Edge(Vertex source, int sourceOrdinal, Vertex destination, int destOrdinal) {
        this.source = source.getName();
        this.sourceOrdinal = sourceOrdinal;

        this.destination = destination.getName();
        this.destOrdinal = destOrdinal;
    }

    /**
     * @return the name of the source vertex
     */
    public String getSource() {
        return source;
    }

    /**
     * @return ordinal of the edge at the source vertex
     */
    public int getSourceOrdinal() {
        return sourceOrdinal;
    }

    /**
     * @return the name of the destination vertex
     */
    public String getDestination() {
        return destination;
    }

    /**
     * @return ordinal of the edge at the destination vertex
     */
    public int getDestOrdinal() {
        return destOrdinal;
    }

    /**
     * Sets the priority of the edge. A lower number means higher priority
     * and the default is {@code Integer.MAX_VALUE}, the lowest possible priority.
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
     * {@code KeyExtractor} function.
     */
    public Edge partitionedByKey(KeyExtractor extractor) {
        this.forwardingPattern = ForwardingPattern.PARTITIONED;
        this.partitioner = new Keyed(extractor);
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
     * Activates the {@link ForwardingPattern#PARTITIONED PARTITIONED} forwarding
     * pattern. All items will be assigned the same, randomly chosen partition ID.
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
     * Says whether this edge is <em>distributed</em>. The effects of this
     * property are discussed in {@link #distributed()}.
     */
    public boolean isDistributed() {
        return isDistributed;
    }

    /**
     * @return the value of edge's <em>priority</em>, as explained on
     * {@link #priority(int)}.
     */
    public int getPriority() {
        return priority;
    }

    /**
     * @return the {@code EdgeConfig} instance associated with this edge.
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

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(source);
        out.writeInt(sourceOrdinal);
        out.writeUTF(destination);
        out.writeInt(destOrdinal);
        out.writeInt(priority);
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
        isDistributed = in.readBoolean();
        forwardingPattern = in.readObject();
        partitioner = CustomClassLoadedObject.read(in);
        config = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return JetDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return JetDataSerializerHook.EDGE;
    }


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

    private static class Keyed extends Default {
        private KeyExtractor extractor;

        Keyed(KeyExtractor extractor) {
            this.extractor = extractor;
        }

        @Override
        public int getPartition(Object item, int partitionCount) {
            return defaultPartitioning.getPartition(extractor.extract(item));
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
