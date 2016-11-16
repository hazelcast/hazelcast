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

package com.hazelcast.jet2;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.UuidUtil;

import java.io.IOException;
import java.io.Serializable;

/**
 * Represents an edge between two vertices in a DAG
 */
public class Edge implements IdentifiedDataSerializable {

    private String source;
    private int outputOrdinal;
    private String destination;
    private int inputOrdinal;

    private int priority = Integer.MAX_VALUE;

    private ForwardingPattern forwardingPattern = ForwardingPattern.ALTERNATING_SINGLE;
    private Partitioner partitioner;
    private boolean isDistributed;

    Edge() {

    }

    /**
     * Creates an edge between two vertices.
     *
     * @param source      the source vertex
     * @param destination the destination vertex
     */
    public Edge(Vertex source,
                Vertex destination) {
        this(source, 0, destination, 0);
    }

    /**
     * Creates an edge between two vertices.
     *
     * @param source        the source vertex
     * @param outputOrdinal ordinal at the source
     * @param destination   the destination vertex
     * @param inputOrdinal  ordinal at the destination
     */
    public Edge(Vertex source, int outputOrdinal,
                Vertex destination, int inputOrdinal) {
        this.source = source.getName();
        this.outputOrdinal = outputOrdinal;

        this.destination = destination.getName();
        this.inputOrdinal = inputOrdinal;
    }

    /**
     * @return Javadoc pending
     */
    public String getSource() {
        return source;
    }

    /**
     * @return Javadoc pending
     */
    public int getOutputOrdinal() {
        return outputOrdinal;
    }

    /**
     * @return Javadoc pending
     */
    public String getDestination() {
        return destination;
    }

    /**
     * @return Javadoc pending
     */
    public int getInputOrdinal() {
        return inputOrdinal;
    }

    /**
     * Sets the priority number for the edge.
     * The edges with the lower priority number will be processed before all others.
     */
    public Edge priority(int priority) {
        this.priority = priority;
        return this;
    }

    /**
     * Partition the edge with the default {@link Partitioner}
     */
    public Edge partitioned() {
        this.forwardingPattern = ForwardingPattern.PARTITIONED;
        this.partitioner = new Default();
        return this;
    }

    /**
     * Partition the edge with the given {@link Partitioner}
     */
    public Edge partitioned(Partitioner partitioner) {
        this.forwardingPattern = ForwardingPattern.PARTITIONED;
        this.partitioner = partitioner;
        return this;
    }

    /**
     * Partition the edge with the default {@link Partitioner}
     * and applies the provided function before partitioning
     */
    public Edge partitioned(KeyExtractor extractor) {
        this.forwardingPattern = ForwardingPattern.PARTITIONED;
        this.partitioner = new Keyed(extractor);
        return this;
    }

    /**
     * Forward the edge to a single point
     */
    public Edge allToOne() {
        return partitioned(new Single());
    }


    /**
     * Javadoc pending
     */
    public Edge broadcast() {
        forwardingPattern = ForwardingPattern.BROADCAST;
        return this;
    }

    /**
     * Javadoc pending
     */
    public Edge distributed() {
        isDistributed = true;
        return this;
    }

    /**
     * @return the partitioned for the edge
     */
    public Partitioner getPartitioner() {
        return partitioner;
    }

    /**
     * @return the {@link ForwardingPattern} for the edge
     */
    public ForwardingPattern getForwardingPattern() {
        return forwardingPattern;
    }

    /**
     * @return Javadoc pending
     */
    public boolean isDistributed() {
        return isDistributed;
    }

    /**
     * @return the priority for the edge
     */
    public int getPriority() {
        return priority;
    }

    @Override
    public String toString() {
        return "Edge{"
                + "source=" + source
                + ", destination=" + destination
                + '}';
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(source);
        out.writeInt(outputOrdinal);

        out.writeUTF(destination);
        out.writeInt(inputOrdinal);

        out.writeInt(priority);
        out.writeBoolean(isDistributed);

        out.writeObject(forwardingPattern);
        out.writeObject(partitioner);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        source = in.readUTF();
        outputOrdinal = in.readInt();

        destination = in.readUTF();
        inputOrdinal = in.readInt();

        priority = in.readInt();
        isDistributed = in.readBoolean();

        forwardingPattern = in.readObject();
        partitioner = in.readObject();
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
     * Javadoc pending
     */
    public enum ForwardingPattern implements Serializable {

        /**
         * Output of the source tasklet is only available to a single destination tasklet,
         * but not necessarily always the same one
         */
        ALTERNATING_SINGLE,

        /**
         * Output of the source tasklet is only available to the destination tasklet with the partition id
         * given by the {@link Partitioner}
         */
        PARTITIONED,

        /**
         * Output of the source tasklet is available to all destination tasklets.
         */
        BROADCAST
    }

    private static class Default implements Partitioner {
        private static final long serialVersionUID = 1L;

        protected transient IPartitionService service;

        @Override
        public void init(IPartitionService service) {
            this.service = service;
        }

        @Override
        public int getPartition(Object item, int numPartitions) {
            return service.getPartitionId(item) % numPartitions;
        }
    }

    private static class Keyed extends Default {
        private KeyExtractor extractor;

        Keyed(KeyExtractor extractor) {
            this.extractor = extractor;
        }

        @Override
        public int getPartition(Object item, int numPartitions) {
            return service.getPartitionId(extractor.extract(item)) % numPartitions;
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
        public void init(IPartitionService service) {
            partition = service.getPartitionId(key);
        }

        @Override
        public int getPartition(Object item, int numPartitions) {
            return partition;
        }
    }
}
