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

package com.hazelcast.jet.dag;

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.impl.actor.ByReferenceDataTransferringStrategy;
import com.hazelcast.jet.impl.strategy.DefaultHashingStrategy;
import com.hazelcast.jet.strategy.DataTransferringStrategy;
import com.hazelcast.jet.strategy.HashingStrategy;
import com.hazelcast.jet.strategy.ProcessingStrategy;
import com.hazelcast.jet.strategy.ShufflingStrategy;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;

import java.io.Serializable;

/**
 * Represents an edge between two vertices in a DAG
 */
public class Edge implements Serializable {
    private Vertex to;
    private String name;
    private Vertex from;
    private boolean shuffled;

    private HashingStrategy hashingStrategy;
    private ShufflingStrategy shufflingStrategy;
    private ProcessingStrategy processingStrategy;
    private PartitioningStrategy partitioningStrategy;
    private DataTransferringStrategy dataTransferringStrategy;

    /**
     * Creates an edge between two vertices.
     *
     * @param name name of the edge
     * @param from the origin vertex
     * @param to the destination vertex
     */
    public Edge(String name,
                Vertex from,
                Vertex to) {
        this(name, from, to, false);
    }

    private Edge(String name,
                Vertex from,
                Vertex to,
                boolean shuffled) {
        this(name, from, to, shuffled, null, null, null, null, null);
    }

    private Edge(String name,
                Vertex from,
                Vertex to,
                boolean shuffled,
                ShufflingStrategy shufflingStrategy,
                ProcessingStrategy processingStrategy,
                PartitioningStrategy partitioningStrategy,
                HashingStrategy hashingStrategy,
                DataTransferringStrategy dataTransferringStrategy) {
        this.to = to;
        this.name = name;
        this.from = from;
        this.shuffled = shuffled;
        this.shufflingStrategy = shufflingStrategy;
        this.hashingStrategy = nvl(hashingStrategy, DefaultHashingStrategy.INSTANCE);
        this.partitioningStrategy = nvl(partitioningStrategy, StringPartitioningStrategy.INSTANCE);
        this.dataTransferringStrategy = nvl(dataTransferringStrategy, ByReferenceDataTransferringStrategy.INSTANCE);
        this.processingStrategy = nvl(processingStrategy, ProcessingStrategy.ROUND_ROBIN);
    }

    private <T> T nvl(T value, T defaultValue) {
        return value == null ? defaultValue : value;
    }

    /**
     * @return output vertex of edge
     */
    public Vertex getOutputVertex() {
        return this.to;
    }

    /**
     * @return true if edge will take into account cluster
     * false if data through edge's channel will be passed only
     * locally without shuffling
     */
    public boolean isShuffled() {
        return this.shuffled;
    }

    /**
     * @return name of the edge
     */
    public String getName() {
        return this.name;
    }

    /**
     * @return input vertex of edge
     */
    public Vertex getInputVertex() {
        return this.from;
    }

    /**
     * @return Edge's shuffling strategy
     */
    public ShufflingStrategy getShufflingStrategy() {
        return this.shufflingStrategy;
    }

    /**
     * @return Edge's processing strategy
     */
    public ProcessingStrategy getProcessingStrategy() {
        return this.processingStrategy;
    }

    /**
     * @return Edge's partitioning strategy
     */
    public PartitioningStrategy getPartitioningStrategy() {
        return this.partitioningStrategy;
    }

    /**
     * @return Edge's hashing strategy
     */
    public HashingStrategy getHashingStrategy() {
        return this.hashingStrategy;
    }

    /**
     * @return Edge's data-transferring strategy
     */
    public DataTransferringStrategy getDataTransferringStrategy() {
        return this.dataTransferringStrategy;
    }


    @Override
    @SuppressWarnings({
            "checkstyle:npathcomplexity",
            "checkstyle:cyclomaticcomplexity"
    })
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Edge edge = (Edge) o;

        if (shuffled != edge.shuffled) {
            return false;
        }

        if (to != null
                ? !to.equals(edge.to)
                : edge.to != null) {
            return false;
        }

        if (name != null
                ? !name.equals(edge.name)
                : edge.name != null) {
            return false;
        }

        if (from != null
                ? !from.equals(edge.from)
                : edge.from != null) {
            return false;
        }

        if (hashingStrategy != null
                ?
                !hashingStrategy.getClass().equals(edge.hashingStrategy.getClass())
                :
                edge.hashingStrategy != null) {
            return false;
        }

        if (shufflingStrategy != null
                ?
                !shufflingStrategy.equals(edge.shufflingStrategy)
                :
                edge.shufflingStrategy != null) {
            return false;
        }

        if (processingStrategy != edge.processingStrategy) {
            return false;
        }

        if (partitioningStrategy != null
                ?
                !partitioningStrategy.getClass().equals(edge.partitioningStrategy.getClass())
                :
                edge.partitioningStrategy != null) {
            return false;
        }

        return !(dataTransferringStrategy != null
                ?
                !dataTransferringStrategy.getClass().equals(edge.dataTransferringStrategy.getClass())
                :
                edge.dataTransferringStrategy != null
        );
    }

    @Override
    @SuppressWarnings({
            "checkstyle:npathcomplexity"
    })
    public int hashCode() {
        int result = to != null ? to.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (from != null ? from.hashCode() : 0);
        result = 31 * result + (shuffled ? 1 : 0);
        result = 31 * result + (hashingStrategy != null ? hashingStrategy.getClass().hashCode() : 0);
        result = 31 * result + (shufflingStrategy != null ? shufflingStrategy.hashCode() : 0);
        result = 31 * result + (processingStrategy != null ? processingStrategy.getClass().hashCode() : 0);
        result = 31 * result + (partitioningStrategy != null ? partitioningStrategy.getClass().hashCode() : 0);
        result = 31 * result + (dataTransferringStrategy != null ? dataTransferringStrategy.getClass().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "EdgeImpl{"
                + "to=" + to
                + ", name='" + name + '\''
                + ", from=" + from
                + ", shuffled=" + shuffled
                + ", hashingStrategy=" + hashingStrategy
                + ", shufflingStrategy=" + shufflingStrategy
                + ", processingStrategy=" + processingStrategy
                + ", partitioningStrategy=" + partitioningStrategy
                + ", dataTransferringStrategy=" + dataTransferringStrategy
                + '}';
    }

    /**
     * A builder class for Edge
     */
    public static class EdgeBuilder {
        private final Edge edge;
        private boolean build;

        /**
         * Creates a builder for an edge between two vertices.
         *
         * @param name name of the edge
         * @param from the origin vertex
         * @param to the destination vertex
         */
        public EdgeBuilder(String name,
                           Vertex from,
                           Vertex to) {
            this.edge = new Edge(name, from, to);
        }

        /**
         * Set if the edge is shuffled
         * @param shuffled true, if the edge is shuffled, false otherwise.
         * @return the builder
         */
        public EdgeBuilder shuffling(boolean shuffled) {
            this.edge.shuffled = shuffled;
            return this;
        }

        /**
         * Set the shuffling strategy on the edge
         * @param shufflingStrategy the shuffling strategy to set
         * @return the builder
         */
        public EdgeBuilder shufflingStrategy(ShufflingStrategy shufflingStrategy) {
            this.edge.shufflingStrategy = shufflingStrategy;
            return this;
        }

        /**
         * Set the processing strategy for the edge
         * @param processingStrategy the processing strategy to set
         * @return the builder
         */
        public EdgeBuilder processingStrategy(ProcessingStrategy processingStrategy) {
            this.edge.processingStrategy = processingStrategy;
            return this;
        }

        /**
         * Set the partitioning strategy for the edge
         * @param partitioningStrategy the partitioning strategy to set
         * @return the builder
         */
        public EdgeBuilder partitioningStrategy(PartitioningStrategy partitioningStrategy) {
            this.edge.partitioningStrategy = partitioningStrategy;
            return this;
        }

        /**
         * Set the hashing strategy for the edge
         * @param hashingStrategy the hashing strategy to set
         * @return the builder
         */
        public EdgeBuilder hashingStrategy(HashingStrategy hashingStrategy) {
            this.edge.hashingStrategy = hashingStrategy;
            return this;
        }

        /**
         * Set the data transferring strategy for the edge
         * @param dataTransferringStrategy the data transferring strategy to set
         * @return the builder
         */
        public EdgeBuilder dataTransferringStrategy(DataTransferringStrategy dataTransferringStrategy) {
            this.edge.dataTransferringStrategy = dataTransferringStrategy;
            return this;
        }

        /**
         * Build and return the edge
         * @return the built edge
         */
        public Edge build() {
            if (this.build) {
                throw new IllegalStateException("Edge has been already built");
            }

            this.build = true;
            return this.edge;
        }
    }
}
