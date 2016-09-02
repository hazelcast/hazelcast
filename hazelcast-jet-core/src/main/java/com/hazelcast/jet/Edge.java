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

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.impl.strategy.SerializedHashingStrategy;
import com.hazelcast.jet.strategy.HashingStrategy;
import com.hazelcast.jet.strategy.MemberDistributionStrategy;
import com.hazelcast.jet.strategy.RoutingStrategy;
import com.hazelcast.partition.strategy.StringAndPartitionAwarePartitioningStrategy;
import java.io.Serializable;

/**
 * Represents an edge between two vertices in a DAG
 */
public class Edge implements Serializable {

    private Vertex to;
    private String name;
    private Vertex from;
    private boolean isLocal = true;

    private HashingStrategy hashingStrategy;
    private MemberDistributionStrategy memberDistributionStrategy;
    private RoutingStrategy routingStrategy;
    private PartitioningStrategy partitioningStrategy;

    /**
     * Creates an edge between two vertices.
     *
     * @param name name of the edge
     * @param from the origin vertex
     * @param to   the destination vertex
     */
    public Edge(String name,
                Vertex from,
                Vertex to) {
        this(name, from, to, true);
    }

    /**
     * Creates an edge between two vertices.
     *
     * @param name    name of the edge
     * @param from    the origin vertex
     * @param to      the destination vertex
     * @param isLocal sets if the edge is local
     */
    Edge(String name,
         Vertex from,
         Vertex to,
         boolean isLocal) {
        this(name, from, to, isLocal, null, null, null, null);
    }

    Edge(String name,
         Vertex from,
         Vertex to,
         boolean isLocal,
         MemberDistributionStrategy memberDistributionStrategy,
         RoutingStrategy routingStrategy,
         PartitioningStrategy partitioningStrategy,
         HashingStrategy hashingStrategy) {
        this.to = to;
        this.name = name;
        this.from = from;
        this.isLocal = isLocal;
        this.memberDistributionStrategy = memberDistributionStrategy;
        this.hashingStrategy = nvl(hashingStrategy, SerializedHashingStrategy.INSTANCE);
        this.partitioningStrategy = nvl(partitioningStrategy, StringAndPartitionAwarePartitioningStrategy.INSTANCE);
        this.routingStrategy = nvl(routingStrategy, RoutingStrategy.ROUND_ROBIN);
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
     * Returns if the edge is local
     */
    public boolean isLocal() {
        return isLocal;
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
     * @return the member distribution strategy for the edge
     */
    public MemberDistributionStrategy getMemberDistributionStrategy() {
        return memberDistributionStrategy;
    }

    /**
     * @return the routing strategy for the edge
     */
    public RoutingStrategy getRoutingStrategy() {
        return routingStrategy;
    }

    /**
     * @return the partitioning strategy for the edge
     */
    public PartitioningStrategy getPartitioningStrategy() {
        return partitioningStrategy;
    }

    /**
     * @return the hashing strategy for the edge
     */
    public HashingStrategy getHashingStrategy() {
        return hashingStrategy;
    }

    /**
     * Sets the edge to be distributed. The output of the producers will be consumed by consumers on other
     * members. When a {@link MemberDistributionStrategy} is not provided, the record will be consumed by the
     * member which is the owner of the partition the record belongs to.
     * If there are several consumers in the target member, the consumer will be determined by
     * {@link RoutingStrategy}
     */
    public Edge distributed() {
        isLocal = false;
        return this;
    }

    /**
     * Sets the edge to be local only. The output of the producers will only be consumed by consumers on the same
     * member.
     * The specific consumer instance will be determined by {@link RoutingStrategy}
     */
    public Edge local() {
        isLocal = true;
        return this;
    }

    /**
     * Sets the edge to be distributed. The output of the producers will be consumed by consumers on other
     * members. The member or members to consume will be determined by the {@link MemberDistributionStrategy}.
     * If there are several consumers in the target member(s), the consumer will be determined by
     * {@link RoutingStrategy}
     *
     * @see com.hazelcast.jet.strategy.AllMembersDistributionStrategy
     * @see com.hazelcast.jet.strategy.SingleMemberDistributionStrategy
     */
    public Edge distributed(MemberDistributionStrategy distributionStrategy) {
        this.isLocal = false;
        this.memberDistributionStrategy = distributionStrategy;
        return this;
    }

    /**
     * Sets the {@link RoutingStrategy} for the edge to broadcast.
     * The output of the producer vertex will be available to all instances of the consumer vertex.
     *
     * @link RoutingStrategy.BROADCAST
     */
    public Edge broadcast() {
        this.routingStrategy = RoutingStrategy.BROADCAST;
        return this;
    }

    /**
     * Sets the {@link RoutingStrategy} for the edge to partitioned.
     * The output of the producer vertex will be partitioned and the partitions will be allocated
     * between instances of the consumer vertex, with the same consumer always receiving the same partitions.
     *
     * @link RoutingStrategy.PARTITIONED
     */
    public Edge partitioned() {
        this.routingStrategy = RoutingStrategy.PARTITIONED;
        return this;
    }

    /**
     * Sets the {@link RoutingStrategy} for the edge to partitioned with a custom partitioning strategy.
     * <p/>
     * The output of the producer vertex will be partitioned and the partitions will be allocated
     * between instances of the consumer vertex, with the same consumer always receiving the same partitions.
     *
     * @link RoutingStrategy.PARTITIONED
     */
    public Edge partitioned(PartitioningStrategy partitioningStrategy) {
        this.routingStrategy = RoutingStrategy.PARTITIONED;
        this.partitioningStrategy = partitioningStrategy;
        return this;
    }

    /**
     * Sets the {@link RoutingStrategy} for the edge to partitioned with a custom hashing strategy.
     * <p/>
     * The output of the producer vertex will be partitioned and the partitions will be allocated
     * between instances of the consumer vertex, with the same consumer always receiving the same partitions.
     *
     * @link RoutingStrategy.PARTITIONED
     * @see HashingStrategy
     */
    public Edge partitioned(HashingStrategy hashingStrategy) {
        this.routingStrategy = RoutingStrategy.PARTITIONED;
        this.hashingStrategy = hashingStrategy;
        return this;
    }

    /**
     * Sets the {@link RoutingStrategy} for the edge to partitioned with a custom partitioning and hashing strategy.
     * <p/>
     * The output of the producer vertex will be partitioned and the partitions will be allocated
     * between instances of the consumer vertex, with the same consumer always receiving the same partitions.
     *
     * @link RoutingStrategy.PARTITIONED
     * @see HashingStrategy
     * @see PartitioningStrategy
     */
    public Edge partitioned(PartitioningStrategy partitioningStrategy, HashingStrategy hashingStrategy) {
        this.routingStrategy = RoutingStrategy.PARTITIONED;
        this.partitioningStrategy = partitioningStrategy;
        this.hashingStrategy = hashingStrategy;
        return this;
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

        if (isLocal != edge.isLocal) {
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

        if (memberDistributionStrategy != null
                ?
                !memberDistributionStrategy.equals(edge.memberDistributionStrategy)
                :
                edge.memberDistributionStrategy != null) {
            return false;
        }

        if (routingStrategy != edge.routingStrategy) {
            return false;
        }

        return partitioningStrategy != null
                ?
                partitioningStrategy.getClass().equals(edge.partitioningStrategy.getClass())
                :
                edge.partitioningStrategy == null;

    }

    @Override
    @SuppressWarnings({
            "checkstyle:npathcomplexity"
    })
    public int hashCode() {
        int result = to != null ? to.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (from != null ? from.hashCode() : 0);
        result = 31 * result + (isLocal ? 1 : 0);
        result = 31 * result + (hashingStrategy != null ? hashingStrategy.getClass().hashCode() : 0);
        result = 31 * result + (memberDistributionStrategy != null ? memberDistributionStrategy.hashCode() : 0);
        result = 31 * result + (routingStrategy != null ? routingStrategy.getClass().hashCode() : 0);
        result = 31 * result + (partitioningStrategy != null ? partitioningStrategy.getClass().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Edge{"
                + "name='" + name + '\''
                + ", from=" + from
                + ", to=" + to
                + '}';
    }
}
