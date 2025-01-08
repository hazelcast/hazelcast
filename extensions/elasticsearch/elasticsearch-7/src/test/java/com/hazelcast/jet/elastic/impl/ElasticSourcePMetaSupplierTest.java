/*
 * Copyright 2025 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.elastic.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.elastic.impl.Shard.Prirep;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static java.util.Collections.emptyList;
import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.util.Lists.newArrayList;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ElasticSourcePMetaSupplierTest {

    @Test
    public void given_singleNodeAddress_when_assignShards_then_shouldAssignAllShardsToSingleAddress() {
        List<Shard> shards = List.of(
                new Shard("elastic-index", 0, Prirep.p, 10, "STARTED", "10.0.0.1", "10.0.0.1:9200", "node1"),
                new Shard("elastic-index", 1, Prirep.p, 10, "STARTED", "10.0.0.1", "10.0.0.1:9200", "node2"),
                new Shard("elastic-index", 2, Prirep.p, 10, "STARTED", "10.0.0.1", "10.0.0.1:9200", "node3")
        );

        List<Address> addresses = addresses("10.0.0.1");
        Map<Address, List<Shard>> assignment = ElasticSourcePMetaSupplier.assignShards(shards, addresses);

        assertThat(assignment.size()).isEqualTo(1);
        assertThat(assignment.get(address("10.0.0.1"))).containsExactlyInAnyOrderElementsOf(shards);
    }

    @Test
    public void given_elasticClusterSubsetOfJetCluster_when_assignShards_then_shouldAssignAllShards() {
        List<Shard> shards = List.of(
                new Shard("elastic-index", 0, Prirep.p, 10, "STARTED", "10.0.0.1", "10.0.0.1:9200", "node1"),
                new Shard("elastic-index", 1, Prirep.p, 10, "STARTED", "10.0.0.2", "10.0.0.2:9200", "node2"),
                new Shard("elastic-index", 2, Prirep.p, 10, "STARTED", "10.0.0.1", "10.0.0.1:9200", "node3")
        );

        List<Address> addresses = addresses("10.0.0.1", "10.0.0.2", "10.0.0.3");
        Map<Address, List<Shard>> assignment = ElasticSourcePMetaSupplier.assignShards(shards, addresses);

        assertThat(assignment.size()).isEqualTo(3);
        assertThat(assignment.get(address("10.0.0.1"))).containsExactlyInAnyOrder(shards.get(0), shards.get(2));
        assertThat(assignment.get(address("10.0.0.2"))).containsExactly(shards.get(1));
        assertThat(assignment.get(address("10.0.0.3"))).isEmpty();
    }

    @Test
    public void given_multipleNodeAddresses_when_assignShards_then_shouldAssignSingleShardToEachAddress() {
        List<Shard> shards = newArrayList(
                new Shard("elastic-index", 0, Prirep.p, 10, "STARTED", "10.0.0.1", "10.0.0.1:9200", "node1"),
                new Shard("elastic-index", 1, Prirep.p, 10, "STARTED", "10.0.0.2", "10.0.0.1:9200", "node2"),
                new Shard("elastic-index", 2, Prirep.p, 10, "STARTED", "10.0.0.3", "10.0.0.1:9200", "node3")
        );

        List<Address> addresses = addresses("10.0.0.1", "10.0.0.2", "10.0.0.3");
        Map<Address, List<Shard>> assignment = ElasticSourcePMetaSupplier.assignShards(shards, addresses);

        assertThat(assignment).containsOnly(
                entry(address("10.0.0.1"), List.of(shards.get(0))),
                entry(address("10.0.0.2"), List.of(shards.get(1))),
                entry(address("10.0.0.3"), List.of(shards.get(2)))
        );
    }

    @Test
    public void given_multipleNodeAddressesOnLocal_when_assignShards_then_shouldAssignSingleShardToEachAddress()
            throws UnknownHostException {
        List<Shard> shards = List.of(
                new Shard("elastic-index", 0, Prirep.p, 10, "STARTED", "127.0.0.1", "127.0.0.1:9200", "node1"),
                new Shard("elastic-index", 1, Prirep.p, 10, "STARTED", "127.0.0.1", "127.0.0.1:9201", "node2"),
                new Shard("elastic-index", 2, Prirep.p, 10, "STARTED", "127.0.0.1", "127.0.0.1:9202", "node3")
        );

        List<Address> addresses = List.of(
                new Address("127.0.0.1", 5701),
                new Address("127.0.0.1", 5702),
                new Address("127.0.0.1", 5703)
        );

        Map<Address, List<Shard>> assignment = ElasticSourcePMetaSupplier.assignShards(shards, addresses);

        assertThat(assignment)
                .containsOnlyKeys(addresses)
                // shards are distributed evenly
                .allSatisfy((address, shardList) -> assertThat(shardList).hasSize(1))
                // all shards are assigned
                .satisfies(a -> assertThat(a.values().stream().flatMap(List::stream))
                        .containsExactlyInAnyOrderElementsOf(shards));
    }

    @Test
    public void given_multipleReplicasForEachShard_when_assignShards_then_shouldAssignOneReplicaOnly() {
        List<Shard> shards = newArrayList(
                new Shard("elastic-index", 0, Prirep.p, 10, "STARTED", "10.0.0.1", "10.0.0.1:9200", "node1"),
                new Shard("elastic-index", 0, Prirep.r, 10, "STARTED", "10.0.0.2", "10.0.0.1:9200", "node2"),
                new Shard("elastic-index", 0, Prirep.r, 10, "STARTED", "10.0.0.3", "10.0.0.1:9200", "node3"),

                new Shard("elastic-index", 1, Prirep.p, 10, "STARTED", "10.0.0.2", "10.0.0.1:9200", "node2"),
                new Shard("elastic-index", 1, Prirep.r, 10, "STARTED", "10.0.0.3", "10.0.0.1:9200", "node3"),
                new Shard("elastic-index", 1, Prirep.r, 10, "STARTED", "10.0.0.1", "10.0.0.1:9200", "node1"),

                new Shard("elastic-index", 2, Prirep.p, 10, "STARTED", "10.0.0.3", "10.0.0.1:9200", "node3"),
                new Shard("elastic-index", 2, Prirep.r, 10, "STARTED", "10.0.0.1", "10.0.0.1:9200", "node1"),
                new Shard("elastic-index", 2, Prirep.r, 10, "STARTED", "10.0.0.2", "10.0.0.1:9200", "node2")
        );

        Collections.shuffle(shards, new Random());

        List<Address> addresses = addresses("10.0.0.1", "10.0.0.2", "10.0.0.3");
        Map<Address, List<Shard>> assignment = ElasticSourcePMetaSupplier.assignShards(shards, addresses);

        assertThat(assignment)
                .containsOnlyKeys(addresses)
                .allSatisfy((address, shardList) -> assertThat(shardList)
                        .hasSize(1)  // shards are distributed evenly
                        .allMatch(shard -> shard.getIp().equals(address.getHost())))  // shards are correctly assigned
                // all shards are assigned
                .satisfies(a -> assertThat(a.values().stream().flatMap(List::stream).map(Shard::indexShard))
                        .containsExactlyInAnyOrder("elastic-index-0", "elastic-index-1", "elastic-index-2"));
    }

    @Test
    public void given_noCandidateForNode_when_assignShards_thenAssignNoShardToNode() {
        List<Shard> shards = List.of(
                new Shard("elastic-index", 0, Prirep.p, 10, "STARTED", "10.0.0.1", "10.0.0.1:9200", "node1"),
                new Shard("elastic-index", 1, Prirep.p, 10, "STARTED", "10.0.0.2", "10.0.0.1:9200", "node2")
        );

        List<Address> addresses = addresses("10.0.0.1", "10.0.0.2", "10.0.0.3");
        Map<Address, List<Shard>> assignment = ElasticSourcePMetaSupplier.assignShards(shards, addresses);

        assertThat(assignment).containsOnly(
                entry(address("10.0.0.1"), List.of(shards.get(0))),
                entry(address("10.0.0.2"), List.of(shards.get(1))),
                entry(address("10.0.0.3"), emptyList())
        );
    }

    @Test
    public void given_noMatchingNode_when_assignShards_thenThrowException() {
        List<Shard> shards = List.of(
                new Shard("elastic-index", 0, Prirep.p, 10, "STARTED", "10.0.0.1", "10.0.0.1:9200", "node1")
        );
        List<Address> addresses = addresses("10.0.0.2");

        assertThatThrownBy(() -> ElasticSourcePMetaSupplier.assignShards(shards, addresses))
                .hasMessage("Selected members do not contain shard 'elastic-index-0'");
    }

    private static List<Address> addresses(String... addresses) {
        return Arrays.stream(addresses).map(ElasticSourcePMetaSupplierTest::address).toList();
    }

    private static Address address(String address) {
        try {
            return new Address(address, 5701);
        } catch (UnknownHostException e) {
            throw new RuntimeException();
        }
    }
}
