/*
 * Copyright 2020 Hazelcast Inc.
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
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.elastic.impl.Shard.Prirep;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.util.Lists.newArrayList;

public class ElasticSourcePMetaSupplierTest {

    @Test
    public void given_singleNodeAddress_when_assignShards_then_shouldAssignAllShardsToSingleAddress() {
        List<Shard> shards = newArrayList(
                new Shard("elastic-index", 0, Prirep.p, 10, "STARTED", "10.0.0.1", "10.0.0.1:9200", "node1"),
                new Shard("elastic-index", 1, Prirep.p, 10, "STARTED", "10.0.0.1", "10.0.0.1:9200", "node2"),
                new Shard("elastic-index", 2, Prirep.p, 10, "STARTED", "10.0.0.1", "10.0.0.1:9200", "node3")
        );

        List<Address> addresses = addresses("10.0.0.1");
        Map<Address, List<Shard>> assignment = ElasticSourcePMetaSupplier.assignShards(shards, addresses);

        assertThat(assignment).contains(
                entry(address("10.0.0.1"), shards)
        );
    }

    @Test
    public void given_elasticClusterSubsetOfJetCluster_when_assignShards_then_shouldAssignAllShards() {
        List<Shard> shards = newArrayList(
                new Shard("elastic-index", 0, Prirep.p, 10, "STARTED", "10.0.0.1", "10.0.0.1:9200", "node1"),
                new Shard("elastic-index", 1, Prirep.p, 10, "STARTED", "10.0.0.2", "10.0.0.2:9200", "node2"),
                new Shard("elastic-index", 2, Prirep.p, 10, "STARTED", "10.0.0.1", "10.0.0.1:9200", "node3")
        );

        List<Address> addresses = addresses("10.0.0.1", "10.0.0.2", "10.0.0.3");
        Map<Address, List<Shard>> assignment = ElasticSourcePMetaSupplier.assignShards(shards, addresses);

        assertThat(assignment).contains(
                entry(address("10.0.0.1"), newArrayList(shards.get(0), shards.get(2))),
                entry(address("10.0.0.2"), newArrayList(shards.get(1)))
        );
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

        assertThat(assignment).contains(
                entry(address("10.0.0.1"), newArrayList(shards.get(0))),
                entry(address("10.0.0.2"), newArrayList(shards.get(1))),
                entry(address("10.0.0.3"), newArrayList(shards.get(2)))
        );
    }

    @Test
    public void given_multipleNodeAddressesOnLocal_when_assignShards_then_shouldAssignSingleShardToEachAddress()
            throws UnknownHostException {
        List<Shard> shards = newArrayList(
                new Shard("elastic-index", 0, Prirep.p, 10, "STARTED", "127.0.0.1", "127.0.0.1:9200", "node1"),
                new Shard("elastic-index", 1, Prirep.p, 10, "STARTED", "127.0.0.1", "127.0.0.1:9201", "node2"),
                new Shard("elastic-index", 2, Prirep.p, 10, "STARTED", "127.0.0.1", "127.0.0.1:9202", "node3")
        );

        List<Address> addresses = newArrayList(
                new Address("127.0.0.1", 5701),
                new Address("127.0.0.1", 5702),
                new Address("127.0.0.1", 5703)
        );
        Map<Address, List<Shard>> assignment = ElasticSourcePMetaSupplier.assignShards(shards, addresses);

        assertThat(assignment).contains(
                entry(addresses.get(0), newArrayList(shards.get(0))),
                entry(addresses.get(1), newArrayList(shards.get(1))),
                entry(addresses.get(2), newArrayList(shards.get(2)))
        );
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

        Collections.shuffle(shards, new Random(1L)); // random but stable shuffle

        List<Address> addresses = addresses("10.0.0.1", "10.0.0.2", "10.0.0.3");
        Map<Address, List<Shard>> assignment = ElasticSourcePMetaSupplier.assignShards(shards, addresses);

        assertThat(assignment).containsKeys(address("10.0.0.1"), address("10.0.0.2"), address("10.0.0.3"));

        assertThat(assignment.get(address("10.0.0.1"))).hasSize(1);
        assertThat(assignment.get(address("10.0.0.2"))).hasSize(1);
        assertThat(assignment.get(address("10.0.0.3"))).hasSize(1);

        assertThat(assignment.get(address("10.0.0.1")).get(0).getIp()).isEqualTo("10.0.0.1");
        assertThat(assignment.get(address("10.0.0.2")).get(0).getIp()).isEqualTo("10.0.0.2");
        assertThat(assignment.get(address("10.0.0.3")).get(0).getIp()).isEqualTo("10.0.0.3");

        List<String> indexShards = assignment.values()
                                             .stream()
                                             .flatMap(Collection::stream)
                                             .map(Shard::indexShard)
                                             .collect(Collectors.toList());

        assertThat(indexShards).containsOnly("elastic-index-0", "elastic-index-1", "elastic-index-2");
    }

    @Test
    public void given_noCandidateForNode_when_assignShards_thenAssignNoShardToNode() {
        List<Shard> shards = newArrayList(
                new Shard("elastic-index", 0, Prirep.p, 10, "STARTED", "10.0.0.1", "10.0.0.1:9200", "node1"),
                new Shard("elastic-index", 1, Prirep.p, 10, "STARTED", "10.0.0.2", "10.0.0.1:9200", "node2")
        );

        List<Address> addresses = addresses("10.0.0.1", "10.0.0.2", "10.0.0.3");
        Map<Address, List<Shard>> assignment = ElasticSourcePMetaSupplier.assignShards(shards, addresses);

        assertThat(assignment).contains(
                entry(address("10.0.0.1"), newArrayList(shards.get(0))),
                entry(address("10.0.0.2"), newArrayList(shards.get(1)))
        );
    }

    @Test(expected = JetException.class)
    public void given_noMatchingNode_when_assignShards_thenThrowException() {
        List<Shard> shards = newArrayList(
                new Shard("elastic-index", 0, Prirep.p, 10, "STARTED", "10.0.0.1", "10.0.0.1:9200", "node1")
        );
        List<Address> addresses = addresses("10.0.0.2");

        ElasticSourcePMetaSupplier.assignShards(shards, addresses);
    }

    private List<Address> addresses(String... addresses) {
        ArrayList<Address> result = new ArrayList<>(addresses.length);
        for (String address : addresses) {
            result.add(address(address));
        }
        return result;
    }

    private Address address(String address) {
        try {
            return new Address(address, 5701);
        } catch (UnknownHostException e) {
            throw new RuntimeException();
        }
    }
}
