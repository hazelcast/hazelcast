/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PartitioningAttributeConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.sql.impl.module.Pojo;
import com.hazelcast.map.IMap;
import com.hazelcast.partition.PartitionAware;
import com.hazelcast.partition.PartitionService;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hazelcast.internal.util.PartitioningStrategyUtil.constructAttributeBasedKey;
import static com.hazelcast.jet.config.JobConfigArguments.KEY_REQUIRED_PARTITIONS;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SqlPartitionPruningE2ETest extends SqlEndToEndTestSupport {
    private String mapName;

    private Tuple2<Set<Address>, Set<Integer>> expectedPartitionsAndMembers;

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(5, null);
    }

    @Before
    public void before() throws Exception {
        mapName = randomName();
    }

    @Test
    public void when_scanWithSimplePruningKey_then_prunable() {
        // Given
        final int c = 2; // constant
        final String query = "SELECT * FROM " + mapName + " WHERE f0 = " + c;

        preparePrunableMap(singletonList("f0"), mapName, c);

        // When
        SqlPlanImpl.SelectPlan selectPlan = assertQueryPlan(query);
        assertQueryResult(selectPlan, singletonList(new Row(c, c, c, "" + c)));

        // Then
        var partitionsToUse = planExecutor.tryUsePrunability(selectPlan, eec);
        assertPrunability(1, partitionsToUse);
    }

    @Test
    public void when_scanWithoutDefinedStrategy_then_nonPrunable() {
        // Given
        final int c = 2; // constant
        final String query = "SELECT * FROM " + mapName + " WHERE f0 = " + c;

        preparePrunableMap(emptyList(), mapName, c);

        SqlPlanImpl.SelectPlan selectPlan = assertQueryPlan(query);

        var partitionsToUse = planExecutor.tryUsePrunability(selectPlan, eec);

        assertEquals(0, partitionsToUse.size());
        assertQueryResult(selectPlan, singletonList(new Row(2, 2, 2, "2")));
    }

    @Test
    public void when_scanWithCompoundPruningKey_then_prunable() {
        // Given
        final int c = 2; // constant
        final String query = "SELECT * FROM " + mapName + " WHERE f0 = " + c + " AND f1 = " + c;

        preparePrunableMap(asList("f0", "f1"), mapName, c);

        SqlPlanImpl.SelectPlan selectPlan = assertQueryPlan(query);

        // When
        assertQueryResult(selectPlan, singletonList(new Row(c, c, c, "" + c)));

        // Then
        var partitionsToUse = planExecutor.tryUsePrunability(selectPlan, eec);
        assertPrunability(1, partitionsToUse);
    }

    @Test
    public void when_renamingScanWithCompoundPruningKey_then_prunable() {
        // Given
        final int c = 2; // constant
        final String query = "SELECT * FROM hazelcast.public." + mapName + " WHERE f0 = " + c + " AND f1 = " + c;

        preparePrunableMap(asList("f0", "f1"), mapName, c);

        SqlPlanImpl.SelectPlan selectPlan = assertQueryPlan(query);

        // When
        assertQueryResult(selectPlan, singletonList(new Row(c, c, c, "" + c)));

        // Then
        var partitionsToUse = planExecutor.tryUsePrunability(selectPlan, eec);
        assertPrunability(1, partitionsToUse);
    }

    @Test
    public void when_renamingNonPartitionedScanWithCompoundPruningKey_then_nonPrunable() {
        // Given
        final int c = 2; // constant
        final String query = "SELECT * FROM hazelcast.public." + mapName + " WHERE f0 = " + c + " AND f1 = " + c;

        preparePrunableMap(emptyList(), mapName, c);

        SqlPlanImpl.SelectPlan selectPlan = assertQueryPlan(query);

        // When
        assertQueryResult(selectPlan, singletonList(new Row(c, c, c, "" + c)));

        var partitionsToUse = planExecutor.tryUsePrunability(selectPlan, eec);
        assertEquals(0, partitionsToUse.size());
    }

    @Test
    public void when_keyWithNestedPartitionAwareKey_then_prunable() {
        // Given
        final long c = 2;
        final PAKey key = new PAKey(c, "" + c);
        preparePrunableMapWithPartitionAwareKey(singletonList("nestedKey"), mapName, false, key);

        // it can happen by accident that the test passes if partition id happens correct, but usually it wil fail
        final String query = "SELECT this FROM " + mapName + " WHERE nestedKey = ?";

        // When
        SqlPlanImpl.SelectPlan selectPlan = assertQueryPlan(query);
        assertQueryResult(selectPlan, singletonList(new Row("" + c)), key);

        // Then
        var partitionsToUse = planExecutor.tryUsePrunability(selectPlan, eec);
        assertPrunability(1, partitionsToUse);
    }

    @Test
    public void when_partitionAwareKeyWithNestedPartitionAwareKey_then_prunable() {
        // Given
        final long c = 2;
        final PAKey key = new PAKey(c, "" + c);
        preparePrunableMapWithPartitionAwareKey(singletonList("nestedKey"), mapName, true, key);

        // it can happen by accident that the test passes if partition id happens correct, but usually it wil fail
        final String query = "SELECT this FROM " + mapName + " WHERE nestedKey = ?";

        // When
        SqlPlanImpl.SelectPlan selectPlan = assertQueryPlan(query);
        assertQueryResult(selectPlan, singletonList(new Row("" + c)), key);

        // Then
        var partitionsToUse = planExecutor.tryUsePrunability(selectPlan, eec);
        assertPrunability(1, partitionsToUse);
    }

    @Test
    public void when_fullyComparePartitionAwareKeyWithNestedPAKey_then_prunable() {
        final long c = 2;
        final PAKey key = new PAKey(c, "" + c);
        final String query = "SELECT this FROM " + mapName + " WHERE __key = ? AND this IS NOT NULL";

        IMap<PAKeyWithPAField, String> map = instance().getMap(mapName);
        createMapping(mapName, PAKeyWithPAField.class, String.class);
        map.put(new PAKeyWithPAField(key), "" + c);

        expectedPartitionsAndMembers = calculateExpectedPartitionsForNestedKeys(true, 1, key);

        // When
        SqlPlanImpl.SelectPlan selectPlan = assertQueryPlan(query);
        assertQueryResult(selectPlan, singletonList(new Row("" + c)), new PAKeyWithPAField(key));

        // Then
        var partitionsToUse = planExecutor.tryUsePrunability(selectPlan, eec);
        assertPrunability(1, partitionsToUse);
    }

    @Test
    public void when_compoundSingleFieldNullKey_nonPrunable() {
        // Given
        final long c = 2;
        final PAKey key = new PAKey(c, "" + c);
        preparePrunableMapWithPartitionAwareKey(singletonList("nestedKey"), mapName, false, key);

        // it can happen by accident that the test passes if partition id happens correct, but usually it wil fail
        final String query = "SELECT this FROM " + mapName + " WHERE nestedKey = ?";

        // When
        SqlPlanImpl.SelectPlan selectPlan = assertQueryPlan(query);
        assertQueryResult(selectPlan, emptyList(), new Object[1]);

        // Then
        var partitionsToUse = planExecutor.tryUsePrunability(selectPlan, eec);
        assertEquals(0, partitionsToUse.size());
    }

    @Test
    public void when_selfUnionAllForOrPredicateAndSimplePruningKey_then_prunable() {
        // Given
        final int[] c = new int[]{2, 3}; // constants
        final String query = "(SELECT * FROM " + mapName + " WHERE f1 = " + c[0] + ")"
                + " UNION ALL "
                + "(SELECT * FROM " + mapName + " WHERE f1 = " + c[1] + ")";

        preparePrunableMap(singletonList("f1"), mapName, c);

        SqlPlanImpl.SelectPlan selectPlan = assertQueryPlan(query);

        // When
        assertQueryResult(selectPlan, asList(
                new Row(c[0], c[0], c[0], "" + c[0]),
                new Row(c[1], c[1], c[1], "" + c[1])));

        // Then
        var partitionsToUse = planExecutor.tryUsePrunability(selectPlan, eec);
        assertPrunability(c.length, partitionsToUse);
    }

    @Test
    public void when_selfUnionAllForOrPredicateAndCompoundPruningKey_then_prunable() {
        // Given
        final int[] c = new int[]{2, 3}; // constants
        final String query = "(SELECT * FROM " + mapName + " WHERE f0 = " + c[0] + " AND f1 = " + c[0] + ")"
                + " UNION ALL "
                + "(SELECT * FROM " + mapName + " WHERE f0 = " + c[1] + " AND f1 = " + c[1] + ")";

        preparePrunableMap(asList("f0", "f1"), mapName, c);

        SqlPlanImpl.SelectPlan selectPlan = assertQueryPlan(query);

        // When
        assertQueryResult(selectPlan, asList(
                new Row(c[0], c[0], c[0], "" + c[0]),
                new Row(c[1], c[1], c[1], "" + c[1])));

        // Then
        var partitionsToUse = planExecutor.tryUsePrunability(selectPlan, eec);
        assertPrunability(c.length, partitionsToUse);
    }

    @Test
    public void when_unionAllTwoMapsWithCompoundPruningKey_then_prunable() {
        final String secondMapName = randomName();
        final int[] c = new int[]{2, 3}; // constants
        final String query = "(SELECT f2 FROM " + mapName + " WHERE f0 = " + c[0] + " AND f1 = " + c[0] + ")"
                + " UNION ALL "
                + "(SELECT f2 FROM " + secondMapName + " WHERE f0 = " + c[1] + " AND f1 = " + c[1] + ")";

        preparePrunableMap(asList("f0", "f1"), mapName, c[0]);
        preparePrunableMap(asList("f0", "f1"), secondMapName, c[1]);

        expectedPartitionsAndMembers = calculateExpectedPartitions(2, c);

        SqlPlanImpl.SelectPlan selectPlan = assertQueryPlan(query);

        assertQueryResult(selectPlan, asList(new Row(c[0]), new Row(c[1])));

        // Then
        var partitionsToUse = planExecutor.tryUsePrunability(selectPlan, eec);
        assertPrunability(2, partitionsToUse);
    }

    @Test
    public void when_unionAllTwoMapsAndOneMapIsNotPrunable_then_nonPrunable() {
        final String secondMapName = randomName();
        final int[] c = new int[]{2, 3}; // constants
        final String query = "(SELECT f2 FROM " + mapName + " WHERE f0 = " + c[0] + " AND f1 = " + c[0] + ")"
                + " UNION ALL "
                + "(SELECT f2 FROM " + secondMapName + " WHERE f0 = " + c[1] + " AND f1 = " + c[1] + ")";

        preparePrunableMap(asList("f0", "f1"), mapName, c);
        // now prepare non-prunable map
        IMap<Pojo, String> map2 = instance().getMap(secondMapName);
        createMapping(secondMapName, Pojo.class, String.class);
        map2.put(new Pojo(c[1], c[1], c[1]), "3");

        SqlPlanImpl.SelectPlan selectPlan = assertQueryPlan(query);

        assertQueryResult(selectPlan, asList(new Row(2), new Row(3)));
        assertEquals(0, planExecutor.tryUsePrunability(selectPlan, eec).size());
    }

    @Ignore("https://hazelcast.atlassian.net/browse/HZ-2796")
    @Test
    public void when_unionWithSimplePruningKey_then_non_prunable() {
        // Note: it is a test for the future implementation of prunable Aggregation.
        //  Union converts to UnionAll + Aggregation, and  prunable Aggregation implementor
        //  easily may miss that fact during testing.
        final String mapName = randomName();
        final int c = 2; // constant
        final String query = "(SELECT f2 FROM " + mapName + " WHERE f0 = " + c + ")"
                + " UNION "
                + "(SELECT f2 FROM " + mapName + " WHERE f0 = " + c + ")";

        preparePrunableMap(emptyList(), mapName, c);

        SqlPlanImpl.SelectPlan selectPlan = assertQueryPlan(query);

        assertQueryResult(selectPlan, singletonList(new Row(c)));
        assertEquals(0, planExecutor.tryUsePrunability(selectPlan, eec).size());
    }

    // region asserts

    protected void assertPrunability(int expectedPartitionsCount, Set<Integer> partitionsToUse) {
        // region planExecutor.tryUsePrunability(...) assertions.
        Set<Integer> expectedPartitionsToUsePE = sqlJobInvocationObserver.jobConfig.getArgument(KEY_REQUIRED_PARTITIONS);
        Set<Integer> expectedPartitionsToUseJCS = jobInvocationObserver.jobConfig.getArgument(KEY_REQUIRED_PARTITIONS);
        Set<Integer> expectedPartitionsToParticipate = expectedPartitionsAndMembers.f1();

        assertEquals(expectedPartitionsCount, partitionsToUse.size());
        assertRequiredPartitions(expectedPartitionsToParticipate, expectedPartitionsToUsePE);
        assertRequiredPartitions(expectedPartitionsToParticipate, expectedPartitionsToUseJCS);
        // endregion

        // region JobCoordinationService feedback assertions.
        assertRequiredMembers(expectedPartitionsAndMembers.f0());
        // endregion
    }

    protected void assertRequiredPartitions(Set<Integer> expectedPartitions, Set<Integer> actualPartitions) {
        assertNotNull(expectedPartitions);
        assertNotNull(actualPartitions);
        assertContainsAll(expectedPartitions, actualPartitions);
    }

    protected void assertRequiredMembers(Set<Address> expectedMemberAddresses) {
        Set<Address> actualMemberAddresses = jobInvocationObserver.getMembers();

        assertNotNull(expectedMemberAddresses);
        assertNotNull(actualMemberAddresses);
        assertContainsAll(expectedMemberAddresses, actualMemberAddresses);
    }

    // endregion

    // region simple keys
    private void preparePrunableMap(List<String> attrs, String mapName, int... constants) {
        if (!attrs.isEmpty()) {
            expectedPartitionsAndMembers = calculateExpectedPartitions(attrs.size(), constants);
            List<PartitioningAttributeConfig> attributes = attrs.stream()
                    .map(PartitioningAttributeConfig::new)
                    .collect(Collectors.toList());

            instance().getConfig().addMapConfig(
                    new MapConfig(mapName).setPartitioningAttributeConfigs(attributes));
        }

        IMap<Pojo, String> map = instance().getMap(mapName);
        createMapping(mapName, Pojo.class, String.class);
        for (int c : constants) {
            map.put(new Pojo(c, c, c), "" + c);
        }
    }


    @SuppressWarnings({"SameParameterValue", "DanglingJavadoc"})
    /**
     * Calculates expected partitions and members to participate in the query execution.
     *
     * @param shouldUseCoordinator           whether coordinator should be included in the expected members
     * @param arity                          number of fields in partitioning attribute key
     *                                       (e.g. 2 for f0, f1)
     * @param partitionedPredicateConstants  constants used in the predicate in query
     */
    private Tuple2<Set<Address>, Set<Integer>> calculateExpectedPartitions(
            boolean shouldUseCoordinator, int arity, int... partitionedPredicateConstants) {
        PartitionService partitionService = instance().getPartitionService();
        Map<Address, int[]> partitionAssignment = getPartitionAssignment(instance());
        Map<Integer, Address> reversedPartitionAssignment = new HashMap<>();
        for (Entry<Address, int[]> entry : partitionAssignment.entrySet()) {
            for (int partitionId : entry.getValue()) {
                reversedPartitionAssignment.put(partitionId, entry.getKey());
            }
        }

        Set<Integer> expectedPartitionsToParticipate = new HashSet<>();
        Set<Address> expectedMembersToParticipate = new HashSet<>();
        for (int equalityConstants : partitionedPredicateConstants) {
            Object[] constants = new Object[arity];
            Arrays.fill(constants, equalityConstants);
            Data keyData = nodeEngine.getSerializationService().toData(constructAttributeBasedKey(constants), v -> v);
            int partitionId = nodeEngine.getPartitionService().getPartitionId(keyData);
            assertTrue(reversedPartitionAssignment.containsKey(partitionId));

            expectedPartitionsToParticipate.add(partitionId);
            expectedMembersToParticipate.add(reversedPartitionAssignment.get(partitionId));
        }

        if (shouldUseCoordinator) {
            expectedMembersToParticipate.add(instance().getCluster().getLocalMember().getAddress());
            expectedPartitionsToParticipate.add(partitionService.getPartition("").getPartitionId());
        }

        return Tuple2.tuple2(expectedMembersToParticipate, expectedPartitionsToParticipate);
    }

    private Tuple2<Set<Address>, Set<Integer>> calculateExpectedPartitions(int arity, int... partitionedPredicateConstants) {
        return calculateExpectedPartitions(true, arity, partitionedPredicateConstants);
    }

    // endregion

    // region nested keys

    private void preparePrunableMapWithPartitionAwareKey(
            List<String> attrs,
            String mapName,
            boolean usePartitionAwareKey,
            PAKey... keys) {
        if (!attrs.isEmpty()) {
            expectedPartitionsAndMembers = calculateExpectedPartitionsForNestedKeys(true, attrs.size(), keys);
            List<PartitioningAttributeConfig> attributes = attrs.stream()
                    .map(PartitioningAttributeConfig::new)
                    .collect(Collectors.toList());

            instance().getConfig().addMapConfig(
                    new MapConfig(mapName).setPartitioningAttributeConfigs(attributes));
        }

        IMap map = instance().getMap(mapName);
        createMapping(mapName, usePartitionAwareKey ? PAKeyWithPAField.class : KeyWithPAField.class, String.class);
        for (PAKey k : keys) {
            map.put(usePartitionAwareKey ? new PAKeyWithPAField(k) : new KeyWithPAField(k), "" + k.id);
        }
    }

    private Tuple2<Set<Address>, Set<Integer>> calculateExpectedPartitionsForNestedKeys(
            boolean shouldUseCoordinator,
            int arity,
            PAKey... keys) {
        PartitionService partitionService = instance().getPartitionService();
        Map<Address, int[]> partitionAssignment = getPartitionAssignment(instance());
        Map<Integer, Address> reversedPartitionAssignment = new HashMap<>();
        for (Entry<Address, int[]> entry : partitionAssignment.entrySet()) {
            for (int partitionId : entry.getValue()) {
                reversedPartitionAssignment.put(partitionId, entry.getKey());
            }
        }

        Set<Integer> expectedPartitionsToParticipate = new HashSet<>();
        Set<Address> expectedMembersToParticipate = new HashSet<>();
        for (PAKey key : keys) {
            PAKey[] paKeys = new PAKey[arity];
            Arrays.fill(paKeys, key);
            Data keyData = nodeEngine.getSerializationService().toData(constructAttributeBasedKey(paKeys), v -> v);
            int partitionId = nodeEngine.getPartitionService().getPartitionId(keyData);
            assertTrue(reversedPartitionAssignment.containsKey(partitionId));

            expectedPartitionsToParticipate.add(partitionId);
            expectedMembersToParticipate.add(reversedPartitionAssignment.get(partitionId));
        }

        if (shouldUseCoordinator) {
            expectedMembersToParticipate.add(instance().getCluster().getLocalMember().getAddress());
            expectedPartitionsToParticipate.add(partitionService.getPartition("").getPartitionId());
        }

        return Tuple2.tuple2(expectedMembersToParticipate, expectedPartitionsToParticipate);
    }

    protected static class PAKey implements Serializable, PartitionAware<String>, Comparable<PAKey> {
        public Long id;
        public String name;

        public PAKey() {
        }

        public PAKey(final Long id, final String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public String getPartitionKey() {
            return "hello";
        }

        @Override
        public int compareTo(@NotNull final PAKey o) {
            return hashCode() - o.hashCode();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final PAKey paKey = (PAKey) o;
            return Objects.equals(id, paKey.id) && Objects.equals(name, paKey.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name);
        }
    }

    protected static class KeyWithPAField implements Serializable {
        private PAKey nestedKey;

        public KeyWithPAField() {
        }

        public KeyWithPAField(PAKey nestedKey) {
            this.nestedKey = nestedKey;
        }

        public PAKey getNestedKey() {
            return nestedKey;
        }

        public void setNestedKey(PAKey nestedKey) {
            this.nestedKey = nestedKey;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            KeyWithPAField that = (KeyWithPAField) o;
            return Objects.equals(nestedKey, that.nestedKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nestedKey);
        }
    }

    protected static class PAKeyWithPAField implements Serializable, PartitionAware<PAKey>, Comparable<PAKeyWithPAField> {
        private PAKey nestedKey;

        public PAKeyWithPAField() {
        }

        public PAKeyWithPAField(PAKey nestedKey) {
            this.nestedKey = nestedKey;
        }

        @Override
        public PAKey getPartitionKey() {
            // this is a very special border case but theoretically valid
            return nestedKey;
        }

        public PAKey getNestedKey() {
            return nestedKey;
        }

        public void setNestedKey(PAKey nestedKey) {
            this.nestedKey = nestedKey;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            KeyWithPAField that = (KeyWithPAField) o;
            return Objects.equals(nestedKey, that.nestedKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nestedKey);
        }

        @Override
        public int compareTo(PAKeyWithPAField o) {
            return nestedKey.compareTo(o.nestedKey);
        }
    }

    // endregion
}
