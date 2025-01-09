/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.UnsupportedClusterVersionException;
import com.hazelcast.client.UnsupportedRoutingModeException;
import com.hazelcast.client.config.ClusterRoutingConfig;
import com.hazelcast.client.impl.ClusterViewListenerService;
import com.hazelcast.client.impl.clientside.SubsetMembersView;
import com.hazelcast.client.impl.connection.tcp.KeyValuePairGenerator;
import com.hazelcast.client.config.RoutingMode;
import com.hazelcast.cluster.Address;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPGroupsSnapshot;
import com.hazelcast.cp.internal.CPMemberInfo;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static com.hazelcast.client.impl.connection.tcp.AuthenticationKeyValuePairConstants.CLUSTER_VERSION;
import static com.hazelcast.client.impl.connection.tcp.AuthenticationKeyValuePairConstants.CP_LEADERS_INFO;
import static com.hazelcast.client.impl.connection.tcp.AuthenticationKeyValuePairConstants.MEMBER_GROUPS_INFO;
import static com.hazelcast.client.impl.connection.tcp.AuthenticationKeyValuePairConstants.checkMinimumClusterVersionForMultiMemberRouting;
import static com.hazelcast.client.impl.connection.tcp.AuthenticationKeyValuePairConstants.checkRequiredFieldsForMultiMemberRoutingExist;
import static com.hazelcast.client.impl.connection.tcp.KeyValuePairGenerator.createKeyValuePairs;
import static com.hazelcast.client.impl.connection.tcp.KeyValuePairGenerator.parseJsonForCPMembership;
import static com.hazelcast.client.impl.connection.tcp.KeyValuePairGenerator.parseJsonForMemberGroups;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(QuickTest.class)
class AuthenticationKeyValuePairConstantsTest {

    private static final MemberVersion VERSION = MemberVersion.of(BuildInfoProvider.getBuildInfo().getVersion());
    private static final UUID DUMMY_LEADER_UUID = UUID.randomUUID();
    private static final CPGroupId DUMMY_RAFT_GROUP_ID = new RaftGroupId("name", 12345L, 67890L);

    private static Stream<Version> unsupportedClusterVersions() {
        return Stream.of(
                Versions.V4_0,
                Versions.V4_1,
                Versions.V4_2,
                Versions.V5_0,
                Versions.V5_1,
                Versions.V5_2,
                Versions.V5_3,
                Versions.V5_4
        );
    }

    @Test
    public void createKeyValuePairs_generated_correct_json_state() {
        // create membersView
        List<MemberInfo> memberInfoList = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            memberInfoList.add(member("" + i));
        }
        MembersView membersView = new MembersView(123, memberInfoList);
        HashSet<UUID> createdMemberUuids = new HashSet<>();
        for (MemberInfo memberInfo : memberInfoList) {
            createdMemberUuids.add(memberInfo.getUuid());
        }

        // create mock member groups
        ClusterViewListenerService clusterViewListenerService = mock(ClusterViewListenerService.class);
        List<UUID> group1 = memberInfoList.stream().map(MemberInfo::getUuid).toList();
        Collection<Collection<UUID>> array = List.of(group1);
        when(clusterViewListenerService.toMemberGroups(any())).thenReturn(array);

        Map<String, String> keyValuePairs = createKeyValuePairs(array, 123, true,
                Version.of(5, 5), new DummyCPGroupsSnapshot());
        String clusterVersionStr = keyValuePairs.get(CLUSTER_VERSION);
        String subsetMemberGroupsStr = keyValuePairs.get(MEMBER_GROUPS_INFO);
        String cpLeadersString = keyValuePairs.get(CP_LEADERS_INFO);

        // parse stringified values
        Version clusterVersion = Version.of(clusterVersionStr);
        KeyValuePairGenerator.MemberGroupsAndVersionHolder memberGroupsAndVersionHolder = parseJsonForMemberGroups(subsetMemberGroupsStr);
        Collection<Collection<UUID>> collections = memberGroupsAndVersionHolder.allMemberGroups();
        Collection<UUID> group = collections.iterator().next();
        SubsetMembersView subsetMembersView = new SubsetMembersView(null,
                Set.copyOf(group), memberGroupsAndVersionHolder.version());
        Map<CPGroupId, UUID> cpLeaders = parseJsonForCPMembership(cpLeadersString);

        // verify parsed values
        assertEquals(Version.of(5, 5), clusterVersion);
        Set<UUID> members = subsetMembersView.members();
        int size = members.size();
        assertEquals(memberInfoList.size(), size);
        assertEquals(membersView.getVersion(), subsetMembersView.version());
        assertEquals(members, createdMemberUuids);
        assertEquals(createSimpleMap(DUMMY_RAFT_GROUP_ID, DUMMY_LEADER_UUID), cpLeaders);
    }

    @Test
    public void testRequiredFieldsForMultiMemberRoutingExist_multiMemberRoutingDisabled() {
        Map<String, String> map = new HashMap<>();
        map.put("mode", "SINGLE_MEMBER");
        map.put("routingStrategy", "PARTITION_GROUPS");
        assertFalse(checkRequiredFieldsForMultiMemberRoutingExist(new ClusterRoutingConfig(), map));
    }

    @Test
    public void testRequiredFieldsForMultiMemberRoutingExist_missingMemberGroups() {
        Map<String, String> map = new HashMap<>();
        ClusterRoutingConfig clusterRoutingConfig = new ClusterRoutingConfig();
        clusterRoutingConfig.setRoutingMode(RoutingMode.MULTI_MEMBER);
        assertThrows(UnsupportedRoutingModeException.class,
                () -> checkRequiredFieldsForMultiMemberRoutingExist(clusterRoutingConfig, map));
    }

    @Test
    public void testRequiredFieldsForMultiMemberRoutingExist_returnsTrue() {
        Map<String, String> map = new HashMap<>();
        map.put("memberGroups", "[]");
        map.put("mode", "MULTI_MEMBER");
        map.put("routingStrategy", "PARTITION_GROUPS");
        ClusterRoutingConfig clusterRoutingConfig = new ClusterRoutingConfig();
        clusterRoutingConfig.setRoutingMode(RoutingMode.MULTI_MEMBER);
        assertTrue(checkRequiredFieldsForMultiMemberRoutingExist(clusterRoutingConfig, map));
    }

    @ParameterizedTest
    @MethodSource("unsupportedClusterVersions")
    void checkClusterVersionThrowsClusterVersionLessThan5_5(Version version) {
        Map<String, String> map = new HashMap<>();
        map.put("clusterVersion", version.toString());
        assertThrows(UnsupportedClusterVersionException.class, () -> checkMinimumClusterVersionForMultiMemberRouting(map));
    }

    @Test
    public void checkClusterVersionThrowsWhenNoClusterVersionPresent() {
        assertThrows(UnsupportedClusterVersionException.class, () -> checkMinimumClusterVersionForMultiMemberRouting(emptyMap()));
    }

    @Test
    public void checkClusterVersion5_5_supported() {
        Map<String, String> map = new HashMap<>();
        map.put("clusterVersion", "5.5");
        checkMinimumClusterVersionForMultiMemberRouting(map);
    }

    @Test
    public void checkThrowsOnUnknownClusterVersion() {
        Map<String, String> map = new HashMap<>();
        map.put("clusterVersion", Version.UNKNOWN.toString());
        assertThrows(UnsupportedClusterVersionException.class, () -> checkMinimumClusterVersionForMultiMemberRouting(map));
    }

    @Nonnull
    private static MemberInfo member(String host) {
        try {
            return new MemberInfo(new Address(host, 5701), UUID.randomUUID(), emptyMap(), false, VERSION);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private static <K, V> Map<K, V> createSimpleMap(K key, V value) {
        return Collections.singletonMap(key, value);
    }

    private static class DummyCPGroupsSnapshot extends CPGroupsSnapshot {
        DummyCPGroupsSnapshot() {
            super(createSimpleMap(DUMMY_RAFT_GROUP_ID, new GroupInfo(new CPMemberInfo(DUMMY_LEADER_UUID, new Address()),
                    Collections.emptySet(), -1)), createSimpleMap(DUMMY_LEADER_UUID, DUMMY_LEADER_UUID));
        }
    }
}
