/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.config.SubsetRoutingConfig;
import com.hazelcast.client.impl.ClusterViewListenerService;
import com.hazelcast.client.impl.clientside.SubsetMembersView;
import com.hazelcast.client.impl.connection.tcp.KeyValuePairGenerator;
import com.hazelcast.cluster.Address;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static com.hazelcast.client.impl.connection.tcp.AuthenticationKeyValuePairConstants.CLUSTER_VERSION;
import static com.hazelcast.client.impl.connection.tcp.AuthenticationKeyValuePairConstants.SUBSET_MEMBER_GROUPS_INFO;
import static com.hazelcast.client.impl.connection.tcp.AuthenticationKeyValuePairConstants.checkMinimumClusterVersionForSubsetRouting;
import static com.hazelcast.client.impl.connection.tcp.AuthenticationKeyValuePairConstants.checkRequiredFieldsForSubsetRoutingExist;
import static com.hazelcast.client.impl.connection.tcp.KeyValuePairGenerator.createKeyValuePairs;
import static com.hazelcast.client.impl.connection.tcp.KeyValuePairGenerator.parseJson;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(QuickTest.class)
public class AuthenticationKeyValuePairConstantsTest {

    private static final MemberVersion VERSION = MemberVersion.of(BuildInfoProvider.getBuildInfo().getVersion());

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

        Map<String, String> keyValuePairs = createKeyValuePairs(array, 123, true, Version.of(5, 5));
        String clusterVersionStr = keyValuePairs.get(CLUSTER_VERSION);
        String jsonValue = keyValuePairs.get(SUBSET_MEMBER_GROUPS_INFO);

        // parse stringified values
        Version clusterVersion = Version.of(clusterVersionStr);
        KeyValuePairGenerator.MemberGroupsAndVersionHolder memberGroupsAndVersionHolder = parseJson(jsonValue);
        Collection<Collection<UUID>> collections = memberGroupsAndVersionHolder.allMemberGroups();
        Collection<UUID> group = collections.iterator().next();
        SubsetMembersView subsetMembersView = new SubsetMembersView(null,
                Set.copyOf(group), memberGroupsAndVersionHolder.version());

        // verify parsed values
        assertEquals(Version.of(5, 5), clusterVersion);
        Set<UUID> members = subsetMembersView.members();
        int size = members.size();
        assertEquals(memberInfoList.size(), size);
        assertEquals(membersView.getVersion(), subsetMembersView.version());
        assertEquals(members, createdMemberUuids);
    }

    @Test
    public void testRequiredFieldsForSubsetRoutingExist_subsetRoutingDisabled() {
        Map<String, String> map = new HashMap<>();
        map.put("enabled", "false");
        map.put("routingStrategy", "PARTITION_GROUPS");
        assertFalse(checkRequiredFieldsForSubsetRoutingExist(new SubsetRoutingConfig(), map));
    }

    @Test
    public void testRequiredFieldsForSubsetRoutingExist_missingMemberGroups() {
        Map<String, String> map = new HashMap<>();
        SubsetRoutingConfig subsetRoutingConfig = new SubsetRoutingConfig();
        subsetRoutingConfig.setEnabled(true);
        assertThrows(UnsupportedRoutingModeException.class,
                () -> checkRequiredFieldsForSubsetRoutingExist(subsetRoutingConfig, map));
    }

    @Test
    public void testRequiredFieldsForSubsetRoutingExist_returnsTrue() {
        Map<String, String> map = new HashMap<>();
        map.put("memberGroups", "[]");
        map.put("enabled", "true");
        map.put("routingStrategy", "PARTITION_GROUPS");
        SubsetRoutingConfig subsetRoutingConfig = new SubsetRoutingConfig();
        subsetRoutingConfig.setEnabled(true);
        assertTrue(checkRequiredFieldsForSubsetRoutingExist(subsetRoutingConfig, map));
    }

    @ParameterizedTest
    @MethodSource("unsupportedClusterVersions")
    void checkClusterVersionThrowsClusterVersionLessThan5_5(Version version) {
        Map<String, String> map = new HashMap<>();
        map.put("clusterVersion", version.toString());
        assertThrows(UnsupportedClusterVersionException.class, () -> checkMinimumClusterVersionForSubsetRouting(map));
    }

    @Test
    public void checkClusterVersionThrowsWhenNoClusterVersionPresent() {
        assertThrows(UnsupportedClusterVersionException.class, () -> checkMinimumClusterVersionForSubsetRouting(emptyMap()));
    }

    @Test
    public void checkClusterVersion5_5_supported() {
        Map<String, String> map = new HashMap<>();
        map.put("clusterVersion", "5.5");
        checkMinimumClusterVersionForSubsetRouting(map);
    }

    @Test
    public void checkThrowsOnUnknownClusterVersion() {
        Map<String, String> map = new HashMap<>();
        map.put("clusterVersion", Version.UNKNOWN.toString());
        assertThrows(UnsupportedClusterVersionException.class, () -> checkMinimumClusterVersionForSubsetRouting(map));
    }

    @Nonnull
    private static MemberInfo member(String host) {
        try {
            return new MemberInfo(new Address(host, 5701), UUID.randomUUID(), emptyMap(), false, VERSION);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }
}
