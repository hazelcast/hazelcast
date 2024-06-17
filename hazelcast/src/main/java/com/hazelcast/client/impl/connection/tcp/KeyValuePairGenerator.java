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

package com.hazelcast.client.impl.connection.tcp;

import com.hazelcast.client.impl.protocol.task.AuthenticationBaseMessageTask;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.version.Version;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.hazelcast.client.impl.connection.tcp.AuthenticationKeyValuePairConstants.CLUSTER_VERSION;
import static com.hazelcast.client.impl.connection.tcp.AuthenticationKeyValuePairConstants.SUBSET_MEMBER_GROUPS_INFO;
import static com.hazelcast.internal.util.JsonUtil.getArray;

/**
 * Utility to create key-value map for client authentication messages.
 *
 * @see AuthenticationBaseMessageTask
 */
public final class KeyValuePairGenerator {

    public static final String GROUPS = "groups";
    public static final String VERSION = "version";

    private KeyValuePairGenerator() { }

    // called on server side
    public static Map<String, String> createKeyValuePairs(Collection<Collection<UUID>> memberGroups,
                                                          int version,
                                                          boolean enterprise,
                                                          Version clusterVersion) {
        Map<String, String> keyValuePairs = new HashMap<>();
        keyValuePairs.put(CLUSTER_VERSION, clusterVersion.toString());
        if (enterprise) {
            keyValuePairs.put(SUBSET_MEMBER_GROUPS_INFO, toJsonString(memberGroups, version));
        }
        return keyValuePairs;
    }

    // called on server side
    private static String toJsonString(Collection<Collection<UUID>> memberGroups, int version) {
        JsonObject root = new JsonObject();
        root.add(VERSION, version);

        JsonArray allMemberGroups = new JsonArray();
        for (Collection<UUID> groupUuids : memberGroups) {
            JsonArray memberGroup = new JsonArray();
            for (UUID memberUuid : groupUuids) {
                memberGroup.add(memberUuid.toString());
            }
            allMemberGroups.add(memberGroup);
        }
        root.add(GROUPS, allMemberGroups);
        return root.toString();
    }

    // called on client side to parse server string
    public static MemberGroupsAndVersionHolder parseJson(String memberGroupsJsonString) {
        JsonValue value = Json.parse(memberGroupsJsonString);
        JsonObject root = value.asObject();
        JsonValue versionValue = root.get(VERSION);
        int version = versionValue.asInt();

        Collection<Collection<UUID>> allMemberGroups = new HashSet<>();
        JsonArray groups = getArray(root, GROUPS);
        List<JsonValue> uuidArrays = groups.values();

        for (JsonValue uuidArrayValue : uuidArrays) {
            JsonArray uuidArray = uuidArrayValue.asArray();
            Set<UUID> memberGroup = new HashSet<>();
            for (JsonValue uuidValue : uuidArray.values()) {
                UUID uuid = UUID.fromString(uuidValue.asString());
                memberGroup.add(uuid);
            }

            allMemberGroups.add(memberGroup);
        }

        return new MemberGroupsAndVersionHolder(allMemberGroups, version);
    }

    /**
     * State holder for all member groups returned from
     * the cluster and for their member list version.
     *
     * @param allMemberGroups all member groups info as a collection of
     *                        member-uuid groups
     * @param version         member list version
     */
    public record MemberGroupsAndVersionHolder(
            Collection<Collection<UUID>> allMemberGroups,
            int version
    ) { }
}
