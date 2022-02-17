/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.monitor.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.management.dto.ClientEndPointDTO;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO;
import com.hazelcast.internal.monitor.HotRestartState;
import com.hazelcast.internal.monitor.LocalOperationStats;
import com.hazelcast.internal.monitor.LocalWanStats;
import com.hazelcast.internal.monitor.MemberPartitionState;
import com.hazelcast.internal.monitor.MemberState;
import com.hazelcast.internal.monitor.NodeState;
import com.hazelcast.json.internal.JsonSerializable;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.internal.util.JsonUtil.getArray;
import static com.hazelcast.internal.util.JsonUtil.getObject;
import static com.hazelcast.internal.util.JsonUtil.getString;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

@SuppressWarnings({"checkstyle:classdataabstractioncoupling",
        "checkstyle:classfanoutcomplexity",
        "checkstyle:methodcount"})
public class MemberStateImpl implements MemberState {

    private String address;
    private UUID uuid;
    private UUID cpMemberUuid;
    private String name;
    private Map<EndpointQualifier, Address> endpoints = emptyMap();
    private Set<String> mapsWithStats = emptySet();
    private Set<String> multiMapsWithStats = emptySet();
    private Set<String> replicatedMapsWithStats = emptySet();
    private Set<String> queuesWithStats = emptySet();
    private Set<String> topicsWithStats = emptySet();
    private Set<String> reliableTopicsWithStats = emptySet();
    private Set<String> pnCountersWithStats = emptySet();
    private Set<String> executorsWithStats = emptySet();
    private Set<String> scheduledExecutorsWithStats = emptySet();
    private Set<String> durableExecutorsWithStats = emptySet();
    private Set<String> cachesWithStats = emptySet();
    private Set<String> flakeIdGeneratorsWithStats = emptySet();
    private Collection<ClientEndPointDTO> clients = emptySet();
    private Map<UUID, String> clientStats = emptyMap();
    private MemberPartitionState memberPartitionState = new MemberPartitionStateImpl();
    private LocalOperationStats operationStats = new LocalOperationStatsImpl();
    private NodeState nodeState = new NodeStateImpl();
    private HotRestartState hotRestartState = new HotRestartStateImpl();
    private ClusterHotRestartStatusDTO clusterHotRestartStatus = new ClusterHotRestartStatusDTO();
    private final Map<String, LocalWanStats> wanStats = new HashMap<>();

    public MemberStateImpl() {
    }

    @Override
    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @Override
    public UUID getUuid() {
        return uuid;
    }

    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }

    @Override
    public UUID getCpMemberUuid() {
        return cpMemberUuid;
    }

    public void setCpMemberUuid(UUID cpMemberUuid) {
        this.cpMemberUuid = cpMemberUuid;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<EndpointQualifier, Address> getEndpoints() {
        return endpoints;
    }

    public void setEndpoints(Map<EndpointQualifier, Address> addressMap) {
        this.endpoints = addressMap;
    }

    public Set<String> getMapsWithStats() {
        return mapsWithStats;
    }

    public void setMapsWithStats(Set<String> mapsWithStats) {
        this.mapsWithStats = mapsWithStats;
    }

    public Set<String> getMultiMapsWithStats() {
        return multiMapsWithStats;
    }

    public void setMultiMapsWithStats(Set<String> multiMapsWithStats) {
        this.multiMapsWithStats = multiMapsWithStats;
    }

    public Set<String> getReplicatedMapsWithStats() {
        return replicatedMapsWithStats;
    }

    public void setReplicatedMapsWithStats(Set<String> replicatedMapsWithStats) {
        this.replicatedMapsWithStats = replicatedMapsWithStats;
    }

    public Set<String> getQueuesWithStats() {
        return queuesWithStats;
    }

    public void setQueuesWithStats(Set<String> queuesWithStats) {
        this.queuesWithStats = queuesWithStats;
    }

    public Set<String> getTopicsWithStats() {
        return topicsWithStats;
    }

    public void setTopicsWithStats(Set<String> topicsWithStats) {
        this.topicsWithStats = topicsWithStats;
    }

    public Set<String> getReliableTopicsWithStats() {
        return reliableTopicsWithStats;
    }

    public void setReliableTopicsWithStats(Set<String> reliableTopicsWithStats) {
        this.reliableTopicsWithStats = reliableTopicsWithStats;
    }

    public Set<String> getPNCountersWithStats() {
        return pnCountersWithStats;
    }

    public void setPNCountersWithStats(Set<String> pnCountersWithStats) {
        this.pnCountersWithStats = pnCountersWithStats;
    }

    public Set<String> getExecutorsWithStats() {
        return executorsWithStats;
    }

    public void setExecutorsWithStats(Set<String> executorsWithStats) {
        this.executorsWithStats = executorsWithStats;
    }

    public void setScheduledExecutorsWithStats(Set<String> scheduledExecutorsWithStats) {
        this.scheduledExecutorsWithStats = scheduledExecutorsWithStats;
    }

    public void setDurableExecutorsWithStats(Set<String> durableExecutorsWithStats) {
        this.durableExecutorsWithStats = durableExecutorsWithStats;
    }

    public Set<String> getCachesWithStats() {
        return cachesWithStats;
    }

    public void setCachesWithStats(Set<String> cachesWithStats) {
        this.cachesWithStats = cachesWithStats;
    }

    public Set<String> getFlakeIdGeneratorsWithStats() {
        return flakeIdGeneratorsWithStats;
    }

    public void setFlakeIdGeneratorsWithStats(Set<String> flakeIdGeneratorsWithStats) {
        this.flakeIdGeneratorsWithStats = flakeIdGeneratorsWithStats;
    }

    public void putLocalWanStats(String name, LocalWanStats localWanStats) {
        wanStats.put(name, localWanStats);
    }

    public Collection<ClientEndPointDTO> getClients() {
        return clients;
    }

    public void setClients(Collection<ClientEndPointDTO> clients) {
        this.clients = clients;
    }

    @Override
    public LocalOperationStats getOperationStats() {
        return operationStats;
    }

    public void setOperationStats(LocalOperationStats operationStats) {
        this.operationStats = operationStats;
    }

    @Override
    public MemberPartitionState getMemberPartitionState() {
        return memberPartitionState;
    }

    @Override
    public NodeState getNodeState() {
        return nodeState;
    }

    public void setNodeState(NodeState nodeState) {
        this.nodeState = nodeState;
    }

    @Override
    public HotRestartState getHotRestartState() {
        return hotRestartState;
    }

    public void setHotRestartState(HotRestartState hotRestartState) {
        this.hotRestartState = hotRestartState;
    }

    @Override
    public ClusterHotRestartStatusDTO getClusterHotRestartStatus() {
        return clusterHotRestartStatus;
    }

    public void setClusterHotRestartStatus(ClusterHotRestartStatusDTO clusterHotRestartStatus) {
        this.clusterHotRestartStatus = clusterHotRestartStatus;
    }

    public Map<UUID, String> getClientStats() {
        return clientStats;
    }

    public void setClientStats(Map<UUID, String> clientStats) {
        this.clientStats = clientStats;
    }

    @Override
    public JsonObject toJson() {
        final JsonObject root = new JsonObject();
        root.add("address", address);
        String uuidString = uuid != null ? uuid.toString() : null;
        root.add("uuid", uuidString);
        String cpMemberUuidString = cpMemberUuid != null ? cpMemberUuid.toString() : null;
        root.add("cpMemberUuid", cpMemberUuidString);
        root.add("name", name);

        final JsonArray endpoints = new JsonArray();
        for (Entry<EndpointQualifier, Address> entry : this.endpoints.entrySet()) {
            JsonObject address = new JsonObject();
            address.set("host", entry.getValue().getHost());
            address.set("port", entry.getValue().getPort());

            JsonObject endpoint = new JsonObject();
            endpoint.set("protocol", entry.getKey().getType().name());
            endpoint.set("address", address);

            if (entry.getKey().getIdentifier() != null) {
                endpoint.set("id", entry.getKey().getIdentifier());
            }

            endpoints.add(endpoint);
        }
        root.add("endpoints", endpoints);

        serializeAsMap(root, "mapStats", mapsWithStats);
        serializeAsMap(root, "multiMapStats", multiMapsWithStats);
        serializeAsMap(root, "replicatedMapStats", replicatedMapsWithStats);
        serializeAsMap(root, "queueStats", queuesWithStats);
        serializeAsMap(root, "topicStats", topicsWithStats);
        serializeAsMap(root, "reliableTopicStats", reliableTopicsWithStats);
        serializeAsMap(root, "pnCounterStats", pnCountersWithStats);
        serializeAsMap(root, "executorStats", executorsWithStats);
        serializeAsMap(root, "scheduledExecutorStats", scheduledExecutorsWithStats);
        serializeAsMap(root, "durableExecutorStats", durableExecutorsWithStats);
        serializeAsMap(root, "cacheStats", cachesWithStats);
        serializeAsMap(root, "flakeIdStats", flakeIdGeneratorsWithStats);
        serializeMap(root, "wanStats", wanStats);

        final JsonArray clientsArray = new JsonArray();
        for (ClientEndPointDTO client : clients) {
            clientsArray.add(client.toJson());
        }
        root.add("clients", clientsArray);
        addJsonIfSerializable(root, "operationStats", operationStats);
        root.add("memberPartitionState", memberPartitionState.toJson());
        root.add("nodeState", nodeState.toJson());
        root.add("hotRestartState", hotRestartState.toJson());
        root.add("clusterHotRestartStatus", clusterHotRestartStatus.toJson());

        JsonObject clientStatsObject = new JsonObject();
        for (Map.Entry<UUID, String> entry : clientStats.entrySet()) {
            clientStatsObject.add(entry.getKey().toString(), entry.getValue());
        }
        root.add("clientStats", clientStatsObject);
        return root;
    }

    private static void serializeMap(JsonObject root, String key, Map<String, ?> map) {
        final JsonObject jsonObject = new JsonObject();
        for (Entry<String, ?> e : map.entrySet()) {
            addJsonIfSerializable(jsonObject, e.getKey(), e.getValue());
        }
        root.add(key, jsonObject);
    }

    /**
     * Keeps JSON representation compatible with IMDG 4.0 by using empty objects as values.
     */
    private static void serializeAsMap(JsonObject root, String key, Set<String> set) {
        JsonObject jsonObject = new JsonObject();
        JsonObject stubValue = new JsonObject();
        for (String s : set) {
            jsonObject.add(s, stubValue);
        }
        root.add(key, jsonObject);
    }

    private static void addJsonIfSerializable(JsonObject root, String key, Object value) {
        if (value instanceof JsonSerializable) {
            root.add(key, ((JsonSerializable) value).toJson());
        }
    }

    private static <T> void putDeserializedIfSerializable(Map<String, T> deserializedValueMap,
                                                          String key,
                                                          JsonObject serializedJson,
                                                          T instance) {
        if (instance instanceof JsonSerializable) {
            ((JsonSerializable) instance).fromJson(serializedJson);
            deserializedValueMap.put(key, instance);
        }
    }

    private static <T> T readJsonIfDeserializable(JsonObject serializedJson, T instance) {
        if (instance instanceof JsonSerializable) {
            ((JsonSerializable) instance).fromJson(serializedJson);
        }
        return instance;
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity", "checkstyle:methodlength"})
    public void fromJson(JsonObject json) {
        address = getString(json, "address");
        String uuidString = getString(json, "uuid", null);
        uuid = uuidString != null ? UUID.fromString(uuidString) : null;
        String cpMemberUuidString = getString(json, "cpMemberUuid", null);
        cpMemberUuid = cpMemberUuidString != null ? UUID.fromString(cpMemberUuidString) : null;
        name = getString(json, "name", null);

        JsonArray jsonEndpoints = getArray(json, "endpoints");
        endpoints = new HashMap<>();
        for (JsonValue obj : jsonEndpoints) {
            JsonObject endpoint = obj.asObject();
            String id = endpoint.getString("id", null);
            ProtocolType type = ProtocolType.valueOf(endpoint.getString("protocol", "MEMBER"));

            JsonValue addr = endpoint.get("address");
            String host = addr.asObject().getString("host", "");
            int port = addr.asObject().getInt("port", 0);
            EndpointQualifier qualifier = EndpointQualifier.resolve(type, id);
            Address address = null;
            try {
                address = new Address(host, port);
            } catch (UnknownHostException e) {
                //ignore
                ignore(e);
            }
            endpoints.put(qualifier, address);
        }

        mapsWithStats = new HashSet<>();
        for (JsonObject.Member next : getObject(json, "mapStats")) {
            mapsWithStats.add(next.getName());
        }
        multiMapsWithStats = new HashSet<>();
        for (JsonObject.Member next : getObject(json, "multiMapStats")) {
            multiMapsWithStats.add(next.getName());
        }
        replicatedMapsWithStats = new HashSet<>();
        for (JsonObject.Member next : getObject(json, "replicatedMapStats")) {
            replicatedMapsWithStats.add(next.getName());
        }
        queuesWithStats = new HashSet<>();
        for (JsonObject.Member next : getObject(json, "queueStats")) {
            queuesWithStats.add(next.getName());
        }
        topicsWithStats = new HashSet<>();
        for (JsonObject.Member next : getObject(json, "topicStats")) {
            topicsWithStats.add(next.getName());
        }
        reliableTopicsWithStats = new HashSet<>();
        for (JsonObject.Member next : getObject(json, "reliableTopicStats")) {
            reliableTopicsWithStats.add(next.getName());
        }
        pnCountersWithStats = new HashSet<>();
        for (JsonObject.Member next : getObject(json, "pnCounterStats")) {
            pnCountersWithStats.add(next.getName());
        }
        executorsWithStats = new HashSet<>();
        for (JsonObject.Member next : getObject(json, "executorStats")) {
            executorsWithStats.add(next.getName());
        }
        scheduledExecutorsWithStats = new HashSet<>();
        for (JsonObject.Member next : getObject(json, "scheduledExecutorStats")) {
            scheduledExecutorsWithStats.add(next.getName());
        }
        durableExecutorsWithStats = new HashSet<>();
        for (JsonObject.Member next : getObject(json, "durableExecutorStats")) {
            durableExecutorsWithStats.add(next.getName());
        }
        cachesWithStats = new HashSet<>();
        for (JsonObject.Member next : getObject(json, "cacheStats")) {
            cachesWithStats.add(next.getName());
        }
        flakeIdGeneratorsWithStats = new HashSet<>();
        for (JsonObject.Member next : getObject(json, "flakeIdStats")) {
            flakeIdGeneratorsWithStats.add(next.getName());
        }
        for (JsonObject.Member next : getObject(json, "wanStats", new JsonObject())) {
            putDeserializedIfSerializable(wanStats, next.getName(), next.getValue().asObject(),
                    new LocalWanStatsImpl());
        }
        JsonArray jsonClients = getArray(json, "clients");
        clients = new ArrayList<>();
        for (JsonValue jsonClient : jsonClients) {
            final ClientEndPointDTO client = new ClientEndPointDTO();
            client.fromJson(jsonClient.asObject());
            clients.add(client);
        }
        JsonObject jsonOperationStats = getObject(json, "operationStats", null);
        if (jsonOperationStats != null) {
            operationStats = readJsonIfDeserializable(jsonOperationStats, operationStats);
        }
        JsonObject jsonMemberPartitionState = getObject(json, "memberPartitionState", null);
        if (jsonMemberPartitionState != null) {
            memberPartitionState = new MemberPartitionStateImpl();
            memberPartitionState.fromJson(jsonMemberPartitionState);
        }
        JsonObject jsonNodeState = getObject(json, "nodeState", null);
        if (jsonNodeState != null) {
            nodeState = new NodeStateImpl();
            nodeState.fromJson(jsonNodeState);
        }
        JsonObject jsonHotRestartState = getObject(json, "hotRestartState", null);
        if (jsonHotRestartState != null) {
            hotRestartState = new HotRestartStateImpl();
            hotRestartState.fromJson(jsonHotRestartState);
        }
        JsonObject jsonClusterHotRestartStatus = getObject(json, "clusterHotRestartStatus", null);
        if (jsonClusterHotRestartStatus != null) {
            clusterHotRestartStatus = new ClusterHotRestartStatusDTO();
            clusterHotRestartStatus.fromJson(jsonClusterHotRestartStatus);
        }
        clientStats = new HashMap<>();
        for (JsonObject.Member next : getObject(json, "clientStats")) {
            clientStats.put(UUID.fromString(next.getName()), next.getValue().asString());
        }
    }

    @Override
    public String toString() {
        return "MemberStateImpl{"
                + "address=" + address
                + ", uuid=" + uuid
                + ", cpMemberUuid=" + cpMemberUuid
                + ", name=" + name
                + ", mapsWithStats=" + mapsWithStats
                + ", multiMapsWithStats=" + multiMapsWithStats
                + ", replicatedMapsWithStats=" + replicatedMapsWithStats
                + ", queuesWithStats=" + queuesWithStats
                + ", topicsWithStats=" + topicsWithStats
                + ", reliableTopicsWithStats=" + reliableTopicsWithStats
                + ", pnCountersWithStats=" + pnCountersWithStats
                + ", executorStats=" + executorsWithStats
                + ", scheduledExecutorStats=" + scheduledExecutorsWithStats
                + ", durableExecutorStats=" + durableExecutorsWithStats
                + ", cachesWithStats=" + cachesWithStats
                + ", flakeIdGeneratorsWithStats=" + flakeIdGeneratorsWithStats
                + ", wanStats=" + wanStats
                + ", operationStats=" + operationStats
                + ", memberPartitionState=" + memberPartitionState
                + ", nodeState=" + nodeState
                + ", hotRestartState=" + hotRestartState
                + ", clusterHotRestartStatus=" + clusterHotRestartStatus
                + ", clientStats=" + clientStats
                + '}';
    }
}
