/*
 * Copyright 2021 Hazelcast Inc.
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

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.elastic.impl.Shard.Prirep;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.hazelcast.jet.elastic.impl.RetryUtils.withRetry;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.logging.Level.FINE;
import static java.util.stream.Collectors.toMap;

/**
 * Wrapper around {@link RestClient} access for /_cat/* endpoints
 */
public class ElasticCatClient implements Closeable {

    private static final ILogger LOG = Logger.getLogger(ElasticCatClient.class);

    private final RestClient client;
    private final int retries;

    public ElasticCatClient(RestClient client, int retries) {
        this.client = client;
        this.retries = retries;
    }

    /**
     * Returns current master of the ES cluster
     */
    public Master master() {
        try {
            Request r = new Request("GET", "/_cat/master");
            r.addParameter("format", "json");
            Response res = withRetry(() -> client.performRequest(r), retries);

            try (InputStreamReader reader = new InputStreamReader(res.getEntity().getContent(), UTF_8)) {
                JsonArray array = Json.parse(reader).asArray();
                JsonObject object = array.get(0).asObject();
                return new Master(
                        object.get("host").asString(),
                        object.get("id").asString(),
                        object.get("ip").asString(),
                        object.get("node").asString()
                );
            }
        } catch (IOException e) {
            throw new JetException("Could not get ES cluster master", e);
        }
    }

    /**
     * Returns list of nodes currently in ES cluster
     */
    public List<Node> nodes() {
        try {
            Request r = new Request("GET", "/_cat/nodes");
            r.addParameter("format", "json");
            r.addParameter("full_id", "true");
            r.addParameter("h", "id,ip,name,http_address,master");
            Response res = withRetry(() -> client.performRequest(r), retries);

            try (InputStreamReader reader = new InputStreamReader(res.getEntity().getContent(), UTF_8)) {
                JsonArray array = Json.parse(reader).asArray();
                List<Node> nodes = new ArrayList<>(array.size());
                for (JsonValue value : array) {
                    Optional<Node> shard = convertToNode(value);
                    shard.ifPresent(nodes::add);
                }

                LOG.fine("Nodes: " + nodes);
                return nodes;
            }
        } catch (IOException e) {
            throw new JetException("Could not get ES cluster nodes", e);
        }
    }

    private Optional<Node> convertToNode(JsonValue value) {
        JsonObject object = value.asObject();
        return of(new Node(
                object.get("id").asString(),
                object.get("ip").asString(),
                object.get("name").asString(),
                object.get("http_address").asString(),
                object.get("master").asString()
        ));
    }

    /**
     * Returns list of shards for given indexes
     *
     * Only STARTED shards are returned.
     *
     * @param indices indexes to return shards for (wildcard format accepted)
     */
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public List<Shard> shards(String... indices) {
        Map<String, String> idToAddress = nodes().stream().collect(toMap(Node::getId, Node::getHttpAddress));

        try {
            Request r = new Request("GET", "/_cat/shards/" + String.join(",", indices));
            r.addParameter("format", "json");
            r.addParameter("h", "id,index,shard,prirep,docs,state,ip,node");
            Response res = withRetry(() -> client.performRequest(r), retries);

            try (InputStreamReader reader = new InputStreamReader(res.getEntity().getContent(), UTF_8)) {
                JsonArray array = Json.parse(reader).asArray();
                List<Shard> shards = new ArrayList<>(array.size());
                for (JsonValue value : array) {
                    Optional<Shard> shard = convertToShard(value, idToAddress);
                    shard.ifPresent(shards::add);
                }

                LOG.log(FINE, "Shards " + shards);
                return shards;
            }
        } catch (IOException e) {
            throw new JetException("Could not get ES shards", e);
        }
    }

    private Optional<Shard> convertToShard(JsonValue value, Map<String, String> idToAddress) {
        JsonObject object = value.asObject();
        // TODO IndexShardState.STARTED but this is deeply inside elastic, should we mirror the enum?
        if ("STARTED".equals(object.get("state").asString())) {
            String id = object.get("id").asString();
            Shard shard = new Shard(
                    object.get("index").asString(),
                    Integer.parseInt(object.get("shard").asString()),
                    Prirep.valueOf(object.get("prirep").asString()),
                    object.get("docs") != null ? Integer.parseInt(object.get("docs").asString()) : 0,
                    object.get("state").asString(),
                    object.get("ip").asString(),
                    idToAddress.get(id),
                    object.get("node").asString()
            );
            return of(shard);
        } else {
            return empty();
        }
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    public static class Master {

        private final String host;

        private final String id;
        private final String ip;
        private final String node;

        public Master(String host, String id, String ip, String node) {
            this.host = host;
            this.id = id;
            this.ip = ip;
            this.node = node;
        }

        public String getHost() {
            return host;
        }

        public String getId() {
            return id;
        }

        public String getIp() {
            return ip;
        }

        public String getNode() {
            return node;
        }

        @Override public String toString() {
            return "Master{" +
                    "host='" + host + '\'' +
                    ", id='" + id + '\'' +
                    ", ip='" + ip + '\'' +
                    ", node='" + node + '\'' +
                    '}';
        }

    }

    public static class Node {

        private final String id;
        private final String ip;
        private final String name;
        private final String httpAddress;
        private final String master;

        public Node(@Nonnull String id, @Nonnull String ip, @Nonnull String name,
                    @Nonnull String httpAddress, @Nonnull String master) {

            this.id = id;
            this.ip = ip;
            this.name = name;
            this.httpAddress = httpAddress;
            this.master = master;
        }

        public String getId() {
            return id;
        }

        public String getIp() {
            return ip;
        }

        public String getName() {
            return name;
        }

        public String getHttpAddress() {
            return httpAddress;
        }

        public String getMaster() {
            return master;
        }

        @Override public String toString() {
            return "Node{" +
                    "ip='" + ip + '\'' +
                    ", name='" + name + '\'' +
                    ", httpAddress='" + httpAddress + '\'' +
                    ", master='" + master + '\'' +
                    '}';
        }
    }

}
