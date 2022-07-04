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

package com.hazelcast.aws;

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.util.StringUtil;

import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.hazelcast.aws.AwsRequestUtils.createRestClient;
import static com.hazelcast.aws.AwsRequestUtils.currentTimestamp;
import static com.hazelcast.aws.AwsRequestUtils.urlFor;
import static java.util.Collections.emptyMap;

/**
 * Responsible for connecting to AWS ECS API.
 *
 * @see <a href="https://docs.aws.amazon.com/AmazonECS/latest/APIReference/Welcome.html">AWS ECS API</a>
 */
class AwsEcsApi {
    private final String endpoint;
    private final AwsConfig awsConfig;
    private final AwsRequestSigner requestSigner;
    private final Clock clock;

    AwsEcsApi(String endpoint, AwsConfig awsConfig, AwsRequestSigner requestSigner, Clock clock) {
        this.endpoint = endpoint;
        this.awsConfig = awsConfig;
        this.requestSigner = requestSigner;
        this.clock = clock;
    }

    List<String> listTasks(String cluster, AwsCredentials credentials) {
        String body = createBodyListTasks(cluster);
        Map<String, String> headers = createHeadersListTasks(body, credentials);
        String response = callAwsService(body, headers);
        return parseListTasks(response);
    }

    private String createBodyListTasks(String cluster) {
        JsonObject body = new JsonObject();
        body.add("cluster", cluster);
        if (!StringUtil.isNullOrEmptyAfterTrim(awsConfig.getFamily())) {
            body.add("family", awsConfig.getFamily());
        }
        if (!StringUtil.isNullOrEmptyAfterTrim(awsConfig.getServiceName())) {
            body.add("serviceName", awsConfig.getServiceName());
        }
        return body.toString();
    }

    private Map<String, String> createHeadersListTasks(String body, AwsCredentials credentials) {
        return createHeaders(body, credentials, "ListTasks");
    }

    private List<String> parseListTasks(String response) {
        return toStream(toJson(response).get("taskArns"))
            .map(JsonValue::asString)
            .collect(Collectors.toList());
    }

    List<Task> describeTasks(String clusterArn, List<String> taskArns, AwsCredentials credentials) {
        String body = createBodyDescribeTasks(clusterArn, taskArns);
        Map<String, String> headers = createHeadersDescribeTasks(body, credentials);
        String response = callAwsService(body, headers);
        return parseDescribeTasks(response);
    }

    private String createBodyDescribeTasks(String cluster, List<String> taskArns) {
        JsonArray jsonArray = new JsonArray();
        taskArns.stream().map(Json::value).forEach(jsonArray::add);
        return new JsonObject()
            .add("tasks", jsonArray)
            .add("cluster", cluster)
            .toString();
    }

    private Map<String, String> createHeadersDescribeTasks(String body, AwsCredentials credentials) {
        return createHeaders(body, credentials, "DescribeTasks");
    }

    private List<Task> parseDescribeTasks(String response) {
        return toStream(toJson(response).get("tasks"))
            .flatMap(e -> toTask(e).map(Stream::of).orElseGet(Stream::empty))
            .collect(Collectors.toList());
    }

    private Optional<Task> toTask(JsonValue taskJson) {
        String availabilityZone = taskJson.asObject().get("availabilityZone").asString();
        return toStream(taskJson.asObject().get("containers"))
            .flatMap(e -> toStream(e.asObject().get("networkInterfaces")))
            .map(e -> e.asObject().get("privateIpv4Address").asString())
            .map(e -> new Task(e, availabilityZone))
            .findFirst();
    }

    private Map<String, String> createHeaders(String body, AwsCredentials credentials, String awsTargetAction) {
        Map<String, String> headers = new HashMap<>();

        if (!StringUtil.isNullOrEmptyAfterTrim(credentials.getToken())) {
            headers.put("X-Amz-Security-Token", credentials.getToken());
        }
        headers.put("Host", endpoint);
        headers.put("X-Amz-Target", String.format("AmazonEC2ContainerServiceV20141113.%s", awsTargetAction));
        headers.put("Content-Type", "application/x-amz-json-1.1");
        headers.put("Accept-Encoding", "identity");
        String timestamp = currentTimestamp(clock);
        headers.put("X-Amz-Date", timestamp);
        headers.put("Authorization", requestSigner.authHeader(emptyMap(), headers, body, credentials, timestamp, "POST"));

        return headers;
    }

    private String callAwsService(String body, Map<String, String> headers) {
        return createRestClient(urlFor(endpoint), awsConfig)
            .withHeaders(headers)
            .withBody(body)
            .post()
            .getBody();
    }

    private static JsonObject toJson(String jsonString) {
        return Json.parse(jsonString).asObject();
    }

    private static Stream<JsonValue> toStream(JsonValue json) {
        return StreamSupport.stream(json.asArray().spliterator(), false);
    }

    static class Task {
        private final String privateAddress;
        private final String availabilityZone;

        Task(String privateAddress, String availabilityZone) {
            this.privateAddress = privateAddress;
            this.availabilityZone = availabilityZone;
        }

        String getPrivateAddress() {
            return privateAddress;
        }

        String getAvailabilityZone() {
            return availabilityZone;
        }
    }
}
