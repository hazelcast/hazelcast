/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.hazelcast.aws.AwsRequestUtils.createRestClient;
import static com.hazelcast.aws.AwsRequestUtils.currentTimestamp;
import static com.hazelcast.aws.AwsRequestUtils.urlFor;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

/**
 * Responsible for connecting to AWS ECS API.
 *
 * @see <a href="https://docs.aws.amazon.com/AmazonECS/latest/APIReference/Welcome.html">AWS ECS API</a>
 */
class AwsEcsApi {

    private static final ILogger LOGGER = Logger.getLogger(AwsEcsApi.class);

    // AWS defaults maxResults to 100 on its ECS listTasks() call
    private static final int ECS_LIST_TASKS_DEFAULT_MAX_RESULTS_PER_REQUEST = 100;

    // AWS limits the number of task arns per describeTasks() call to 100
    private static final int ECS_DESCRIBE_TASKS_TASK_ARNS_MAX_LENGTH_PER_REQUEST = 100;

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

    List<String> listTaskPrivateAddresses(String cluster, AwsCredentials credentials) {
        LOGGER.fine(String.format("Listing tasks from cluster: '%s'", cluster));
        List<String> taskArns = listTasks(cluster, credentials);
        LOGGER.fine(String.format("AWS ECS ListTasks found the following tasks: %s", taskArns));
        if (!taskArns.isEmpty()) {
            List<Task> tasks = describeTasks(cluster, taskArns, credentials);
            if (!tasks.isEmpty()) {
                return tasks.stream().map(Task::getPrivateAddress).collect(Collectors.toList());
            }
        }
        return emptyList();
    }

    private List<String> listTasks(String cluster, AwsCredentials credentials) {

        List<String> taskArns = new ArrayList<String>();

        String nextToken = null;
        do {
            String body = createBodyListTasks(cluster, ECS_LIST_TASKS_DEFAULT_MAX_RESULTS_PER_REQUEST, nextToken);
            Map<String, String> headers = createHeadersListTasks(body, credentials);
            String response = callAwsService(body, headers);

            // add in the task arns from this response
            JsonObject responseJson = toJson(response);
            taskArns.addAll(parseListTasks(responseJson));

            // if the response included "nextToken", go back and get more task arns
            nextToken = responseJson.getString("nextToken", null);
        } while (nextToken != null);

        return taskArns;
    }

    private String createBodyListTasks(String cluster, int maxResults, String nextToken) {
        JsonObject body = new JsonObject();
        body.add("cluster", cluster);
        if (!StringUtil.isNullOrEmptyAfterTrim(awsConfig.getFamily())) {
            body.add("family", awsConfig.getFamily());
        }
        if (!StringUtil.isNullOrEmptyAfterTrim(awsConfig.getServiceName())) {
            body.add("serviceName", awsConfig.getServiceName());
        }
        if (nextToken != null) {
            body.add("nextToken", nextToken);
        }
        if (maxResults > -1) {
            body.add("maxResults", maxResults);
        }
        return body.toString();
    }

    private Map<String, String> createHeadersListTasks(String body, AwsCredentials credentials) {
        return createHeaders(body, credentials, "ListTasks");
    }

    private List<String> parseListTasks(JsonObject responseJson) {
        return toStream(responseJson.get("taskArns"))
                .map(JsonValue::asString)
                .collect(Collectors.toList());
    }

    List<Task> describeTasks(String clusterArn, List<String> taskArns, AwsCredentials credentials)
    {
        List<Task> results = new ArrayList<Task>();
        if (taskArns.size() == 0) {
            return results;
        }

        // split up task arns into sub lists with a max size of ECS_DESCRIBE_TASKS_TASK_ARNS_PER_REQUEST
        List<List<String>> partitionedTaskArnsList = new ArrayList<List<String>>();
        for (int i = 0; i < taskArns.size(); i += ECS_DESCRIBE_TASKS_TASK_ARNS_MAX_LENGTH_PER_REQUEST) {
            partitionedTaskArnsList.add(taskArns.subList(i, Math.min(i + ECS_DESCRIBE_TASKS_TASK_ARNS_MAX_LENGTH_PER_REQUEST, taskArns.size())));
        }

        // make separate requests for each partitioned task arn list
        for (int i = 0; i < partitionedTaskArnsList.size(); i++) {
            List<String> partitionedTaskArns = partitionedTaskArnsList.get(i);
            String body = createBodyDescribeTasks(clusterArn, partitionedTaskArns);
            Map<String, String> headers = createHeadersDescribeTasks(body, credentials);
            String response = callAwsService(body, headers);
            results.addAll(parseDescribeTasks(response));
        }

        return results;
    }

    private String createBodyDescribeTasks(String cluster, List<String> taskArns) {
        JsonArray jsonArray = new JsonArray();
        taskArns.stream().map(Json::value).forEach(jsonArray::add);
        return new JsonObject()
                .add("tasks", jsonArray)
                .add("include", new JsonArray().add("TAGS"))
                .add("cluster", cluster)
                .toString();
    }

    private Map<String, String> createHeadersDescribeTasks(String body, AwsCredentials credentials) {
        return createHeaders(body, credentials, "DescribeTasks");
    }

    private List<Task> parseDescribeTasks(String response) {
        return toStream(toJson(response).get("tasks"))
                .filter(this::filterByTags)
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

    private static Map<String, String> parseTaskTags(JsonValue taskJson) {
        Map<String, String> tags = new HashMap<>();
        for (JsonValue tag : taskJson.asObject().get("tags").asArray()) {
            JsonObject object = tag.asObject();
            tags.put(object.getString("key", ""), object.getString("value", ""));
        }
        return tags;
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

    private boolean filterByTags(JsonValue taskJson) {
        if (!awsConfig.getTags().isEmpty()) {
            Map<String, String> tags = parseTaskTags(taskJson);
            return awsConfig.getTags().stream().allMatch(t -> Objects.equals(tags.get(t.getKey()), t.getValue()));
        }
        return true;
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