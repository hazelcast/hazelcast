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

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.collect.ImmutableMap;
import com.hazelcast.aws.AwsEcsApi.Task;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.util.StringUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.Assert.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.ArgumentMatchers.any;

@RunWith(MockitoJUnitRunner.class)
public class AwsEcsApiTest {
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(1585909518929L), ZoneId.systemDefault());
    private static final String AUTHORIZATION_HEADER = "authorization-header";
    private static final String TOKEN = "IQoJb3JpZ2luX2VjEFIaDGV1LWNlbnRyYWwtMSJGM==";
    private static final AwsCredentials CREDENTIALS = AwsCredentials.builder()
            .setAccessKey("AKIDEXAMPLE")
            .setSecretKey("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY")
            .setToken(TOKEN)
            .build();

    @Mock
    private AwsRequestSigner requestSigner;

    private String endpoint;

    private AwsEcsApi awsEcsApi;

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());

    @Before
    public void setUp() {
        given(requestSigner.authHeader(any(), any(), any(), any(), any(), any())).willReturn(AUTHORIZATION_HEADER);

        endpoint = String.format("http://localhost:%s", wireMockRule.port());
        AwsConfig awsConfig = AwsConfig.builder().build();
        awsEcsApi = new AwsEcsApi(endpoint, awsConfig, requestSigner, CLOCK);
    }

    @Test
    public void listTasks() {
        // given
        String cluster = "arn:aws:ecs:eu-central-1:665466731577:cluster/rafal-test-cluster";
        stubListTasks(cluster, null);
        Map<String, String> tasksArnToIp = ImmutableMap.of(
                "arn:aws:ecs:us-east-1:012345678910:task/0b69d5c0-d655-4695-98cd-5d2d526d9d5a", "10.0.1.16",
                "arn:aws:ecs:us-east-1:012345678910:task/51a01bdf-d00e-487e-ab14-7645330b6207", "10.0.1.219");
        stubDescribeTasks(tasksArnToIp, cluster);

        // when
        List<String> tasksPrivateIps = awsEcsApi.listTaskPrivateAddresses(cluster, CREDENTIALS);

        // then
        assertThat(tasksPrivateIps).containsExactlyInAnyOrder("10.0.1.16", "10.0.1.219");
    }

    @Test
    public void listTasksFiltered() {
        // given
        String cluster = "arn:aws:ecs:eu-central-1:665466731577:cluster/rafal-test-cluster";
        AwsConfig awsConfig = AwsConfig.builder()
                                       .setFamily("family-name")
                                       .build();
        AwsEcsApi awsEcsApi = new AwsEcsApi(endpoint, awsConfig, requestSigner, CLOCK);

        stubListTasks("arn:aws:ecs:eu-central-1:665466731577:cluster/rafal-test-cluster", "family-name");
        stubDescribeTasks(ImmutableMap.of(
                        "arn:aws:ecs:us-east-1:012345678910:task/0b69d5c0-d655-4695-98cd-5d2d526d9d5a", "10.0.1.16",
                        "arn:aws:ecs:us-east-1:012345678910:task/51a01bdf-d00e-487e-ab14-7645330b6207", "10.0.1.219"),
                cluster);

        // when
        List<String> ips = awsEcsApi.listTaskPrivateAddresses(cluster, CREDENTIALS);

        // then
        assertThat(ips).containsExactlyInAnyOrder("10.0.1.16", "10.0.1.219");
    }

    @Test
    public void listTasksFilteredByTags() {
        // given
        String cluster = "arn:aws:ecs:eu-central-1:665466731577:cluster/rafal-test-cluster";
        AwsConfig awsConfig = AwsConfig.builder()
                .setTagKey("tag-key")
                .setTagValue("51a01bdf-d00e-487e-ab14-7645330b6207")
                .build();
        AwsEcsApi awsEcsApi = new AwsEcsApi(endpoint, awsConfig, requestSigner, CLOCK);

        stubListTasks("arn:aws:ecs:eu-central-1:665466731577:cluster/rafal-test-cluster", null);
        stubDescribeTasks(ImmutableMap.of(
                        "arn:aws:ecs:us-east-1:012345678910:task/0b69d5c0-d655-4695-98cd-5d2d526d9d5a", "10.0.1.16",
                        "arn:aws:ecs:us-east-1:012345678910:task/51a01bdf-d00e-487e-ab14-7645330b6207", "10.0.1.219"),
                cluster);

        // when
        List<String> ips = awsEcsApi.listTaskPrivateAddresses(cluster, CREDENTIALS);

        // then
        assertEquals(1, ips.size());
        assertThat(ips).contains("10.0.1.219");
    }

    @Test
    public void describeTasks() {
        // given
        String cluster = "arn:aws:ecs:eu-central-1:665466731577:cluster/rafal-test-cluster";
        Map<String, String> tasks = ImmutableMap.of(
                "arn:aws:ecs:eu-central-1-east-1:012345678910:task/0b69d5c0-d655-4695-98cd-5d2d526d9d5a", "10.0.1.16",
                "arn:aws:ecs:eu-central-1:012345678910:task/51a01bdf-d00e-487e-ab14-7645330b6207", "10.0.1.219");
        stubDescribeTasks(tasks, cluster);

        // when
        List<Task> result = awsEcsApi.describeTasks(cluster, new ArrayList<>(tasks.keySet()), CREDENTIALS);

        // then
        assertEquals(2, result.size());
        assertThat(result.stream().map(Task::getPrivateAddress).collect(Collectors.toList())).containsExactlyInAnyOrder("10.0.1.16", "10.0.1.219");
        assertThat(result.stream().map(Task::getAvailabilityZone).collect(Collectors.toList())).containsExactlyInAnyOrder("eu-central-1a", "eu-central-1a");
    }

    @Test
    public void awsError() {
        // given
        int errorCode = 401;
        String errorMessage = "Error message retrieved from AWS";
        stubFor(post(urlMatching("/.*"))
                .willReturn(aResponse().withStatus(errorCode).withBody(errorMessage)));

        // when
        Exception exception = assertThrows(
                Exception.class, () -> awsEcsApi.listTaskPrivateAddresses("cluster-arn", CREDENTIALS));

        // then
        assertTrue(exception.getMessage().contains(Integer.toString(errorCode)));
        assertTrue(exception.getMessage().contains(errorMessage));
    }

    private void stubDescribeTasks(Map<String, String> taskArnToIp, String cluster) {
        JsonArray tasksJson = new JsonArray();
        taskArnToIp.keySet().forEach(tasksJson::add);
        String requestBody = new JsonObject()
                .add("tasks", tasksJson)
                .add("include", new JsonArray().add("TAGS"))
                .add("cluster", cluster)
                .toString();

        JsonArray responseTasks = new JsonArray();
        for (Map.Entry<String, String> task : taskArnToIp.entrySet()) {
            responseTasks.add(new JsonObject()
                    .add("taskArn", task.getKey())
                    .add("availabilityZone", "eu-central-1a")
                    .add("containers", new JsonArray().add(new JsonObject()
                            .add("taskArn", task.getKey())
                            .add("networkInterfaces", new JsonArray().add(new JsonObject()
                                    .add("privateIpv4Address", task.getValue())))))
                    .add("tags", new JsonArray().add(new JsonObject()
                            .add("key", "tag-key")
                            .add("value", task.getKey().substring(task.getKey().lastIndexOf("/") + 1))))
            );
        }
        String responseBody = new JsonObject()
                .add("tasks", responseTasks)
                .toString();

        stubFor(post("/")
                .withHeader("X-Amz-Date", equalTo("20200403T102518Z"))
                .withHeader("Authorization", equalTo(AUTHORIZATION_HEADER))
                .withHeader("X-Amz-Target", equalTo("AmazonEC2ContainerServiceV20141113.DescribeTasks"))
                .withHeader("Content-Type", equalTo("application/x-amz-json-1.1"))
                .withHeader("Accept-Encoding", equalTo("identity"))
                .withHeader("X-Amz-Security-Token", equalTo(TOKEN))
                .withRequestBody(equalToJson(requestBody))
                .willReturn(aResponse().withStatus(200).withBody(responseBody)));
    }

    private void stubListTasks(String cluster, String familyName) {
        JsonObject requestBody = new JsonObject();
        requestBody.add("cluster", cluster);
        if (!StringUtil.isNullOrEmptyAfterTrim(familyName)) {
            requestBody.add("family", familyName);
        }

        String response = new JsonObject()
                .add("taskArns", new JsonArray()
                        .add("arn:aws:ecs:us-east-1:012345678910:task/0b69d5c0-d655-4695-98cd-5d2d526d9d5a")
                        .add("arn:aws:ecs:us-east-1:012345678910:task/51a01bdf-d00e-487e-ab14-7645330b6207"))
                .toString();

        stubFor(post("/")
                .withHeader("X-Amz-Date", equalTo("20200403T102518Z"))
                .withHeader("Authorization", equalTo(AUTHORIZATION_HEADER))
                .withHeader("X-Amz-Target", equalTo("AmazonEC2ContainerServiceV20141113.ListTasks"))
                .withHeader("Content-Type", equalTo("application/x-amz-json-1.1"))
                .withHeader("Accept-Encoding", equalTo("identity"))
                .withHeader("X-Amz-Security-Token", equalTo(TOKEN))
                .withRequestBody(equalToJson(requestBody.toString()))
                .willReturn(aResponse().withStatus(200).withBody(response)));
    }
}
