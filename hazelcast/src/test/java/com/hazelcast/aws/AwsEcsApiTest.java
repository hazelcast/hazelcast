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

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.hazelcast.aws.AwsEcsApi.Task;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;

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

        //language=JSON
        String requestBody = "{\n"
            + "  \"cluster\": \"arn:aws:ecs:eu-central-1:665466731577:cluster/rafal-test-cluster\"\n"
            + "}";

        //language=JSON
        String response = "{\n"
            + "  \"taskArns\": [\n"
            + "    \"arn:aws:ecs:us-east-1:012345678910:task/0b69d5c0-d655-4695-98cd-5d2d526d9d5a\",\n"
            + "    \"arn:aws:ecs:us-east-1:012345678910:task/51a01bdf-d00e-487e-ab14-7645330b6207\"\n"
            + "  ]\n"
            + "}";

        stubFor(post("/")
            .withHeader("X-Amz-Date", equalTo("20200403T102518Z"))
            .withHeader("Authorization", equalTo(AUTHORIZATION_HEADER))
            .withHeader("X-Amz-Target", equalTo("AmazonEC2ContainerServiceV20141113.ListTasks"))
            .withHeader("Content-Type", equalTo("application/x-amz-json-1.1"))
            .withHeader("Accept-Encoding", equalTo("identity"))
            .withHeader("X-Amz-Security-Token", equalTo(TOKEN))
            .withRequestBody(equalToJson(requestBody))
            .willReturn(aResponse().withStatus(200).withBody(response)));

        // when
        List<String> tasks = awsEcsApi.listTasks(cluster, CREDENTIALS);

        // then
        assertThat(tasks, hasItems(
            "arn:aws:ecs:us-east-1:012345678910:task/0b69d5c0-d655-4695-98cd-5d2d526d9d5a",
            "arn:aws:ecs:us-east-1:012345678910:task/51a01bdf-d00e-487e-ab14-7645330b6207"
            )
        );
    }

    @Test
    public void listTasksFiltered() {
        // given
        String cluster = "arn:aws:ecs:eu-central-1:665466731577:cluster/rafal-test-cluster";
        AwsConfig awsConfig = AwsConfig.builder()
            .setFamily("family-name")
            .build();
        AwsEcsApi awsEcsApi = new AwsEcsApi(endpoint, awsConfig, requestSigner, CLOCK);

        //language=JSON
        String requestBody = "{\n"
            + "  \"cluster\": \"arn:aws:ecs:eu-central-1:665466731577:cluster/rafal-test-cluster\",\n"
            + "  \"family\": \"family-name\"\n"
            + "}";

        //language=JSON
        String response = "{\n"
            + "  \"taskArns\": [\n"
            + "    \"arn:aws:ecs:us-east-1:012345678910:task/0b69d5c0-d655-4695-98cd-5d2d526d9d5a\",\n"
            + "    \"arn:aws:ecs:us-east-1:012345678910:task/51a01bdf-d00e-487e-ab14-7645330b6207\"\n"
            + "  ]\n"
            + "}";

        stubFor(post("/")
            .withHeader("X-Amz-Date", equalTo("20200403T102518Z"))
            .withHeader("Authorization", equalTo(AUTHORIZATION_HEADER))
            .withHeader("X-Amz-Target", equalTo("AmazonEC2ContainerServiceV20141113.ListTasks"))
            .withHeader("Content-Type", equalTo("application/x-amz-json-1.1"))
            .withHeader("Accept-Encoding", equalTo("identity"))
            .withHeader("X-Amz-Security-Token", equalTo(TOKEN))
            .withRequestBody(equalToJson(requestBody))
            .willReturn(aResponse().withStatus(200).withBody(response)));

        // when
        List<String> tasks = awsEcsApi.listTasks(cluster, CREDENTIALS);

        // then
        assertThat(tasks, hasItems(
            "arn:aws:ecs:us-east-1:012345678910:task/0b69d5c0-d655-4695-98cd-5d2d526d9d5a",
            "arn:aws:ecs:us-east-1:012345678910:task/51a01bdf-d00e-487e-ab14-7645330b6207"
            )
        );
    }

    @Test
    public void describeTasks() {
        // given
        String cluster = "arn:aws:ecs:eu-central-1:665466731577:cluster/rafal-test-cluster";
        List<String> tasks = asList(
            "arn:aws:ecs:eu-central-1-east-1:012345678910:task/0b69d5c0-d655-4695-98cd-5d2d526d9d5a",
            "arn:aws:ecs:eu-central-1:012345678910:task/51a01bdf-d00e-487e-ab14-7645330b6207"
        );

        //language=JSON
        String requestBody = "{\n"
            + "  \"cluster\" : \"arn:aws:ecs:eu-central-1:665466731577:cluster/rafal-test-cluster\",\n"
            + "  \"tasks\": [\n"
            + "    \"arn:aws:ecs:eu-central-1-east-1:012345678910:task/0b69d5c0-d655-4695-98cd-5d2d526d9d5a\",\n"
            + "    \"arn:aws:ecs:eu-central-1:012345678910:task/51a01bdf-d00e-487e-ab14-7645330b6207\"\n"
            + "  ]\n"
            + "}";

        //language=JSON
        String response = "{\n"
            + "  \"tasks\": [\n"
            + "    {\n"
            + "      \"taskArn\": \"arn:aws:ecs:eu-central-1-east-1:012345678910:task/0b69d5c0-d655-4695-98cd-5d2d526d9d5a\",\n"
            + "      \"availabilityZone\": \"eu-central-1a\",\n"
            + "      \"containers\": [\n"
            + "        {\n"
            + "          \"taskArn\": \"arn:aws:ecs:eu-central-1-east-1:012345678910:task/0b69d5c0-d655-4695-98cd-5d2d526d9d5a\",\n"
            + "          \"networkInterfaces\": [\n"
            + "            {\n"
            + "              \"privateIpv4Address\": \"10.0.1.16\"\n"
            + "            }\n"
            + "          ]\n"
            + "        }\n"
            + "      ]\n"
            + "    },\n"
            + "    {\n"
            + "      \"taskArn\": \"arn:aws:ecs:eu-central-1:012345678910:task/51a01bdf-d00e-487e-ab14-7645330b6207\",\n"
            + "      \"availabilityZone\": \"eu-central-1a\",\n"
            + "      \"containers\": [\n"
            + "        {\n"
            + "          \"taskArn\": \"arn:aws:ecs:eu-central-1:012345678910:task/51a01bdf-d00e-487e-ab14-7645330b6207\",\n"
            + "          \"networkInterfaces\": [\n"
            + "            {\n"
            + "              \"privateIpv4Address\": \"10.0.1.219\"\n"
            + "            }\n"
            + "          ]\n"
            + "        }\n"
            + "      ]\n"
            + "    }\n"
            + "  ]\n"
            + "}";

        stubFor(post("/")
            .withHeader("X-Amz-Date", equalTo("20200403T102518Z"))
            .withHeader("Authorization", equalTo(AUTHORIZATION_HEADER))
            .withHeader("X-Amz-Target", equalTo("AmazonEC2ContainerServiceV20141113.DescribeTasks"))
            .withHeader("Content-Type", equalTo("application/x-amz-json-1.1"))
            .withHeader("Accept-Encoding", equalTo("identity"))
            .withHeader("X-Amz-Security-Token", equalTo(TOKEN))
            .withRequestBody(equalToJson(requestBody))
            .willReturn(aResponse().withStatus(200).withBody(response)));

        // when
        List<Task> result = awsEcsApi.describeTasks(cluster, tasks, CREDENTIALS);

        // then
        assertEquals("10.0.1.16", result.get(0).getPrivateAddress());
        assertEquals("eu-central-1a", result.get(0).getAvailabilityZone());
        assertEquals("10.0.1.219", result.get(1).getPrivateAddress());
        assertEquals("eu-central-1a", result.get(1).getAvailabilityZone());
    }

    @Test
    public void awsError() {
        // given
        int errorCode = 401;
        String errorMessage = "Error message retrieved from AWS";
        stubFor(post(urlMatching("/.*"))
            .willReturn(aResponse().withStatus(errorCode).withBody(errorMessage)));

        // when
        Exception exception = assertThrows(Exception.class, () -> awsEcsApi.listTasks("cluster-arn", CREDENTIALS));

        // then
        assertTrue(exception.getMessage().contains(Integer.toString(errorCode)));
        assertTrue(exception.getMessage().contains(errorMessage));
    }
}
