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
import com.hazelcast.aws.AwsMetadataApi.EcsMetadata;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Optional;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.exactly;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.moreThan;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class AwsMetadataApiTest {

    private final String GROUP_NAME_URL = "/placement/group-name/";
    private final String PARTITION_NO_URL = "/placement/partition-number/";
    private final int RETRY_COUNT = 3;

    private AwsMetadataApi awsMetadataApi;

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());

    @Before
    public void setUp() {
        AwsConfig awsConfig = AwsConfig.builder().setConnectionRetries(RETRY_COUNT).build();
        String endpoint = String.format("http://localhost:%s", wireMockRule.port());
        awsMetadataApi = new AwsMetadataApi(endpoint, endpoint, endpoint, awsConfig);
    }

    @Test
    public void availabilityZoneEc2() {
        // given
        String availabilityZone = "eu-central-1b";
        stubFor(get(urlEqualTo("/placement/availability-zone/"))
            .willReturn(aResponse().withStatus(200).withBody(availabilityZone)));

        // when
        String result = awsMetadataApi.availabilityZoneEc2();

        // then
        assertEquals(availabilityZone, result);
    }

    @Test
    public void placementGroupEc2() {
        // given
        String placementGroup = "placement-group-1";
        stubFor(get(urlEqualTo(GROUP_NAME_URL))
                .willReturn(aResponse().withStatus(200).withBody(placementGroup)));

        // when
        Optional<String> result = awsMetadataApi.placementGroupEc2();

        // then
        assertEquals(placementGroup, result.orElse("N/A"));
        verify(exactly(1), getRequestedFor(urlEqualTo(GROUP_NAME_URL)));
    }

    @Test
    public void partitionPlacementGroupEc2() {
        // given
        String partitionNumber = "42";
        stubFor(get(urlEqualTo(PARTITION_NO_URL))
                .willReturn(aResponse().withStatus(200).withBody(partitionNumber)));

        // when
        Optional<String> result = awsMetadataApi.placementPartitionNumberEc2();

        // then
        assertEquals(partitionNumber, result.orElse("N/A"));
        verify(exactly(1), getRequestedFor(urlEqualTo(PARTITION_NO_URL)));
    }

    @Test
    public void missingPlacementGroupEc2() {
        // given
        stubFor(get(urlEqualTo(GROUP_NAME_URL))
                .willReturn(aResponse().withStatus(404).withBody("Not found")));
        stubFor(get(urlEqualTo(PARTITION_NO_URL))
                .willReturn(aResponse().withStatus(404).withBody("Not found")));

        // when
        Optional<String> placementGroupResult = awsMetadataApi.placementGroupEc2();
        Optional<String> partitionNumberResult = awsMetadataApi.placementPartitionNumberEc2();

        // then
        assertEquals(Optional.empty(), placementGroupResult);
        assertEquals(Optional.empty(), partitionNumberResult);
        verify(exactly(1), getRequestedFor(urlEqualTo(GROUP_NAME_URL)));
        verify(exactly(1), getRequestedFor(urlEqualTo(PARTITION_NO_URL)));
    }

    @Test
    public void failToFetchPlacementGroupEc2() {
        // given
        stubFor(get(urlEqualTo(GROUP_NAME_URL))
                .willReturn(aResponse().withStatus(500).withBody("Service Unavailable")));

        // when
        Optional<String> placementGroupResult = awsMetadataApi.placementGroupEc2();

        // then
        assertEquals(Optional.empty(), placementGroupResult);
        verify(moreThan(RETRY_COUNT), getRequestedFor(urlEqualTo(GROUP_NAME_URL)));
    }

    @Test
    public void defaultIamRoleEc2() {
        // given
        String defaultIamRole = "default-role-name";
        stubFor(get(urlEqualTo("/iam/security-credentials/"))
            .willReturn(aResponse().withStatus(200).withBody(defaultIamRole)));

        // when
        String result = awsMetadataApi.defaultIamRoleEc2();

        // then
        assertEquals(defaultIamRole, result);
    }

    @Test
    public void credentialsEc2() {
        // given
        String iamRole = "some-iam-role";
        String response = "{\n"
            + "  \"Code\": \"Success\",\n"
            + "  \"AccessKeyId\": \"Access1234\",\n"
            + "  \"SecretAccessKey\": \"Secret1234\",\n"
            + "  \"Token\": \"Token1234\",\n"
            + "  \"Expiration\": \"2020-03-27T21:01:33Z\"\n"
            + "}";
        stubFor(get(urlEqualTo(String.format("/iam/security-credentials/%s", iamRole)))
            .willReturn(aResponse().withStatus(200).withBody(response)));

        // when
        AwsCredentials result = awsMetadataApi.credentialsEc2(iamRole);

        // then
        assertEquals("Access1234", result.getAccessKey());
        assertEquals("Secret1234", result.getSecretKey());
        assertEquals("Token1234", result.getToken());
    }

    @Test
    public void credentialsEcs() {
        // given
        String response = "{\n"
            + "  \"Code\": \"Success\",\n"
            + "  \"AccessKeyId\": \"Access1234\",\n"
            + "  \"SecretAccessKey\": \"Secret1234\",\n"
            + "  \"Token\": \"Token1234\",\n"
            + "  \"Expiration\": \"2020-03-27T21:01:33Z\"\n"
            + "}";
        stubFor(get(urlEqualTo("/"))
            .willReturn(aResponse().withStatus(200).withBody(response)));

        // when
        AwsCredentials result = awsMetadataApi.credentialsEcs();

        // then
        assertEquals("Access1234", result.getAccessKey());
        assertEquals("Secret1234", result.getSecretKey());
        assertEquals("Token1234", result.getToken());
    }

    @Test
    public void metadataEcs() {
        // given
        //language=JSON
        String response = "{\n"
            + "  \"Name\": \"container-name\",\n"
            + "  \"Labels\": {\n"
            + "    \"com.amazonaws.ecs.cluster\": \"arn:aws:ecs:eu-central-1:665466731577:cluster/default\",\n"
            + "    \"com.amazonaws.ecs.container-name\": \"container-name\",\n"
            + "    \"com.amazonaws.ecs.task-arn\": \"arn:aws:ecs:eu-central-1:665466731577:task/default/0dcf990c3ef3436c84e0c7430d14a3d4\",\n"
            + "    \"com.amazonaws.ecs.task-definition-family\": \"family-name\"\n"
            + "  },\n"
            + "  \"Networks\": [\n"
            + "    {\n"
            + "      \"NetworkMode\": \"awsvpc\",\n"
            + "      \"IPv4Addresses\": [\n"
            + "        \"10.0.1.174\"\n"
            + "      ]\n"
            + "    }\n"
            + "  ]\n"
            + "}";
        stubFor(get("/").willReturn(aResponse().withStatus(200).withBody(response)));

        // when
        EcsMetadata result = awsMetadataApi.metadataEcs();

        // then
        assertEquals("arn:aws:ecs:eu-central-1:665466731577:task/default/0dcf990c3ef3436c84e0c7430d14a3d4",
            result.getTaskArn());
        assertEquals("arn:aws:ecs:eu-central-1:665466731577:cluster/default", result.getClusterArn());
    }

    @Test
    public void awsError() {
        // given
        int errorCode = 401;
        String errorMessage = "Error message retrieved from AWS";
        stubFor(get(urlMatching("/.*"))
            .willReturn(aResponse().withStatus(errorCode).withBody(errorMessage)));

        // when
        Exception exception = assertThrows(Exception.class, () -> awsMetadataApi.defaultIamRoleEc2());

        // then
        assertTrue(exception.getMessage().contains(Integer.toString(errorCode)));
        assertTrue(exception.getMessage().contains(errorMessage));
        verify(moreThan(RETRY_COUNT), getRequestedFor(urlMatching("/.*")));
    }
}
