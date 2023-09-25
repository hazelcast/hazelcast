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

import com.hazelcast.spi.discovery.integration.DiscoveryMode;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.never;

@RunWith(MockitoJUnitRunner.Silent.class)
public class AwsEcsClientTest {
    private static final String CLUSTER = "cluster-arn";
    private static final AwsCredentials CREDENTIALS = AwsCredentials.builder()
        .setAccessKey("access-key")
        .setSecretKey("secret-key")
        .setToken("token")
        .build();

    @Mock
    private AwsEcsApi awsEcsApi;

    @Mock
    private AwsEc2Api awsEc2Api;

    @Mock
    private AwsMetadataApi awsMetadataApi;

    @Mock
    private AwsCredentialsProvider awsCredentialsProvider;

    private AwsEcsClient awsEcsClient;

    @Before
    public void setUp() {
        given(awsMetadataApi.clusterEcs()).willReturn(CLUSTER);
        AwsConfig awsConfig = AwsConfig.builder()
                .setDiscoveryMode(DiscoveryMode.Member)
                .build();
        given(awsCredentialsProvider.credentials()).willReturn(CREDENTIALS);
        awsEcsClient = new AwsEcsClient(CLUSTER, awsConfig, awsEcsApi, awsEc2Api, awsMetadataApi, awsCredentialsProvider);
    }

    @Test
    public void getAddressesWithAwsConfig() {
        // given
        List<String> privateIps = singletonList("123.12.1.0");
        Map<String, String> expectedResult = singletonMap("123.12.1.0", "1.4.6.2");
        given(awsEcsApi.listTaskPrivateAddresses(CLUSTER, CREDENTIALS)).willReturn(privateIps);
        given(awsEc2Api.describeNetworkInterfaces(privateIps, CREDENTIALS)).willReturn(expectedResult);

        // when
        Map<String, String> result = awsEcsClient.getAddresses();

        // then
        assertEquals(expectedResult, result);
    }

    @Test
    public void getAddressesNoPublicAddresses() {
        // given
        List<String> privateIps = singletonList("123.12.1.0");
        Map<String, String> privateToPublicIps = singletonMap("123.12.1.0", null);
        given(awsEcsApi.listTaskPrivateAddresses(CLUSTER, CREDENTIALS)).willReturn(privateIps);
        given(awsEc2Api.describeNetworkInterfaces(privateIps, CREDENTIALS)).willReturn(privateToPublicIps);

        // when
        Map<String, String> result = awsEcsClient.getAddresses();

        // then
        assertEquals(singletonMap("123.12.1.0", null), result);
    }

    @Test
    public void getAddressesNoTasks() {
        // given
        given(awsEcsApi.listTaskPrivateAddresses(CLUSTER, CREDENTIALS)).willReturn(emptyList());

        // when
        Map<String, String> result = awsEcsClient.getAddresses();

        // then
        assertTrue(result.isEmpty());
    }

    @Test
    public void getAvailabilityZone() {
        // given
        String expectedResult = "us-east-1a";
        given(awsMetadataApi.availabilityZoneEcs()).willReturn(expectedResult);

        // when
        String result = awsEcsClient.getAvailabilityZone();

        // then
        assertEquals(expectedResult, result);
    }

    @Test
    public void getPlacementGroup() {
        // when
        Optional<String> placementGroup = awsEcsClient.getPlacementGroup();
        Optional<String> placementPartitionNumber = awsEcsClient.getPlacementPartitionNumber();

        // then
        // Placement aware is not supported for ECS
        assertEquals(Optional.empty(), placementGroup);
        assertEquals(Optional.empty(), placementPartitionNumber);
    }

    @Test
    public void doNotGetAddressesForEC2Member() {
        // given
        Map<String, String> expectedResult = singletonMap("123.12.1.0", "1.4.6.2");
        given(awsEcsApi.listTaskPrivateAddresses(CLUSTER, CREDENTIALS)).willReturn(emptyList());
        given(awsEc2Api.describeInstances(CREDENTIALS)).willReturn(expectedResult);

        // when
        Map<String, String> result = awsEcsClient.getAddresses();

        // then
        then(awsEc2Api).should(never()).describeInstances(CREDENTIALS);
        assertEquals(emptyMap(), result);
    }

    @Test
    public void getAddressesForEC2Client() {
        // given
        AwsConfig awsConfig = AwsConfig.builder().setDiscoveryMode(DiscoveryMode.Client).build();
        awsEcsClient = new AwsEcsClient(CLUSTER, awsConfig, awsEcsApi, awsEc2Api, awsMetadataApi, awsCredentialsProvider);
        Map<String, String> expectedResult = singletonMap("123.12.1.0", "1.4.6.2");
        given(awsEcsApi.listTaskPrivateAddresses(CLUSTER, CREDENTIALS)).willReturn(emptyList());
        given(awsEc2Api.describeInstances(CREDENTIALS)).willReturn(expectedResult);

        // when
        Map<String, String> result = awsEcsClient.getAddresses();

        // then
        assertEquals(expectedResult, result);
    }

    @Test
    public void doNotGetEc2AddressesIfEcsConfigured() {
        // given
        AwsConfig awsConfig = AwsConfig.builder()
                .setCluster(CLUSTER)
                .setDiscoveryMode(DiscoveryMode.Client)
                .build();
        awsEcsClient = new AwsEcsClient(CLUSTER, awsConfig, awsEcsApi, awsEc2Api, awsMetadataApi, awsCredentialsProvider);
        given(awsEcsApi.listTaskPrivateAddresses(CLUSTER, CREDENTIALS)).willReturn(emptyList());

        // when
        Map<String, String> result = awsEcsClient.getAddresses();

        // then
        then(awsEc2Api).should(never()).describeInstances(CREDENTIALS);
        assertEquals(emptyMap(), result);
    }

    @Test
    public void doNotGetEcsAddressesIfEc2Configured() {
        // given
        AwsConfig awsConfig = AwsConfig.builder()
                .setSecurityGroupName("my-security-group")
                .setDiscoveryMode(DiscoveryMode.Client)
                .build();
        awsEcsClient = new AwsEcsClient(CLUSTER, awsConfig, awsEcsApi, awsEc2Api, awsMetadataApi, awsCredentialsProvider);
        Map<String, String> expectedResult = singletonMap("123.12.1.0", "1.4.6.2");
        given(awsEc2Api.describeInstances(CREDENTIALS)).willReturn(expectedResult);

        // when
        Map<String, String> result = awsEcsClient.getAddresses();

        // then
        then(awsEcsApi).should(never()).listTaskPrivateAddresses(CLUSTER, CREDENTIALS);
        assertEquals(expectedResult, result);
    }
}
