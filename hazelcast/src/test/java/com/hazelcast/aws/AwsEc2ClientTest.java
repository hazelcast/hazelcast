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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.never;

@RunWith(MockitoJUnitRunner.Silent.class)
public class AwsEc2ClientTest {
    @Mock
    private AwsEc2Api awsEc2Api;
    @Mock
    private AwsEcsApi awsEcsApi;
    @Mock
    private AwsMetadataApi awsMetadataApi;
    @Mock
    private AwsCredentialsProvider awsCredentialsProvider;
    private AwsEc2Client awsEc2Client;
    private AutoCloseable closeable;

    @Before
    public void setUp() throws Exception {
        closeable = MockitoAnnotations.openMocks(this);
        AwsConfig awsConfig = AwsConfig.builder()
                .setDiscoveryMode(DiscoveryMode.Member)
                .build();
        awsEc2Client = new AwsEc2Client(awsEc2Api, awsEcsApi, awsMetadataApi, awsCredentialsProvider, awsConfig);
    }

    @After
    public void tearDown() throws Exception {
        closeable.close();
    }

    @Test
    public void getAddresses() {
        // given
        AwsCredentials credentials = AwsCredentials.builder()
                .setAccessKey("access-key")
                .setSecretKey("secret-key")
                .setToken("token")
                .build();
        Map<String, String> expectedResult = singletonMap("123.12.1.0", "1.4.6.2");

        given(awsCredentialsProvider.credentials()).willReturn(credentials);
        given(awsEc2Api.describeInstances(credentials)).willReturn(expectedResult);

        // when
        Map<String, String> result = awsEc2Client.getAddresses();

        // then
        assertEquals(expectedResult, result);
    }

    @Test
    public void getAvailabilityZone() {
        // given
        String expectedResult = "us-east-1a";
        given(awsMetadataApi.availabilityZoneEc2()).willReturn(expectedResult);

        // when
        String result = awsEc2Client.getAvailabilityZone();

        // then
        assertEquals(expectedResult, result);
    }

    @Test
    public void getPlacementGroup() {
        // given
        String placementGroup = "placement-group";
        String partitionNumber = "42";
        given(awsMetadataApi.placementGroupEc2()).willReturn(Optional.of(placementGroup));
        given(awsMetadataApi.placementPartitionNumberEc2()).willReturn(Optional.of(partitionNumber));

        // when
        Optional<String> placementGroupResult = awsEc2Client.getPlacementGroup();
        Optional<String> partitionNumberResult = awsEc2Client.getPlacementPartitionNumber();

        // then
        assertEquals(placementGroup, placementGroupResult.orElse("N/A"));
        assertEquals(partitionNumber, partitionNumberResult.orElse("N/A"));
    }

    @Test
    public void doNotGetEcsAddressesMember() {
        AwsCredentials credentials = AwsCredentials.builder()
                .setAccessKey("access-key")
                .setSecretKey("secret-key")
                .setToken("token")
                .build();
        given(awsCredentialsProvider.credentials()).willReturn(credentials);
        given(awsEc2Api.describeInstances(credentials)).willReturn(emptyMap());

        // when
        Map<String, String> result = awsEc2Client.getAddresses();

        // then
        then(awsEcsApi).should(never()).listTaskPrivateAddresses("CLUSTER", credentials);
        then(awsEc2Api).should(never()).describeNetworkInterfaces(anyList(), any(AwsCredentials.class));
        assertEquals(emptyMap(), result);
    }

    @Test
    public void getEcsAddressesClient() {
        AwsCredentials credentials = AwsCredentials.builder()
                .setAccessKey("access-key")
                .setSecretKey("secret-key")
                .setToken("token")
                .build();
        Map<String, String> expectedResult = singletonMap("123.12.1.0", "1.4.6.2");
        ArrayList<String> privateIps = new ArrayList<>(expectedResult.keySet());

        AwsConfig awsConfig = AwsConfig.builder()
                .setCluster("CLUSTER")
                .setDiscoveryMode(DiscoveryMode.Client)
                .build();
        awsEc2Client = new AwsEc2Client(awsEc2Api, awsEcsApi, awsMetadataApi, awsCredentialsProvider, awsConfig);
        given(awsCredentialsProvider.credentials()).willReturn(credentials);
        given(awsEc2Api.describeInstances(credentials)).willReturn(emptyMap());
        given(awsEcsApi.listTaskPrivateAddresses("CLUSTER", credentials)).willReturn(privateIps);
        given(awsEc2Api.describeNetworkInterfaces(privateIps, credentials)).willReturn(expectedResult);

        // when
        Map<String, String> result = awsEc2Client.getAddresses();

        // then
        assertEquals(expectedResult, result);
    }

    @Test
    public void doNotGetEcsAddressesWhenEc2Configured() {
        AwsCredentials credentials = AwsCredentials.builder()
                .setAccessKey("access-key")
                .setSecretKey("secret-key")
                .setToken("token")
                .build();
        AwsConfig awsConfig = AwsConfig.builder()
                .setDiscoveryMode(DiscoveryMode.Client)
                .setSecurityGroupName("my-security-group")
                .build();
        awsEc2Client = new AwsEc2Client(awsEc2Api, awsEcsApi, awsMetadataApi, awsCredentialsProvider, awsConfig);
        given(awsCredentialsProvider.credentials()).willReturn(credentials);
        given(awsEc2Api.describeInstances(credentials)).willReturn(emptyMap());

        // when
        Map<String, String> result = awsEc2Client.getAddresses();

        // then
        then(awsEcsApi).should(never()).listTaskPrivateAddresses("CLUSTER", credentials);
        then(awsEc2Api).should(never()).describeNetworkInterfaces(anyList(), any(AwsCredentials.class));
        assertEquals(emptyMap(), result);
    }

    @Test
    public void doNotGetEc2AddressesWhenEcsConfigured() {
        AwsCredentials credentials = AwsCredentials.builder()
                .setAccessKey("access-key")
                .setSecretKey("secret-key")
                .setToken("token")
                .build();
        AwsConfig awsConfig = AwsConfig.builder()
                .setCluster("CLUSTER")
                .setDiscoveryMode(DiscoveryMode.Client)
                .build();
        awsEc2Client = new AwsEc2Client(awsEc2Api, awsEcsApi, awsMetadataApi, awsCredentialsProvider, awsConfig);

        Map<String, String> expectedResult = singletonMap("123.12.1.0", "1.4.6.2");
        ArrayList<String> privateIps = new ArrayList<>(expectedResult.keySet());
        given(awsCredentialsProvider.credentials()).willReturn(credentials);
        given(awsEcsApi.listTaskPrivateAddresses("CLUSTER", credentials)).willReturn(privateIps);
        given(awsEc2Api.describeNetworkInterfaces(privateIps, credentials)).willReturn(expectedResult);

        // when
        Map<String, String> result = awsEc2Client.getAddresses();

        // then
        then(awsEc2Api).should(never()).describeInstances(credentials);
        assertEquals(expectedResult, result);
    }
}
