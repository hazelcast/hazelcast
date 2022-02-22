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

import com.hazelcast.aws.AwsEcsApi.Task;
import com.hazelcast.aws.AwsMetadataApi.EcsMetadata;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class AwsEcsClientTest {
    private static final String TASK_ARN = "task-arn";
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
        EcsMetadata ecsMetadata = mock(EcsMetadata.class);
        given(ecsMetadata.getTaskArn()).willReturn(TASK_ARN);
        given(ecsMetadata.getClusterArn()).willReturn(CLUSTER);
        given(awsMetadataApi.metadataEcs()).willReturn(ecsMetadata);
        given(awsCredentialsProvider.credentials()).willReturn(CREDENTIALS);

        awsEcsClient = new AwsEcsClient(CLUSTER, awsEcsApi, awsEc2Api, awsMetadataApi, awsCredentialsProvider);
    }

    @Test
    public void getAddresses() {
        // given
        List<String> taskArns = singletonList("task-arn");
        List<String> privateIps = singletonList("123.12.1.0");
        List<Task> tasks = singletonList(new Task("123.12.1.0", null));
        Map<String, String> expectedResult = singletonMap("123.12.1.0", "1.4.6.2");
        given(awsEcsApi.listTasks(CLUSTER, CREDENTIALS)).willReturn(taskArns);
        given(awsEcsApi.describeTasks(CLUSTER, taskArns, CREDENTIALS)).willReturn(tasks);
        given(awsEc2Api.describeNetworkInterfaces(privateIps, CREDENTIALS)).willReturn(expectedResult);

        // when
        Map<String, String> result = awsEcsClient.getAddresses();

        // then
        assertEquals(expectedResult, result);
    }

    @Test
    public void getAddressesWithAwsConfig() {
        // given
        List<String> taskArns = singletonList("task-arn");
        List<String> privateIps = singletonList("123.12.1.0");
        List<Task> tasks = singletonList(new Task("123.12.1.0", null));
        Map<String, String> expectedResult = singletonMap("123.12.1.0", "1.4.6.2");
        given(awsEcsApi.listTasks(CLUSTER, CREDENTIALS)).willReturn(taskArns);
        given(awsEcsApi.describeTasks(CLUSTER, taskArns, CREDENTIALS)).willReturn(tasks);
        given(awsEc2Api.describeNetworkInterfaces(privateIps, CREDENTIALS)).willReturn(expectedResult);

        // when
        Map<String, String> result = awsEcsClient.getAddresses();

        // then
        assertEquals(expectedResult, result);
    }

    @Test
    public void getAddressesNoPublicAddresses() {
        // given
        List<String> taskArns = singletonList("task-arn");
        List<String> privateIps = singletonList("123.12.1.0");
        List<Task> tasks = singletonList(new Task("123.12.1.0", null));
        given(awsEcsApi.listTasks(CLUSTER, CREDENTIALS)).willReturn(taskArns);
        given(awsEcsApi.describeTasks(CLUSTER, taskArns, CREDENTIALS)).willReturn(tasks);
        given(awsEc2Api.describeNetworkInterfaces(privateIps, CREDENTIALS)).willThrow(new RuntimeException());

        // when
        Map<String, String> result = awsEcsClient.getAddresses();

        // then
        assertEquals(singletonMap("123.12.1.0", null), result);
    }

    @Test
    public void getAddressesNoTasks() {
        // given
        List<String> tasks = emptyList();
        given(awsEcsApi.listTasks(CLUSTER, CREDENTIALS)).willReturn(tasks);

        // when
        Map<String, String> result = awsEcsClient.getAddresses();

        // then
        assertTrue(result.isEmpty());
    }

    @Test
    public void getAvailabilityZone() {
        // given
        String availabilityZone = "us-east-1";
        given(awsEcsApi.describeTasks(CLUSTER, singletonList(TASK_ARN), CREDENTIALS))
            .willReturn(singletonList(new Task(null, availabilityZone)));

        // when
        String result = awsEcsClient.getAvailabilityZone();

        // then
        assertEquals(availabilityZone, result);
    }

    @Test
    public void getAvailabilityZoneUnknown() {
        // given
        given(awsEcsApi.describeTasks(CLUSTER, singletonList(TASK_ARN), CREDENTIALS)).willReturn(emptyList());

        // when
        String result = awsEcsClient.getAvailabilityZone();

        // then
        assertEquals("unknown", result);
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
}
