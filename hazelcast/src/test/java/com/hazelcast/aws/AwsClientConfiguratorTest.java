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

import com.hazelcast.aws.AwsMetadataApi.EcsMetadata;
import com.hazelcast.config.InvalidConfigurationException;
import org.junit.Test;

import static com.hazelcast.aws.AwsClientConfigurator.resolveEc2Endpoint;
import static com.hazelcast.aws.AwsClientConfigurator.resolveEcsEndpoint;
import static com.hazelcast.aws.AwsClientConfigurator.resolveRegion;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class AwsClientConfiguratorTest {

    @Test
    public void resolveRegionAwsConfig() {
        // given
        String region = "us-east-1";
        AwsConfig awsConfig = AwsConfig.builder().setRegion(region).build();
        AwsMetadataApi awsMetadataApi = mock(AwsMetadataApi.class);
        Environment environment = mock(Environment.class);

        // when
        String result = resolveRegion(awsConfig, awsMetadataApi, environment);

        // then
        assertEquals(region, result);
    }

    @Test
    public void resolveRegionEcsConfig() {
        // given
        String region = "us-east-1";
        AwsConfig awsConfig = AwsConfig.builder().build();
        AwsMetadataApi awsMetadataApi = mock(AwsMetadataApi.class);
        Environment environment = mock(Environment.class);
        given(environment.getAwsRegionOnEcs()).willReturn(region);
        given(environment.isRunningOnEcs()).willReturn(true);

        // when
        String result = resolveRegion(awsConfig, awsMetadataApi, environment);

        // then
        assertEquals(region, result);
    }

    @Test
    public void resolveRegionEc2Metadata() {
        // given
        AwsConfig awsConfig = AwsConfig.builder().build();
        AwsMetadataApi awsMetadataApi = mock(AwsMetadataApi.class);
        Environment environment = mock(Environment.class);
        given(awsMetadataApi.availabilityZoneEc2()).willReturn("us-east-1a");

        // when
        String result = resolveRegion(awsConfig, awsMetadataApi, environment);

        // then
        assertEquals("us-east-1", result);
    }

    @Test
    public void resolveEc2Endpoints() {
        assertEquals("ec2.us-east-1.amazonaws.com", resolveEc2Endpoint(AwsConfig.builder().build(), "us-east-1"));
        assertEquals("ec2.us-east-1.amazonaws.com", resolveEc2Endpoint(AwsConfig.builder().setHostHeader("ecs").build(), "us-east-1"));
        assertEquals("ec2.us-east-1.amazonaws.com", resolveEc2Endpoint(AwsConfig.builder().setHostHeader("ec2").build(), "us-east-1"));
        assertEquals("ec2.us-east-1.something",
            resolveEc2Endpoint(AwsConfig.builder().setHostHeader("ec2.something").build(), "us-east-1"));
    }

    @Test
    public void resolveEcsEndpoints() {
        assertEquals("ecs.us-east-1.amazonaws.com", resolveEcsEndpoint(AwsConfig.builder().build(), "us-east-1"));
        assertEquals("ecs.us-east-1.amazonaws.com",
            resolveEcsEndpoint(AwsConfig.builder().setHostHeader("ecs").build(), "us-east-1"));
        assertEquals("ecs.us-east-1.something",
            resolveEcsEndpoint(AwsConfig.builder().setHostHeader("ecs.something").build(), "us-east-1"));
    }

    @Test
    public void explicitlyEcsConfigured() {
        assertTrue(AwsClientConfigurator.explicitlyEcsConfigured(AwsConfig.builder().setHostHeader("ecs").build()));
        assertTrue(AwsClientConfigurator.explicitlyEcsConfigured(
            AwsConfig.builder().setHostHeader("ecs.us-east-1.amazonaws.com").build()));
        assertTrue(AwsClientConfigurator.explicitlyEcsConfigured(AwsConfig.builder().setCluster("cluster").build()));
        assertFalse(AwsClientConfigurator.explicitlyEcsConfigured(AwsConfig.builder().build()));
    }

    @Test
    public void explicitlyEc2Configured() {
        assertTrue(AwsClientConfigurator.explicitlyEc2Configured(AwsConfig.builder().setHostHeader("ec2").build()));
        assertTrue(AwsClientConfigurator.explicitlyEc2Configured(
            AwsConfig.builder().setHostHeader("ec2.us-east-1.amazonaws.com").build()));
        assertFalse(AwsClientConfigurator.explicitlyEc2Configured(
            AwsConfig.builder().setHostHeader("ecs.us-east-1.amazonaws.com").build()));
        assertFalse(AwsClientConfigurator.explicitlyEc2Configured(AwsConfig.builder().build()));
    }

    @Test
    public void resolveClusterAwsConfig() {
        // given
        String cluster = "service-name";
        AwsConfig config = AwsConfig.builder().setCluster(cluster).build();

        // when
        String result = AwsClientConfigurator.resolveCluster(config, null, null);

        // then
        assertEquals(cluster, result);
    }

    @Test
    public void resolveClusterAwsEcsMetadata() {
        // given
        String cluster = "service-name";
        AwsConfig config = AwsConfig.builder().build();
        AwsMetadataApi metadataApi = mock(AwsMetadataApi.class);
        given(metadataApi.metadataEcs()).willReturn(new EcsMetadata(null, cluster));
        Environment environment = mock(Environment.class);
        given(environment.isRunningOnEcs()).willReturn(true);

        // when
        String result = AwsClientConfigurator.resolveCluster(config, metadataApi, environment);

        // then
        assertEquals(cluster, result);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void resolveClusterInvalidConfiguration() {
        // given
        AwsConfig config = AwsConfig.builder().build();
        Environment environment = mock(Environment.class);
        given(environment.isRunningOnEcs()).willReturn(false);

        // when
        AwsClientConfigurator.resolveCluster(config, null, environment);

        // then
        // throws exception
    }

}
