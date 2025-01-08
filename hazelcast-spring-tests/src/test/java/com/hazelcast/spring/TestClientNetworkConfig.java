/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spring;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.RoutingStrategy;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.connection.tcp.RoutingMode;
import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.AzureConfig;
import com.hazelcast.config.EurekaConfig;
import com.hazelcast.config.GcpConfig;
import com.hazelcast.config.KubernetesConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.core.Hazelcast;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith({SpringExtension.class, CustomSpringExtension.class})
@ContextConfiguration(locations = {"clientNetworkConfig-applicationContext.xml"})
class TestClientNetworkConfig {

    @Autowired
    private HazelcastClientProxy client;

    @BeforeAll
    @AfterAll
    public static void start() {
        System.setProperty("test.keyStore", "private.jks");
        System.setProperty("test.trustStore", "trust.jks");

        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    void smokeMember() {
        int memberCountInConfigurationXml = 10;
        ClientConfig config = client.getClientConfig();
        assertEquals(memberCountInConfigurationXml, config.getNetworkConfig().getAddresses().size());
    }

    @Test
    void smokeSocketOptions() {
        int bufferSizeInConfigurationXml = 32;
        ClientConfig config = client.getClientConfig();
        assertEquals(bufferSizeInConfigurationXml, config.getNetworkConfig().getSocketOptions().getBufferSize());
    }

    @Test
    void smokeSocketInterceptor() {
        ClientConfig config = client.getClientConfig();
        SocketInterceptorConfig socketInterceptorConfig = config.getNetworkConfig().getSocketInterceptorConfig();
        assertFalse(socketInterceptorConfig.isEnabled());
        assertEquals(DummySocketInterceptor.class.getName(), socketInterceptorConfig.getClassName());
    }

    @Test
    void smokeSSLConfig() {
        ClientConfig config = client.getClientConfig();
        assertEquals("com.hazelcast.nio.ssl.BasicSSLContextFactory",
                config.getNetworkConfig().getSSLConfig().getFactoryClassName());
    }

    @Test
    void smokeClusterRoutingConfig() {
        ClientConfig config = client.getClientConfig();
        assertEquals(RoutingMode.ALL_MEMBERS, config.getNetworkConfig().getClusterRoutingConfig().getRoutingMode());
        assertEquals(RoutingStrategy.PARTITION_GROUPS,
                config.getNetworkConfig().getClusterRoutingConfig().getRoutingStrategy());
    }

    @Test
    void smokeAwsConfig() {
        AwsConfig aws = client.getClientConfig().getNetworkConfig().getAwsConfig();
        assertFalse(aws.isEnabled());
        assertEquals("sample-access-key", aws.getProperty("access-key"));
        assertEquals("sample-secret-key", aws.getProperty("secret-key"));
        assertEquals("sample-region", aws.getProperty("region"));
        assertEquals("sample-header", aws.getProperty("host-header"));
        assertEquals("sample-group", aws.getProperty("security-group-name"));
        assertEquals("sample-tag-key", aws.getProperty("tag-key"));
        assertEquals("sample-tag-value", aws.getProperty("tag-value"));
        assertEquals("sample-role", aws.getProperty("iam-role"));
    }

    @Test
    void smokeGcpConfig() {
        GcpConfig gcp = client.getClientConfig().getNetworkConfig().getGcpConfig();
        assertFalse(gcp.isEnabled());
        assertEquals("us-east1-b,us-east1-c", gcp.getProperty("zones"));
    }

    @Test
    void smokeAzureConfig() {
        AzureConfig azure = client.getClientConfig().getNetworkConfig().getAzureConfig();
        assertFalse(azure.isEnabled());
        assertEquals("false", azure.getProperty("instance-metadata-available"));
        assertEquals("CLIENT_ID", azure.getProperty("client-id"));
        assertEquals("CLIENT_SECRET", azure.getProperty("client-secret"));
        assertEquals("TENANT_ID", azure.getProperty("tenant-id"));
        assertEquals("SUB_ID", azure.getProperty("subscription-id"));
        assertEquals("RESOURCE-GROUP-NAME", azure.getProperty("resource-group"));
        assertEquals("SCALE-SET", azure.getProperty("scale-set"));
        assertEquals("TAG-NAME=HZLCAST001", azure.getProperty("tag"));
    }

    @Test
    void smokeKubernetesConfig() {
        KubernetesConfig kubernetes = client.getClientConfig().getNetworkConfig().getKubernetesConfig();
        assertFalse(kubernetes.isEnabled());
        assertEquals("MY-KUBERNETES-NAMESPACE", kubernetes.getProperty("namespace"));
        assertEquals("MY-SERVICE-NAME", kubernetes.getProperty("service-name"));
        assertEquals("MY-SERVICE-LABEL-NAME", kubernetes.getProperty("service-label-name"));
        assertEquals("MY-SERVICE-LABEL-VALUE", kubernetes.getProperty("service-label-value"));
    }

    @Test
    void smokeEurekaConfig() {
        EurekaConfig eureka = client.getClientConfig().getNetworkConfig().getEurekaConfig();
        assertFalse(eureka.isEnabled());
        assertEquals("true", eureka.getProperty("self-registration"));
        assertEquals("hazelcast", eureka.getProperty("namespace"));
    }

    @Test
    void smokeOutboundPorts() {
        Collection<String> allowedPorts = client.getClientConfig().getNetworkConfig().getOutboundPortDefinitions();
        assertEquals(2, allowedPorts.size());
        assertTrue(allowedPorts.contains("34600"));
        assertTrue(allowedPorts.contains("34700-34710"));
    }
}
