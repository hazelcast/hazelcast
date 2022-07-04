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

package com.hazelcast.client.bluegreen;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientFailoverConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.client.impl.clientside.FailoverClientConfigSupport.resolveClientFailoverConfig;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FailoverConfigTest {

    @Test
    public void testAllClientConfigsAreHandledInMultipleClientConfigSupport() {
        Set<String> allClientConfigMethods = new HashSet<>();
        Collections.addAll(allClientConfigMethods, "setProperty", "getProperty", "getClassLoader", "getProperties",
                "setProperties", "getLabels", "getClusterName", "getSecurityConfig", "isSmartRouting", "setSmartRouting",
                "getSocketInterceptorConfig", "setSocketInterceptorConfig", "getConnectionTimeout",
                "setConnectionTimeout", "addAddress", "setAddresses", "getAddresses", "isRedoOperation", "setRedoOperation",
                "getSocketOptions", "setSocketOptions", "setConfigPatternMatcher", "getConfigPatternMatcher", "setSecurityConfig",
                "getNetworkConfig", "setNetworkConfig", "addReliableTopicConfig", "getReliableTopicConfig", "addNearCacheConfig",
                "addListenerConfig", "addProxyFactoryConfig", "getNearCacheConfig", "getNearCacheConfigMap",
                "setNearCacheConfigMap", "getFlakeIdGeneratorConfigMap", "findFlakeIdGeneratorConfig",
                "getFlakeIdGeneratorConfig", "addFlakeIdGeneratorConfig", "setFlakeIdGeneratorConfigMap",
                "setReliableTopicConfigMap", "getReliableTopicConfigMap", "getCredentials", "setCredentials", "setClusterName",
                "getListenerConfigs", "setListenerConfigs", "getLoadBalancer", "setLoadBalancer", "getLoadBalancerClassName",
                "setLoadBalancerClassName", "setClassLoader", "getManagedContext", "setManagedContext", "getExecutorPoolSize",
                "setExecutorPoolSize", "getProxyFactoryConfigs", "setProxyFactoryConfigs", "getSerializationConfig",
                "setSerializationConfig", "getNativeMemoryConfig", "setNativeMemoryConfig", "getLicenseKey", "setLicenseKey",
                "addQueryCacheConfig", "getQueryCacheConfigs", "setQueryCacheConfigs", "getInstanceName", "setInstanceName",
                "getConnectionStrategyConfig", "setConnectionStrategyConfig", "getUserCodeDeploymentConfig", "setUserCodeDeploymentConfig",
                "getOrCreateQueryCacheConfig", "getOrNullQueryCacheConfig", "addLabel", "setLabels", "setUserContext",
                "getUserContext", "setMetricsConfig", "load", "setBackupAckToClientEnabled", "isBackupAckToClientEnabled",
                "getMetricsConfig", "equals", "hashCode", "toString", "setInstanceTrackingConfig", "getInstanceTrackingConfig");
        Method[] declaredMethods = ClientConfig.class.getDeclaredMethods();
        for (Method method : declaredMethods) {
            if (Modifier.isPrivate(method.getModifiers())) {
                continue;
            }
            String methodName = method.getName();
            if (!methodName.startsWith("$") && !allClientConfigMethods.contains(methodName)) {
                throw new IllegalStateException("There is a new method on client config. " + methodName
                        + ". Handle it on FailoverClientConfigSupport first, and add it to  allClientConfigMethods set above.");
            }
        }
    }

    @Test
    public void testAllClientNetworkConfigsAreHandledInMultipleClientConfigSupport() {
        Set<String> allClientNetworkConfigMethods = new HashSet<>();
        Collections.addAll(allClientNetworkConfigMethods, "isSmartRouting", "setSmartRouting", "getSocketInterceptorConfig",
                "setSocketInterceptorConfig", "getConnectionTimeout", "setConnectionTimeout", "addAddress", "setAddresses",
                "getAddresses", "isRedoOperation", "setRedoOperation", "getSocketOptions", "setSocketOptions",
                "getDiscoveryConfig", "setDiscoveryConfig", "getSSLConfig", "setSSLConfig", "setAwsConfig", "getAwsConfig",
                "setGcpConfig", "getGcpConfig", "setAzureConfig", "getAzureConfig", "setKubernetesConfig", "getKubernetesConfig",
                "setEurekaConfig", "getEurekaConfig", "setAutoDetectionConfig", "getAutoDetectionConfig", "isAutoDetectionEnabled",
                "getCloudConfig", "setCloudConfig", "getOutboundPortDefinitions", "getOutboundPorts", "setOutboundPortDefinitions",
                "setOutboundPorts", "addOutboundPort", "addOutboundPortDefinition", "getClientIcmpPingConfig",
                "setClientIcmpPingConfig", "equals", "hashCode", "toString");
        Method[] declaredMethods = ClientNetworkConfig.class.getDeclaredMethods();
        for (Method method : declaredMethods) {
            if (!method.getName().startsWith("$") && !allClientNetworkConfigMethods.contains(method.getName())) {
                throw new IllegalStateException("There is a new method on client network config. " + method
                        + "Handle it on FailoverClientConfigSupport first,"
                        + " and add it to allClientNetworkConfigMethods set above.");
            }
        }
    }

    @Test
    public void testClientConfigWithSameClusterName() {
        ClientFailoverConfig clientFailoverConfig = new ClientFailoverConfig();
        clientFailoverConfig.addClientConfig(new ClientConfig());
        clientFailoverConfig.addClientConfig(new ClientConfig());
        resolveClientFailoverConfig(clientFailoverConfig);
    }

    @Test
    public void testClientConfigWithDifferentClusterName() {
        ClientFailoverConfig clientFailoverConfig = new ClientFailoverConfig();
        clientFailoverConfig.addClientConfig(new ClientConfig());

        ClientConfig alternativeConfig = new ClientConfig();
        alternativeConfig.setClusterName("alternative");
        clientFailoverConfig.addClientConfig(alternativeConfig);

        resolveClientFailoverConfig(clientFailoverConfig);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testClientConfigWith_withAnInvalidChange() {
        ClientFailoverConfig clientFailoverConfig = new ClientFailoverConfig();
        clientFailoverConfig.addClientConfig(new ClientConfig());

        ClientConfig alternativeConfig = new ClientConfig();
        alternativeConfig.setClusterName("alternative");
        alternativeConfig.setProperty("newProperty", "newValue");
        clientFailoverConfig.addClientConfig(alternativeConfig);

        resolveClientFailoverConfig(clientFailoverConfig);
    }

    @Test
    public void testClientConfigWith_withAValidChange() {
        ClientFailoverConfig clientFailoverConfig = new ClientFailoverConfig();
        clientFailoverConfig.addClientConfig(new ClientConfig());

        ClientConfig alternativeConfig = new ClientConfig();
        alternativeConfig.setClusterName("alternative");
        CredentialsFactoryConfig credentialsFactoryConfig = new CredentialsFactoryConfig();
        credentialsFactoryConfig.setClassName("CustomCredentials");
        alternativeConfig.getSecurityConfig().setCredentialsFactoryConfig(credentialsFactoryConfig);
        clientFailoverConfig.addClientConfig(alternativeConfig);

        resolveClientFailoverConfig(clientFailoverConfig);
    }

    @Test(expected = HazelcastException.class)
    public void test_throwsException_whenFailoverConfigIsIntended_butPassedNull() {
        ClientFailoverConfig clientFailoverConfig = resolveClientFailoverConfig(null);
        assertEquals(1, clientFailoverConfig.getClientConfigs().size());
        assertEquals("dev", clientFailoverConfig.getClientConfigs().get(0).getClusterName());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void test_throwsException_whenFailoverConfigIsEmpty() {
        resolveClientFailoverConfig(new ClientFailoverConfig());
    }
}
