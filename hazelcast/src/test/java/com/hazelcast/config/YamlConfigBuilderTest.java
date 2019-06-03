/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.google.common.collect.ImmutableSet;
import com.hazelcast.config.cp.CPSemaphoreConfig;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.config.cp.FencedLockConfig;
import com.hazelcast.config.cp.RaftAlgorithmConfig;
import com.hazelcast.config.helpers.DummyMapStore;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.quorum.impl.ProbabilisticQuorumFunction;
import com.hazelcast.quorum.impl.RecentlyActiveQuorumFunction;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.TopicOverloadPolicy;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.URL;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.config.EvictionPolicy.LRU;
import static com.hazelcast.config.PermissionConfig.PermissionType.CACHE;
import static com.hazelcast.config.PermissionConfig.PermissionType.CONFIG;
import static com.hazelcast.config.WANQueueFullBehavior.DISCARD_AFTER_MUTATION;
import static java.io.File.createTempFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * YAML specific implementation of the tests that should be maintained in
 * both XML and YAML configuration builder tests
 * <p>
 *
 * NOTE: This test class must not define test cases, it is meant only to
 * implement test cases defined in {@link AbstractConfigBuilderTest}.
 * <p>
 *
 * NOTE2: Test cases specific to YAML should be added to {@link YamlOnlyConfigBuilderTest}
 *
 * @see AbstractConfigBuilderTest
 * @see XMLConfigBuilderTest
 * @see YamlOnlyConfigBuilderTest
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class YamlConfigBuilderTest extends AbstractConfigBuilderTest {

    @Override
    @Test
    public void testConfigurationURL() throws Exception {
        URL configURL = getClass().getClassLoader().getResource("hazelcast-default.yaml");
        Config config = new YamlConfigBuilder(configURL).build();
        assertEquals(configURL, config.getConfigurationUrl());
        assertNull(config.getConfigurationFile());
    }

    @Override
    @Test
    public void testConfigurationWithFileName() throws Exception {
        assumeThatNotZingJDK6(); // https://github.com/hazelcast/hazelcast/issues/9044

        File file = createTempFile("foo", "bar");
        file.deleteOnExit();

        String yaml = ""
                + "hazelcast:\n"
                + "  group:\n"
                + "    name: foobar\n"
                + "    password: dev-pass";
        Writer writer = new PrintWriter(file, "UTF-8");
        writer.write(yaml);
        writer.close();

        String path = file.getAbsolutePath();
        Config config = new YamlConfigBuilder(path).build();
        assertEquals(path, config.getConfigurationFile().getAbsolutePath());
        assertNull(config.getConfigurationUrl());
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void testConfiguration_withNullInputStream() {
        new YamlConfigBuilder((InputStream) null);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testInvalidRootElement() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "  group:\n"
                + "    name: dev\n"
                + "    password: clusterpass";
        buildConfig(yaml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testJoinValidation() {
        String yaml = ""
                + "hazelcast:\n"
                + "  network:\n"
                + "    join:\n"
                + "      multicast:\n"
                + "        enabled: true\n"
                + "      tcp-ip:\n"
                + "        enabled: true\n";
        buildConfig(yaml);
    }

    @Override
    @Test
    public void testSecurityInterceptorConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  security:\n"
                + "    enabled: true\n"
                + "    security-interceptors:\n"
                + "      - foo\n"
                + "      - bar\n"
                + "    client-block-unmapped-actions: false\n"
                + "    member-credentials-factory:\n"
                + "      class-name: MyCredentialsFactory\n"
                + "      properties:\n"
                + "        property: value\n"
                + "    member-login-modules:\n"
                + "      - class-name: MyRequiredLoginModule\n"
                + "        usage: REQUIRED\n"
                + "        properties:\n"
                + "          login-property: login-value\n"
                + "      - class-name: MyRequiredLoginModule2\n"
                + "        usage: SUFFICIENT\n"
                + "        properties:\n"
                + "          login-property2: login-value2\n"
                + "    client-login-modules:\n"
                + "      - class-name: MyOptionalLoginModule\n"
                + "        usage: OPTIONAL\n"
                + "        properties:\n"
                + "          client-property: client-value\n"
                + "      - class-name: MyRequiredLoginModule\n"
                + "        usage: REQUIRED\n"
                + "        properties:\n"
                + "          client-property2: client-value2\n"
                + "    client-permission-policy:\n"
                + "      class-name: MyPermissionPolicy\n"
                + "      properties:\n"
                + "        permission-property: permission-value\n";

        Config config = buildConfig(yaml);
        SecurityConfig securityConfig = config.getSecurityConfig();
        List<SecurityInterceptorConfig> interceptorConfigs = securityConfig.getSecurityInterceptorConfigs();

        assertEquals(2, interceptorConfigs.size());
        assertEquals("foo", interceptorConfigs.get(0).className);
        assertEquals("bar", interceptorConfigs.get(1).className);
        assertFalse(securityConfig.getClientBlockUnmappedActions());

        // member-credentials-factory
        CredentialsFactoryConfig memberCredentialsConfig = securityConfig.getMemberCredentialsConfig();
        assertEquals("MyCredentialsFactory", memberCredentialsConfig.getClassName());
        assertEquals(1, memberCredentialsConfig.getProperties().size());
        assertEquals("value", memberCredentialsConfig.getProperties().getProperty("property"));

        // member-login-modules
        List<LoginModuleConfig> memberLoginModuleConfigs = securityConfig.getMemberLoginModuleConfigs();
        assertEquals(2, memberLoginModuleConfigs.size());
        Iterator<LoginModuleConfig> memberLoginIterator = memberLoginModuleConfigs.iterator();

        LoginModuleConfig memberLoginModuleCfg1 = memberLoginIterator.next();
        assertEquals("MyRequiredLoginModule", memberLoginModuleCfg1.getClassName());
        assertEquals(LoginModuleConfig.LoginModuleUsage.REQUIRED, memberLoginModuleCfg1.getUsage());
        assertEquals(1, memberLoginModuleCfg1.getProperties().size());
        assertEquals("login-value", memberLoginModuleCfg1.getProperties().getProperty("login-property"));

        LoginModuleConfig memberLoginModuleCfg2 = memberLoginIterator.next();
        assertEquals("MyRequiredLoginModule2", memberLoginModuleCfg2.getClassName());
        assertEquals(LoginModuleConfig.LoginModuleUsage.SUFFICIENT, memberLoginModuleCfg2.getUsage());
        assertEquals(1, memberLoginModuleCfg2.getProperties().size());
        assertEquals("login-value2", memberLoginModuleCfg2.getProperties().getProperty("login-property2"));

        // client-login-modules
        List<LoginModuleConfig> clientLoginModuleConfigs = securityConfig.getClientLoginModuleConfigs();
        assertEquals(2, clientLoginModuleConfigs.size());
        Iterator<LoginModuleConfig> clientLoginIterator = clientLoginModuleConfigs.iterator();

        LoginModuleConfig clientLoginModuleCfg1 = clientLoginIterator.next();
        assertEquals("MyOptionalLoginModule", clientLoginModuleCfg1.getClassName());
        assertEquals(LoginModuleConfig.LoginModuleUsage.OPTIONAL, clientLoginModuleCfg1.getUsage());
        assertEquals(1, clientLoginModuleCfg1.getProperties().size());
        assertEquals("client-value", clientLoginModuleCfg1.getProperties().getProperty("client-property"));

        LoginModuleConfig clientLoginModuleCfg2 = clientLoginIterator.next();
        assertEquals("MyRequiredLoginModule", clientLoginModuleCfg2.getClassName());
        assertEquals(LoginModuleConfig.LoginModuleUsage.REQUIRED, clientLoginModuleCfg2.getUsage());
        assertEquals(1, clientLoginModuleCfg2.getProperties().size());
        assertEquals("client-value2", clientLoginModuleCfg2.getProperties().getProperty("client-property2"));

        // client-permission-policy
        PermissionPolicyConfig permissionPolicyConfig = securityConfig.getClientPolicyConfig();
        assertEquals("MyPermissionPolicy", permissionPolicyConfig.getClassName());
        assertEquals(1, permissionPolicyConfig.getProperties().size());
        assertEquals("permission-value", permissionPolicyConfig.getProperties().getProperty("permission-property"));
    }

    @Override
    @Test
    public void readAwsConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  network:\n"
                + "    port:\n"
                + "      auto-increment: true\n"
                + "      port: 5701\n"
                + "    join:\n"
                + "      multicast:\n"
                + "        enabled: false\n"
                + "      aws:\n"
                + "        enabled: true\n"
                + "        use-public-ip: true\n"
                + "        connection-timeout-seconds: 10\n"
                + "        access-key: sample-access-key\n"
                + "        secret-key: sample-secret-key\n"
                + "        iam-role: sample-role\n"
                + "        region: sample-region\n"
                + "        host-header: sample-header\n"
                + "        security-group-name: sample-group\n"
                + "        tag-key: sample-tag-key\n"
                + "        tag-value: sample-tag-value\n";

        Config config = buildConfig(yaml);

        AwsConfig awsConfig = config.getNetworkConfig().getJoin().getAwsConfig();
        assertTrue(awsConfig.isEnabled());
        assertTrue(awsConfig.isUsePublicIp());
        assertAwsConfig(awsConfig);
    }

    @Override
    @Test
    public void readGcpConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  network:\n"
                + "    join:\n"
                + "      multicast:\n"
                + "        enabled: false\n"
                + "      gcp:\n"
                + "        enabled: true\n"
                + "        use-public-ip: true\n"
                + "        zones: us-east1-b\n";

        Config config = buildConfig(yaml);

        GcpConfig gcpConfig = config.getNetworkConfig().getJoin().getGcpConfig();

        assertTrue(gcpConfig.isEnabled());
        assertTrue(gcpConfig.isUsePublicIp());
        assertEquals("us-east1-b", gcpConfig.getProperty("zones"));
    }

    @Override
    @Test
    public void readAzureConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  network:\n"
                + "    join:\n"
                + "      multicast:\n"
                + "        enabled: false\n"
                + "      azure:\n"
                + "        enabled: true\n"
                + "        use-public-ip: true\n"
                + "        client-id: 123456789!\n";

        Config config = buildConfig(yaml);

        AzureConfig azureConfig = config.getNetworkConfig().getJoin().getAzureConfig();

        assertTrue(azureConfig.isEnabled());
        assertTrue(azureConfig.isUsePublicIp());
        assertEquals("123456789!", azureConfig.getProperty("client-id"));
    }

    @Override
    @Test
    public void readKubernetesConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  network:\n"
                + "    join:\n"
                + "      multicast:\n"
                + "        enabled: false\n"
                + "      kubernetes:\n"
                + "        enabled: true\n"
                + "        use-public-ip: true\n"
                + "        namespace: hazelcast\n";

        Config config = buildConfig(yaml);

        KubernetesConfig kubernetesConfig = config.getNetworkConfig().getJoin().getKubernetesConfig();

        assertTrue(kubernetesConfig.isEnabled());
        assertTrue(kubernetesConfig.isUsePublicIp());
        assertEquals("hazelcast", kubernetesConfig.getProperty("namespace"));
    }

    @Override
    @Test
    public void readEurekaConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  network:\n"
                + "    join:\n"
                + "      multicast:\n"
                + "        enabled: false\n"
                + "      eureka:\n"
                + "        enabled: true\n"
                + "        use-public-ip: true\n"
                + "        namespace: hazelcast\n";

        Config config = buildConfig(yaml);

        EurekaConfig eurekaConfig = config.getNetworkConfig().getJoin().getEurekaConfig();

        assertTrue(eurekaConfig.isEnabled());
        assertTrue(eurekaConfig.isUsePublicIp());
        assertEquals("hazelcast", eurekaConfig.getProperty("namespace"));
    }

    @Override
    @Test
    public void readDiscoveryConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  network:\n"
                + "    join:\n"
                + "      multicast:\n"
                + "        enabled: false\n"
                + "      discovery-strategies:\n"
                + "        node-filter:\n"
                + "          class: DummyFilterClass\n"
                + "        discovery-strategies:\n"
                + "          - class: DummyDiscoveryStrategy1\n"
                + "            enabled: true\n"
                + "            properties:\n"
                + "              key-string: foo\n"
                + "              key-int: 123\n"
                + "              key-boolean: true\n"
                + "          - class: DummyDiscoveryStrategy2\n"
                + "            enabled: true\n"
                + "            properties:\n"
                + "              key-string: foobar\n"
                + "              key-int: 321\n"
                + "              key-boolean: false\n";

        Config config = buildConfig(yaml);
        DiscoveryConfig discoveryConfig = config.getNetworkConfig().getJoin().getDiscoveryConfig();
        assertTrue(discoveryConfig.isEnabled());
        assertDiscoveryConfig(discoveryConfig);
    }

    @Override
    @Test
    public void testSSLConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  network:\n"
                + "    ssl:\n"
                + "      enabled: true\r\n"
                + "      factory-class-name: com.hazelcast.nio.ssl.BasicSSLContextFactory\r\n"
                + "      properties:\r\n"
                + "        protocol: TLS\r\n";

        Config config = buildConfig(yaml);
        SSLConfig sslConfig = config.getNetworkConfig().getSSLConfig();
        assertTrue(sslConfig.isEnabled());
        assertEquals("com.hazelcast.nio.ssl.BasicSSLContextFactory", sslConfig.getFactoryClassName());
        assertEquals(1, sslConfig.getProperties().size());
        assertEquals("TLS", sslConfig.getProperties().get("protocol"));
    }

    @Override
    @Test
    public void testSymmetricEncryptionConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  network:\n"
                + "    symmetric-encryption:\n"
                + "      enabled: true\n"
                + "      algorithm: AES\n"
                + "      salt: some-salt\n"
                + "      password: some-pass\n"
                + "      iteration-count: 7531\n";

        Config config = buildConfig(yaml);
        SymmetricEncryptionConfig symmetricEncryptionConfig = config.getNetworkConfig().getSymmetricEncryptionConfig();
        assertTrue(symmetricEncryptionConfig.isEnabled());
        assertEquals("AES", symmetricEncryptionConfig.getAlgorithm());
        assertEquals("some-salt", symmetricEncryptionConfig.getSalt());
        assertEquals("some-pass", symmetricEncryptionConfig.getPassword());
        assertEquals(7531, symmetricEncryptionConfig.getIterationCount());
    }

    @Override
    @Test
    public void readPortCount() {
        // check when it is explicitly set
        Config config = buildConfig(""
                + "hazelcast:\n"
                + "  network:\n"
                + "    port:\n"
                + "      port-count: 200\n"
                + "      port: 5702\n");

        assertEquals(200, config.getNetworkConfig().getPortCount());
        assertEquals(5702, config.getNetworkConfig().getPort());

        // check if the default is passed in correctly
        config = buildConfig(""
                + "hazelcast:\n"
                + "  network:\n"
                + "    port: 5701\n");
        assertEquals(100, config.getNetworkConfig().getPortCount());
    }

    @Override
    @Test
    public void readPortAutoIncrement() {
        // explicitly set
        Config config = buildConfig(""
                + "hazelcast:\n"
                + "  network:\n"
                + "    port:\n"
                + "      auto-increment: false\n"
                + "      port: 5701\n");
        assertFalse(config.getNetworkConfig().isPortAutoIncrement());

        // check if the default is picked up correctly
        config = buildConfig(""
                + "hazelcast:\n"
                + "  network:\n"
                + "    port: 5701\n");
        assertTrue(config.getNetworkConfig().isPortAutoIncrement());
    }

    @Override
    @Test
    public void networkReuseAddress() {
        Config config = buildConfig(""
                + "hazelcast:\n"
                + "  network:\n"
                + "    reuse-address: true\n");
        assertTrue(config.getNetworkConfig().isReuseAddress());
    }

    @Override
    @Test
    public void readSemaphoreConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  semaphore:\n"
                + "    default:\n"
                + "      initial-permits: 1\n"
                + "    custom:\n"
                + "      initial-permits: 10\n"
                + "      quorum-ref: customQuorumRule";

        Config config = buildConfig(yaml);
        SemaphoreConfig defaultConfig = config.getSemaphoreConfig("default");
        SemaphoreConfig customConfig = config.getSemaphoreConfig("custom");
        assertEquals(1, defaultConfig.getInitialPermits());
        assertEquals(10, customConfig.getInitialPermits());
        assertEquals("customQuorumRule", customConfig.getQuorumName());
    }

    @Override
    @Test
    public void readQueueConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  queue:\n"
                + "    custom:\n"
                + "      statistics-enabled: false\n"
                + "      max-size: 100\n"
                + "      backup-count: 2\n"
                + "      async-backup-count: 1\n"
                + "      empty-queue-ttl: 1\n"
                + "      item-listeners:\n"
                + "        - class-name: com.hazelcast.examples.ItemListener\n"
                + "          include-value: false\n"
                + "        - class-name: com.hazelcast.examples.ItemListener2\n"
                + "          include-value: true\n"
                + "      queue-store:\n"
                + "        enabled: false\n"
                + "        class-name: com.hazelcast.QueueStoreImpl\n"
                + "        properties:\n"
                + "          binary: false\n"
                + "          memory-limit: 1000\n"
                + "          bulk-load: 500\n"
                + "      quorum-ref: customQuorumRule\n"
                + "      merge-policy:\n"
                + "        batch-size: 23\n"
                + "        class-name: CustomMergePolicy\n"
                + "    default:\n"
                + "      max-size: 42\n";

        Config config = buildConfig(yaml);
        QueueConfig customQueueConfig = config.getQueueConfig("custom");
        assertFalse(customQueueConfig.isStatisticsEnabled());
        assertEquals(100, customQueueConfig.getMaxSize());
        assertEquals(2, customQueueConfig.getBackupCount());
        assertEquals(1, customQueueConfig.getAsyncBackupCount());
        assertEquals(1, customQueueConfig.getEmptyQueueTtl());

        MergePolicyConfig mergePolicyConfig = customQueueConfig.getMergePolicyConfig();
        assertEquals("CustomMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(23, mergePolicyConfig.getBatchSize());

        assertEquals(2, customQueueConfig.getItemListenerConfigs().size());
        Iterator<ItemListenerConfig> itemListenerIterator = customQueueConfig.getItemListenerConfigs().iterator();
        ItemListenerConfig listenerConfig1 = itemListenerIterator.next();
        assertEquals("com.hazelcast.examples.ItemListener", listenerConfig1.getClassName());
        assertFalse(listenerConfig1.isIncludeValue());
        ItemListenerConfig listenerConfig2 = itemListenerIterator.next();
        assertEquals("com.hazelcast.examples.ItemListener2", listenerConfig2.getClassName());
        assertTrue(listenerConfig2.isIncludeValue());

        QueueStoreConfig storeConfig = customQueueConfig.getQueueStoreConfig();
        assertNotNull(storeConfig);
        assertFalse(storeConfig.isEnabled());
        assertEquals("com.hazelcast.QueueStoreImpl", storeConfig.getClassName());

        Properties storeConfigProperties = storeConfig.getProperties();
        assertEquals(3, storeConfigProperties.size());
        assertEquals("500", storeConfigProperties.getProperty("bulk-load"));
        assertEquals("1000", storeConfigProperties.getProperty("memory-limit"));
        assertEquals("false", storeConfigProperties.getProperty("binary"));

        assertEquals("customQuorumRule", customQueueConfig.getQuorumName());

        QueueConfig defaultQueueConfig = config.getQueueConfig("default");
        assertEquals(42, defaultQueueConfig.getMaxSize());
    }

    @Override
    @Test
    public void readListConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  list:\n"
                + "    myList:\n"
                + "      statistics-enabled: false\n"
                + "      max-size: 100\n"
                + "      backup-count: 2\n"
                + "      async-backup-count: 1\n"
                + "      item-listeners:\n"
                + "        - class-name: com.hazelcast.examples.ItemListener\n"
                + "          include-value: false\n"
                + "        - class-name: com.hazelcast.examples.ItemListener2\n"
                + "          include-value: true\n"
                + "      merge-policy:\n"
                + "        class-name: PassThroughMergePolicy\n"
                + "        batch-size: 4223\n"
                + "    default:\n"
                + "      max-size: 42\n";

        Config config = buildConfig(yaml);
        ListConfig myListConfig = config.getListConfig("myList");

        assertEquals("myList", myListConfig.getName());
        assertFalse(myListConfig.isStatisticsEnabled());
        assertEquals(100, myListConfig.getMaxSize());
        assertEquals(2, myListConfig.getBackupCount());
        assertEquals(1, myListConfig.getAsyncBackupCount());

        assertEquals(2, myListConfig.getItemListenerConfigs().size());
        Iterator<ItemListenerConfig> itemListenerIterator = myListConfig.getItemListenerConfigs().iterator();
        ItemListenerConfig listenerConfig1 = itemListenerIterator.next();
        assertEquals("com.hazelcast.examples.ItemListener", listenerConfig1.getClassName());
        assertFalse(listenerConfig1.isIncludeValue());
        ItemListenerConfig listenerConfig2 = itemListenerIterator.next();
        assertEquals("com.hazelcast.examples.ItemListener2", listenerConfig2.getClassName());
        assertTrue(listenerConfig2.isIncludeValue());

        MergePolicyConfig mergePolicyConfig = myListConfig.getMergePolicyConfig();
        assertEquals("PassThroughMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(4223, mergePolicyConfig.getBatchSize());

        ListConfig defaultListConfig = config.getListConfig("default");
        assertEquals(42, defaultListConfig.getMaxSize());
    }

    @Override
    @Test
    public void readSetConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  set:\n"
                + "    mySet:\n"
                + "      statistics-enabled: false\n"
                + "      max-size: 100\n"
                + "      backup-count: 2\n"
                + "      async-backup-count: 1\n"
                + "      item-listeners:\n"
                + "        - class-name: com.hazelcast.examples.ItemListener\n"
                + "          include-value: false\n"
                + "        - class-name: com.hazelcast.examples.ItemListener2\n"
                + "          include-value: true\n"
                + "      merge-policy:\n"
                + "        class-name: PassThroughMergePolicy\n"
                + "        batch-size: 4223\n"
                + "    default:\n"
                + "      max-size: 42\n";

        Config config = buildConfig(yaml);
        SetConfig setConfig = config.getSetConfig("mySet");

        assertEquals("mySet", setConfig.getName());
        assertFalse(setConfig.isStatisticsEnabled());
        assertEquals(100, setConfig.getMaxSize());
        assertEquals(2, setConfig.getBackupCount());
        assertEquals(1, setConfig.getAsyncBackupCount());

        assertEquals(2, setConfig.getItemListenerConfigs().size());
        Iterator<ItemListenerConfig> itemListenerIterator = setConfig.getItemListenerConfigs().iterator();
        ItemListenerConfig listenerConfig1 = itemListenerIterator.next();
        assertEquals("com.hazelcast.examples.ItemListener", listenerConfig1.getClassName());
        assertFalse(listenerConfig1.isIncludeValue());
        ItemListenerConfig listenerConfig2 = itemListenerIterator.next();
        assertEquals("com.hazelcast.examples.ItemListener2", listenerConfig2.getClassName());
        assertTrue(listenerConfig2.isIncludeValue());

        MergePolicyConfig mergePolicyConfig = setConfig.getMergePolicyConfig();
        assertEquals("PassThroughMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(4223, mergePolicyConfig.getBatchSize());

        SetConfig defaultSetConfig = config.getSetConfig("default");
        assertEquals(42, defaultSetConfig.getMaxSize());
    }

    @Override
    @Test
    public void readLockConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  lock:\n"
                + "    default:\n"
                + "      quorum-ref: quorumRuleWithThreeNodes\n"
                + "    custom:\n"
                + "      quorum-ref: customQuorumRule\n";

        Config config = buildConfig(yaml);
        LockConfig defaultConfig = config.getLockConfig("default");
        LockConfig customConfig = config.getLockConfig("custom");
        assertEquals("quorumRuleWithThreeNodes", defaultConfig.getQuorumName());
        assertEquals("customQuorumRule", customConfig.getQuorumName());
    }

    @Override
    @Test
    public void readReliableTopic() {
        String yaml = ""
                + "hazelcast:\n"
                + "  reliable-topic:\n"
                + "    custom:\n"
                + "      read-batch-size: 35\n"
                + "      statistics-enabled: false\n"
                + "      topic-overload-policy: DISCARD_OLDEST\n"
                + "      message-listeners:\n"
                + "        - MessageListenerImpl\n"
                + "        - MessageListenerImpl2\n"
                + "    default:\n"
                + "      read-batch-size: 42\n";

        Config config = buildConfig(yaml);

        ReliableTopicConfig topicConfig = config.getReliableTopicConfig("custom");

        assertEquals(35, topicConfig.getReadBatchSize());
        assertFalse(topicConfig.isStatisticsEnabled());
        assertEquals(TopicOverloadPolicy.DISCARD_OLDEST, topicConfig.getTopicOverloadPolicy());

        // checking listener configuration
        assertEquals(2, topicConfig.getMessageListenerConfigs().size());
        ListenerConfig listenerConfig1 = topicConfig.getMessageListenerConfigs().get(0);
        assertEquals("MessageListenerImpl", listenerConfig1.getClassName());
        assertNull(listenerConfig1.getImplementation());
        ListenerConfig listenerConfig2 = topicConfig.getMessageListenerConfigs().get(1);
        assertEquals("MessageListenerImpl2", listenerConfig2.getClassName());
        assertNull(listenerConfig2.getImplementation());

        ReliableTopicConfig defaultReliableTopicConfig = config.getReliableTopicConfig("default");
        assertEquals(42, defaultReliableTopicConfig.getReadBatchSize());
    }

    @Override
    @Test
    public void readRingbuffer() {
        String yaml = ""
                + "hazelcast:\n"
                + "  ringbuffer:\n"
                + "    custom:\n"
                + "      capacity: 10\n"
                + "      backup-count: 2\n"
                + "      async-backup-count: 1\n"
                + "      time-to-live-seconds: 9\n"
                + "      in-memory-format: OBJECT\n"
                + "      ringbuffer-store:\n"
                + "        enabled: false\n"
                + "        class-name: com.hazelcast.RingbufferStoreImpl\n"
                + "        properties:\n"
                + "          store-path: .//tmp//bufferstore\n"
                + "      quorum-ref: customQuorumRule\n"
                + "      merge-policy:\n"
                + "        class-name: CustomMergePolicy\n"
                + "        batch-size: 2342\n"
                + "    default:\n"
                + "      capacity: 42\n";

        Config config = buildConfig(yaml);
        RingbufferConfig ringbufferConfig = config.getRingbufferConfig("custom");

        assertEquals(10, ringbufferConfig.getCapacity());
        assertEquals(2, ringbufferConfig.getBackupCount());
        assertEquals(1, ringbufferConfig.getAsyncBackupCount());
        assertEquals(9, ringbufferConfig.getTimeToLiveSeconds());
        assertEquals(InMemoryFormat.OBJECT, ringbufferConfig.getInMemoryFormat());

        RingbufferStoreConfig ringbufferStoreConfig = ringbufferConfig.getRingbufferStoreConfig();
        assertFalse(ringbufferStoreConfig.isEnabled());
        assertEquals("com.hazelcast.RingbufferStoreImpl", ringbufferStoreConfig.getClassName());
        Properties ringbufferStoreProperties = ringbufferStoreConfig.getProperties();
        assertEquals(".//tmp//bufferstore", ringbufferStoreProperties.get("store-path"));
        assertEquals("customQuorumRule", ringbufferConfig.getQuorumName());

        MergePolicyConfig mergePolicyConfig = ringbufferConfig.getMergePolicyConfig();
        assertEquals("CustomMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(2342, mergePolicyConfig.getBatchSize());

        RingbufferConfig defaultRingBufferConfig = config.getRingbufferConfig("default");
        assertEquals(42, defaultRingBufferConfig.getCapacity());
    }

    @Override
    @Test
    public void readAtomicLong() {
        String yaml = ""
                + "hazelcast:\n"
                + "  atomic-long:\n"
                + "    custom:\n"
                + "      merge-policy:\n"
                + "        class-name: CustomMergePolicy\n"
                + "        batch-size: 23\n"
                + "      quorum-ref: customQuorumRule\n"
                + "    default:\n"
                + "      quorum-ref: customQuorumRule2\n";

        Config config = buildConfig(yaml);
        AtomicLongConfig atomicLongConfig = config.getAtomicLongConfig("custom");
        assertEquals("custom", atomicLongConfig.getName());
        assertEquals("customQuorumRule", atomicLongConfig.getQuorumName());

        MergePolicyConfig mergePolicyConfig = atomicLongConfig.getMergePolicyConfig();
        assertEquals("CustomMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(23, mergePolicyConfig.getBatchSize());

        AtomicLongConfig defaultAtomicLongConfig = config.getAtomicLongConfig("default");
        assertEquals("customQuorumRule2", defaultAtomicLongConfig.getQuorumName());
    }

    @Override
    @Test
    public void readAtomicReference() {
        String yaml = ""
                + "hazelcast:\n"
                + "  atomic-reference:\n"
                + "    custom:\n"
                + "      merge-policy:\n"
                + "        class-name: CustomMergePolicy\n"
                + "        batch-size: 23\n"
                + "      quorum-ref: customQuorumRule\n"
                + "    default:\n"
                + "      quorum-ref: customQuorumRule2\n";

        Config config = buildConfig(yaml);
        AtomicReferenceConfig atomicReferenceConfig = config.getAtomicReferenceConfig("custom");
        assertEquals("custom", atomicReferenceConfig.getName());
        assertEquals("customQuorumRule", atomicReferenceConfig.getQuorumName());

        MergePolicyConfig mergePolicyConfig = atomicReferenceConfig.getMergePolicyConfig();
        assertEquals("CustomMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(23, mergePolicyConfig.getBatchSize());

        AtomicReferenceConfig defaultAtomicReferenceConfig = config.getAtomicReferenceConfig("default");
        assertEquals("customQuorumRule2", defaultAtomicReferenceConfig.getQuorumName());
    }

    @Override
    @Test
    public void readCountDownLatch() {
        String yaml = ""
                + "hazelcast:\n"
                + "  count-down-latch:\n"
                + "    custom:\n"
                + "      quorum-ref: customQuorumRule\n"
                + "    default:\n"
                + "      quorum-ref: customQuorumRule2\n";

        Config config = buildConfig(yaml);
        CountDownLatchConfig countDownLatchConfig = config.getCountDownLatchConfig("custom");
        assertEquals("custom", countDownLatchConfig.getName());
        assertEquals("customQuorumRule", countDownLatchConfig.getQuorumName());

        CountDownLatchConfig defaultCountDownLatchConfig = config.getCountDownLatchConfig("default");
        assertEquals("customQuorumRule2", defaultCountDownLatchConfig.getQuorumName());
    }

    @Override
    @Test
    public void testCaseInsensitivityOfSettings() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    testCaseInsensitivity:\n"
                + "      in-memory-format: BINARY\n"
                + "      backup-count: 1\n"
                + "      async-backup-count: 0\n"
                + "      time-to-live-seconds: 0\n"
                + "      max-idle-seconds: 0\n"
                + "      eviction-policy: NONE\n"
                + "      max-size:\n"
                + "        policy: per_partition\n"
                + "        max-size: 0\n"
                + "      eviction-percentage: 25\n"
                + "      merge-policy:\n"
                + "        class-name: CustomMergePolicy\n"
                + "        batch-size: 2342\n";

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("testCaseInsensitivity");

        assertEquals(InMemoryFormat.BINARY, mapConfig.getInMemoryFormat());
        assertEquals(EvictionPolicy.NONE, mapConfig.getEvictionPolicy());
        assertEquals(MaxSizeConfig.MaxSizePolicy.PER_PARTITION, mapConfig.getMaxSizeConfig().getMaxSizePolicy());

        MergePolicyConfig mergePolicyConfig = mapConfig.getMergePolicyConfig();
        assertEquals("CustomMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(2342, mergePolicyConfig.getBatchSize());
    }

    @Override
    @Test
    public void testManagementCenterConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  management-center:\n"
                + "    enabled: true\n"
                + "    scripting-enabled: false\n"
                + "    url: someUrl\n";

        Config config = buildConfig(yaml);
        ManagementCenterConfig manCenterCfg = config.getManagementCenterConfig();

        assertTrue(manCenterCfg.isEnabled());
        assertFalse(manCenterCfg.isScriptingEnabled());
        assertEquals("someUrl", manCenterCfg.getUrl());
    }

    @Override
    @Test
    public void testManagementCenterConfigComplex() {
        String yaml = ""
                + "hazelcast:\n"
                + "  management-center:\n"
                + "    enabled: true\n"
                + "    url: wowUrl\n"
                + "    mutual-auth:\n"
                + "      enabled: true\n"
                + "      properties:\n"
                + "        keyStore: /tmp/foo_keystore\n"
                + "        trustStore: /tmp/foo_truststore\n";

        Config config = buildConfig(yaml);
        ManagementCenterConfig manCenterCfg = config.getManagementCenterConfig();

        assertTrue(manCenterCfg.isEnabled());
        assertEquals("wowUrl", manCenterCfg.getUrl());
        assertTrue(manCenterCfg.getMutualAuthConfig().isEnabled());
        assertEquals("/tmp/foo_keystore", manCenterCfg.getMutualAuthConfig().getProperty("keyStore"));
        assertEquals("/tmp/foo_truststore", manCenterCfg.getMutualAuthConfig().getProperty("trustStore"));
    }

    @Override
    @Test
    public void testNullManagementCenterConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  management-center: {}";

        Config config = buildConfig(yaml);
        ManagementCenterConfig manCenterCfg = config.getManagementCenterConfig();

        assertFalse(manCenterCfg.isEnabled());
        assertNull(manCenterCfg.getUrl());
    }

    @Override
    @Test
    public void testEmptyManagementCenterConfig() {
        String yaml = "hazelcast: {}";

        Config config = buildConfig(yaml);
        ManagementCenterConfig manCenterCfg = config.getManagementCenterConfig();

        assertFalse(manCenterCfg.isEnabled());
        assertNull(manCenterCfg.getUrl());
    }

    @Override
    @Test
    public void testNotEnabledManagementCenterConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  management-center:\n"
                + "    enabled: false\n";

        Config config = buildConfig(yaml);
        ManagementCenterConfig manCenterCfg = config.getManagementCenterConfig();
        assertFalse(manCenterCfg.isEnabled());
        assertNull(manCenterCfg.getUrl());
    }

    @Override
    @Test
    public void testNotEnabledWithURLManagementCenterConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  management-center:\n"
                + "    enabled: false\n"
                + "    url: http://localhost:8080/mancenter\n";

        Config config = buildConfig(yaml);
        ManagementCenterConfig manCenterCfg = config.getManagementCenterConfig();

        assertFalse(manCenterCfg.isEnabled());
        assertEquals("http://localhost:8080/mancenter", manCenterCfg.getUrl());
    }

    @Override
    @Test
    public void testManagementCenterConfigComplexDisabledMutualAuth() {
        String yaml = ""
                + "hazelcast:\n"
                + "  management-center:\n"
                + "    enabled: true\n"
                + "    url: wowUrl\n"
                + "    mutual-auth:\n"
                + "      enabled: false\n";

        Config config = buildConfig(yaml);
        ManagementCenterConfig manCenterCfg = config.getManagementCenterConfig();

        assertTrue(manCenterCfg.isEnabled());
        assertEquals("wowUrl", manCenterCfg.getUrl());
        assertFalse(manCenterCfg.getMutualAuthConfig().isEnabled());
    }

    @Override
    @Test
    public void testMapStoreInitialModeLazy() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    mymap:\n"
                + "      map-store:\n"
                + "        enabled: true\n"
                + "        initial-mode: LAZY\n";

        Config config = buildConfig(yaml);
        MapStoreConfig mapStoreConfig = config.getMapConfig("mymap").getMapStoreConfig();

        assertTrue(mapStoreConfig.isEnabled());
        assertEquals(MapStoreConfig.InitialLoadMode.LAZY, mapStoreConfig.getInitialLoadMode());
    }

    @Override
    @Test
    public void testMapConfig_minEvictionCheckMillis() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    mymap:\n"
                + "      min-eviction-check-millis: 123456789\n";

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertEquals(123456789L, mapConfig.getMinEvictionCheckMillis());
    }

    @Override
    @Test
    public void testMapConfig_minEvictionCheckMillis_defaultValue() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    mymap: {}\n";

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertEquals(MapConfig.DEFAULT_MIN_EVICTION_CHECK_MILLIS, mapConfig.getMinEvictionCheckMillis());
    }

    @Override
    @Test
    public void testMapConfig_metadataPolicy() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    mymap:\n"
                + "      metadata-policy: OFF";

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertEquals(MetadataPolicy.OFF, mapConfig.getMetadataPolicy());
    }

    @Override
    @Test
    public void testMapConfig_metadataPolicy_defaultValue() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    mymap: {}";

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertEquals(MetadataPolicy.CREATE_ON_UPDATE, mapConfig.getMetadataPolicy());
    }

    @Override
    @Test
    public void testMapConfig_evictions() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    lruMap:\n"
                + "      eviction-policy: LRU\n"
                + ""
                + "    lfuMap:\n"
                + "      eviction-policy: LFU\n"
                + ""
                + "    noneMap:\n"
                + "      eviction-policy: NONE\n"
                + ""
                + "    randomMap:\n"
                + "      eviction-policy: RANDOM\n";

        Config config = buildConfig(yaml);

        assertEquals(EvictionPolicy.LRU, config.getMapConfig("lruMap").getEvictionPolicy());
        assertEquals(EvictionPolicy.LFU, config.getMapConfig("lfuMap").getEvictionPolicy());
        assertEquals(EvictionPolicy.NONE, config.getMapConfig("noneMap").getEvictionPolicy());
        assertEquals(EvictionPolicy.RANDOM, config.getMapConfig("randomMap").getEvictionPolicy());
    }

    @Override
    @Test
    public void testMapConfig_optimizeQueries() {
        String yaml1 = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    mymap1:\n"
                + "      optimize-queries: true\n";

        Config config1 = buildConfig(yaml1);
        MapConfig mapConfig1 = config1.getMapConfig("mymap1");
        assertEquals(CacheDeserializedValues.ALWAYS, mapConfig1.getCacheDeserializedValues());

        String yaml2 = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    mymap2:\n"
                + "      optimize-queries: false\n";

        Config config2 = buildConfig(yaml2);
        MapConfig mapConfig2 = config2.getMapConfig("mymap2");

        assertEquals(CacheDeserializedValues.INDEX_ONLY, mapConfig2.getCacheDeserializedValues());
    }

    @Override
    @Test
    public void testMapConfig_cacheValueConfig_defaultValue() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    mymap: {}\n";

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertEquals(CacheDeserializedValues.INDEX_ONLY, mapConfig.getCacheDeserializedValues());
    }

    @Override
    @Test
    public void testMapConfig_cacheValueConfig_never() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    mymap:\n"
                + "      cache-deserialized-values: NEVER\n";

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertEquals(CacheDeserializedValues.NEVER, mapConfig.getCacheDeserializedValues());
    }

    @Override
    @Test
    public void testMapConfig_cacheValueConfig_always() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    mymap:\n"
                + "      cache-deserialized-values: ALWAYS\n";

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertEquals(CacheDeserializedValues.ALWAYS, mapConfig.getCacheDeserializedValues());
    }

    @Override
    @Test
    public void testMapConfig_cacheValueConfig_indexOnly() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    mymap:\n"
                + "      cache-deserialized-values: INDEX-ONLY\n";

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertEquals(CacheDeserializedValues.INDEX_ONLY, mapConfig.getCacheDeserializedValues());
    }

    @Override
    @Test
    public void testMapStoreInitialModeEager() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    mymap:\n"
                + "      map-store:\n"
                + "        enabled: true\n"
                + "        initial-mode: EAGER\n";

        Config config = buildConfig(yaml);
        MapStoreConfig mapStoreConfig = config.getMapConfig("mymap").getMapStoreConfig();

        assertTrue(mapStoreConfig.isEnabled());
        assertEquals(MapStoreConfig.InitialLoadMode.EAGER, mapStoreConfig.getInitialLoadMode());
    }

    @Override
    @Test
    public void testMapStoreWriteBatchSize() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    mymap:\n"
                + "      map-store:\n"
                + "        write-batch-size: 23\n";

        Config config = buildConfig(yaml);
        MapStoreConfig mapStoreConfig = config.getMapConfig("mymap").getMapStoreConfig();

        assertEquals(23, mapStoreConfig.getWriteBatchSize());
    }

    @Override
    @Test
    public void testMapStoreConfig_writeCoalescing_whenDefault() {
        MapStoreConfig mapStoreConfig = getWriteCoalescingMapStoreConfig(MapStoreConfig.DEFAULT_WRITE_COALESCING, true);

        assertTrue(mapStoreConfig.isWriteCoalescing());
    }

    @Override
    @Test
    public void testMapStoreConfig_writeCoalescing_whenSetFalse() {
        MapStoreConfig mapStoreConfig = getWriteCoalescingMapStoreConfig(false, false);

        assertFalse(mapStoreConfig.isWriteCoalescing());
    }

    @Override
    @Test
    public void testMapStoreConfig_writeCoalescing_whenSetTrue() {
        MapStoreConfig mapStoreConfig = getWriteCoalescingMapStoreConfig(true, false);

        assertTrue(mapStoreConfig.isWriteCoalescing());
    }

    private MapStoreConfig getWriteCoalescingMapStoreConfig(boolean writeCoalescing, boolean useDefault) {
        String yaml = getWriteCoalescingConfigYaml(writeCoalescing, useDefault);
        Config config = buildConfig(yaml);
        return config.getMapConfig("mymap").getMapStoreConfig();
    }

    private String getWriteCoalescingConfigYaml(boolean value, boolean useDefault) {
        return ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    mymap:\n"
                + "      map-store:"
                + (useDefault ? "{}" : "\n        write-coalescing: " + String.valueOf(value) + "\n");
    }

    @Override
    @Test
    public void testNearCacheInMemoryFormat() {
        String mapName = "testMapNearCacheInMemoryFormat";
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    " + mapName + ":\n"
                + "      near-cache:\n"
                + "        in-memory-format: OBJECT\n";

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig(mapName);
        NearCacheConfig ncConfig = mapConfig.getNearCacheConfig();

        assertEquals(InMemoryFormat.OBJECT, ncConfig.getInMemoryFormat());
    }

    @Override
    @Test
    public void testNearCacheInMemoryFormatNative_withKeysByReference() {
        String mapName = "testMapNearCacheInMemoryFormatNative";
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    " + mapName + ":\n"
                + "      near-cache:\n"
                + "        in-memory-format: NATIVE\n"
                + "        serialize-keys: false\n";

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig(mapName);
        NearCacheConfig ncConfig = mapConfig.getNearCacheConfig();

        assertEquals(InMemoryFormat.NATIVE, ncConfig.getInMemoryFormat());
        assertTrue(ncConfig.isSerializeKeys());
    }

    @Override
    @Test
    public void testNearCacheEvictionPolicy() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    lfuNearCache:\n"
                + "      near-cache:\n"
                + "        eviction:\n"
                + "          eviction-policy: LFU\n"
                + ""
                + "    lruNearCache:\n"
                + "      near-cache:\n"
                + "        eviction:\n"
                + "          eviction-policy: LRU\n"
                + ""
                + "    noneNearCache:\n"
                + "      near-cache:\n"
                + "        eviction:\n"
                + "          eviction-policy: NONE\n"
                + "    randomNearCache:\n"
                + "      near-cache:\n"
                + "        eviction:\n"
                + "          eviction-policy: RANDOM\n";

        Config config = buildConfig(yaml);
        assertEquals(EvictionPolicy.LFU, getNearCacheEvictionPolicy("lfuNearCache", config));
        assertEquals(EvictionPolicy.LRU, getNearCacheEvictionPolicy("lruNearCache", config));
        assertEquals(EvictionPolicy.NONE, getNearCacheEvictionPolicy("noneNearCache", config));
        assertEquals(EvictionPolicy.RANDOM, getNearCacheEvictionPolicy("randomNearCache", config));
    }

    private EvictionPolicy getNearCacheEvictionPolicy(String mapName, Config config) {
        return config.getMapConfig(mapName).getNearCacheConfig().getEvictionConfig().getEvictionPolicy();
    }

    @Override
    @Test
    public void testPartitionGroupZoneAware() {
        String yaml = ""
                + "hazelcast:\n"
                + "  partition-group:\n"
                + "    enabled: true\n"
                + "    group-type: ZONE_AWARE\n";

        Config config = buildConfig(yaml);
        PartitionGroupConfig partitionGroupConfig = config.getPartitionGroupConfig();
        assertTrue(partitionGroupConfig.isEnabled());
        assertEquals(PartitionGroupConfig.MemberGroupType.ZONE_AWARE, partitionGroupConfig.getGroupType());
    }

    @Override
    @Test
    public void testPartitionGroupSPI() {
        String yaml = ""
                + "hazelcast:\n"
                + "  partition-group:\n"
                + "    enabled: true\n"
                + "    group-type: SPI\n";

        Config config = buildConfig(yaml);
        assertEquals(PartitionGroupConfig.MemberGroupType.SPI, config.getPartitionGroupConfig().getGroupType());
    }

    @Override
    @Test
    public void testPartitionGroupMemberGroups() {
        String yaml = ""
                + "hazelcast:\n"
                + "  partition-group:\n"
                + "    enabled: true\n"
                + "    group-type: SPI\n"
                + "    member-group:\n"
                + "      -\n"
                + "        - 10.10.1.1\n"
                + "        - 10.10.1.2\n"
                + "      -\n"
                + "        - 10.10.1.3\n"
                + "        - 10.10.1.4\n";

        Config config = buildConfig(yaml);
        Collection<MemberGroupConfig> memberGroupConfigs = config.getPartitionGroupConfig().getMemberGroupConfigs();
        assertEquals(2, memberGroupConfigs.size());
        Iterator<MemberGroupConfig> iterator = memberGroupConfigs.iterator();

        MemberGroupConfig memberGroupConfig1 = iterator.next();
        assertEquals(2, memberGroupConfig1.getInterfaces().size());
        assertTrue(memberGroupConfig1.getInterfaces().contains("10.10.1.1"));
        assertTrue(memberGroupConfig1.getInterfaces().contains("10.10.1.2"));

        MemberGroupConfig memberGroupConfig2 = iterator.next();
        assertEquals(2, memberGroupConfig2.getInterfaces().size());
        assertTrue(memberGroupConfig2.getInterfaces().contains("10.10.1.3"));
        assertTrue(memberGroupConfig2.getInterfaces().contains("10.10.1.4"));
    }

    @Override
    @Test
    public void testNearCacheFullConfig() {
        String mapName = "testNearCacheFullConfig";
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    " + mapName + ":\n"
                + "      near-cache:\n"
                + "        name: test\n"
                + "        in-memory-format: OBJECT\n"
                + "        serialize-keys: false\n"
                + "        max-size: 1234\n"
                + "        time-to-live-seconds: 77\n"
                + "        max-idle-seconds: 92\n"
                + "        eviction-policy: LFU\n"
                + "        invalidate-on-change: false\n"
                + "        cache-local-entries: false\n"
                + "        eviction:\n"
                + "          eviction-policy: LRU\n"
                + "          max-size-policy: ENTRY_COUNT\n"
                + "          size: 3333";

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig(mapName);
        NearCacheConfig nearCacheConfig = mapConfig.getNearCacheConfig();

        assertEquals(InMemoryFormat.OBJECT, nearCacheConfig.getInMemoryFormat());
        assertEquals(1234, nearCacheConfig.getMaxSize());
        assertEquals(77, nearCacheConfig.getTimeToLiveSeconds());
        assertEquals(92, nearCacheConfig.getMaxIdleSeconds());
        assertEquals("LFU", nearCacheConfig.getEvictionPolicy());
        assertFalse(nearCacheConfig.isInvalidateOnChange());
        assertFalse(nearCacheConfig.isCacheLocalEntries());
        assertEquals(LRU, nearCacheConfig.getEvictionConfig().getEvictionPolicy());
        assertEquals(ENTRY_COUNT, nearCacheConfig.getEvictionConfig().getMaximumSizePolicy());
        assertEquals(3333, nearCacheConfig.getEvictionConfig().getSize());
        assertEquals("test", nearCacheConfig.getName());
    }

    @Override
    @Test
    public void testMapWanReplicationRef() {
        String mapName = "testMapWanReplicationRef";
        String refName = "test";
        String mergePolicy = "TestMergePolicy";
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    " + mapName + ":\n"
                + "      wan-replication-ref:\n"
                + "        test:\n"
                + "          merge-policy: TestMergePolicy\n"
                + "          filters:\n"
                + "            - com.example.SampleFilter\n";

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig(mapName);
        WanReplicationRef wanRef = mapConfig.getWanReplicationRef();

        assertEquals(refName, wanRef.getName());
        assertEquals(mergePolicy, wanRef.getMergePolicy());
        assertTrue(wanRef.isRepublishingEnabled());
        assertEquals(1, wanRef.getFilters().size());
        assertEquals("com.example.SampleFilter", wanRef.getFilters().get(0));
    }

    @Override
    @Test
    public void testWanReplicationConfig() {
        String configName = "test";
        String yaml = ""
                + "hazelcast:\n"
                + "  wan-replication:\n"
                + "    " + configName + ":\n"
                + "      wan-publisher:\n"
                + "        publisherId:\n"
                + "          group-name: nyc\n"
                + "          class-name: PublisherClassName\n"
                + "          queue-capacity: 15000\n"
                + "          queue-full-behavior: DISCARD_AFTER_MUTATION\n"
                + "          initial-publisher-state: STOPPED\n"
                + "          properties:\n"
                + "            propName1: propValue1\n"
                + "      wan-consumer:\n"
                + "        class-name: ConsumerClassName\n"
                + "        properties:\n"
                + "          propName1: propValue1\n";

        Config config = buildConfig(yaml);
        WanReplicationConfig wanReplicationConfig = config.getWanReplicationConfig(configName);

        assertEquals(configName, wanReplicationConfig.getName());

        WanConsumerConfig consumerConfig = wanReplicationConfig.getWanConsumerConfig();
        assertNotNull(consumerConfig);
        assertEquals("ConsumerClassName", consumerConfig.getClassName());

        Map<String, Comparable> properties = consumerConfig.getProperties();
        assertNotNull(properties);
        assertEquals(1, properties.size());
        assertEquals("propValue1", properties.get("propName1"));

        List<WanPublisherConfig> publishers = wanReplicationConfig.getWanPublisherConfigs();
        assertNotNull(publishers);
        assertEquals(1, publishers.size());
        WanPublisherConfig publisherConfig = publishers.get(0);
        assertEquals("PublisherClassName", publisherConfig.getClassName());
        assertEquals("nyc", publisherConfig.getGroupName());
        assertEquals("publisherId", publisherConfig.getPublisherId());
        assertEquals(15000, publisherConfig.getQueueCapacity());
        assertEquals(DISCARD_AFTER_MUTATION, publisherConfig.getQueueFullBehavior());
        assertEquals(WanPublisherState.STOPPED, publisherConfig.getInitialPublisherState());

        properties = publisherConfig.getProperties();
        assertNotNull(properties);
        assertEquals(1, properties.size());
        assertEquals("propValue1", properties.get("propName1"));
    }

    @Override
    @Test
    public void testDefaultOfPersistWanReplicatedDataIsFalse() {
        String configName = "test";
        String yaml = ""
                + "hazelcast:\n"
                + "  wan-replication:\n"
                + "    " + configName + ":\n"
                + "      wan-consumer: {}\n";

        Config config = buildConfig(yaml);
        WanReplicationConfig wanReplicationConfig = config.getWanReplicationConfig(configName);
        WanConsumerConfig consumerConfig = wanReplicationConfig.getWanConsumerConfig();
        assertFalse(consumerConfig.isPersistWanReplicatedData());
    }

    @Override
    @Test
    public void testWanReplicationSyncConfig() {
        String configName = "test";
        String yaml = ""
                + "hazelcast:\n"
                + "  wan-replication:\n"
                + "    " + configName + ":\n"
                + "      wan-publisher:\n"
                + "        nyc:\n"
                + "          class-name: PublisherClassName\n"
                + "          wan-sync:\n"
                + "            consistency-check-strategy: MERKLE_TREES\n";

        Config config = buildConfig(yaml);
        WanReplicationConfig wanReplicationConfig = config.getWanReplicationConfig(configName);

        assertEquals(configName, wanReplicationConfig.getName());

        List<WanPublisherConfig> publishers = wanReplicationConfig.getWanPublisherConfigs();
        assertNotNull(publishers);
        assertEquals(1, publishers.size());
        WanPublisherConfig publisherConfig = publishers.get(0);
        assertEquals(ConsistencyCheckStrategy.MERKLE_TREES, publisherConfig.getWanSyncConfig()
                                                                           .getConsistencyCheckStrategy());
    }

    @Override
    @Test
    public void testMapEventJournalConfig() {
        String journalName = "mapName";
        String journal2Name = "map2Name";
        String yaml = ""
                + "hazelcast:\n"
                + "  event-journal:\n"
                + "    map:\n"
                + "      " + journalName + ":\n"
                + "        enabled: false\n"
                + "        capacity: 120\n"
                + "        time-to-live-seconds: 20\n"
                + "      " + journal2Name + ":\n"
                + "        enabled: true\n";

        Config config = buildConfig(yaml);
        EventJournalConfig journalConfig = config.getMapEventJournalConfig(journalName);

        assertFalse(journalConfig.isEnabled());
        assertEquals(120, journalConfig.getCapacity());
        assertEquals(20, journalConfig.getTimeToLiveSeconds());

        EventJournalConfig journal2Config = config.getMapEventJournalConfig(journal2Name);
        assertTrue(journal2Config.isEnabled());
    }

    @Override
    @Test
    public void testMapMerkleTreeConfig() {
        String mapName = "mapName";
        String map2Name = "map2Name";
        String yaml = ""
                + "hazelcast:\n"
                + "  merkle-tree:\n"
                + "    map:\n"
                + "      " + mapName + ":\n"
                + "        enabled: true\n"
                + "        depth: 20\n"
                + "      " + map2Name + ":\n"
                + "        enabled: false\n"
                + "        depth: 20\n";

        Config config = buildConfig(yaml);
        MerkleTreeConfig treeConfig = config.getMapMerkleTreeConfig(mapName);

        assertTrue(treeConfig.isEnabled());
        assertEquals(20, treeConfig.getDepth());

        MerkleTreeConfig tree2Config = config.getMapMerkleTreeConfig(map2Name);
        assertFalse(tree2Config.isEnabled());
    }

    @Override
    @Test
    public void testCacheEventJournalConfig() {
        String journalName = "cacheName";
        String journal2Name = "cache2Name";
        String yaml = ""
                + "hazelcast:\n"
                + "  event-journal:\n"
                + "    cache:\n"
                + "      " + journalName + ":\n"
                + "        enabled: true\n"
                + "        capacity: 120\n"
                + "        time-to-live-seconds: 20\n"
                + "      " + journal2Name + ":\n"
                + "        enabled: false\n";

        Config config = buildConfig(yaml);
        EventJournalConfig journalConfig = config.getCacheEventJournalConfig(journalName);

        assertTrue(journalConfig.isEnabled());
        assertEquals(120, journalConfig.getCapacity());
        assertEquals(20, journalConfig.getTimeToLiveSeconds());

        EventJournalConfig journal2Config = config.getCacheEventJournalConfig(journal2Name);
        assertFalse(journal2Config.isEnabled());
    }

    @Override
    @Test
    public void testFlakeIdGeneratorConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  flake-id-generator:\n"
                + "    gen:\n"
                + "      prefetch-count: 3\n"
                + "      prefetch-validity-millis: 10\n"
                + "      id-offset: 20\n"
                + "      node-id-offset: 30\n"
                + "      statistics-enabled: false\n"
                + "    gen2:\n"
                + "      statistics-enabled: true";

        Config config = buildConfig(yaml);
        FlakeIdGeneratorConfig fConfig = config.findFlakeIdGeneratorConfig("gen");
        assertEquals("gen", fConfig.getName());
        assertEquals(3, fConfig.getPrefetchCount());
        assertEquals(10L, fConfig.getPrefetchValidityMillis());
        assertEquals(20L, fConfig.getIdOffset());
        assertEquals(30L, fConfig.getNodeIdOffset());
        assertFalse(fConfig.isStatisticsEnabled());

        FlakeIdGeneratorConfig f2Config = config.findFlakeIdGeneratorConfig("gen2");
        assertTrue(f2Config.isStatisticsEnabled());
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testParseExceptionIsNotSwallowed() {
        String invalidYaml = "invalid-yaml";
        buildConfig(invalidYaml);

        // if we (for any reason) get through the parsing, then fail
        fail();
    }

    @Override
    @Test
    public void setMapStoreConfigImplementationTest() {
        String mapName = "mapStoreImpObjTest";
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    " + mapName + ":\n"
                + "      map-store:\n"
                + "        enabled: true\n"
                + "        class-name: com.hazelcast.config.helpers.DummyMapStore\n"
                + "        write-delay-seconds: 5";

        Config config = buildConfig(yaml);
        HazelcastInstance hz = createHazelcastInstance(config);
        IMap<String, String> map = hz.getMap(mapName);
        // MapStore is not instantiated until the MapContainer is created lazily
        map.put("sample", "data");

        MapConfig mapConfig = hz.getConfig().getMapConfig(mapName);
        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        Object o = mapStoreConfig.getImplementation();

        assertNotNull(o);
        assertTrue(o instanceof DummyMapStore);
    }

    @Override
    @Test
    public void testMapPartitionLostListenerConfig() {
        String mapName = "map1";
        String listenerName = "DummyMapPartitionLostListenerImpl";
        String yaml = createMapPartitionLostListenerConfiguredYaml(mapName, listenerName);

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("map1");
        assertMapPartitionLostListener(listenerName, mapConfig);
    }

    @Override
    @Test
    public void testMapPartitionLostListenerConfigReadOnly() {
        String mapName = "map1";
        String listenerName = "DummyMapPartitionLostListenerImpl";
        String yaml = createMapPartitionLostListenerConfiguredYaml(mapName, listenerName);

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.findMapConfig("map1");
        assertMapPartitionLostListener(listenerName, mapConfig);
    }

    private void assertMapPartitionLostListener(String listenerName, MapConfig mapConfig) {
        assertFalse(mapConfig.getPartitionLostListenerConfigs().isEmpty());
        assertEquals(listenerName, mapConfig.getPartitionLostListenerConfigs().get(0).getClassName());
    }

    private String createMapPartitionLostListenerConfiguredYaml(String mapName, String listenerName) {
        return ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    " + mapName + ":\n"
                + "      partition-lost-listeners:\n"
                + "        - " + listenerName + "\n";
    }

    @Override
    @Test
    public void testCachePartitionLostListenerConfig() {
        String cacheName = "cache1";
        String listenerName = "DummyCachePartitionLostListenerImpl";
        String yaml = createCachePartitionLostListenerConfiguredYaml(cacheName, listenerName);

        Config config = buildConfig(yaml);
        CacheSimpleConfig cacheConfig = config.getCacheConfig("cache1");
        assertCachePartitionLostListener(listenerName, cacheConfig);
    }

    @Override
    @Test
    public void testCachePartitionLostListenerConfigReadOnly() {
        String cacheName = "cache1";
        String listenerName = "DummyCachePartitionLostListenerImpl";
        String yaml = createCachePartitionLostListenerConfiguredYaml(cacheName, listenerName);

        Config config = buildConfig(yaml);
        CacheSimpleConfig cacheConfig = config.findCacheConfig("cache1");
        assertCachePartitionLostListener(listenerName, cacheConfig);
    }

    private void assertCachePartitionLostListener(String listenerName, CacheSimpleConfig cacheConfig) {
        assertFalse(cacheConfig.getPartitionLostListenerConfigs().isEmpty());
        assertEquals(listenerName, cacheConfig.getPartitionLostListenerConfigs().get(0).getClassName());
    }

    private String createCachePartitionLostListenerConfiguredYaml(String cacheName, String listenerName) {
        return ""
                + "hazelcast:\n"
                + "  cache:\n"
                + "    " + cacheName + ":\n"
                + "      partition-lost-listeners:\n"
                + "        - " + listenerName + "\n";
    }

    @Override
    @Test
    public void readMulticastConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  network:\n"
                + "    join:\n"
                + "      multicast:\n"
                + "        enabled: false\n"
                + "        loopbackModeEnabled: true\n"
                + "        multicast-group: 224.2.2.4\n"
                + "        multicast-port: 65438\n"
                + "        multicast-timeout-seconds: 4\n"
                + "        multicast-time-to-live: 42\n"
                + "        trusted-interfaces:\n"
                + "          - 127.0.0.1\n"
                + "          - 0.0.0.0\n";

        Config config = buildConfig(yaml);
        MulticastConfig multicastConfig = config.getNetworkConfig().getJoin().getMulticastConfig();

        assertFalse(multicastConfig.isEnabled());
        assertTrue(multicastConfig.isLoopbackModeEnabled());
        assertEquals("224.2.2.4", multicastConfig.getMulticastGroup());
        assertEquals(65438, multicastConfig.getMulticastPort());
        assertEquals(4, multicastConfig.getMulticastTimeoutSeconds());
        assertEquals(42, multicastConfig.getMulticastTimeToLive());
        assertEquals(2, multicastConfig.getTrustedInterfaces().size());
        assertTrue(multicastConfig.getTrustedInterfaces().containsAll(ImmutableSet.of("127.0.0.1", "0.0.0.0")));
    }

    @Override
    @Test
    public void testWanConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  wan-replication:\n"
                + "    my-wan-cluster:\n"
                + "      wan-publisher:\n"
                + "        istanbulPublisherId:\n"
                + "          group-name: istanbul\n"
                + "          class-name: com.hazelcast.wan.custom.WanPublisher\n"
                + "          queue-full-behavior: THROW_EXCEPTION\n"
                + "          queue-capacity: 21\n"
                + "          aws:\n"
                + "            enabled: false\n"
                + "            connection-timeout-seconds: 10\n"
                + "            access-key: sample-access-key\n"
                + "            secret-key: sample-secret-key\n"
                + "            iam-role: sample-role\n"
                + "            region: sample-region\n"
                + "            host-header: sample-header\n"
                + "            security-group-name: sample-group\n"
                + "            tag-key: sample-tag-key\n"
                + "            tag-value: sample-tag-value\n"
                + "          discovery-strategies:\n"
                + "            node-filter:\n"
                + "              class: DummyFilterClass\n"
                + "            discovery-strategies:\n"
                + "              - class: DummyDiscoveryStrategy1\n"
                + "                enabled: true\n"
                + "                properties:\n"
                + "                  key-string: foo\n"
                + "                  key-int: 123\n"
                + "                  key-boolean: true\n"
                + "              - class: DummyDiscoveryStrategy2\n"
                + "                enabled: true\n"
                + "                properties:\n"
                + "                  key-string: foobar\n"
                + "                  key-int: 321\n"
                + "                  key-boolean: false\n"
                + "          properties:\n"
                + "            custom.prop.publisher: prop.publisher\n"
                + "            discovery.period: 5\n"
                + "            maxEndpoints: 2\n"
                + "        ankara:\n"
                + "          class-name: com.hazelcast.wan.custom.WanPublisher>\n"
                + "          queue-full-behavior: THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE\n"
                + "          initial-publisher-state: STOPPED\n"
                + "      wan-consumer:\n"
                + "        class-name: com.hazelcast.wan.custom.WanConsumer\n"
                + "        properties:\n"
                + "          custom.prop.consumer: prop.consumer\n"
                + "        persist-wan-replicated-data: false\n";

        Config config = buildConfig(yaml);
        WanReplicationConfig wanConfig = config.getWanReplicationConfig("my-wan-cluster");
        assertNotNull(wanConfig);

        List<WanPublisherConfig> publisherConfigs = wanConfig.getWanPublisherConfigs();
        assertEquals(2, publisherConfigs.size());
        WanPublisherConfig publisherConfig1 = publisherConfigs.get(0);
        assertEquals("istanbul", publisherConfig1.getGroupName());
        assertEquals("istanbulPublisherId", publisherConfig1.getPublisherId());
        assertEquals("com.hazelcast.wan.custom.WanPublisher", publisherConfig1.getClassName());
        assertEquals(WANQueueFullBehavior.THROW_EXCEPTION, publisherConfig1.getQueueFullBehavior());
        assertEquals(WanPublisherState.REPLICATING, publisherConfig1.getInitialPublisherState());
        assertEquals(21, publisherConfig1.getQueueCapacity());
        Map<String, Comparable> pubProperties = publisherConfig1.getProperties();
        assertEquals("prop.publisher", pubProperties.get("custom.prop.publisher"));
        assertEquals("5", pubProperties.get("discovery.period"));
        assertEquals("2", pubProperties.get("maxEndpoints"));
        assertFalse(publisherConfig1.getAwsConfig().isEnabled());
        assertAwsConfig(publisherConfig1.getAwsConfig());
        assertFalse(publisherConfig1.getGcpConfig().isEnabled());
        assertFalse(publisherConfig1.getAzureConfig().isEnabled());
        assertFalse(publisherConfig1.getKubernetesConfig().isEnabled());
        assertFalse(publisherConfig1.getEurekaConfig().isEnabled());
        assertDiscoveryConfig(publisherConfig1.getDiscoveryConfig());

        WanPublisherConfig publisherConfig2 = publisherConfigs.get(1);
        assertEquals("ankara", publisherConfig2.getGroupName());
        assertNull(publisherConfig2.getPublisherId());
        assertEquals(WANQueueFullBehavior.THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE, publisherConfig2.getQueueFullBehavior());
        assertEquals(WanPublisherState.STOPPED, publisherConfig2.getInitialPublisherState());

        WanConsumerConfig consumerConfig = wanConfig.getWanConsumerConfig();
        assertEquals("com.hazelcast.wan.custom.WanConsumer", consumerConfig.getClassName());
        Map<String, Comparable> consProperties = consumerConfig.getProperties();
        assertEquals("prop.consumer", consProperties.get("custom.prop.consumer"));
        assertFalse(consumerConfig.isPersistWanReplicatedData());
    }

    protected Config buildConfig(String yaml) {
        ByteArrayInputStream bis = new ByteArrayInputStream(yaml.getBytes());
        YamlConfigBuilder configBuilder = new YamlConfigBuilder(bis);
        return configBuilder.build();
    }

    private void assertDiscoveryConfig(DiscoveryConfig c) {
        assertEquals("DummyFilterClass", c.getNodeFilterClass());
        assertEquals(2, c.getDiscoveryStrategyConfigs().size());

        Iterator<DiscoveryStrategyConfig> iterator = c.getDiscoveryStrategyConfigs().iterator();
        DiscoveryStrategyConfig config = iterator.next();
        assertEquals("DummyDiscoveryStrategy1", config.getClassName());

        Map<String, Comparable> props = config.getProperties();
        assertEquals("foo", props.get("key-string"));
        assertEquals("123", props.get("key-int"));
        assertEquals("true", props.get("key-boolean"));

        DiscoveryStrategyConfig config2 = iterator.next();
        assertEquals("DummyDiscoveryStrategy2", config2.getClassName());

        Map<String, Comparable> props2 = config2.getProperties();
        assertEquals("foobar", props2.get("key-string"));
        assertEquals("321", props2.get("key-int"));
        assertEquals("false", props2.get("key-boolean"));
    }

    @Override
    @Test
    public void testQuorumConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  quorum:\n"
                + "    myQuorum:\n"
                + "      enabled: true\n"
                + "      quorum-size: 3\n"
                + "      quorum-function-class-name: com.my.quorum.function\n"
                + "      quorum-type: READ\n";

        Config config = buildConfig(yaml);
        QuorumConfig quorumConfig = config.getQuorumConfig("myQuorum");

        assertTrue("quorum should be enabled", quorumConfig.isEnabled());
        assertEquals(3, quorumConfig.getSize());
        assertEquals(QuorumType.READ, quorumConfig.getType());
        assertEquals("com.my.quorum.function", quorumConfig.getQuorumFunctionClassName());
        assertTrue(quorumConfig.getListenerConfigs().isEmpty());
    }

    @Override
    @Test
    public void testQuorumListenerConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  quorum:\n"
                + "    myQuorum:\n"
                + "      enabled: true\n"
                + "      quorum-size: 3\n"
                + "      quorum-listeners:\n"
                + "         - com.abc.my.quorum.listener\n"
                + "         - com.abc.my.second.listener\n"
                + "      quorum-function-class-name: com.hazelcast.SomeQuorumFunction\n";

        Config config = buildConfig(yaml);
        QuorumConfig quorumConfig = config.getQuorumConfig("myQuorum");

        assertFalse(quorumConfig.getListenerConfigs().isEmpty());
        assertEquals("com.abc.my.quorum.listener", quorumConfig.getListenerConfigs().get(0).getClassName());
        assertEquals("com.abc.my.second.listener", quorumConfig.getListenerConfigs().get(1).getClassName());
        assertEquals("com.hazelcast.SomeQuorumFunction", quorumConfig.getQuorumFunctionClassName());
    }

    @Override
    @Test(expected = ConfigurationException.class)
    public void testQuorumConfig_whenClassNameAndRecentlyActiveQuorumDefined_exceptionIsThrown() {
        String yaml = ""
                + "hazelcast:\n"
                + "  quorum:\n"
                + "    myQuorum:\n"
                + "      enabled: true\n"
                + "      quorum-size: 3\n"
                + "      quorum-function-class-name: com.hazelcast.SomeQuorumFunction\n"
                + "      recently-active-quorum: {}";

        buildConfig(yaml);
    }

    @Override
    @Test(expected = ConfigurationException.class)
    public void testQuorumConfig_whenClassNameAndProbabilisticQuorumDefined_exceptionIsThrown() {
        String yaml = ""
                + "hazelcast:\n"
                + "  quorum:\n"
                + "    myQuorum:\n"
                + "      enabled: true\n"
                + "      quorum-size: 3\n"
                + "      quorum-function-class-name: com.hazelcast.SomeQuorumFunction\n"
                + "      probabilistic-quorum: {}";

        buildConfig(yaml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    @Ignore("Schema validation is supposed to fail, two quorum implementation is defined")
    public void testQuorumConfig_whenBothBuiltinQuorumsDefined_exceptionIsThrown() {
        String yaml = ""
                + "hazelcast:\n"
                + "  quorum:\n"
                + "    myQuorum:\n"
                + "      enabled: true\n"
                + "      quorum-size: 3\n"
                + "      probabilistic-quorum: {}\n"
                + "      recently-active-quorum: {}\n";

        buildConfig(yaml);
    }

    @Override
    @Test
    public void testQuorumConfig_whenRecentlyActiveQuorum_withDefaultValues() {
        String yaml = ""
                + "hazelcast:\n"
                + "  quorum:\n"
                + "    myQuorum:\n"
                + "      enabled: true\n"
                + "      quorum-size: 3\n"
                + "      recently-active-quorum: {}";

        Config config = buildConfig(yaml);
        QuorumConfig quorumConfig = config.getQuorumConfig("myQuorum");
        assertInstanceOf(RecentlyActiveQuorumFunction.class, quorumConfig.getQuorumFunctionImplementation());
        RecentlyActiveQuorumFunction quorumFunction = (RecentlyActiveQuorumFunction) quorumConfig
                .getQuorumFunctionImplementation();
        assertEquals(RecentlyActiveQuorumConfigBuilder.DEFAULT_HEARTBEAT_TOLERANCE_MILLIS,
                quorumFunction.getHeartbeatToleranceMillis());
    }

    @Override
    @Test
    public void testQuorumConfig_whenRecentlyActiveQuorum_withCustomValues() {
        String yaml = ""
                + "hazelcast:\n"
                + "  quorum:\n"
                + "    myQuorum:\n"
                + "      enabled: true\n"
                + "      quorum-size: 3\n"
                + "      recently-active-quorum:\n"
                + "        heartbeat-tolerance-millis: 13000\n";

        Config config = buildConfig(yaml);
        QuorumConfig quorumConfig = config.getQuorumConfig("myQuorum");
        assertEquals(3, quorumConfig.getSize());
        assertInstanceOf(RecentlyActiveQuorumFunction.class, quorumConfig.getQuorumFunctionImplementation());
        RecentlyActiveQuorumFunction quorumFunction = (RecentlyActiveQuorumFunction) quorumConfig
                .getQuorumFunctionImplementation();
        assertEquals(13000, quorumFunction.getHeartbeatToleranceMillis());
    }

    @Override
    @Test
    public void testQuorumConfig_whenProbabilisticQuorum_withDefaultValues() {
        String yaml = ""
                + "hazelcast:\n"
                + "  quorum:\n"
                + "    myQuorum:\n"
                + "      enabled: true\n"
                + "      quorum-size: 3\n"
                + "      probabilistic-quorum: {}";

        Config config = buildConfig(yaml);
        QuorumConfig quorumConfig = config.getQuorumConfig("myQuorum");
        assertInstanceOf(ProbabilisticQuorumFunction.class, quorumConfig.getQuorumFunctionImplementation());
        ProbabilisticQuorumFunction quorumFunction = (ProbabilisticQuorumFunction) quorumConfig.getQuorumFunctionImplementation();
        assertEquals(ProbabilisticQuorumConfigBuilder.DEFAULT_HEARTBEAT_INTERVAL_MILLIS,
                quorumFunction.getHeartbeatIntervalMillis());
        assertEquals(ProbabilisticQuorumConfigBuilder.DEFAULT_HEARTBEAT_PAUSE_MILLIS,
                quorumFunction.getAcceptableHeartbeatPauseMillis());
        assertEquals(ProbabilisticQuorumConfigBuilder.DEFAULT_MIN_STD_DEVIATION,
                quorumFunction.getMinStdDeviationMillis());
        assertEquals(ProbabilisticQuorumConfigBuilder.DEFAULT_PHI_THRESHOLD, quorumFunction.getSuspicionThreshold(), 0.01);
        assertEquals(ProbabilisticQuorumConfigBuilder.DEFAULT_SAMPLE_SIZE, quorumFunction.getMaxSampleSize());
    }

    @Override
    @Test
    public void testQuorumConfig_whenProbabilisticQuorum_withCustomValues() {
        String yaml = ""
                + "hazelcast:\n"
                + "  quorum:\n"
                + "    myQuorum:\n"
                + "      enabled: true\n"
                + "      quorum-size: 3\n"
                + "      probabilistic-quorum:\n"
                + "        acceptable-heartbeat-pause-millis: 37400\n"
                + "        suspicion-threshold: 3.14592\n"
                + "        max-sample-size: 42\n"
                + "        min-std-deviation-millis: 1234\n"
                + "        heartbeat-interval-millis: 4321";

        Config config = buildConfig(yaml);
        QuorumConfig quorumConfig = config.getQuorumConfig("myQuorum");
        assertInstanceOf(ProbabilisticQuorumFunction.class, quorumConfig.getQuorumFunctionImplementation());
        ProbabilisticQuorumFunction quorumFunction = (ProbabilisticQuorumFunction) quorumConfig.getQuorumFunctionImplementation();
        assertEquals(4321, quorumFunction.getHeartbeatIntervalMillis());
        assertEquals(37400, quorumFunction.getAcceptableHeartbeatPauseMillis());
        assertEquals(1234, quorumFunction.getMinStdDeviationMillis());
        assertEquals(3.14592d, quorumFunction.getSuspicionThreshold(), 0.001d);
        assertEquals(42, quorumFunction.getMaxSampleSize());
    }

    @Override
    @Test
    public void testCacheConfig() {
        // TODO do we really need to keep the 'class-name' keys?
        String yaml = ""
                + "hazelcast:\n"
                + "  cache:\n"
                + "    foobar:\n"
                + "      quorum-ref: customQuorumRule\n"
                + "      key-type:\n"
                + "        class-name: java.lang.Object\n"
                + "      value-type:\n"
                + "        class-name: java.lang.Object\n"
                + "      statistics-enabled: false\n"
                + "      management-enabled: false\n"
                + "      read-through: true\n"
                + "      write-through: true\n"
                + "      cache-loader-factory:\n"
                + "        class-name: com.example.cache.MyCacheLoaderFactory\n"
                + "      cache-writer-factory:\n"
                + "        class-name: com.example.cache.MyCacheWriterFactory\n"
                + "      expiry-policy-factory:\n"
                + "        class-name: com.example.cache.MyExpirePolicyFactory\n"
                + "      in-memory-format: BINARY\n"
                + "      backup-count: 1\n"
                + "      async-backup-count: 0\n"
                + "      eviction:\n"
                + "        size: 1000\n"
                + "        max-size-policy: ENTRY_COUNT\n"
                + "        eviction-policy: LFU\n"
                + "      merge-policy: com.hazelcast.cache.merge.LatestAccessCacheMergePolicy\n"
                + "      disable-per-entry-invalidation-events: true\n"
                + "      hot-restart:\n"
                + "        enabled: false\n"
                + "        fsync: false\n"
                + "      partition-lost-listeners:\n"
                + "        - com.your-package.YourPartitionLostListener\n"
                + "      cache-entry-listeners:\n"
                + "        cache-entry-listener:\n"
                + "          old-value-required: false\n"
                + "          synchronous: false\n"
                + "          cache-entry-listener-factory:\n"
                + "            class-name: com.example.cache.MyEntryListenerFactory\n"
                + "          cache-entry-event-filter-factory:\n"
                + "            class-name: com.example.cache.MyEntryEventFilterFactory\n";

        Config config = buildConfig(yaml);
        CacheSimpleConfig cacheConfig = config.getCacheConfig("foobar");

        assertFalse(config.getCacheConfigs().isEmpty());
        assertEquals("customQuorumRule", cacheConfig.getQuorumName());
        assertEquals("java.lang.Object", cacheConfig.getKeyType());
        assertEquals("java.lang.Object", cacheConfig.getValueType());
        assertFalse(cacheConfig.isStatisticsEnabled());
        assertFalse(cacheConfig.isManagementEnabled());
        assertTrue(cacheConfig.isReadThrough());
        assertTrue(cacheConfig.isWriteThrough());
        assertEquals("com.example.cache.MyCacheLoaderFactory", cacheConfig.getCacheLoaderFactory());
        assertEquals("com.example.cache.MyCacheWriterFactory", cacheConfig.getCacheWriterFactory());
        assertEquals("com.example.cache.MyExpirePolicyFactory", cacheConfig.getExpiryPolicyFactoryConfig().getClassName());
        assertEquals(InMemoryFormat.BINARY, cacheConfig.getInMemoryFormat());
        assertEquals(1, cacheConfig.getBackupCount());
        assertEquals(0, cacheConfig.getAsyncBackupCount());
        assertEquals(1000, cacheConfig.getEvictionConfig().getSize());
        assertEquals(EvictionConfig.MaxSizePolicy.ENTRY_COUNT, cacheConfig.getEvictionConfig().getMaximumSizePolicy());
        assertEquals(EvictionPolicy.LFU, cacheConfig.getEvictionConfig().getEvictionPolicy());
        assertEquals("com.hazelcast.cache.merge.LatestAccessCacheMergePolicy", cacheConfig.getMergePolicy());
        assertTrue(cacheConfig.isDisablePerEntryInvalidationEvents());
        assertFalse(cacheConfig.getHotRestartConfig().isEnabled());
        assertFalse(cacheConfig.getHotRestartConfig().isFsync());
        assertEquals(1, cacheConfig.getPartitionLostListenerConfigs().size());
        assertEquals("com.your-package.YourPartitionLostListener",
                cacheConfig.getPartitionLostListenerConfigs().get(0).getClassName());
        assertEquals(1, cacheConfig.getCacheEntryListeners().size());
        assertEquals("com.example.cache.MyEntryListenerFactory",
                cacheConfig.getCacheEntryListeners().get(0).getCacheEntryListenerFactory());
        assertEquals("com.example.cache.MyEntryEventFilterFactory",
                cacheConfig.getCacheEntryListeners().get(0).getCacheEntryEventFilterFactory());
    }

    @Override
    @Test
    public void testExecutorConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  executor-service:\n"
                + "    foobar:\n"
                + "      pool-size: 2\n"
                + "      quorum-ref: customQuorumRule\n"
                + "      statistics-enabled: true\n"
                + "      queue-capacity: 0\n";

        Config config = buildConfig(yaml);
        ExecutorConfig executorConfig = config.getExecutorConfig("foobar");

        assertFalse(config.getExecutorConfigs().isEmpty());
        assertEquals(2, executorConfig.getPoolSize());
        assertEquals("customQuorumRule", executorConfig.getQuorumName());
        assertTrue(executorConfig.isStatisticsEnabled());
        assertEquals(0, executorConfig.getQueueCapacity());
    }

    @Override
    @Test
    public void testDurableExecutorConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  durable-executor-service:\n"
                + "    foobar:\n"
                + "      pool-size: 2\n"
                + "      durability: 3\n"
                + "      capacity: 4\n"
                + "      quorum-ref: customQuorumRule\n";

        Config config = buildConfig(yaml);
        DurableExecutorConfig durableExecutorConfig = config.getDurableExecutorConfig("foobar");

        assertFalse(config.getDurableExecutorConfigs().isEmpty());
        assertEquals(2, durableExecutorConfig.getPoolSize());
        assertEquals(3, durableExecutorConfig.getDurability());
        assertEquals(4, durableExecutorConfig.getCapacity());
        assertEquals("customQuorumRule", durableExecutorConfig.getQuorumName());
    }

    @Override
    @Test
    public void testScheduledExecutorConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  scheduled-executor-service:\n"
                + "    foobar:\n"
                + "      durability: 4\n"
                + "      pool-size: 5\n"
                + "      capacity: 2\n"
                + "      quorum-ref: customQuorumRule\n"
                + "      merge-policy:\n"
                + "        batch-size: 99\n"
                + "        class-name: PutIfAbsent";

        Config config = buildConfig(yaml);
        ScheduledExecutorConfig scheduledExecutorConfig = config.getScheduledExecutorConfig("foobar");

        assertFalse(config.getScheduledExecutorConfigs().isEmpty());
        assertEquals(4, scheduledExecutorConfig.getDurability());
        assertEquals(5, scheduledExecutorConfig.getPoolSize());
        assertEquals(2, scheduledExecutorConfig.getCapacity());
        assertEquals("customQuorumRule", scheduledExecutorConfig.getQuorumName());
        assertEquals(99, scheduledExecutorConfig.getMergePolicyConfig().getBatchSize());
        assertEquals("PutIfAbsent", scheduledExecutorConfig.getMergePolicyConfig().getPolicy());
    }

    @Override
    @Test
    public void testCardinalityEstimatorConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  cardinality-estimator:\n"
                + "    foobar:\n"
                + "      backup-count: 2\n"
                + "      async-backup-count: 3\n"
                + "      quorum-ref: customQuorumRule\n"
                + "      merge-policy:\n"
                + "        class-name: com.hazelcast.spi.merge.HyperLogLogMergePolicy";

        Config config = buildConfig(yaml);
        CardinalityEstimatorConfig cardinalityEstimatorConfig = config.getCardinalityEstimatorConfig("foobar");

        assertFalse(config.getCardinalityEstimatorConfigs().isEmpty());
        assertEquals(2, cardinalityEstimatorConfig.getBackupCount());
        assertEquals(3, cardinalityEstimatorConfig.getAsyncBackupCount());
        assertEquals("com.hazelcast.spi.merge.HyperLogLogMergePolicy",
                cardinalityEstimatorConfig.getMergePolicyConfig().getPolicy());
        assertEquals("customQuorumRule", cardinalityEstimatorConfig.getQuorumName());
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testCardinalityEstimatorConfigWithInvalidMergePolicy() {
        String yaml = ""
                + "hazelcast:\n"
                + "  cardinality-estimator:\n"
                + "    foobar:\n"
                + "      backup-count: 2\n"
                + "      async-backup-count: 3\n"
                + "      quorum-ref: customQuorumRule\n"
                + "      merge-policy:\n"
                + "        class-name: CustomMergePolicy";

        buildConfig(yaml);
        fail();
    }

    @Override
    @Test
    public void testPNCounterConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  pn-counter:\n"
                + "    pn-counter-1:\n"
                + "      replica-count: 100\n"
                + "      quorum-ref: quorumRuleWithThreeMembers\n"
                + "      statistics-enabled: false\n";

        Config config = buildConfig(yaml);
        PNCounterConfig pnCounterConfig = config.getPNCounterConfig("pn-counter-1");

        assertFalse(config.getPNCounterConfigs().isEmpty());
        assertEquals(100, pnCounterConfig.getReplicaCount());
        assertEquals("quorumRuleWithThreeMembers", pnCounterConfig.getQuorumName());
        assertFalse(pnCounterConfig.isStatisticsEnabled());
    }

    @Override
    @Test
    public void testMultiMapConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  multimap:\n"
                + "    myMultiMap:\n"
                + "      backup-count: 2\n"
                + "      async-backup-count: 3\n"
                + "      binary: false\n"
                + "      value-collection-type: SET\n"
                + "      quorum-ref: customQuorumRule\n"
                + "      entry-listeners:\n"
                + "        - class-name: com.hazelcast.examples.EntryListener\n"
                + "          include-value: true\n"
                + "          local: true\n"
                + "      merge-policy:\n"
                + "        batch-size: 23\n"
                + "        class-name: CustomMergePolicy";

        Config config = buildConfig(yaml);
        assertFalse(config.getMultiMapConfigs().isEmpty());

        MultiMapConfig multiMapConfig = config.getMultiMapConfig("myMultiMap");
        assertEquals(2, multiMapConfig.getBackupCount());
        assertEquals(3, multiMapConfig.getAsyncBackupCount());
        assertFalse(multiMapConfig.isBinary());
        assertEquals(MultiMapConfig.ValueCollectionType.SET, multiMapConfig.getValueCollectionType());
        assertEquals(1, multiMapConfig.getEntryListenerConfigs().size());
        assertEquals("com.hazelcast.examples.EntryListener", multiMapConfig.getEntryListenerConfigs().get(0).getClassName());
        assertTrue(multiMapConfig.getEntryListenerConfigs().get(0).isIncludeValue());
        assertTrue(multiMapConfig.getEntryListenerConfigs().get(0).isLocal());

        MergePolicyConfig mergePolicyConfig = multiMapConfig.getMergePolicyConfig();
        assertEquals("CustomMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals("customQuorumRule", multiMapConfig.getQuorumName());
        assertEquals(23, mergePolicyConfig.getBatchSize());
    }

    @Override
    @Test
    public void testReplicatedMapConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  replicatedmap:\n"
                + "    foobar:\n"
                + "      in-memory-format: BINARY\n"
                + "      async-fillup: false\n"
                + "      statistics-enabled: false\n"
                + "      quorum-ref: CustomQuorumRule\n"
                + "      merge-policy:\n"
                + "        batch-size: 2342\n"
                + "        class-name: CustomMergePolicy\n";

        Config config = buildConfig(yaml);
        ReplicatedMapConfig replicatedMapConfig = config.getReplicatedMapConfig("foobar");

        assertFalse(config.getReplicatedMapConfigs().isEmpty());
        assertEquals(InMemoryFormat.BINARY, replicatedMapConfig.getInMemoryFormat());
        assertFalse(replicatedMapConfig.isAsyncFillup());
        assertFalse(replicatedMapConfig.isStatisticsEnabled());
        assertEquals("CustomQuorumRule", replicatedMapConfig.getQuorumName());

        MergePolicyConfig mergePolicyConfig = replicatedMapConfig.getMergePolicyConfig();
        assertEquals("CustomMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(2342, mergePolicyConfig.getBatchSize());
    }

    @Override
    @Test
    public void testListConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  list:\n"
                + "    foobar:\n"
                + "      quorum-ref: customQuorumRule\n"
                + "      statistics-enabled: false\n"
                + "      max-size: 42\n"
                + "      backup-count: 2\n"
                + "      async-backup-count: 1\n"
                + "      merge-policy:\n"
                + "        batch-size: 100\n"
                + "        class-name: SplitBrainMergePolicy\n"
                + "      item-listeners:\n"
                + "         - include-value: true\n"
                + "           class-name: com.hazelcast.examples.ItemListener\n";

        Config config = buildConfig(yaml);
        ListConfig listConfig = config.getListConfig("foobar");

        assertFalse(config.getListConfigs().isEmpty());
        assertEquals("customQuorumRule", listConfig.getQuorumName());
        assertEquals(42, listConfig.getMaxSize());
        assertEquals(2, listConfig.getBackupCount());
        assertEquals(1, listConfig.getAsyncBackupCount());
        assertEquals(1, listConfig.getItemListenerConfigs().size());
        assertEquals("com.hazelcast.examples.ItemListener", listConfig.getItemListenerConfigs().get(0).getClassName());

        MergePolicyConfig mergePolicyConfig = listConfig.getMergePolicyConfig();
        assertEquals(100, mergePolicyConfig.getBatchSize());
        assertEquals("SplitBrainMergePolicy", mergePolicyConfig.getPolicy());
    }

    @Override
    @Test
    public void testSetConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  set:\n"
                + "    foobar:\n"
                + "     quorum-ref: customQuorumRule\n"
                + "     backup-count: 2\n"
                + "     async-backup-count: 1\n"
                + "     max-size: 42\n"
                + "     merge-policy:\n"
                + "       batch-size: 42\n"
                + "       class-name: SplitBrainMergePolicy\n"
                + "     item-listeners:\n"
                + "         - include-value: true\n"
                + "           class-name: com.hazelcast.examples.ItemListener\n";

        Config config = buildConfig(yaml);
        SetConfig setConfig = config.getSetConfig("foobar");

        assertFalse(config.getSetConfigs().isEmpty());
        assertEquals("customQuorumRule", setConfig.getQuorumName());
        assertEquals(2, setConfig.getBackupCount());
        assertEquals(1, setConfig.getAsyncBackupCount());
        assertEquals(42, setConfig.getMaxSize());
        assertEquals(1, setConfig.getItemListenerConfigs().size());
        assertTrue(setConfig.getItemListenerConfigs().get(0).isIncludeValue());
        assertEquals("com.hazelcast.examples.ItemListener", setConfig.getItemListenerConfigs().get(0).getClassName());

        MergePolicyConfig mergePolicyConfig = setConfig.getMergePolicyConfig();
        assertEquals(42, mergePolicyConfig.getBatchSize());
        assertEquals("SplitBrainMergePolicy", mergePolicyConfig.getPolicy());
    }

    @Override
    @Test
    public void testMapConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    foobar:\n"
                + "      quorum-ref: customQuorumRule\n"
                + "      in-memory-format: BINARY\n"
                + "      statistics-enabled: true\n"
                + "      optimize-queries: false\n"
                + "      cache-deserialized-values: INDEX-ONLY\n"
                + "      backup-count: 2\n"
                + "      async-backup-count: 1\n"
                + "      time-to-live-seconds: 42\n"
                + "      max-idle-seconds: 42\n"
                + "      eviction-policy: RANDOM\n"
                + "      max-size:\n"
                + "        policy: PER_NODE\n"
                + "        max-size: 42\n"
                + "      eviction-percentage: 25\n"
                + "      min-eviction-check-millis: 256\n"
                + "      read-backup-data: true\n"
                + "      hot-restart:\n"
                + "        enabled: false\n"
                + "        fsync: false\n"
                + "      map-store:\n"
                + "        enabled: true \n"
                + "        initial-mode: LAZY\n"
                + "        class-name: com.hazelcast.examples.DummyStore\n"
                + "        write-delay-seconds: 42\n"
                + "        write-batch-size: 42\n"
                + "        write-coalescing: true\n"
                + "        properties:\n"
                + "           jdbc_url: my.jdbc.com\n"
                + "      near-cache:\n"
                + "        max-size: 5000\n"
                + "        time-to-live-seconds: 42\n"
                + "        max-idle-seconds: 42\n"
                + "        eviction-policy: LRU\n"
                + "        invalidate-on-change: true\n"
                + "        in-memory-format: BINARY\n"
                + "        cache-local-entries: false\n"
                + "        eviction:\n"
                + "          size: 1000\n"
                + "          max-size-policy: ENTRY_COUNT\n"
                + "          eviction-policy: LFU\n"
                + "      wan-replication-ref:\n"
                + "        my-wan-cluster-batch:\n"
                + "          merge-policy: com.hazelcast.map.merge.PassThroughMergePolicy\n"
                + "          filters:\n"
                + "            - com.example.SampleFilter\n"
                + "          republishing-enabled: false\n"
                + "      indexes:\n"
                + "        age:\n"
                + "          ordered: true\n"
                + "      attributes:\n"
                + "        currency:\n"
                + "          extractor: com.bank.CurrencyExtractor\n"
                + "      partition-lost-listeners:\n"
                + "         - com.your-package.YourPartitionLostListener\n"
                + "      entry-listeners:\n"
                + "         - class-name: com.your-package.MyEntryListener\n"
                + "           include-value: false\n"
                + "           local: false\n";

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("foobar");

        assertFalse(config.getMapConfigs().isEmpty());
        assertEquals("customQuorumRule", mapConfig.getQuorumName());
        assertEquals(InMemoryFormat.BINARY, mapConfig.getInMemoryFormat());
        assertTrue(mapConfig.isStatisticsEnabled());
        assertFalse(mapConfig.isOptimizeQueries());
        assertEquals(CacheDeserializedValues.INDEX_ONLY, mapConfig.getCacheDeserializedValues());
        assertEquals(2, mapConfig.getBackupCount());
        assertEquals(1, mapConfig.getAsyncBackupCount());
        assertEquals(1, mapConfig.getAsyncBackupCount());
        assertEquals(42, mapConfig.getTimeToLiveSeconds());
        assertEquals(42, mapConfig.getMaxIdleSeconds());
        assertEquals(EvictionPolicy.RANDOM, mapConfig.getEvictionPolicy());
        assertEquals(MaxSizeConfig.MaxSizePolicy.PER_NODE, mapConfig.getMaxSizeConfig().getMaxSizePolicy());
        assertEquals(42, mapConfig.getMaxSizeConfig().getSize());
        assertEquals(25, mapConfig.getEvictionPercentage());
        assertEquals(256, mapConfig.getMinEvictionCheckMillis());
        assertTrue(mapConfig.isReadBackupData());
        assertEquals(1, mapConfig.getMapIndexConfigs().size());
        assertEquals("age", mapConfig.getMapIndexConfigs().get(0).getAttribute());
        assertTrue(mapConfig.getMapIndexConfigs().get(0).isOrdered());
        assertEquals(1, mapConfig.getMapAttributeConfigs().size());
        assertEquals("com.bank.CurrencyExtractor", mapConfig.getMapAttributeConfigs().get(0).getExtractor());
        assertEquals("currency", mapConfig.getMapAttributeConfigs().get(0).getName());
        assertEquals(1, mapConfig.getPartitionLostListenerConfigs().size());
        assertEquals("com.your-package.YourPartitionLostListener",
                mapConfig.getPartitionLostListenerConfigs().get(0).getClassName());
        assertEquals(1, mapConfig.getEntryListenerConfigs().size());
        assertFalse(mapConfig.getEntryListenerConfigs().get(0).isIncludeValue());
        assertFalse(mapConfig.getEntryListenerConfigs().get(0).isLocal());
        assertEquals("com.your-package.MyEntryListener", mapConfig.getEntryListenerConfigs().get(0).getClassName());
        assertFalse(mapConfig.getHotRestartConfig().isEnabled());
        assertFalse(mapConfig.getHotRestartConfig().isFsync());

        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        assertNotNull(mapStoreConfig);
        assertTrue(mapStoreConfig.isEnabled());
        assertEquals(MapStoreConfig.InitialLoadMode.LAZY, mapStoreConfig.getInitialLoadMode());
        assertEquals(42, mapStoreConfig.getWriteDelaySeconds());
        assertEquals(42, mapStoreConfig.getWriteBatchSize());
        assertTrue(mapStoreConfig.isWriteCoalescing());
        assertEquals("com.hazelcast.examples.DummyStore", mapStoreConfig.getClassName());
        assertEquals(1, mapStoreConfig.getProperties().size());
        assertEquals("my.jdbc.com", mapStoreConfig.getProperties().getProperty("jdbc_url"));

        NearCacheConfig nearCacheConfig = mapConfig.getNearCacheConfig();
        assertNotNull(nearCacheConfig);
        assertEquals(5000, nearCacheConfig.getMaxSize());
        assertEquals(42, nearCacheConfig.getMaxIdleSeconds());
        assertEquals(42, nearCacheConfig.getTimeToLiveSeconds());
        assertEquals(InMemoryFormat.BINARY, nearCacheConfig.getInMemoryFormat());
        assertFalse(nearCacheConfig.isCacheLocalEntries());
        assertTrue(nearCacheConfig.isInvalidateOnChange());
        assertEquals(1000, nearCacheConfig.getEvictionConfig().getSize());
        assertEquals(EvictionPolicy.LFU, nearCacheConfig.getEvictionConfig().getEvictionPolicy());
        assertEquals(EvictionConfig.MaxSizePolicy.ENTRY_COUNT, nearCacheConfig.getEvictionConfig().getMaximumSizePolicy());

        WanReplicationRef wanReplicationRef = mapConfig.getWanReplicationRef();
        assertNotNull(wanReplicationRef);
        assertFalse(wanReplicationRef.isRepublishingEnabled());
        assertEquals("com.hazelcast.map.merge.PassThroughMergePolicy", wanReplicationRef.getMergePolicy());
        assertEquals(1, wanReplicationRef.getFilters().size());
        assertEquals("com.example.SampleFilter".toLowerCase(), wanReplicationRef.getFilters().get(0).toLowerCase());
    }

    @Override
    @Test
    public void testIndexesConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    people:\n"
                + "      indexes:\n"
                + "        name:\n"
                + "          ordered: false\n"
                + "        age:\n"
                + "          ordered: true\n";

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("people");

        assertFalse(mapConfig.getMapIndexConfigs().isEmpty());
        assertIndexEqual("name", false, mapConfig.getMapIndexConfigs().get(0));
        assertIndexEqual("age", true, mapConfig.getMapIndexConfigs().get(1));
    }

    @Override
    @Test
    public void testAttributeConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    people:\n"
                + "      attributes:\n"
                + "        power:\n"
                + "          extractor: com.car.PowerExtractor\n"
                + "        weight:\n"
                + "          extractor: com.car.WeightExtractor\n";

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("people");

        assertFalse(mapConfig.getMapAttributeConfigs().isEmpty());
        assertAttributeEqual("power", "com.car.PowerExtractor", mapConfig.getMapAttributeConfigs().get(0));
        assertAttributeEqual("weight", "com.car.WeightExtractor", mapConfig.getMapAttributeConfigs().get(1));
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void testAttributeConfig_noName_emptyTag() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    people:\n"
                + "      attributes:\n"
                + "        - extractor: com.car.WeightExtractor\n";

        buildConfig(yaml);
    }

    private static void assertAttributeEqual(String expectedName, String expectedExtractor, MapAttributeConfig attributeConfig) {
        assertEquals(expectedName, attributeConfig.getName());
        assertEquals(expectedExtractor, attributeConfig.getExtractor());
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void testAttributeConfig_noName_singleTag() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "   people:\n"
                + "     attributes:\n"
                + "       - extractor: com.car.WeightExtractor\n";
        buildConfig(yaml);
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void testAttributeConfig_noExtractor() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    people:\n"
                + "      attributes:\n"
                + "        weight: {}\n";
        buildConfig(yaml);
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void testAttributeConfig_emptyExtractor() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    people:\n"
                + "      attributes:\n"
                + "        weight:\n"
                + "          extractor: \"\"\n";
        buildConfig(yaml);
    }

    @Override
    @Test
    public void testQueryCacheFullConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    test:\n"
                + "      query-caches:\n"
                + "        cache-name:\n"
                + "          entry-listeners:\n"
                + "            - class-name: com.hazelcast.examples.EntryListener\n"
                + "              include-value: true\n"
                + "              local: false\n"
                + "          include-value: true\n"
                + "          batch-size: 1\n"
                + "          buffer-size: 16\n"
                + "          delay-seconds: 0\n"
                + "          in-memory-format: BINARY\n"
                + "          coalesce: false\n"
                + "          populate: true\n"
                + "          indexes:\n"
                + "            name:\n"
                + "              ordered: false\n"
                + "          predicate:\n"
                + "            class-name: com.hazelcast.examples.SimplePredicate\n"
                + "          eviction:\n"
                + "            eviction-policy: LRU\n"
                + "            max-size-policy: ENTRY_COUNT\n"
                + "            size: 133\n";

        Config config = buildConfig(yaml);
        QueryCacheConfig queryCacheConfig = config.getMapConfig("test").getQueryCacheConfigs().get(0);
        EntryListenerConfig entryListenerConfig = queryCacheConfig.getEntryListenerConfigs().get(0);

        assertEquals("cache-name", queryCacheConfig.getName());
        assertTrue(entryListenerConfig.isIncludeValue());
        assertFalse(entryListenerConfig.isLocal());
        assertEquals("com.hazelcast.examples.EntryListener", entryListenerConfig.getClassName());
        assertTrue(queryCacheConfig.isIncludeValue());
        assertEquals(1, queryCacheConfig.getBatchSize());
        assertEquals(16, queryCacheConfig.getBufferSize());
        assertEquals(0, queryCacheConfig.getDelaySeconds());
        assertEquals(InMemoryFormat.BINARY, queryCacheConfig.getInMemoryFormat());
        assertFalse(queryCacheConfig.isCoalesce());
        assertTrue(queryCacheConfig.isPopulate());
        assertIndexesEqual(queryCacheConfig);
        assertEquals("com.hazelcast.examples.SimplePredicate", queryCacheConfig.getPredicateConfig().getClassName());
        assertEquals(LRU, queryCacheConfig.getEvictionConfig().getEvictionPolicy());
        assertEquals(ENTRY_COUNT, queryCacheConfig.getEvictionConfig().getMaximumSizePolicy());
        assertEquals(133, queryCacheConfig.getEvictionConfig().getSize());
    }

    private void assertIndexesEqual(QueryCacheConfig queryCacheConfig) {
        for (MapIndexConfig mapIndexConfig : queryCacheConfig.getIndexConfigs()) {
            assertEquals("name", mapIndexConfig.getAttribute());
            assertFalse(mapIndexConfig.isOrdered());
        }
    }

    @Override
    @Test
    public void testMapQueryCachePredicate() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    test:\n"
                + "      query-caches:\n"
                + "        cache-class-name:\n"
                + "          predicate:\n"
                + "            class-name: com.hazelcast.examples.SimplePredicate\n"
                + "        cache-sql:\n"
                + "          predicate:\n"
                + "            sql: \"%age=40\"\n";

        Config config = buildConfig(yaml);
        QueryCacheConfig queryCacheClassNameConfig = config.getMapConfig("test").getQueryCacheConfigs().get(0);
        assertEquals("com.hazelcast.examples.SimplePredicate", queryCacheClassNameConfig.getPredicateConfig().getClassName());

        QueryCacheConfig queryCacheSqlConfig = config.getMapConfig("test").getQueryCacheConfigs().get(1);
        assertEquals("%age=40", queryCacheSqlConfig.getPredicateConfig().getSql());
    }

    @Override
    @Test
    public void testLiteMemberConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  lite-member:\n"
                + "    enabled: true\n";

        Config config = buildConfig(yaml);

        assertTrue(config.isLiteMember());
    }

    @Override
    @Test
    public void testNonLiteMemberConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  lite-member:\n"
                + "    enabled: false\n";

        Config config = buildConfig(yaml);

        assertFalse(config.isLiteMember());
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    @Ignore("Schema validation is supposed to fail with missing mandatory field: enabled")
    public void testNonLiteMemberConfigWithoutEnabledField() {
        String yaml = ""
                + "hazelcast:\n"
                + "  lite-member: {}\n";

        buildConfig(yaml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    @Ignore("Schema validation is supposed to fail with invalid boolean in enabled")
    public void testInvalidLiteMemberConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  lite-member:\n"
                + "    enabled: dummytext\n";

        buildConfig(yaml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testDuplicateLiteMemberConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  lite-member:\n"
                + "    enabled: true\n"
                + "  lite-member:\n"
                + "    enabled: true\n";

        buildConfig(yaml);
        fail();
    }

    private static void assertIndexEqual(String expectedAttribute, boolean expectedOrdered, MapIndexConfig indexConfig) {
        assertEquals(expectedAttribute, indexConfig.getAttribute());
        assertEquals(expectedOrdered, indexConfig.isOrdered());
    }

    @Override
    @Test
    public void testMapNativeMaxSizePolicy() {
        String yamlFormat = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    mymap:\n"
                + "      in-memory-format: NATIVE\n"
                + "      max-size:\n"
                + "        policy: \"{0}\"\n"
                + "        max-size: 9991\n";
        MessageFormat messageFormat = new MessageFormat(yamlFormat);

        MaxSizeConfig.MaxSizePolicy[] maxSizePolicies = MaxSizeConfig.MaxSizePolicy.values();
        for (MaxSizeConfig.MaxSizePolicy maxSizePolicy : maxSizePolicies) {
            Object[] objects = {maxSizePolicy.toString()};
            String yaml = messageFormat.format(objects);
            Config config = buildConfig(yaml);
            MapConfig mapConfig = config.getMapConfig("mymap");
            MaxSizeConfig maxSizeConfig = mapConfig.getMaxSizeConfig();

            assertEquals(9991, maxSizeConfig.getSize());
            assertEquals(maxSizePolicy, maxSizeConfig.getMaxSizePolicy());
        }
    }

    @Override
    @Test
    public void testInstanceName() {
        String name = randomName();
        String yaml = ""
                + "hazelcast:\n"
                + " instance-name: " + name + "\n";

        Config config = buildConfig(yaml);
        assertEquals(name, config.getInstanceName());
    }

    @Override
    @Test
    public void testUserCodeDeployment() {
        String yaml = ""
                + "hazelcast:\n"
                + "  user-code-deployment:\n"
                + "    enabled: true\n"
                + "    class-cache-mode: OFF\n"
                + "    provider-mode: LOCAL_CLASSES_ONLY\n"
                + "    blacklist-prefixes: com.blacklisted,com.other.blacklisted\n"
                + "    whitelist-prefixes: com.whitelisted,com.other.whitelisted\n"
                + "    provider-filter: HAS_ATTRIBUTE:foo\n";

        Config config = new InMemoryYamlConfig(yaml);
        UserCodeDeploymentConfig dcConfig = config.getUserCodeDeploymentConfig();
        assertTrue(dcConfig.isEnabled());
        assertEquals(UserCodeDeploymentConfig.ClassCacheMode.OFF, dcConfig.getClassCacheMode());
        assertEquals(UserCodeDeploymentConfig.ProviderMode.LOCAL_CLASSES_ONLY, dcConfig.getProviderMode());
        assertEquals("com.blacklisted,com.other.blacklisted", dcConfig.getBlacklistedPrefixes());
        assertEquals("com.whitelisted,com.other.whitelisted", dcConfig.getWhitelistedPrefixes());
        assertEquals("HAS_ATTRIBUTE:foo", dcConfig.getProviderFilter());
    }

    @Override
    @Test
    public void testCRDTReplicationConfig() {
        final String yaml = ""
                + "hazelcast:\n"
                + "  crdt-replication:\n"
                + "    max-concurrent-replication-targets: 10\n"
                + "    replication-period-millis: 2000\n";

        final Config config = new InMemoryYamlConfig(yaml);
        final CRDTReplicationConfig replicationConfig = config.getCRDTReplicationConfig();
        assertEquals(10, replicationConfig.getMaxConcurrentReplicationTargets());
        assertEquals(2000, replicationConfig.getReplicationPeriodMillis());
    }

    @Override
    @Test
    public void testGlobalSerializer() {
        String name = randomName();
        String val = "true";
        String yaml = ""
                + "hazelcast:\n"
                + "  serialization:\n"
                + "    global-serializer:\n"
                + "      class-name: " + name + "\n"
                + "      override-java-serialization: " + val + "\n";

        Config config = new InMemoryYamlConfig(yaml);
        GlobalSerializerConfig globalSerializerConfig = config.getSerializationConfig().getGlobalSerializerConfig();
        assertEquals(name, globalSerializerConfig.getClassName());
        assertTrue(globalSerializerConfig.isOverrideJavaSerialization());
    }

    @Override
    @Test
    public void testJavaSerializationFilter() {
        String yaml = ""
                + "hazelcast:\n"
                + "  serialization:\n"
                + "    java-serialization-filter:\n"
                + "      defaults-disabled: true\n"
                + "      whitelist:\n"
                + "        class:\n"
                + "          - java.lang.String\n"
                + "          - example.Foo\n"
                + "        package:\n"
                + "          - com.acme.app\n"
                + "          - com.acme.app.subpkg\n"
                + "        prefix:\n"
                + "          - java\n"
                + "          - com.hazelcast.\n"
                + "          - \"[\"\n"
                + "      blacklist:\n"
                + "        class:\n"
                + "          - com.acme.app.BeanComparator\n";

        Config config = new InMemoryYamlConfig(yaml);
        JavaSerializationFilterConfig javaSerializationFilterConfig
                = config.getSerializationConfig().getJavaSerializationFilterConfig();
        assertNotNull(javaSerializationFilterConfig);
        ClassFilter blackList = javaSerializationFilterConfig.getBlacklist();
        assertNotNull(blackList);
        ClassFilter whiteList = javaSerializationFilterConfig.getWhitelist();
        assertNotNull(whiteList);
        assertTrue(whiteList.getClasses().contains("java.lang.String"));
        assertTrue(whiteList.getClasses().contains("example.Foo"));
        assertTrue(whiteList.getPackages().contains("com.acme.app"));
        assertTrue(whiteList.getPackages().contains("com.acme.app.subpkg"));
        assertTrue(whiteList.getPrefixes().contains("java"));
        assertTrue(whiteList.getPrefixes().contains("["));
        assertTrue(blackList.getClasses().contains("com.acme.app.BeanComparator"));
    }

    @Override
    @Test
    public void testHotRestart() {
        String dir = "/mnt/hot-restart-root/";
        String backupDir = "/mnt/hot-restart-backup/";
        int parallelism = 3;
        int validationTimeout = 13131;
        int dataLoadTimeout = 45454;
        HotRestartClusterDataRecoveryPolicy policy = HotRestartClusterDataRecoveryPolicy.PARTIAL_RECOVERY_MOST_RECENT;
        String yaml = ""
                + "hazelcast:\n"
                + "  hot-restart-persistence:\n"
                + "    enabled: true\n"
                + "    base-dir: " + dir + "\n"
                + "    backup-dir: " + backupDir + "\n"
                + "    parallelism: " + parallelism + "\n"
                + "    validation-timeout-seconds: " + validationTimeout + "\n"
                + "    data-load-timeout-seconds: " + dataLoadTimeout + "\n"
                + "    cluster-data-recovery-policy: " + policy + "\n";

        Config config = new InMemoryYamlConfig(yaml);
        HotRestartPersistenceConfig hotRestartPersistenceConfig = config.getHotRestartPersistenceConfig();

        assertTrue(hotRestartPersistenceConfig.isEnabled());
        assertEquals(new File(dir).getAbsolutePath(), hotRestartPersistenceConfig.getBaseDir().getAbsolutePath());
        assertEquals(new File(backupDir).getAbsolutePath(), hotRestartPersistenceConfig.getBackupDir().getAbsolutePath());
        assertEquals(parallelism, hotRestartPersistenceConfig.getParallelism());
        assertEquals(validationTimeout, hotRestartPersistenceConfig.getValidationTimeoutSeconds());
        assertEquals(dataLoadTimeout, hotRestartPersistenceConfig.getDataLoadTimeoutSeconds());
        assertEquals(policy, hotRestartPersistenceConfig.getClusterDataRecoveryPolicy());
    }

    @Override
    @Test
    public void testMapEvictionPolicyClassName() {
        String mapEvictionPolicyClassName = "com.hazelcast.map.eviction.LRUEvictionPolicy";
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    test:\n"
                + "      map-eviction-policy-class-name: " + mapEvictionPolicyClassName;

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("test");

        assertEquals(mapEvictionPolicyClassName, mapConfig.getMapEvictionPolicy().getClass().getName());
    }

    @Override
    @Test
    public void testMapEvictionPolicyIsSelected_whenEvictionPolicySet() {
        String mapEvictionPolicyClassName = "com.hazelcast.map.eviction.LRUEvictionPolicy";
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    test:\n"
                + "      map-eviction-policy-class-name: " + mapEvictionPolicyClassName + "\n"
                + "      eviction-policy: LFU";

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("test");

        assertEquals(mapEvictionPolicyClassName, mapConfig.getMapEvictionPolicy().getClass().getName());
    }

    @Override
    @Test
    public void testCachePermission() {
        String yaml = ""
                + "hazelcast:\n"
                + "  security:\n"
                + "    enabled: true\n"
                + "    client-permissions:\n"
                + "      cache:\n"
                + "        - name: /hz/cachemanager1/cache1\n"
                + "          principal: dev\n"
                + "          actions:\n"
                + "            - create\n"
                + "            - destroy\n"
                + "            - add\n"
                + "            - remove\n";

        Config config = buildConfig(yaml);
        PermissionConfig expected = new PermissionConfig(CACHE, "/hz/cachemanager1/cache1", "dev");
        expected.addAction("create").addAction("destroy").addAction("add").addAction("remove");
        assertPermissionConfig(expected, config);
    }

    @Override
    @Test
    public void testOnJoinPermissionOperation() {
        for (OnJoinPermissionOperationName onJoinOp : OnJoinPermissionOperationName.values()) {
            String yaml = ""
                    + "hazelcast:\n"
                    + "  security:\n"
                    + "    client-permissions:\n"
                    + "      on-join-operation: " + onJoinOp.name();
            Config config = buildConfig(yaml);
            assertSame(onJoinOp, config.getSecurityConfig().getOnJoinPermissionOperation());
        }
    }

    @Override
    @Test
    public void testConfigPermission() {
        String yaml = ""
                + "hazelcast:\n"
                + "  security:\n"
                + "    enabled: true\n"
                + "    client-permissions:\n"
                + "      config:\n"
                + "        principal: dev\n"
                + "        endpoints:\n"
                + "          - 127.0.0.1";

        Config config = buildConfig(yaml);
        PermissionConfig expected = new PermissionConfig(CONFIG, "*", "dev");
        expected.getEndpoints().add("127.0.0.1");
        assertPermissionConfig(expected, config);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testCacheConfig_withNativeInMemoryFormat_failsFastInOSS() {
        String yaml = ""
                + "hazelcast:\n"
                + "  cache:\n"
                + "    cache:\n"
                + "      eviction:\n"
                + "        size: 10000000\n"
                + "        max-size-policy: ENTRY_COUNT\n"
                + "        eviction-policy: LFU\n"
                + "      in-memory-format: NATIVE\n";

        buildConfig(yaml);
    }

    @Override
    @Test
    public void testAllPermissionsCovered() {
        InputStream yamlResource = YamlConfigBuilderTest.class.getClassLoader().getResourceAsStream("hazelcast-fullconfig.yaml");
        Config config = null;
        try {
            config = new YamlConfigBuilder(yamlResource).build();
        } finally {
            IOUtil.closeResource(yamlResource);
        }
        Set<PermissionConfig.PermissionType> permTypes = new HashSet<PermissionConfig.PermissionType>(Arrays
                .asList(PermissionConfig.PermissionType.values()));
        for (PermissionConfig pc : config.getSecurityConfig().getClientPermissionConfigs()) {
            permTypes.remove(pc.getType());
        }
        assertTrue("All permission types should be listed in hazelcast-fullconfig.yaml. Not found ones: " + permTypes,
                permTypes.isEmpty());
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    @Ignore("Schema validation is supposed to fail with missing mandatory field: class-name")
    public void testMemberAddressProvider_classNameIsMandatory() {
        String yaml = ""
                + "hazelcast:\n"
                + "  network:\n"
                + "    member-address-provider:\n"
                + "      enabled: true\n";

        buildConfig(yaml);
    }

    @Override
    @Test
    public void testMemberAddressProviderEnabled() {
        String yaml = ""
                + "hazelcast:\n"
                + "  network:\n"
                + "    member-address-provider:\n"
                + "      enabled: true\n"
                + "      class-name: foo.bar.Clazz\n";

        Config config = buildConfig(yaml);
        MemberAddressProviderConfig memberAddressProviderConfig = config.getNetworkConfig().getMemberAddressProviderConfig();

        assertTrue(memberAddressProviderConfig.isEnabled());
        assertEquals("foo.bar.Clazz", memberAddressProviderConfig.getClassName());
    }

    @Override
    @Test
    public void testMemberAddressProviderEnabled_withProperties() {
        String yaml = ""
                + "hazelcast:\n"
                + "  network:\n"
                + "    member-address-provider:\n"
                + "      enabled: true\n"
                + "      class-name: foo.bar.Clazz\n"
                + "      properties:\n"
                + "        propName1: propValue1\n";

        Config config = buildConfig(yaml);
        MemberAddressProviderConfig memberAddressProviderConfig = config.getNetworkConfig().getMemberAddressProviderConfig();

        Properties properties = memberAddressProviderConfig.getProperties();
        assertEquals(1, properties.size());
        assertEquals("propValue1", properties.get("propName1"));
    }

    @Override
    @Test
    public void testFailureDetector_withProperties() {
        String yaml = ""
                + "hazelcast:\n"
                + "  network:\n"
                + "    failure-detector:\n"
                + "      icmp:\n"
                + "        enabled: true\n"
                + "        timeout-milliseconds: 42\n"
                + "        fail-fast-on-startup: true\n"
                + "        interval-milliseconds: 4200\n"
                + "        max-attempts: 42\n"
                + "        parallel-mode: true\n"
                + "        ttl: 255\n";

        Config config = buildConfig(yaml);
        NetworkConfig networkConfig = config.getNetworkConfig();
        IcmpFailureDetectorConfig icmpFailureDetectorConfig = networkConfig.getIcmpFailureDetectorConfig();
        assertNotNull(icmpFailureDetectorConfig);

        assertTrue(icmpFailureDetectorConfig.isEnabled());
        assertTrue(icmpFailureDetectorConfig.isParallelMode());
        assertTrue(icmpFailureDetectorConfig.isFailFastOnStartup());
        assertEquals(42, icmpFailureDetectorConfig.getTimeoutMilliseconds());
        assertEquals(42, icmpFailureDetectorConfig.getMaxAttempts());
        assertEquals(4200, icmpFailureDetectorConfig.getIntervalMilliseconds());
    }

    @Override
    @Test
    public void testHandleMemberAttributes() {
        String yaml = ""
                + "hazelcast:\n"
                + "  member-attributes:\n"
                + "    IDENTIFIER:\n"
                + "      type: string\n"
                + "      value: ID\n";

        Config config = buildConfig(yaml);
        MemberAttributeConfig memberAttributeConfig = config.getMemberAttributeConfig();
        assertNotNull(memberAttributeConfig);
        assertEquals("ID", memberAttributeConfig.getAttribute("IDENTIFIER"));
    }

    @Override
    @Test
    public void testMemcacheProtocolEnabled() {
        String yaml = ""
                + "hazelcast:\n"
                + "  network:\n"
                + "    memcache-protocol:\n"
                + "      enabled: true\n";
        Config config = buildConfig(yaml);
        MemcacheProtocolConfig memcacheProtocolConfig = config.getNetworkConfig().getMemcacheProtocolConfig();
        assertNotNull(memcacheProtocolConfig);
        assertTrue(memcacheProtocolConfig.isEnabled());
    }

    @Override
    @Test
    public void testRestApiDefaults() {
        String yaml = ""
                + "hazelcast:\n"
                + "  network:\n"
                + "    rest-api:\n"
                + "      enabled: false";
        Config config = buildConfig(yaml);
        RestApiConfig restApiConfig = config.getNetworkConfig().getRestApiConfig();
        assertNotNull(restApiConfig);
        assertFalse(restApiConfig.isEnabled());
        for (RestEndpointGroup group : RestEndpointGroup.values()) {
            assertEquals("Unexpected status of group " + group, group.isEnabledByDefault(),
                    restApiConfig.isGroupEnabled(group));
        }
    }

    @Override
    @Test
    public void testRestApiEndpointGroups() {
        String yaml = ""
                + "hazelcast:\n"
                + "  network:\n"
                + "    rest-api:\n"
                + "      enabled: true\n"
                + "      endpoint-groups:\n"
                + "        HEALTH_CHECK:\n "
                + "          enabled: true\n"
                + "        DATA:\n"
                + "          enabled: true\n"
                + "        CLUSTER_READ:\n"
                + "          enabled: false";
        Config config = buildConfig(yaml);
        RestApiConfig restApiConfig = config.getNetworkConfig().getRestApiConfig();
        assertTrue(restApiConfig.isEnabled());
        assertTrue(restApiConfig.isGroupEnabled(RestEndpointGroup.HEALTH_CHECK));
        assertFalse(restApiConfig.isGroupEnabled(RestEndpointGroup.CLUSTER_READ));
        assertEquals(RestEndpointGroup.CLUSTER_WRITE.isEnabledByDefault(),
                restApiConfig.isGroupEnabled(RestEndpointGroup.CLUSTER_WRITE));

    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testUnknownRestApiEndpointGroup() {
        String yaml = ""
                + "hazelcast:\n"
                + "  network:\n"
                + "    rest-api:\n"
                + "      enabled: true\n"
                + "      endpoint-groups:\n"
                + "        TEST:\n"
                + "          enabled: true";
        buildConfig(yaml);
    }

    @Override
    @Test
    public void testDefaultAdvancedNetworkConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  advanced-network: {}\n";

        Config config = buildConfig(yaml);
        AdvancedNetworkConfig advancedNetworkConfig = config.getAdvancedNetworkConfig();
        JoinConfig joinConfig = advancedNetworkConfig.getJoin();
        IcmpFailureDetectorConfig fdConfig = advancedNetworkConfig.getIcmpFailureDetectorConfig();
        MemberAddressProviderConfig providerConfig = advancedNetworkConfig.getMemberAddressProviderConfig();

        assertFalse(advancedNetworkConfig.isEnabled());
        assertTrue(joinConfig.getMulticastConfig().isEnabled());
        assertNull(fdConfig);
        assertFalse(providerConfig.isEnabled());

        assertTrue(advancedNetworkConfig.getEndpointConfigs().containsKey(EndpointQualifier.MEMBER));
        assertEquals(1, advancedNetworkConfig.getEndpointConfigs().size());
    }

    @Override
    @Test
    public void testAmbiguousNetworkConfig_throwsException() {
        String yaml = ""
                + "hazelcast:\n"
                + "  advanced-network:\n"
                + "    enabled: true\n"
                + "  network:\n"
                + "    port: 9999";

        expected.expect(InvalidConfigurationException.class);
        buildConfig(yaml);
    }

    @Override
    @Test
    public void testNetworkConfigUnambiguous_whenAdvancedNetworkDisabled() {
        String yaml = ""
                + "hazelcast:\n"
                + "  advanced-network: {}\n"
                + "  network:\n"
                + "    port:\n"
                + "      port: 9999\n";

        Config config = buildConfig(yaml);
        assertFalse(config.getAdvancedNetworkConfig().isEnabled());
        assertEquals(9999, config.getNetworkConfig().getPort());
    }

    @Override
    @Test
    public void testMultipleMemberEndpointConfigs_throwsException() {
        String yaml = ""
                + "hazelcast:\n"
                + "advanced-network:\n"
                + "  member-server-socket-endpoint-config: {}\n"
                + "  member-server-socket-endpoint-config: {}";

        expected.expect(InvalidConfigurationException.class);
        buildConfig(yaml);

    }

    @Override
    protected Config buildCompleteAdvancedNetworkConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  advanced-network:\n"
                + "    enabled: true\n"
                + "    join:\n"
                + "      multicast:\n"
                + "        enabled: false\n"
                + "      tcp-ip:\n"
                + "        enabled: true\n"
                + "        required-member: 10.10.1.10\n"
                + "        member-list:\n"
                + "          - 10.10.1.11\n"
                + "          - 10.10.1.12\n"
                + "    failure-detector:\n"
                + "      icmp:\n"
                + "        enabled: true\n"
                + "        timeout-milliseconds: 42\n"
                + "        fail-fast-on-startup: true\n"
                + "        interval-milliseconds: 4200\n"
                + "        max-attempts: 42\n"
                + "        parallel-mode: true\n"
                + "        ttl: 255\n"
                + "    member-address-provider:\n"
                + "      class-name: com.hazelcast.test.Provider\n"
                + "    member-server-socket-endpoint-config:\n"
                + "      name: member-server-socket\n"
                + "      outbound-ports:\n"
                + "        ports: 33000-33100\n"
                + "      interfaces:\n"
                + "        enabled: true\n"
                + "        interfaces:\n"
                + "          - 10.10.0.1\n"
                + "      ssl:\n"
                + "        enabled: true\n"
                + "        factory-class-name: com.hazelcast.examples.MySSLContextFactory\n"
                + "        properties:\n"
                + "          foo: bar\n"
                + "      socket-interceptor:\n"
                + "        enabled: true\n"
                + "        class-name: com.hazelcast.examples.MySocketInterceptor\n"
                + "        properties:\n"
                + "          foo: baz\n"
                + "      socket-options:\n"
                + "        buffer-direct: true\n"
                + "        tcp-no-delay: true\n"
                + "        keep-alive: true\n"
                + "        connect-timeout-seconds: 33\n"
                + "        send-buffer-size-kb: 34\n"
                + "        receive-buffer-size-kb: 67\n"
                + "        linger-seconds: 11\n"
                + "      symmetric-encryption:\n"
                + "        enabled: true\n"
                + "        algorithm: Algorithm\n"
                + "        salt: thesalt\n"
                + "        password: thepassword\n"
                + "        iteration-count: 1000\n"
                + "      port:\n"
                + "        port-count: 93\n"
                + "        auto-increment: false\n"
                + "        port: 9191\n"
                + "      public-address: 10.20.10.10\n"
                + "      reuse-address: true\n"
                + "    rest-server-socket-endpoint-config:\n"
                + "      name: REST\n"
                + "      port:\n"
                + "        port: 8080\n"
                + "      endpoint-groups:\n"
                + "        WAN:\n"
                + "          enabled: true\n"
                + "        CLUSTER_READ:\n"
                + "          enabled: true\n"
                + "        CLUSTER_WRITE:\n"
                + "          enabled: false\n"
                + "        HEALTH_CHECK:\n"
                + "          enabled: true\n"
                + "    memcache-server-socket-endpoint-config:\n"
                + "      name: MEMCACHE\n"
                + "      outbound-ports:\n"
                + "        ports: 42000-42100\n"
                + "    wan-server-socket-endpoint-config:\n"
                + "      WAN_SERVER1:\n"
                + "        outbound-ports:\n"
                + "          ports: 52000-52100\n"
                + "      WAN_SERVER2:\n"
                + "        outbound-ports:\n"
                + "          ports: 53000-53100\n"
                + "    wan-endpoint-config:\n"
                + "      WAN_ENDPOINT1:\n"
                + "        outbound-ports:\n"
                + "          ports: 62000-62100\n"
                + "      WAN_ENDPOINT2:\n"
                + "        outbound-ports:\n"
                + "          ports: 63000-63100\n"
                + "    client-server-socket-endpoint-config:\n"
                + "      name: CLIENT\n"
                + "      outbound-ports:\n"
                + "        ports: 72000-72100\n";

        return buildConfig(yaml);
    }

    @Override
    public void testCPSubsystemConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  cp-subsystem:\n"
                + "    cp-member-count: 10\n"
                + "    group-size: 5\n"
                + "    session-time-to-live-seconds: 15\n"
                + "    session-heartbeat-interval-seconds: 3\n"
                + "    missing-cp-member-auto-removal-seconds: 120\n"
                + "    fail-on-indeterminate-operation-state: true\n"
                + "    raft-algorithm:\n"
                + "      leader-election-timeout-in-millis: 500\n"
                + "      leader-heartbeat-period-in-millis: 100\n"
                + "      max-missed-leader-heartbeat-count: 3\n"
                + "      append-request-max-entry-count: 25\n"
                + "      commit-index-advance-count-to-snapshot: 250\n"
                + "      uncommitted-entry-count-to-reject-new-appends: 75\n"
                + "      append-request-backoff-timeout-in-millis: 50\n"
                + "    semaphores:\n"
                + "      sem1:\n"
                + "        jdk-compatible: true\n"
                + "      sem2:\n"
                + "        jdk-compatible: false\n"
                + "    locks:\n"
                + "      lock1:\n"
                + "        lock-acquire-limit: 1\n"
                + "      lock2:\n"
                + "        lock-acquire-limit: 2\n";
        Config config = buildConfig(yaml);
        CPSubsystemConfig cpSubsystemConfig = config.getCPSubsystemConfig();
        assertEquals(10, cpSubsystemConfig.getCPMemberCount());
        assertEquals(5, cpSubsystemConfig.getGroupSize());
        assertEquals(15, cpSubsystemConfig.getSessionTimeToLiveSeconds());
        assertEquals(3, cpSubsystemConfig.getSessionHeartbeatIntervalSeconds());
        assertEquals(120, cpSubsystemConfig.getMissingCPMemberAutoRemovalSeconds());
        assertTrue(cpSubsystemConfig.isFailOnIndeterminateOperationState());
        RaftAlgorithmConfig raftAlgorithmConfig = cpSubsystemConfig.getRaftAlgorithmConfig();
        assertEquals(500, raftAlgorithmConfig.getLeaderElectionTimeoutInMillis());
        assertEquals(100, raftAlgorithmConfig.getLeaderHeartbeatPeriodInMillis());
        assertEquals(3, raftAlgorithmConfig.getMaxMissedLeaderHeartbeatCount());
        assertEquals(25, raftAlgorithmConfig.getAppendRequestMaxEntryCount());
        assertEquals(250, raftAlgorithmConfig.getCommitIndexAdvanceCountToSnapshot());
        assertEquals(75, raftAlgorithmConfig.getUncommittedEntryCountToRejectNewAppends());
        assertEquals(50, raftAlgorithmConfig.getAppendRequestBackoffTimeoutInMillis());
        CPSemaphoreConfig semaphoreConfig1 = cpSubsystemConfig.findSemaphoreConfig("sem1");
        CPSemaphoreConfig semaphoreConfig2 = cpSubsystemConfig.findSemaphoreConfig("sem2");
        assertNotNull(semaphoreConfig1);
        assertNotNull(semaphoreConfig2);
        assertTrue(semaphoreConfig1.isJDKCompatible());
        assertFalse(semaphoreConfig2.isJDKCompatible());
        FencedLockConfig lockConfig1 = cpSubsystemConfig.findLockConfig("lock1");
        FencedLockConfig lockConfig2 = cpSubsystemConfig.findLockConfig("lock2");
        assertNotNull(lockConfig1);
        assertNotNull(lockConfig2);
        assertEquals(1, lockConfig1.getLockAcquireLimit());
        assertEquals(2, lockConfig2.getLockAcquireLimit());
    }

    @Override
    public void testWhitespaceInNonSpaceStrings() {
        String yaml = ""
                + "hazelcast:\n"
                + "  quorum:\n"
                + "    enabled: true\n"
                + "    name: q\n"
                + "    quorum-type:   WRITE   \n";

        buildConfig(yaml);
    }

}
