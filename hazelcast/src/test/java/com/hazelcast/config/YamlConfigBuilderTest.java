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

package com.hazelcast.config;

import com.google.common.collect.Sets;
import com.hazelcast.config.LoginModuleConfig.LoginModuleUsage;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.config.cp.CPMapConfig;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.config.cp.FencedLockConfig;
import com.hazelcast.config.cp.RaftAlgorithmConfig;
import com.hazelcast.config.cp.SemaphoreConfig;
import com.hazelcast.config.security.AccessControlServiceConfig;
import com.hazelcast.config.security.KerberosAuthenticationConfig;
import com.hazelcast.config.security.KerberosIdentityConfig;
import com.hazelcast.config.security.LdapAuthenticationConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.config.security.SimpleAuthenticationConfig;
import com.hazelcast.config.tpc.TpcConfig;
import com.hazelcast.config.tpc.TpcSocketConfig;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.config.SchemaViolationConfigurationException;
import com.hazelcast.internal.config.YamlMemberDomConfigProcessor;
import com.hazelcast.internal.namespace.ResourceDefinition;
import com.hazelcast.internal.serialization.impl.compact.CompactTestUtil;
import com.hazelcast.jet.config.ResourceType;
import com.hazelcast.memory.Capacity;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionOn;
import com.hazelcast.splitbrainprotection.impl.ProbabilisticSplitBrainProtectionFunction;
import com.hazelcast.splitbrainprotection.impl.RecentlyActiveSplitBrainProtectionFunction;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.TopicOverloadPolicy;
import com.hazelcast.wan.WanPublisherState;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hazelcast.config.DynamicConfigurationConfig.DEFAULT_BACKUP_COUNT;
import static com.hazelcast.config.DynamicConfigurationConfig.DEFAULT_BACKUP_DIR;
import static com.hazelcast.config.EvictionPolicy.LRU;
import static com.hazelcast.config.LocalDeviceConfig.DEFAULT_BLOCK_SIZE_IN_BYTES;
import static com.hazelcast.config.LocalDeviceConfig.DEFAULT_DEVICE_BASE_DIR;
import static com.hazelcast.config.LocalDeviceConfig.DEFAULT_DEVICE_NAME;
import static com.hazelcast.config.LocalDeviceConfig.DEFAULT_READ_IO_THREAD_COUNT;
import static com.hazelcast.config.LocalDeviceConfig.DEFAULT_WRITE_IO_THREAD_COUNT;
import static com.hazelcast.config.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.config.MemoryTierConfig.DEFAULT_CAPACITY;
import static com.hazelcast.config.PermissionConfig.PermissionType.CACHE;
import static com.hazelcast.config.PermissionConfig.PermissionType.CONFIG;
import static com.hazelcast.config.PersistentMemoryMode.MOUNTED;
import static com.hazelcast.config.PersistentMemoryMode.SYSTEM_MEMORY;
import static com.hazelcast.config.WanQueueFullBehavior.DISCARD_AFTER_MUTATION;
import static com.hazelcast.config.WanQueueFullBehavior.THROW_EXCEPTION;
import static com.hazelcast.internal.util.StringUtil.lowerCaseInternal;
import static java.io.File.createTempFile;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * YAML specific implementation of the tests that should be maintained in
 * both XML and YAML configuration builder tests
 * <p>
 * <p>
 * NOTE: This test class must not define test cases, it is meant only to
 * implement test cases defined in {@link AbstractConfigBuilderTest}.
 * <p>
 * <p>
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
    public void testConfigurationURL()
            throws Exception {
        URL configURL = getClass().getClassLoader().getResource("hazelcast-default.yaml");
        Config config = new YamlConfigBuilder(configURL).build();
        assertEquals(configURL, config.getConfigurationUrl());
        assertNull(config.getConfigurationFile());
    }

    @Override
    @Test
    public void testClusterName() {
        String yaml = """
                hazelcast:
                  cluster-name: my-cluster
                """;

        Config config = buildConfig(yaml);
        assertEquals("my-cluster", config.getClusterName());
    }

    @Override
    @Test
    public void testConfigurationWithFileName()
            throws Exception {
        File file = createTempFile("foo", "bar");
        file.deleteOnExit();

        String yaml = """
                hazelcast:
                  map:
                    my-map:
                      backup-count: 1""";
        Writer writer = new PrintWriter(file, StandardCharsets.UTF_8);
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
    public void testJoinValidation() {
        String yaml = """
                hazelcast:
                  network:
                    join:
                      multicast:
                        enabled: true
                      tcp-ip:
                        enabled: true
                """;
        buildConfig(yaml);
    }

    @Override
    @Test
    public void testSecurityConfig() {
        String yaml = """
                hazelcast:
                  security:
                    enabled: true
                    security-interceptors:
                      - foo
                      - bar
                    client-block-unmapped-actions: false
                    member-authentication:
                      realm: mr
                    client-authentication:
                      realm: cr
                    realms:
                      - name: mr
                        authentication:
                          jaas:
                            - class-name: org.example.EmptyLoginModule
                              usage: REQUIRED
                              properties:
                                login-property: login-value
                            - class-name: org.example.EmptyLoginModule
                              usage: SUFFICIENT
                              properties:
                                login-property2: login-value2
                        identity:
                          credentials-factory:
                            class-name: MyCredentialsFactory
                            properties:
                              property: value
                      - name: cr
                        authentication:
                          jaas:
                            - class-name: org.example.EmptyLoginModule
                              usage: OPTIONAL
                              properties:
                                client-property: client-value
                            - class-name: org.example.EmptyLoginModule
                              usage: REQUIRED
                              properties:
                                client-property2: client-value2
                      - name: kerberos
                        authentication:
                          kerberos:
                            skip-role: false
                            relax-flags-check: true
                            use-name-without-realm: true
                            security-realm: krb5Acceptor
                            principal: jduke@HAZELCAST.COM
                            keytab-file: /opt/jduke.keytab
                            ldap:
                              url: ldap://127.0.0.1
                        identity:
                          kerberos:
                            realm: HAZELCAST.COM
                            security-realm: krb5Initializer
                            principal: jduke@HAZELCAST.COM
                            keytab-file: /opt/jduke.keytab
                            use-canonical-hostname: true
                      - name: simple
                        authentication:
                          simple:
                            skip-role: true
                            users:
                              - username: test
                                password: 'a1234'
                                roles:
                                  - monitor
                                  - hazelcast
                              - username: dev
                                password: secret
                                roles:
                                  - root
                        access-control-service:
                          factory-class-name: 'com.acme.access.AccessControlServiceFactory'
                          properties:
                            decisionFile: '/opt/acl.xml'
                    client-permission-policy:
                      class-name: MyPermissionPolicy
                      properties:
                        permission-property: permission-value
                """;

        Config config = buildConfig(yaml);
        SecurityConfig securityConfig = config.getSecurityConfig();
        List<SecurityInterceptorConfig> interceptorConfigs = securityConfig.getSecurityInterceptorConfigs();

        assertEquals(2, interceptorConfigs.size());
        assertEquals("foo", interceptorConfigs.get(0).className);
        assertEquals("bar", interceptorConfigs.get(1).className);
        assertFalse(securityConfig.getClientBlockUnmappedActions());

        RealmConfig memberRealm = securityConfig.getRealmConfig(securityConfig.getMemberRealm());
        CredentialsFactoryConfig memberCredentialsConfig = memberRealm.getCredentialsFactoryConfig();
        assertEquals("MyCredentialsFactory", memberCredentialsConfig.getClassName());
        assertEquals(1, memberCredentialsConfig.getProperties().size());
        assertEquals("value", memberCredentialsConfig.getProperties().getProperty("property"));

        List<LoginModuleConfig> memberLoginModuleConfigs = memberRealm.getJaasAuthenticationConfig().getLoginModuleConfigs();
        assertEquals(2, memberLoginModuleConfigs.size());
        Iterator<LoginModuleConfig> memberLoginIterator = memberLoginModuleConfigs.iterator();

        LoginModuleConfig memberLoginModuleCfg1 = memberLoginIterator.next();
        assertEquals("org.example.EmptyLoginModule", memberLoginModuleCfg1.getClassName());
        assertEquals(LoginModuleUsage.REQUIRED, memberLoginModuleCfg1.getUsage());
        assertEquals(1, memberLoginModuleCfg1.getProperties().size());
        assertEquals("login-value", memberLoginModuleCfg1.getProperties().getProperty("login-property"));

        LoginModuleConfig memberLoginModuleCfg2 = memberLoginIterator.next();
        assertEquals("org.example.EmptyLoginModule", memberLoginModuleCfg2.getClassName());
        assertEquals(LoginModuleUsage.SUFFICIENT, memberLoginModuleCfg2.getUsage());
        assertEquals(1, memberLoginModuleCfg2.getProperties().size());
        assertEquals("login-value2", memberLoginModuleCfg2.getProperties().getProperty("login-property2"));

        RealmConfig clientRealm = securityConfig.getRealmConfig(securityConfig.getClientRealm());
        List<LoginModuleConfig> clientLoginModuleConfigs = clientRealm.getJaasAuthenticationConfig().getLoginModuleConfigs();
        assertEquals(2, clientLoginModuleConfigs.size());
        Iterator<LoginModuleConfig> clientLoginIterator = clientLoginModuleConfigs.iterator();

        LoginModuleConfig clientLoginModuleCfg1 = clientLoginIterator.next();
        assertEquals("org.example.EmptyLoginModule", clientLoginModuleCfg1.getClassName());
        assertEquals(LoginModuleUsage.OPTIONAL, clientLoginModuleCfg1.getUsage());
        assertEquals(1, clientLoginModuleCfg1.getProperties().size());
        assertEquals("client-value", clientLoginModuleCfg1.getProperties().getProperty("client-property"));

        LoginModuleConfig clientLoginModuleCfg2 = clientLoginIterator.next();
        assertEquals("org.example.EmptyLoginModule", clientLoginModuleCfg2.getClassName());
        assertEquals(LoginModuleUsage.REQUIRED, clientLoginModuleCfg2.getUsage());
        assertEquals(1, clientLoginModuleCfg2.getProperties().size());
        assertEquals("client-value2", clientLoginModuleCfg2.getProperties().getProperty("client-property2"));

        RealmConfig kerberosRealm = securityConfig.getRealmConfig("kerberos");
        assertNotNull(kerberosRealm);
        KerberosIdentityConfig kerbIdentity = kerberosRealm.getKerberosIdentityConfig();
        assertNotNull(kerbIdentity);
        assertEquals("HAZELCAST.COM", kerbIdentity.getRealm());
        assertEquals("krb5Initializer", kerbIdentity.getSecurityRealm());
        assertEquals("jduke@HAZELCAST.COM", kerbIdentity.getPrincipal());
        assertEquals("/opt/jduke.keytab", kerbIdentity.getKeytabFile());
        assertTrue(kerbIdentity.getUseCanonicalHostname());

        KerberosAuthenticationConfig kerbAuthentication = kerberosRealm.getKerberosAuthenticationConfig();
        assertNotNull(kerbAuthentication);
        assertEquals(Boolean.TRUE, kerbAuthentication.getRelaxFlagsCheck());
        assertEquals(Boolean.FALSE, kerbAuthentication.getSkipRole());
        assertNull(kerbAuthentication.getSkipIdentity());
        assertEquals("krb5Acceptor", kerbAuthentication.getSecurityRealm());
        assertEquals("jduke@HAZELCAST.COM", kerbAuthentication.getPrincipal());
        assertEquals("/opt/jduke.keytab", kerbAuthentication.getKeytabFile());
        assertTrue(kerbAuthentication.getUseNameWithoutRealm());

        LdapAuthenticationConfig kerbLdapAuthentication = kerbAuthentication.getLdapAuthenticationConfig();
        assertNotNull(kerbLdapAuthentication);
        assertEquals("ldap://127.0.0.1", kerbLdapAuthentication.getUrl());

        RealmConfig simpleRealm = securityConfig.getRealmConfig("simple");
        assertNotNull(simpleRealm);
        SimpleAuthenticationConfig simpleAuthnCfg = simpleRealm.getSimpleAuthenticationConfig();
        assertNotNull(simpleAuthnCfg);
        assertEquals(2, simpleAuthnCfg.getUsernames().size());
        assertTrue(simpleAuthnCfg.getUsernames().contains("test"));
        assertEquals("a1234", simpleAuthnCfg.getPassword("test"));
        Set<String> expectedRoles = new HashSet<>();
        expectedRoles.add("monitor");
        expectedRoles.add("hazelcast");
        assertEquals(expectedRoles, simpleAuthnCfg.getRoles("test"));
        assertEquals(Boolean.TRUE, simpleAuthnCfg.getSkipRole());
        AccessControlServiceConfig acs = simpleRealm.getAccessControlServiceConfig();
        assertNotNull(acs);
        assertEquals("com.acme.access.AccessControlServiceFactory", acs.getFactoryClassName());
        assertEquals("/opt/acl.xml", acs.getProperty("decisionFile"));

        // client-permission-policy
        PermissionPolicyConfig permissionPolicyConfig = securityConfig.getClientPolicyConfig();
        assertEquals("MyPermissionPolicy", permissionPolicyConfig.getClassName());
        assertEquals(1, permissionPolicyConfig.getProperties().size());
        assertEquals("permission-value", permissionPolicyConfig.getProperties().getProperty("permission-property"));
    }

    @Override
    @Test
    public void readAwsConfig() {
        String yaml = """
                hazelcast:
                  network:
                    port:
                      auto-increment: true
                      port: 5701
                    join:
                      multicast:
                        enabled: false
                      aws:
                        enabled: true
                        use-public-ip: true
                        connection-timeout-seconds: 10
                        access-key: sample-access-key
                        secret-key: sample-secret-key
                        iam-role: sample-role
                        region: sample-region
                        host-header: sample-header
                        security-group-name: sample-group
                        tag-key: sample-tag-key
                        tag-value: sample-tag-value
                """;

        Config config = buildConfig(yaml);

        AwsConfig awsConfig = config.getNetworkConfig().getJoin().getAwsConfig();
        assertTrue(awsConfig.isEnabled());
        assertTrue(awsConfig.isUsePublicIp());
        assertAwsConfig(awsConfig);
    }

    @Override
    @Test
    public void readGcpConfig() {
        String yaml = """
                hazelcast:
                  network:
                    join:
                      multicast:
                        enabled: false
                      gcp:
                        enabled: true
                        use-public-ip: true
                        zones: us-east1-b
                """;

        Config config = buildConfig(yaml);

        GcpConfig gcpConfig = config.getNetworkConfig().getJoin().getGcpConfig();

        assertTrue(gcpConfig.isEnabled());
        assertTrue(gcpConfig.isUsePublicIp());
        assertEquals("us-east1-b", gcpConfig.getProperty("zones"));
    }

    @Override
    @Test
    public void readAzureConfig() {
        String yaml = """
                hazelcast:
                  network:
                    join:
                      multicast:
                        enabled: false
                      azure:
                        enabled: true
                        use-public-ip: true
                        client-id: 123456789!
                """;

        Config config = buildConfig(yaml);

        AzureConfig azureConfig = config.getNetworkConfig().getJoin().getAzureConfig();

        assertTrue(azureConfig.isEnabled());
        assertTrue(azureConfig.isUsePublicIp());
        assertEquals("123456789!", azureConfig.getProperty("client-id"));
    }

    @Override
    @Test
    public void readKubernetesConfig() {
        String yaml = """
                hazelcast:
                  network:
                    join:
                      multicast:
                        enabled: false
                      kubernetes:
                        enabled: true
                        use-public-ip: true
                        namespace: hazelcast
                """;

        Config config = buildConfig(yaml);

        KubernetesConfig kubernetesConfig = config.getNetworkConfig().getJoin().getKubernetesConfig();

        assertTrue(kubernetesConfig.isEnabled());
        assertTrue(kubernetesConfig.isUsePublicIp());
        assertEquals("hazelcast", kubernetesConfig.getProperty("namespace"));
    }

    @Override
    @Test
    public void readEurekaConfig() {
        String yaml = """
                hazelcast:
                  network:
                    join:
                      multicast:
                        enabled: false
                      eureka:
                        enabled: true
                        use-public-ip: true
                        shouldUseDns: false
                        serviceUrl.default: http://localhost:8082/eureka
                        namespace: hazelcast
                """;

        Config config = buildConfig(yaml);

        EurekaConfig eurekaConfig = config.getNetworkConfig().getJoin().getEurekaConfig();

        assertTrue(eurekaConfig.isEnabled());
        assertTrue(eurekaConfig.isUsePublicIp());
        assertEquals("hazelcast", eurekaConfig.getProperty("namespace"));
        assertEquals("false", eurekaConfig.getProperty("shouldUseDns"));
        assertEquals("http://localhost:8082/eureka", eurekaConfig.getProperty("serviceUrl.default"));
    }

    @Override
    @Test
    public void readDiscoveryConfig() {
        String yaml = """
                hazelcast:
                  network:
                    join:
                      multicast:
                        enabled: false
                      discovery-strategies:
                        node-filter:
                          class: DummyFilterClass
                        discovery-strategies:
                          - class: DummyDiscoveryStrategy1
                            enabled: true
                            properties:
                              key-string: foo
                              key-int: 123
                              key-boolean: true
                """;

        Config config = buildConfig(yaml);
        DiscoveryConfig discoveryConfig = config.getNetworkConfig().getJoin().getDiscoveryConfig();
        assertTrue(discoveryConfig.isEnabled());
        assertDiscoveryConfig(discoveryConfig);
    }

    @Override
    @Test
    public void testSSLConfig() {
        String yaml = """
                hazelcast:
                  network:
                    ssl:
                      enabled: true\r
                      factory-class-name: com.hazelcast.nio.ssl.BasicSSLContextFactory\r
                      properties:\r
                        protocol: TLS\r
                """;

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
        String yaml = """
                hazelcast:
                  network:
                    symmetric-encryption:
                      enabled: true
                      algorithm: AES
                      salt: some-salt
                      password: some-pass
                      iteration-count: 7531
                """;

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
        Config config = buildConfig("""
                hazelcast:
                  network:
                    port:
                      port-count: 200
                      port: 5702
                """);

        assertEquals(200, config.getNetworkConfig().getPortCount());
        assertEquals(5702, config.getNetworkConfig().getPort());

        // check if the default is passed in correctly
        config = buildConfig("""
                hazelcast:
                  network:
                    port:
                      port: 5703
                """);
        assertEquals(100, config.getNetworkConfig().getPortCount());
        assertEquals(5703, config.getNetworkConfig().getPort());
    }

    @Override
    @Test
    public void readPortAutoIncrement() {
        // explicitly set
        Config config = buildConfig("""
                hazelcast:
                  network:
                    port:
                      auto-increment: false
                      port: 5701
                """);
        assertFalse(config.getNetworkConfig().isPortAutoIncrement());

        // check if the default is picked up correctly
        config = buildConfig("""
                hazelcast:
                  network:
                    port:\s
                      port: 5801
                """);
        assertTrue(config.getNetworkConfig().isPortAutoIncrement());
        assertEquals(5801, config.getNetworkConfig().getPort());
    }

    @Override
    @Test
    public void networkReuseAddress() {
        Config config = buildConfig("""
                hazelcast:
                  network:
                    reuse-address: true
                """);
        assertTrue(config.getNetworkConfig().isReuseAddress());
    }

    @Override
    @Test
    public void readQueueConfig() {
        String yaml = """
                hazelcast:
                  queue:
                    custom:
                      priority-comparator-class-name: com.hazelcast.collection.impl.queue.model.PriorityElementComparator
                      statistics-enabled: false
                      max-size: 100
                      backup-count: 2
                      async-backup-count: 1
                      empty-queue-ttl: 1
                      item-listeners:
                        - class-name: com.hazelcast.examples.ItemListener
                          include-value: false
                        - class-name: com.hazelcast.examples.ItemListener2
                          include-value: true
                      queue-store:
                        enabled: false
                        class-name: com.hazelcast.QueueStoreImpl
                        properties:
                          binary: false
                          memory-limit: 1000
                          bulk-load: 500
                      split-brain-protection-ref: customSplitBrainProtectionRule
                      merge-policy:
                        batch-size: 23
                        class-name: CustomMergePolicy
                      user-code-namespace: ns1
                    default:
                      max-size: 42
                """;

        Config config = buildConfig(yaml);
        QueueConfig customQueueConfig = config.getQueueConfig("custom");
        assertFalse(customQueueConfig.isStatisticsEnabled());
        assertEquals(100, customQueueConfig.getMaxSize());
        assertEquals(2, customQueueConfig.getBackupCount());
        assertEquals(1, customQueueConfig.getAsyncBackupCount());
        assertEquals(1, customQueueConfig.getEmptyQueueTtl());
        assertEquals("com.hazelcast.collection.impl.queue.model.PriorityElementComparator",
                customQueueConfig.getPriorityComparatorClassName());
        assertEquals("ns1", customQueueConfig.getUserCodeNamespace());

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

        assertEquals("customSplitBrainProtectionRule", customQueueConfig.getSplitBrainProtectionName());

        QueueConfig defaultQueueConfig = config.getQueueConfig("default");
        assertEquals(42, defaultQueueConfig.getMaxSize());
    }

    @Override
    @Test
    public void readListConfig() {
        String yaml = """
                hazelcast:
                  list:
                    myList:
                      statistics-enabled: false
                      max-size: 100
                      backup-count: 2
                      async-backup-count: 1
                      item-listeners:
                        - class-name: com.hazelcast.examples.ItemListener
                          include-value: false
                        - class-name: com.hazelcast.examples.ItemListener2
                          include-value: true
                      merge-policy:
                        class-name: PassThroughMergePolicy
                        batch-size: 4223
                    default:
                      max-size: 42
                """;

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
        String yaml = """
                hazelcast:
                  set:
                    mySet:
                      statistics-enabled: false
                      max-size: 100
                      backup-count: 2
                      async-backup-count: 1
                      item-listeners:
                        - class-name: com.hazelcast.examples.ItemListener
                          include-value: false
                        - class-name: com.hazelcast.examples.ItemListener2
                          include-value: true
                      merge-policy:
                        class-name: PassThroughMergePolicy
                        batch-size: 4223
                    default:
                      max-size: 42
                """;

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
    public void readReliableTopicConfig() {
        String yaml = """
                hazelcast:
                  reliable-topic:
                    custom:
                      read-batch-size: 35
                      statistics-enabled: false
                      topic-overload-policy: DISCARD_OLDEST
                      message-listeners:
                        - MessageListenerImpl
                        - MessageListenerImpl2
                      user-code-namespace: ns1
                    default:
                      read-batch-size: 42
                """;

        Config config = buildConfig(yaml);

        ReliableTopicConfig topicConfig = config.getReliableTopicConfig("custom");

        assertEquals(35, topicConfig.getReadBatchSize());
        assertFalse(topicConfig.isStatisticsEnabled());
        assertEquals(TopicOverloadPolicy.DISCARD_OLDEST, topicConfig.getTopicOverloadPolicy());
        assertEquals("ns1", topicConfig.getUserCodeNamespace());

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
    public void readTopicConfig() {
        String yaml = """
                hazelcast:
                  topic:
                    custom:
                      statistics-enabled: false
                      global-ordering-enabled: true
                      multi-threading-enabled: false
                      message-listeners:
                        - MessageListenerImpl
                        - MessageListenerImpl2
                      user-code-namespace: ns1
                    default:
                      user-code-namespace: ns2
                """;

        Config config = buildConfig(yaml);

        TopicConfig topicConfig = config.getTopicConfig("custom");

        assertFalse(topicConfig.isStatisticsEnabled());
        assertTrue(topicConfig.isGlobalOrderingEnabled());
        assertFalse(topicConfig.isMultiThreadingEnabled());
        assertEquals("ns1", topicConfig.getUserCodeNamespace());

        // checking listener configuration
        assertEquals(2, topicConfig.getMessageListenerConfigs().size());
        ListenerConfig listenerConfig1 = topicConfig.getMessageListenerConfigs().get(0);
        assertEquals("MessageListenerImpl", listenerConfig1.getClassName());
        assertNull(listenerConfig1.getImplementation());
        ListenerConfig listenerConfig2 = topicConfig.getMessageListenerConfigs().get(1);
        assertEquals("MessageListenerImpl2", listenerConfig2.getClassName());
        assertNull(listenerConfig2.getImplementation());

        TopicConfig defaultTopicConfig = config.getTopicConfig("default");
        assertEquals("ns2", defaultTopicConfig.getUserCodeNamespace());
    }

    @Override
    @Test
    public void readRingbuffer() {
        String yaml = """
                hazelcast:
                  ringbuffer:
                    custom:
                      capacity: 10
                      backup-count: 2
                      async-backup-count: 1
                      time-to-live-seconds: 9
                      in-memory-format: OBJECT
                      ringbuffer-store:
                        enabled: false
                        class-name: com.hazelcast.RingbufferStoreImpl
                        properties:
                          store-path: .//tmp//bufferstore
                      split-brain-protection-ref: customSplitBrainProtectionRule
                      merge-policy:
                        class-name: CustomMergePolicy
                        batch-size: 2342
                      user-code-namespace: ns1
                    default:
                      capacity: 42
                """;

        Config config = buildConfig(yaml);
        RingbufferConfig ringbufferConfig = config.getRingbufferConfig("custom");

        assertEquals(10, ringbufferConfig.getCapacity());
        assertEquals(2, ringbufferConfig.getBackupCount());
        assertEquals(1, ringbufferConfig.getAsyncBackupCount());
        assertEquals(9, ringbufferConfig.getTimeToLiveSeconds());
        assertEquals(InMemoryFormat.OBJECT, ringbufferConfig.getInMemoryFormat());
        assertEquals("ns1", ringbufferConfig.getUserCodeNamespace());

        RingbufferStoreConfig ringbufferStoreConfig = ringbufferConfig.getRingbufferStoreConfig();
        assertFalse(ringbufferStoreConfig.isEnabled());
        assertEquals("com.hazelcast.RingbufferStoreImpl", ringbufferStoreConfig.getClassName());
        Properties ringbufferStoreProperties = ringbufferStoreConfig.getProperties();
        assertEquals(".//tmp//bufferstore", ringbufferStoreProperties.get("store-path"));
        assertEquals("customSplitBrainProtectionRule", ringbufferConfig.getSplitBrainProtectionName());

        MergePolicyConfig mergePolicyConfig = ringbufferConfig.getMergePolicyConfig();
        assertEquals("CustomMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(2342, mergePolicyConfig.getBatchSize());

        RingbufferConfig defaultRingBufferConfig = config.getRingbufferConfig("default");
        assertEquals(42, defaultRingBufferConfig.getCapacity());
    }

    @Override
    @Test
    @Ignore
    public void testCaseInsensitivityOfSettings() {
        String yaml = """
                hazelcast:
                  map:
                    testCaseInsensitivity:
                      in-memory-format: BINARY
                      backup-count: 1
                      async-backup-count: 0
                      time-to-live-seconds: 0
                      max-idle-seconds: 0
                      eviction:
                         eviction-policy: NONE
                         max-size-policy: per_partition
                         size: 0
                      merge-policy:
                        class-name: CustomMergePolicy
                        batch-size: 2342
                """;

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("testCaseInsensitivity");

        assertEquals(InMemoryFormat.BINARY, mapConfig.getInMemoryFormat());
        assertEquals(EvictionPolicy.NONE, mapConfig.getEvictionConfig().getEvictionPolicy());
        assertEquals(MaxSizePolicy.PER_PARTITION, mapConfig.getEvictionConfig().getMaxSizePolicy());

        MergePolicyConfig mergePolicyConfig = mapConfig.getMergePolicyConfig();
        assertEquals("CustomMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(2342, mergePolicyConfig.getBatchSize());
    }

    @Override
    @Test
    public void testManagementCenterConfig() {
        String yaml = """
                hazelcast:
                  management-center:
                    scripting-enabled: true
                    console-enabled: true
                    data-access-enabled: false
                    trusted-interfaces:
                      - 127.0.0.1
                      - 192.168.1.*
                """;

        Config config = buildConfig(yaml);
        ManagementCenterConfig mcConfig = config.getManagementCenterConfig();

        assertTrue(mcConfig.isScriptingEnabled());
        assertTrue(mcConfig.isConsoleEnabled());
        assertFalse(mcConfig.isDataAccessEnabled());
        assertEquals(2, mcConfig.getTrustedInterfaces().size());
        assertTrue(mcConfig.getTrustedInterfaces().containsAll(Set.of("127.0.0.1", "192.168.1.*")));
    }

    @Override
    @Test
    public void testNullManagementCenterConfig() {
        String yaml = """
                hazelcast:
                  management-center: {}
                """;
        Config config = buildConfig(yaml);
        ManagementCenterConfig mcConfig = config.getManagementCenterConfig();

        assertFalse(mcConfig.isScriptingEnabled());
        assertFalse(mcConfig.isConsoleEnabled());
        assertTrue(mcConfig.isDataAccessEnabled());
    }

    @Override
    @Test
    public void testEmptyManagementCenterConfig() {
        String yaml = "hazelcast: {}";

        Config config = buildConfig(yaml);
        ManagementCenterConfig mcConfig = config.getManagementCenterConfig();

        assertFalse(mcConfig.isScriptingEnabled());
        assertFalse(mcConfig.isConsoleEnabled());
        assertTrue(mcConfig.isDataAccessEnabled());
    }

    @Override
    @Test
    public void testMapStoreInitialModeLazy() {
        String yaml = """
                hazelcast:
                  map:
                    mymap:
                      map-store:
                        enabled: true
                        initial-mode: LAZY
                """;

        Config config = buildConfig(yaml);
        MapStoreConfig mapStoreConfig = config.getMapConfig("mymap").getMapStoreConfig();

        assertTrue(mapStoreConfig.isEnabled());
        assertEquals(MapStoreConfig.InitialLoadMode.LAZY, mapStoreConfig.getInitialLoadMode());
    }

    @Override
    @Test
    public void testMapConfig_metadataPolicy() {
        String yaml = """
                hazelcast:
                  map:
                    mymap:
                      metadata-policy: OFF""";

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertEquals(MetadataPolicy.OFF, mapConfig.getMetadataPolicy());
    }

    @Override
    public void testMapConfig_statisticsEnable() {
        String yaml = """
                hazelcast:
                  map:
                    mymap:
                      statistics-enabled: false""";

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertFalse(mapConfig.isStatisticsEnabled());
    }

    @Override
    public void testMapConfig_perEntryStatsEnabled() {
        String yaml = """
                hazelcast:
                  map:
                    mymap:
                      per-entry-stats-enabled: true""";

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertTrue(mapConfig.isStatisticsEnabled());
    }

    @Override
    @Test
    public void testMapConfig_metadataPolicy_defaultValue() {
        String yaml = """
                hazelcast:
                  map:
                    mymap: {}""";

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertEquals(MetadataPolicy.CREATE_ON_UPDATE, mapConfig.getMetadataPolicy());
    }

    @Override
    @Test
    public void testMapConfig_evictions() {
        String yaml = """
                hazelcast:
                  map:
                    lruMap:
                         eviction:
                             eviction-policy: LRU
                    lfuMap:
                          eviction:
                             eviction-policy: LFU
                    noneMap:
                         eviction:
                             eviction-policy: NONE
                    randomMap:
                        eviction:
                             eviction-policy: RANDOM
                """;

        Config config = buildConfig(yaml);

        assertEquals(EvictionPolicy.LRU, config.getMapConfig("lruMap").getEvictionConfig().getEvictionPolicy());
        assertEquals(EvictionPolicy.LFU, config.getMapConfig("lfuMap").getEvictionConfig().getEvictionPolicy());
        assertEquals(EvictionPolicy.NONE, config.getMapConfig("noneMap").getEvictionConfig().getEvictionPolicy());
        assertEquals(EvictionPolicy.RANDOM, config.getMapConfig("randomMap").getEvictionConfig().getEvictionPolicy());
    }

    @Override
    @Test
    public void testMapConfig_cacheValueConfig_defaultValue() {
        String yaml = """
                hazelcast:
                  map:
                    mymap: {}
                """;

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertEquals(CacheDeserializedValues.INDEX_ONLY, mapConfig.getCacheDeserializedValues());
    }

    @Override
    @Test
    public void testMapConfig_cacheValueConfig_never() {
        String yaml = """
                hazelcast:
                  map:
                    mymap:
                      cache-deserialized-values: NEVER
                """;

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertEquals(CacheDeserializedValues.NEVER, mapConfig.getCacheDeserializedValues());
    }

    @Override
    @Test
    public void testMapConfig_cacheValueConfig_always() {
        String yaml = """
                hazelcast:
                  map:
                    mymap:
                      cache-deserialized-values: ALWAYS
                """;

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertEquals(CacheDeserializedValues.ALWAYS, mapConfig.getCacheDeserializedValues());
    }

    @Override
    @Test
    public void testMapConfig_cacheValueConfig_indexOnly() {
        String yaml = """
                hazelcast:
                  map:
                    mymap:
                      cache-deserialized-values: INDEX-ONLY
                """;

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertEquals(CacheDeserializedValues.INDEX_ONLY, mapConfig.getCacheDeserializedValues());
    }

    @Override
    @Test
    public void testMapStoreInitialModeEager() {
        String yaml = """
                hazelcast:
                  map:
                    mymap:
                      map-store:
                        enabled: true
                        initial-mode: EAGER
                """;

        Config config = buildConfig(yaml);
        MapStoreConfig mapStoreConfig = config.getMapConfig("mymap").getMapStoreConfig();

        assertTrue(mapStoreConfig.isEnabled());
        assertEquals(MapStoreConfig.InitialLoadMode.EAGER, mapStoreConfig.getInitialLoadMode());
    }

    @Override
    @Test
    public void testMapStoreEnabled() {
        String yaml = """
                hazelcast:
                  map:
                    mymap:
                      map-store:
                        enabled: true
                        initial-mode: EAGER
                """;

        Config config = buildConfig(yaml);
        MapStoreConfig mapStoreConfig = config.getMapConfig("mymap").getMapStoreConfig();

        assertTrue(mapStoreConfig.isEnabled());
    }

    @Override
    @Test
    public void testMapStoreEnabledIfNotDisabled() {
        String yaml = """
                hazelcast:
                  map:
                    mymap:
                      map-store:
                        initial-mode: EAGER
                """;

        Config config = buildConfig(yaml);
        MapStoreConfig mapStoreConfig = config.getMapConfig("mymap").getMapStoreConfig();

        assertTrue(mapStoreConfig.isEnabled());
    }

    @Override
    @Test
    public void testMapStoreDisabled() {
        String yaml = """
                hazelcast:
                  map:
                    mymap:
                      map-store:
                        enabled: false
                        initial-mode: EAGER
                """;

        Config config = buildConfig(yaml);
        MapStoreConfig mapStoreConfig = config.getMapConfig("mymap").getMapStoreConfig();

        assertFalse(mapStoreConfig.isEnabled());
    }

    @Override
    @Test
    public void testMapStoreConfig_offload_whenDefault() {
        MapStoreConfig mapStoreConfig = getOffloadMapStoreConfig(MapStoreConfig.DEFAULT_OFFLOAD, true);

        assertTrue(mapStoreConfig.isOffload());
    }

    @Override
    @Test
    public void testMapStoreConfig_offload_whenSetFalse() {
        MapStoreConfig mapStoreConfig = getOffloadMapStoreConfig(false, false);

        assertFalse(mapStoreConfig.isOffload());
    }

    @Override
    @Test
    public void testMapStoreConfig_offload_whenSetTrue() {
        MapStoreConfig mapStoreConfig = getOffloadMapStoreConfig(true, false);

        assertTrue(mapStoreConfig.isOffload());
    }

    @Override
    @Test
    public void testMapStoreWriteBatchSize() {
        String yaml = """
                hazelcast:
                  map:
                    mymap:
                      map-store:
                        write-batch-size: 23
                """;

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
                + (useDefault ? " {}" : "\n        write-coalescing: " + value + "\n");
    }

    private MapStoreConfig getOffloadMapStoreConfig(boolean offload, boolean useDefault) {
        String xml = getOffloadConfigYaml(offload, useDefault);
        Config config = buildConfig(xml);
        return config.getMapConfig("mymap").getMapStoreConfig();
    }

    private String getOffloadConfigYaml(boolean value, boolean useDefault) {
        return ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    mymap:\n"
                + "      map-store:"
                + (useDefault ? " {}" : "\n        offload: " + value + "\n");
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
        String yaml = """
                hazelcast:
                  map:
                    lfuNearCache:
                      near-cache:
                        eviction:
                          eviction-policy: LFU
                    lruNearCache:
                      near-cache:
                        eviction:
                          eviction-policy: LRU
                    noneNearCache:
                      near-cache:
                        eviction:
                          eviction-policy: NONE
                    randomNearCache:
                      near-cache:
                        eviction:
                          eviction-policy: RANDOM
                """;

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
        String yaml = """
                hazelcast:
                  partition-group:
                    enabled: true
                    group-type: ZONE_AWARE
                """;

        Config config = buildConfig(yaml);
        PartitionGroupConfig partitionGroupConfig = config.getPartitionGroupConfig();
        assertTrue(partitionGroupConfig.isEnabled());
        assertEquals(PartitionGroupConfig.MemberGroupType.ZONE_AWARE, partitionGroupConfig.getGroupType());
    }

    @Override
    @Test
    public void testPartitionGroupNodeAware() {
        String yaml = """
                hazelcast:
                  partition-group:
                    enabled: true
                    group-type: NODE_AWARE
                """;

        Config config = buildConfig(yaml);
        PartitionGroupConfig partitionGroupConfig = config.getPartitionGroupConfig();
        assertTrue(partitionGroupConfig.isEnabled());
        assertEquals(PartitionGroupConfig.MemberGroupType.NODE_AWARE, partitionGroupConfig.getGroupType());
    }

    @Override
    @Test
    public void testPartitionGroupPlacementAware() {
        String yaml = """
                hazelcast:
                  partition-group:
                    enabled: true
                    group-type: PLACEMENT_AWARE
                """;

        Config config = buildConfig(yaml);
        PartitionGroupConfig partitionGroupConfig = config.getPartitionGroupConfig();
        assertTrue(partitionGroupConfig.isEnabled());
        assertEquals(PartitionGroupConfig.MemberGroupType.PLACEMENT_AWARE, partitionGroupConfig.getGroupType());
    }

    @Override
    @Test
    public void testPartitionGroupSPI() {
        String yaml = """
                hazelcast:
                  partition-group:
                    enabled: true
                    group-type: SPI
                """;

        Config config = buildConfig(yaml);
        assertEquals(PartitionGroupConfig.MemberGroupType.SPI, config.getPartitionGroupConfig().getGroupType());
    }

    @Override
    @Test
    public void testPartitionGroupMemberGroups() {
        String yaml = """
                hazelcast:
                  partition-group:
                    enabled: true
                    group-type: SPI
                    member-group:
                      -
                        - 10.10.1.1
                        - 10.10.1.2
                      -
                        - 10.10.1.3
                        - 10.10.1.4
                """;

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
                + "        time-to-live-seconds: 77\n"
                + "        max-idle-seconds: 92\n"
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
        assertEquals(77, nearCacheConfig.getTimeToLiveSeconds());
        assertEquals(92, nearCacheConfig.getMaxIdleSeconds());
        assertFalse(nearCacheConfig.isInvalidateOnChange());
        assertFalse(nearCacheConfig.isCacheLocalEntries());
        assertEquals(LRU, nearCacheConfig.getEvictionConfig().getEvictionPolicy());
        assertEquals(ENTRY_COUNT, nearCacheConfig.getEvictionConfig().getMaxSizePolicy());
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
                + "          merge-policy-class-name: TestMergePolicy\n"
                + "          filters:\n"
                + "            - com.example.SampleFilter\n";

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig(mapName);
        WanReplicationRef wanRef = mapConfig.getWanReplicationRef();

        assertEquals(refName, wanRef.getName());
        assertEquals(mergePolicy, wanRef.getMergePolicyClassName());
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
                + "      batch-publisher:\n"
                + "        publisherId:\n"
                + "          cluster-name: nyc\n"
                + "          batch-size: 1000\n"
                + "          batch-max-delay-millis: 2000\n"
                + "          response-timeout-millis: 60000\n"
                + "          acknowledge-type: ACK_ON_RECEIPT\n"
                + "          initial-publisher-state: STOPPED\n"
                + "          snapshot-enabled: true\n"
                + "          idle-max-park-ns: 2000\n"
                + "          idle-min-park-ns: 1000\n"
                + "          max-concurrent-invocations: 100\n"
                + "          discovery-period-seconds: 20\n"
                + "          use-endpoint-private-address: true\n"
                + "          queue-full-behavior: DISCARD_AFTER_MUTATION\n"
                + "          max-target-endpoints: 200\n"
                + "          queue-capacity: 15000\n"
                + "          target-endpoints: 10.3.5.1:5701,10.3.5.2:5701\n"
                + "          properties:\n"
                + "            propName1: propValue1\n"
                + "      custom-publisher:\n"
                + "        customPublisherId:\n"
                + "          class-name: PublisherClassName\n"
                + "          properties:\n"
                + "            propName1: propValue1\n"
                + "      consumer:\n"
                + "        class-name: ConsumerClassName\n"
                + "        properties:\n"
                + "          propName1: propValue1\n";

        Config config = buildConfig(yaml);
        WanReplicationConfig wanReplicationConfig = config.getWanReplicationConfig(configName);

        assertEquals(configName, wanReplicationConfig.getName());

        WanConsumerConfig consumerConfig = wanReplicationConfig.getConsumerConfig();
        assertNotNull(consumerConfig);
        assertEquals("ConsumerClassName", consumerConfig.getClassName());

        Map<String, Comparable> properties = consumerConfig.getProperties();
        assertNotNull(properties);
        assertEquals(1, properties.size());
        assertEquals("propValue1", properties.get("propName1"));

        List<WanBatchPublisherConfig> batchPublishers = wanReplicationConfig.getBatchPublisherConfigs();
        assertNotNull(batchPublishers);
        assertEquals(1, batchPublishers.size());
        WanBatchPublisherConfig publisherConfig = batchPublishers.get(0);
        assertEquals("nyc", publisherConfig.getClusterName());
        assertEquals("publisherId", publisherConfig.getPublisherId());
        assertEquals(1000, publisherConfig.getBatchSize());
        assertEquals(2000, publisherConfig.getBatchMaxDelayMillis());
        assertEquals(60000, publisherConfig.getResponseTimeoutMillis());
        assertEquals(WanAcknowledgeType.ACK_ON_RECEIPT, publisherConfig.getAcknowledgeType());
        assertEquals(WanPublisherState.STOPPED, publisherConfig.getInitialPublisherState());
        assertTrue(publisherConfig.isSnapshotEnabled());
        assertEquals(2000, publisherConfig.getIdleMaxParkNs());
        assertEquals(1000, publisherConfig.getIdleMinParkNs());
        assertEquals(100, publisherConfig.getMaxConcurrentInvocations());
        assertEquals(20, publisherConfig.getDiscoveryPeriodSeconds());
        assertTrue(publisherConfig.isUseEndpointPrivateAddress());
        assertEquals(DISCARD_AFTER_MUTATION, publisherConfig.getQueueFullBehavior());
        assertEquals(200, publisherConfig.getMaxTargetEndpoints());
        assertEquals(15000, publisherConfig.getQueueCapacity());
        assertEquals("10.3.5.1:5701,10.3.5.2:5701", publisherConfig.getTargetEndpoints());
        properties = publisherConfig.getProperties();
        assertNotNull(properties);
        assertEquals(1, properties.size());
        assertEquals("propValue1", properties.get("propName1"));

        List<WanCustomPublisherConfig> customPublishers = wanReplicationConfig.getCustomPublisherConfigs();
        assertNotNull(customPublishers);
        assertEquals(1, customPublishers.size());
        WanCustomPublisherConfig customPublisher = customPublishers.get(0);
        assertEquals("customPublisherId", customPublisher.getPublisherId());
        assertEquals("PublisherClassName", customPublisher.getClassName());
        properties = customPublisher.getProperties();
        assertNotNull(properties);
        assertEquals(1, properties.size());
        assertEquals("propValue1", properties.get("propName1"));
    }

    @Test
    public void testWanReplicationConfig_withMultipleBatchPublishersAndSameClusterName() {

        String yaml = ""
                + "hazelcast:\n"
                + "  wan-replication:\n"
                + "    fanout-test:\n"
                + "      batch-publisher:\n"
                + "        to-gtb:\n"
                + "          cluster-name: same\n"
                + "          publisher-id: 'gtb'\n"
                + "          target-endpoints: 127.0.0.1:7901,127.0.0.1:7902\n"
                + "        to-mta:\n"
                + "          cluster-name: same\n"
                + "          publisher-id: 'mta'\n"
                + "          target-endpoints: 127.0.0.1:8901,127.0.0.1:8902\n"
                + "        to-mtb:\n"
                + "          cluster-name: same\n"
                + "          publisher-id: 'mtb'\n"
                + "          target-endpoints: 127.0.0.1:9901,127.0.0.1:9902\n";

        Config config = buildConfig(yaml);
        WanReplicationConfig wanReplicationConfig = config.getWanReplicationConfig("fanout-test");
        List<WanBatchPublisherConfig> batchPublishers = wanReplicationConfig.getBatchPublisherConfigs();

        assertEquals("fanout-test", wanReplicationConfig.getName());
        assertEquals(3, batchPublishers.size());

        WanBatchPublisherConfig publisherConfig = batchPublishers.get(0);
        assertEquals("same", publisherConfig.getClusterName());
        assertEquals("gtb", publisherConfig.getPublisherId());

        publisherConfig = batchPublishers.get(1);
        assertEquals("same", publisherConfig.getClusterName());
        assertEquals("mta", publisherConfig.getPublisherId());

        publisherConfig = batchPublishers.get(2);
        assertEquals("same", publisherConfig.getClusterName());
        assertEquals("mtb", publisherConfig.getPublisherId());
    }

    @Override
    @Test
    public void testDefaultOfPersistWanReplicatedDataIsFalse() {
        String configName = "test";
        String yaml = ""
                + "hazelcast:\n"
                + "  wan-replication:\n"
                + "    " + configName + ":\n"
                + "      consumer: {}\n";

        Config config = buildConfig(yaml);
        WanReplicationConfig wanReplicationConfig = config.getWanReplicationConfig(configName);
        WanConsumerConfig consumerConfig = wanReplicationConfig.getConsumerConfig();
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
                + "      batch-publisher:\n"
                + "        nyc:\n"
                + "          sync:\n"
                + "            consistency-check-strategy: MERKLE_TREES\n";

        Config config = buildConfig(yaml);
        WanReplicationConfig wanReplicationConfig = config.getWanReplicationConfig(configName);

        assertEquals(configName, wanReplicationConfig.getName());

        List<WanBatchPublisherConfig> publishers = wanReplicationConfig.getBatchPublisherConfigs();
        assertNotNull(publishers);
        assertEquals(1, publishers.size());
        WanBatchPublisherConfig publisherConfig = publishers.get(0);
        assertEquals(ConsistencyCheckStrategy.MERKLE_TREES, publisherConfig.getSyncConfig()
                .getConsistencyCheckStrategy());
    }

    @Override
    @Test
    public void testFlakeIdGeneratorConfig() {
        String yaml = """
                hazelcast:
                  flake-id-generator:
                    gen:
                      prefetch-count: 3
                      prefetch-validity-millis: 10
                      epoch-start: 1514764800001
                      node-id-offset: 30
                      bits-sequence: 22
                      bits-node-id: 33
                      allowed-future-millis: 20000
                      statistics-enabled: false
                    gen2:
                      statistics-enabled: true""";

        Config config = buildConfig(yaml);
        FlakeIdGeneratorConfig fConfig = config.findFlakeIdGeneratorConfig("gen");
        assertEquals("gen", fConfig.getName());
        assertEquals(3, fConfig.getPrefetchCount());
        assertEquals(10L, fConfig.getPrefetchValidityMillis());
        assertEquals(1514764800001L, fConfig.getEpochStart());
        assertEquals(30L, fConfig.getNodeIdOffset());
        assertEquals(22, fConfig.getBitsSequence());
        assertEquals(33, fConfig.getBitsNodeId());
        assertEquals(20000L, fConfig.getAllowedFutureMillis());
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
        String yaml = """
                hazelcast:
                  network:
                    join:
                      multicast:
                        enabled: false
                        loopbackModeEnabled: true
                        multicast-group: 224.2.2.4
                        multicast-port: 65438
                        multicast-timeout-seconds: 4
                        multicast-time-to-live: 42
                        trusted-interfaces:
                          - 127.0.0.1
                          - 0.0.0.0
                """;

        Config config = buildConfig(yaml);
        MulticastConfig multicastConfig = config.getNetworkConfig().getJoin().getMulticastConfig();

        assertFalse(multicastConfig.isEnabled());
        assertEquals(Boolean.TRUE, multicastConfig.getLoopbackModeEnabled());
        assertEquals("224.2.2.4", multicastConfig.getMulticastGroup());
        assertEquals(65438, multicastConfig.getMulticastPort());
        assertEquals(4, multicastConfig.getMulticastTimeoutSeconds());
        assertEquals(42, multicastConfig.getMulticastTimeToLive());
        assertEquals(2, multicastConfig.getTrustedInterfaces().size());
        assertTrue(multicastConfig.getTrustedInterfaces().containsAll(Set.of("127.0.0.1", "0.0.0.0")));
    }

    @Override
    @Test
    public void testWanConfig() {
        String yaml = """
                hazelcast:
                  wan-replication:
                    my-wan-cluster:
                      batch-publisher:
                        istanbulPublisherId:
                          cluster-name: istanbul
                          batch-size: 100
                          batch-max-delay-millis: 200
                          response-timeout-millis: 300
                          acknowledge-type: ACK_ON_RECEIPT
                          initial-publisher-state: STOPPED
                          snapshot-enabled: true
                          idle-min-park-ns: 400
                          idle-max-park-ns: 500
                          max-concurrent-invocations: 600
                          discovery-period-seconds: 700
                          use-endpoint-private-address: true
                          queue-full-behavior: THROW_EXCEPTION
                          max-target-endpoints: 800
                          queue-capacity: 21
                          target-endpoints: a,b,c,d
                          aws:
                            enabled: false
                            connection-timeout-seconds: 10
                            access-key: sample-access-key
                            secret-key: sample-secret-key
                            iam-role: sample-role
                            region: sample-region
                            host-header: sample-header
                            security-group-name: sample-group
                            tag-key: sample-tag-key
                            tag-value: sample-tag-value
                          discovery-strategies:
                            node-filter:
                              class: DummyFilterClass
                            discovery-strategies:
                              - class: DummyDiscoveryStrategy1
                                enabled: true
                                properties:
                                  key-string: foo
                                  key-int: 123
                                  key-boolean: true
                          properties:
                            custom.prop.publisher: prop.publisher
                        ankara:
                          queue-full-behavior: THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE
                          initial-publisher-state: STOPPED
                      consumer:
                        class-name: com.hazelcast.wan.custom.WanConsumer
                        properties:
                          custom.prop.consumer: prop.consumer
                        persist-wan-replicated-data: false
                """;

        Config config = buildConfig(yaml);
        WanReplicationConfig wanReplicationConfig = config.getWanReplicationConfig("my-wan-cluster");
        assertNotNull(wanReplicationConfig);

        List<WanBatchPublisherConfig> publisherConfigs = wanReplicationConfig.getBatchPublisherConfigs();
        assertEquals(2, publisherConfigs.size());
        WanBatchPublisherConfig pc1 = publisherConfigs.get(0);
        assertEquals("istanbul", pc1.getClusterName());
        assertEquals("istanbulPublisherId", pc1.getPublisherId());
        assertEquals(100, pc1.getBatchSize());
        assertEquals(200, pc1.getBatchMaxDelayMillis());
        assertEquals(300, pc1.getResponseTimeoutMillis());
        assertEquals(WanAcknowledgeType.ACK_ON_RECEIPT, pc1.getAcknowledgeType());
        assertEquals(WanPublisherState.STOPPED, pc1.getInitialPublisherState());
        assertTrue(pc1.isSnapshotEnabled());
        assertEquals(400, pc1.getIdleMinParkNs());
        assertEquals(500, pc1.getIdleMaxParkNs());
        assertEquals(600, pc1.getMaxConcurrentInvocations());
        assertEquals(700, pc1.getDiscoveryPeriodSeconds());
        assertTrue(pc1.isUseEndpointPrivateAddress());
        assertEquals(THROW_EXCEPTION, pc1.getQueueFullBehavior());
        assertEquals(800, pc1.getMaxTargetEndpoints());
        assertEquals(21, pc1.getQueueCapacity());
        assertEquals("a,b,c,d", pc1.getTargetEndpoints());

        Map<String, Comparable> pubProperties = pc1.getProperties();
        assertEquals("prop.publisher", pubProperties.get("custom.prop.publisher"));
        assertFalse(pc1.getAwsConfig().isEnabled());
        assertAwsConfig(pc1.getAwsConfig());
        assertFalse(pc1.getGcpConfig().isEnabled());
        assertFalse(pc1.getAzureConfig().isEnabled());
        assertFalse(pc1.getKubernetesConfig().isEnabled());
        assertFalse(pc1.getEurekaConfig().isEnabled());
        assertDiscoveryConfig(pc1.getDiscoveryConfig());

        WanBatchPublisherConfig pc2 = publisherConfigs.get(1);
        assertEquals("ankara", pc2.getClusterName());
        assertNull(pc2.getPublisherId());
        assertEquals(WanQueueFullBehavior.THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE, pc2.getQueueFullBehavior());
        assertEquals(WanPublisherState.STOPPED, pc2.getInitialPublisherState());

        WanConsumerConfig consumerConfig = wanReplicationConfig.getConsumerConfig();
        assertEquals("com.hazelcast.wan.custom.WanConsumer", consumerConfig.getClassName());
        Map<String, Comparable> consProperties = consumerConfig.getProperties();
        assertEquals("prop.consumer", consProperties.get("custom.prop.consumer"));
        assertFalse(consumerConfig.isPersistWanReplicatedData());
    }

    protected static Config buildConfig(String yaml) {
        ByteArrayInputStream bis = new ByteArrayInputStream(yaml.getBytes());
        YamlConfigBuilder configBuilder = new YamlConfigBuilder(bis);
        return configBuilder.build();
    }

    private void assertDiscoveryConfig(DiscoveryConfig c) {
        assertEquals("DummyFilterClass", c.getNodeFilterClass());
        assertEquals(1, c.getDiscoveryStrategyConfigs().size());

        Iterator<DiscoveryStrategyConfig> iterator = c.getDiscoveryStrategyConfigs().iterator();
        DiscoveryStrategyConfig config = iterator.next();
        assertEquals("DummyDiscoveryStrategy1", config.getClassName());

        Map<String, Comparable> props = config.getProperties();
        assertEquals("foo", props.get("key-string"));
        assertEquals("123", props.get("key-int"));
        assertEquals("true", props.get("key-boolean"));
    }

    @Override
    @Test
    public void testSplitBrainProtectionConfig() {
        String yaml = """
                hazelcast:
                  split-brain-protection:
                    mySplitBrainProtection:
                      enabled: true
                      minimum-cluster-size: 3
                      function-class-name: com.my.splitbrainprotection.function
                      protect-on: READ
                """;

        Config config = buildConfig(yaml);
        SplitBrainProtectionConfig splitBrainProtectionConfig = config.getSplitBrainProtectionConfig("mySplitBrainProtection");

        assertTrue("split brain protection should be enabled", splitBrainProtectionConfig.isEnabled());
        assertEquals(3, splitBrainProtectionConfig.getMinimumClusterSize());
        assertEquals(SplitBrainProtectionOn.READ, splitBrainProtectionConfig.getProtectOn());
        assertEquals("com.my.splitbrainprotection.function", splitBrainProtectionConfig.getFunctionClassName());
        assertTrue(splitBrainProtectionConfig.getListenerConfigs().isEmpty());
    }

    @Override
    @Test
    public void testSplitBrainProtectionListenerConfig() {
        String yaml = """
                hazelcast:
                  split-brain-protection:
                    mySplitBrainProtection:
                      enabled: true
                      minimum-cluster-size: 3
                      listeners:
                         - com.abc.my.splitbrainprotection.listener
                         - com.abc.my.second.listener
                      function-class-name: com.hazelcast.SomeSplitBrainProtectionFunction
                """;

        Config config = buildConfig(yaml);
        SplitBrainProtectionConfig splitBrainProtectionConfig = config.getSplitBrainProtectionConfig("mySplitBrainProtection");

        assertFalse(splitBrainProtectionConfig.getListenerConfigs().isEmpty());
        assertEquals("com.abc.my.splitbrainprotection.listener",
                splitBrainProtectionConfig.getListenerConfigs().get(0).getClassName());
        assertEquals("com.abc.my.second.listener", splitBrainProtectionConfig.getListenerConfigs().get(1).getClassName());
        assertEquals("com.hazelcast.SomeSplitBrainProtectionFunction", splitBrainProtectionConfig.getFunctionClassName());
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testConfig_whenClassNameAndRecentlyActiveSplitBrainProtectionDefined_exceptionIsThrown() {
        String yaml = """
                hazelcast:
                  split-brain-protection:
                    mysplit-brain-protection:
                      enabled: true
                      minimum-cluster-size: 3
                      function-class-name: com.hazelcast.SomeSplitBrainProtectionFunction
                      recently-active-split-brain-protection: {}""";

        buildConfig(yaml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testConfig_whenClassNameAndProbabilisticSplitBrainProtectionDefined_exceptionIsThrown() {
        String yaml = """
                hazelcast:
                  split-brain-protection:
                    mysplit-brain-protection:
                      enabled: true
                      minimum-cluster-size: 3
                      function-class-name: com.hazelcast.SomeSplitBrainProtectionFunction
                      probabilistic-split-brain-protection: {}""";

        buildConfig(yaml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    @Ignore("Schema validation is supposed to fail, two split brain protection implementation is defined")
    public void testConfig_whenBothBuiltinSplitBrainProtectionsDefined_exceptionIsThrown() {
        String yaml = """
                hazelcast:
                  split-brain-protection:
                    mysplit-brain-protection:
                      enabled: true
                      minimum-cluster-size: 3
                      probabilistic-split-brain-protection: {}
                      recently-active-split-brain-protection: {}
                """;

        buildConfig(yaml);
    }

    @Override
    @Test
    public void testConfig_whenRecentlyActiveSplitBrainProtection_withDefaultValues() {
        String yaml = """
                hazelcast:
                  split-brain-protection:
                    mySplitBrainProtection:
                      enabled: true
                      minimum-cluster-size: 3
                      recently-active-split-brain-protection: {}""";

        Config config = buildConfig(yaml);
        SplitBrainProtectionConfig splitBrainProtectionConfig = config.getSplitBrainProtectionConfig("mySplitBrainProtection");
        assertInstanceOf(RecentlyActiveSplitBrainProtectionFunction.class,
                splitBrainProtectionConfig.getFunctionImplementation());
        RecentlyActiveSplitBrainProtectionFunction splitBrainProtectionFunction = (RecentlyActiveSplitBrainProtectionFunction) splitBrainProtectionConfig
                .getFunctionImplementation();
        assertEquals(RecentlyActiveSplitBrainProtectionConfigBuilder.DEFAULT_HEARTBEAT_TOLERANCE_MILLIS,
                splitBrainProtectionFunction.getHeartbeatToleranceMillis());
    }

    @Override
    @Test
    public void testConfig_whenRecentlyActiveSplitBrainProtection_withCustomValues() {
        String yaml = """
                hazelcast:
                  split-brain-protection:
                    mySplitBrainProtection:
                      enabled: true
                      minimum-cluster-size: 3
                      recently-active-split-brain-protection:
                        heartbeat-tolerance-millis: 13000
                """;

        Config config = buildConfig(yaml);
        SplitBrainProtectionConfig splitBrainProtectionConfig = config.getSplitBrainProtectionConfig("mySplitBrainProtection");
        assertEquals(3, splitBrainProtectionConfig.getMinimumClusterSize());
        assertInstanceOf(RecentlyActiveSplitBrainProtectionFunction.class,
                splitBrainProtectionConfig.getFunctionImplementation());
        RecentlyActiveSplitBrainProtectionFunction splitBrainProtectionFunction = (RecentlyActiveSplitBrainProtectionFunction) splitBrainProtectionConfig
                .getFunctionImplementation();
        assertEquals(13000, splitBrainProtectionFunction.getHeartbeatToleranceMillis());
    }

    @Override
    @Test
    public void testConfig_whenProbabilisticSplitBrainProtection_withDefaultValues() {
        String yaml = """
                hazelcast:
                  split-brain-protection:
                    mySplitBrainProtection:
                      enabled: true
                      minimum-cluster-size: 3
                      probabilistic-split-brain-protection: {}""";

        Config config = buildConfig(yaml);
        SplitBrainProtectionConfig splitBrainProtectionConfig = config.getSplitBrainProtectionConfig("mySplitBrainProtection");
        assertInstanceOf(ProbabilisticSplitBrainProtectionFunction.class, splitBrainProtectionConfig.getFunctionImplementation());
        ProbabilisticSplitBrainProtectionFunction splitBrainProtectionFunction = (ProbabilisticSplitBrainProtectionFunction) splitBrainProtectionConfig.getFunctionImplementation();
        assertEquals(ProbabilisticSplitBrainProtectionConfigBuilder.DEFAULT_HEARTBEAT_INTERVAL_MILLIS,
                splitBrainProtectionFunction.getHeartbeatIntervalMillis());
        assertEquals(ProbabilisticSplitBrainProtectionConfigBuilder.DEFAULT_HEARTBEAT_PAUSE_MILLIS,
                splitBrainProtectionFunction.getAcceptableHeartbeatPauseMillis());
        assertEquals(ProbabilisticSplitBrainProtectionConfigBuilder.DEFAULT_MIN_STD_DEVIATION,
                splitBrainProtectionFunction.getMinStdDeviationMillis());
        assertEquals(ProbabilisticSplitBrainProtectionConfigBuilder.DEFAULT_PHI_THRESHOLD,
                splitBrainProtectionFunction.getSuspicionThreshold(), 0.01);
        assertEquals(ProbabilisticSplitBrainProtectionConfigBuilder.DEFAULT_SAMPLE_SIZE,
                splitBrainProtectionFunction.getMaxSampleSize());
    }

    @Override
    @Test
    public void testConfig_whenProbabilisticSplitBrainProtection_withCustomValues() {
        String yaml = """
                hazelcast:
                  split-brain-protection:
                    mySplitBrainProtection:
                      enabled: true
                      minimum-cluster-size: 3
                      probabilistic-split-brain-protection:
                        acceptable-heartbeat-pause-millis: 37400
                        suspicion-threshold: 3.14592
                        max-sample-size: 42
                        min-std-deviation-millis: 1234
                        heartbeat-interval-millis: 4321""";

        Config config = buildConfig(yaml);
        SplitBrainProtectionConfig splitBrainProtectionConfig = config.getSplitBrainProtectionConfig("mySplitBrainProtection");
        assertInstanceOf(ProbabilisticSplitBrainProtectionFunction.class, splitBrainProtectionConfig.getFunctionImplementation());
        ProbabilisticSplitBrainProtectionFunction splitBrainProtectionFunction = (ProbabilisticSplitBrainProtectionFunction) splitBrainProtectionConfig.getFunctionImplementation();
        assertEquals(4321, splitBrainProtectionFunction.getHeartbeatIntervalMillis());
        assertEquals(37400, splitBrainProtectionFunction.getAcceptableHeartbeatPauseMillis());
        assertEquals(1234, splitBrainProtectionFunction.getMinStdDeviationMillis());
        assertEquals(3.14592d, splitBrainProtectionFunction.getSuspicionThreshold(), 0.001d);
        assertEquals(42, splitBrainProtectionFunction.getMaxSampleSize());
    }

    @Override
    @Test
    public void testCacheConfig() {
        // TODO do we really need to keep the 'class-name' keys?
        String yaml = """
                hazelcast:
                  cache:
                    foobar:
                      split-brain-protection-ref: customSplitBrainProtectionRule
                      key-type:
                        class-name: java.lang.Object
                      value-type:
                        class-name: java.lang.Object
                      statistics-enabled: false
                      management-enabled: false
                      read-through: true
                      write-through: true
                      cache-loader-factory:
                        class-name: com.example.cache.MyCacheLoaderFactory
                      cache-writer-factory:
                        class-name: com.example.cache.MyCacheWriterFactory
                      expiry-policy-factory:
                        class-name: com.example.cache.MyExpirePolicyFactory
                      in-memory-format: BINARY
                      backup-count: 1
                      async-backup-count: 0
                      eviction:
                        size: 1000
                        max-size-policy: ENTRY_COUNT
                        eviction-policy: LFU
                      merge-policy:
                         batch-size: 100
                         class-name: LatestAccessMergePolicy
                      disable-per-entry-invalidation-events: true
                      merkle-tree:
                        enabled: true
                        depth: 20
                      hot-restart:
                        enabled: false
                        fsync: false
                      data-persistence:
                        enabled: true
                        fsync: true
                      event-journal:
                        enabled: true
                        capacity: 120
                        time-to-live-seconds: 20
                      partition-lost-listeners:
                        - com.your-package.YourPartitionLostListener
                      cache-entry-listeners:
                        - old-value-required: false
                          synchronous: false
                          cache-entry-listener-factory:
                            class-name: com.example.cache.MyEntryListenerFactory
                          cache-entry-event-filter-factory:
                            class-name: com.example.cache.MyEntryEventFilterFactory
                      user-code-namespace: ns1
                """;

        Config config = buildConfig(yaml);
        CacheSimpleConfig cacheConfig = config.getCacheConfig("foobar");

        assertFalse(config.getCacheConfigs().isEmpty());
        assertEquals("customSplitBrainProtectionRule", cacheConfig.getSplitBrainProtectionName());
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
        assertEquals("ns1", cacheConfig.getUserCodeNamespace());

        assertEquals(1000, cacheConfig.getEvictionConfig().getSize());
        assertEquals(MaxSizePolicy.ENTRY_COUNT,
                cacheConfig.getEvictionConfig().getMaxSizePolicy());
        assertEquals(EvictionPolicy.LFU, cacheConfig.getEvictionConfig().getEvictionPolicy());
        assertEquals("LatestAccessMergePolicy",
                cacheConfig.getMergePolicyConfig().getPolicy());
        assertTrue(cacheConfig.isDisablePerEntryInvalidationEvents());
        assertTrue(cacheConfig.getMerkleTreeConfig().isEnabled());
        assertEquals(20, cacheConfig.getMerkleTreeConfig().getDepth());
        // overrides by the conflicting dataPersistenceConfig
        assertTrue(cacheConfig.getHotRestartConfig().isEnabled());
        assertTrue(cacheConfig.getHotRestartConfig().isFsync());
        assertTrue(cacheConfig.getDataPersistenceConfig().isEnabled());
        assertTrue(cacheConfig.getDataPersistenceConfig().isFsync());

        EventJournalConfig journalConfig = cacheConfig.getEventJournalConfig();
        assertTrue(journalConfig.isEnabled());
        assertEquals(120, journalConfig.getCapacity());
        assertEquals(20, journalConfig.getTimeToLiveSeconds());

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
        String yaml = """
                hazelcast:
                  executor-service:
                    foobar:
                      pool-size: 2
                      split-brain-protection-ref: customSplitBrainProtectionRule
                      statistics-enabled: false
                      queue-capacity: 0
                      user-code-namespace: ns1
                """;

        Config config = buildConfig(yaml);
        ExecutorConfig executorConfig = config.getExecutorConfig("foobar");

        assertFalse(config.getExecutorConfigs().isEmpty());
        assertEquals(2, executorConfig.getPoolSize());
        assertEquals("customSplitBrainProtectionRule", executorConfig.getSplitBrainProtectionName());
        assertFalse(executorConfig.isStatisticsEnabled());
        assertEquals(0, executorConfig.getQueueCapacity());
        assertEquals("ns1", executorConfig.getUserCodeNamespace());
    }

    @Override
    @Test
    public void testDurableExecutorConfig() {
        String yaml = """
                hazelcast:
                  durable-executor-service:
                    foobar:
                      pool-size: 2
                      durability: 3
                      capacity: 4
                      split-brain-protection-ref: customSplitBrainProtectionRule
                      statistics-enabled: false
                      user-code-namespace: ns1
                """;

        Config config = buildConfig(yaml);
        DurableExecutorConfig durableExecutorConfig = config.getDurableExecutorConfig("foobar");

        assertFalse(config.getDurableExecutorConfigs().isEmpty());
        assertEquals(2, durableExecutorConfig.getPoolSize());
        assertEquals(3, durableExecutorConfig.getDurability());
        assertEquals(4, durableExecutorConfig.getCapacity());
        assertEquals("customSplitBrainProtectionRule", durableExecutorConfig.getSplitBrainProtectionName());
        assertFalse(durableExecutorConfig.isStatisticsEnabled());
        assertEquals("ns1", durableExecutorConfig.getUserCodeNamespace());
    }

    @Override
    @Test
    public void testScheduledExecutorConfig() {
        String yaml = """
                hazelcast:
                  scheduled-executor-service:
                    foobar:
                      durability: 4
                      pool-size: 5
                      capacity: 2
                      split-brain-protection-ref: customSplitBrainProtectionRule
                      statistics-enabled: false
                      merge-policy:
                        batch-size: 99
                        class-name: PutIfAbsent
                      user-code-namespace: ns1
                """;

        Config config = buildConfig(yaml);
        ScheduledExecutorConfig scheduledExecutorConfig = config.getScheduledExecutorConfig("foobar");

        assertFalse(config.getScheduledExecutorConfigs().isEmpty());
        assertEquals(4, scheduledExecutorConfig.getDurability());
        assertEquals(5, scheduledExecutorConfig.getPoolSize());
        assertEquals(2, scheduledExecutorConfig.getCapacity());
        assertEquals("customSplitBrainProtectionRule", scheduledExecutorConfig.getSplitBrainProtectionName());
        assertEquals(99, scheduledExecutorConfig.getMergePolicyConfig().getBatchSize());
        assertEquals("PutIfAbsent", scheduledExecutorConfig.getMergePolicyConfig().getPolicy());
        assertFalse(scheduledExecutorConfig.isStatisticsEnabled());
        assertEquals("ns1", scheduledExecutorConfig.getUserCodeNamespace());
    }

    @Override
    @Test
    public void testCardinalityEstimatorConfig() {
        String yaml = """
                hazelcast:
                  cardinality-estimator:
                    foobar:
                      backup-count: 2
                      async-backup-count: 3
                      split-brain-protection-ref: customSplitBrainProtectionRule
                      merge-policy:
                        class-name: com.hazelcast.spi.merge.HyperLogLogMergePolicy""";

        Config config = buildConfig(yaml);
        CardinalityEstimatorConfig cardinalityEstimatorConfig = config.getCardinalityEstimatorConfig("foobar");

        assertFalse(config.getCardinalityEstimatorConfigs().isEmpty());
        assertEquals(2, cardinalityEstimatorConfig.getBackupCount());
        assertEquals(3, cardinalityEstimatorConfig.getAsyncBackupCount());
        assertEquals("com.hazelcast.spi.merge.HyperLogLogMergePolicy",
                cardinalityEstimatorConfig.getMergePolicyConfig().getPolicy());
        assertEquals("customSplitBrainProtectionRule", cardinalityEstimatorConfig.getSplitBrainProtectionName());
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testCardinalityEstimatorConfigWithInvalidMergePolicy() {
        String yaml = """
                hazelcast:
                  cardinality-estimator:
                    foobar:
                      backup-count: 2
                      async-backup-count: 3
                      split-brain-protection-ref: customSplitBrainProtectionRule
                      merge-policy:
                        class-name: CustomMergePolicy""";

        buildConfig(yaml);
        fail();
    }

    @Override
    @Test
    public void testPNCounterConfig() {
        String yaml = """
                hazelcast:
                  pn-counter:
                    pn-counter-1:
                      replica-count: 100
                      split-brain-protection-ref: splitBrainProtectionRuleWithThreeMembers
                      statistics-enabled: false
                """;

        Config config = buildConfig(yaml);
        PNCounterConfig pnCounterConfig = config.getPNCounterConfig("pn-counter-1");

        assertFalse(config.getPNCounterConfigs().isEmpty());
        assertEquals(100, pnCounterConfig.getReplicaCount());
        assertEquals("splitBrainProtectionRuleWithThreeMembers", pnCounterConfig.getSplitBrainProtectionName());
        assertFalse(pnCounterConfig.isStatisticsEnabled());
    }

    @Override
    @Test
    public void testMultiMapConfig() {
        String yaml = """
                hazelcast:
                  multimap:
                    myMultiMap:
                      backup-count: 2
                      async-backup-count: 3
                      binary: false
                      value-collection-type: SET
                      split-brain-protection-ref: customSplitBrainProtectionRule
                      entry-listeners:
                        - class-name: com.hazelcast.examples.EntryListener
                          include-value: true
                          local: true
                      merge-policy:
                        batch-size: 23
                        class-name: CustomMergePolicy
                      user-code-namespace: ns1
                """;

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
        assertEquals("ns1", multiMapConfig.getUserCodeNamespace());

        MergePolicyConfig mergePolicyConfig = multiMapConfig.getMergePolicyConfig();
        assertEquals("CustomMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals("customSplitBrainProtectionRule", multiMapConfig.getSplitBrainProtectionName());
        assertEquals(23, mergePolicyConfig.getBatchSize());
    }

    @Override
    @Test
    public void testReplicatedMapConfig() {
        String yaml = """
                hazelcast:
                  replicatedmap:
                    foobar:
                      in-memory-format: BINARY
                      async-fillup: false
                      statistics-enabled: false
                      split-brain-protection-ref: customSplitBrainProtectionRule
                      merge-policy:
                        batch-size: 2342
                        class-name: CustomMergePolicy
                      user-code-namespace: ns1
                """;

        Config config = buildConfig(yaml);
        ReplicatedMapConfig replicatedMapConfig = config.getReplicatedMapConfig("foobar");

        assertFalse(config.getReplicatedMapConfigs().isEmpty());
        assertEquals(InMemoryFormat.BINARY, replicatedMapConfig.getInMemoryFormat());
        assertFalse(replicatedMapConfig.isAsyncFillup());
        assertFalse(replicatedMapConfig.isStatisticsEnabled());
        assertEquals("customSplitBrainProtectionRule", replicatedMapConfig.getSplitBrainProtectionName());
        assertEquals("ns1", replicatedMapConfig.getUserCodeNamespace());

        MergePolicyConfig mergePolicyConfig = replicatedMapConfig.getMergePolicyConfig();
        assertEquals("CustomMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(2342, mergePolicyConfig.getBatchSize());
    }

    @Override
    @Test
    public void testListConfig() {
        String yaml = """
                hazelcast:
                  list:
                    foobar:
                      split-brain-protection-ref: customSplitBrainProtectionRule
                      statistics-enabled: false
                      max-size: 42
                      backup-count: 2
                      async-backup-count: 1
                      merge-policy:
                        batch-size: 100
                        class-name: SplitBrainMergePolicy
                      item-listeners:
                         - include-value: true
                           class-name: com.hazelcast.examples.ItemListener
                      user-code-namespace: ns1
                """;

        Config config = buildConfig(yaml);
        ListConfig listConfig = config.getListConfig("foobar");

        assertFalse(config.getListConfigs().isEmpty());
        assertEquals("customSplitBrainProtectionRule", listConfig.getSplitBrainProtectionName());
        assertEquals(42, listConfig.getMaxSize());
        assertEquals(2, listConfig.getBackupCount());
        assertEquals(1, listConfig.getAsyncBackupCount());
        assertEquals(1, listConfig.getItemListenerConfigs().size());
        assertEquals("com.hazelcast.examples.ItemListener", listConfig.getItemListenerConfigs().get(0).getClassName());
        assertEquals("ns1", listConfig.getUserCodeNamespace());

        MergePolicyConfig mergePolicyConfig = listConfig.getMergePolicyConfig();
        assertEquals(100, mergePolicyConfig.getBatchSize());
        assertEquals("SplitBrainMergePolicy", mergePolicyConfig.getPolicy());
    }

    @Override
    @Test
    public void testSetConfig() {
        String yaml = """
                hazelcast:
                  set:
                    foobar:
                     split-brain-protection-ref: customSplitBrainProtectionRule
                     backup-count: 2
                     async-backup-count: 1
                     max-size: 42
                     merge-policy:
                       batch-size: 42
                       class-name: SplitBrainMergePolicy
                     item-listeners:
                         - include-value: true
                           class-name: com.hazelcast.examples.ItemListener
                     user-code-namespace: ns1
                """;

        Config config = buildConfig(yaml);
        SetConfig setConfig = config.getSetConfig("foobar");

        assertFalse(config.getSetConfigs().isEmpty());
        assertEquals("customSplitBrainProtectionRule", setConfig.getSplitBrainProtectionName());
        assertEquals(2, setConfig.getBackupCount());
        assertEquals(1, setConfig.getAsyncBackupCount());
        assertEquals(42, setConfig.getMaxSize());
        assertEquals(1, setConfig.getItemListenerConfigs().size());
        assertTrue(setConfig.getItemListenerConfigs().get(0).isIncludeValue());
        assertEquals("com.hazelcast.examples.ItemListener", setConfig.getItemListenerConfigs().get(0).getClassName());
        assertEquals("ns1", setConfig.getUserCodeNamespace());

        MergePolicyConfig mergePolicyConfig = setConfig.getMergePolicyConfig();
        assertEquals(42, mergePolicyConfig.getBatchSize());
        assertEquals("SplitBrainMergePolicy", mergePolicyConfig.getPolicy());
    }

    @Override
    @Test
    public void testMapConfig() {
        String yaml = """
                hazelcast:
                  map:
                    foobar:
                      split-brain-protection-ref: customSplitBrainProtectionRule
                      in-memory-format: BINARY
                      statistics-enabled: true
                      cache-deserialized-values: INDEX-ONLY
                      backup-count: 2
                      async-backup-count: 1
                      time-to-live-seconds: 42
                      max-idle-seconds: 42
                      eviction:
                         eviction-policy: RANDOM
                         max-size-policy: PER_NODE
                         size: 42
                      read-backup-data: true
                      merkle-tree:
                        enabled: true
                        depth: 20
                      event-journal:
                        enabled: true
                        capacity: 120
                        time-to-live-seconds: 20
                      hot-restart:
                        enabled: false
                        fsync: false
                      data-persistence:
                        enabled: true
                        fsync: true
                      map-store:
                        enabled: true\s
                        initial-mode: LAZY
                        class-name: com.hazelcast.examples.DummyStore
                        write-delay-seconds: 42
                        write-batch-size: 42
                        write-coalescing: true
                        properties:
                           jdbc_url: my.jdbc.com
                      near-cache:
                        time-to-live-seconds: 42
                        max-idle-seconds: 42
                        invalidate-on-change: true
                        in-memory-format: BINARY
                        cache-local-entries: false
                        eviction:
                          size: 1000
                          max-size-policy: ENTRY_COUNT
                          eviction-policy: LFU
                      wan-replication-ref:
                        my-wan-cluster-batch:
                          merge-policy-class-name: PassThroughMergePolicy
                          filters:
                            - com.example.SampleFilter
                          republishing-enabled: false
                      indexes:
                        - attributes:
                          - "age"
                      attributes:
                        currency:
                          extractor-class-name: com.bank.CurrencyExtractor
                      partition-lost-listeners:
                         - com.your-package.YourPartitionLostListener
                      entry-listeners:
                         - class-name: com.your-package.MyEntryListener
                           include-value: false
                           local: false
                      user-code-namespace: ns1
                """;

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("foobar");

        assertFalse(config.getMapConfigs().isEmpty());
        assertEquals("customSplitBrainProtectionRule", mapConfig.getSplitBrainProtectionName());
        assertEquals(InMemoryFormat.BINARY, mapConfig.getInMemoryFormat());
        assertTrue(mapConfig.isStatisticsEnabled());
        assertEquals(CacheDeserializedValues.INDEX_ONLY, mapConfig.getCacheDeserializedValues());
        assertEquals(2, mapConfig.getBackupCount());
        assertEquals(1, mapConfig.getAsyncBackupCount());
        assertEquals(1, mapConfig.getAsyncBackupCount());
        assertEquals(42, mapConfig.getTimeToLiveSeconds());
        assertEquals(42, mapConfig.getMaxIdleSeconds());
        assertEquals(EvictionPolicy.RANDOM, mapConfig.getEvictionConfig().getEvictionPolicy());
        assertEquals(MaxSizePolicy.PER_NODE, mapConfig.getEvictionConfig().getMaxSizePolicy());
        assertEquals(42, mapConfig.getEvictionConfig().getSize());
        assertTrue(mapConfig.isReadBackupData());
        assertEquals("ns1", mapConfig.getUserCodeNamespace());

        assertEquals(1, mapConfig.getIndexConfigs().size());
        assertEquals("age", mapConfig.getIndexConfigs().get(0).getAttributes().get(0));
        assertSame(mapConfig.getIndexConfigs().get(0).getType(), IndexType.SORTED);
        assertEquals(1, mapConfig.getAttributeConfigs().size());
        assertEquals("com.bank.CurrencyExtractor", mapConfig.getAttributeConfigs().get(0).getExtractorClassName());
        assertEquals("currency", mapConfig.getAttributeConfigs().get(0).getName());
        assertEquals(1, mapConfig.getPartitionLostListenerConfigs().size());
        assertEquals("com.your-package.YourPartitionLostListener",
                mapConfig.getPartitionLostListenerConfigs().get(0).getClassName());
        assertEquals(1, mapConfig.getEntryListenerConfigs().size());
        assertFalse(mapConfig.getEntryListenerConfigs().get(0).isIncludeValue());
        assertFalse(mapConfig.getEntryListenerConfigs().get(0).isLocal());
        assertEquals("com.your-package.MyEntryListener", mapConfig.getEntryListenerConfigs().get(0).getClassName());
        assertTrue(mapConfig.getMerkleTreeConfig().isEnabled());
        assertEquals(20, mapConfig.getMerkleTreeConfig().getDepth());
        // conflict with dataPersistenceConfig, so overrides occur
        assertTrue(mapConfig.getHotRestartConfig().isEnabled());
        assertTrue(mapConfig.getHotRestartConfig().isFsync());
        assertTrue(mapConfig.getDataPersistenceConfig().isEnabled());
        assertTrue(mapConfig.getDataPersistenceConfig().isFsync());

        EventJournalConfig journalConfig = mapConfig.getEventJournalConfig();
        assertTrue(journalConfig.isEnabled());
        assertEquals(120, journalConfig.getCapacity());
        assertEquals(20, journalConfig.getTimeToLiveSeconds());

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
        assertEquals(42, nearCacheConfig.getMaxIdleSeconds());
        assertEquals(42, nearCacheConfig.getTimeToLiveSeconds());
        assertEquals(InMemoryFormat.BINARY, nearCacheConfig.getInMemoryFormat());
        assertFalse(nearCacheConfig.isCacheLocalEntries());
        assertTrue(nearCacheConfig.isInvalidateOnChange());
        assertEquals(1000, nearCacheConfig.getEvictionConfig().getSize());
        assertEquals(EvictionPolicy.LFU, nearCacheConfig.getEvictionConfig().getEvictionPolicy());
        assertEquals(MaxSizePolicy.ENTRY_COUNT, nearCacheConfig.getEvictionConfig().getMaxSizePolicy());

        WanReplicationRef wanReplicationRef = mapConfig.getWanReplicationRef();
        assertNotNull(wanReplicationRef);
        assertFalse(wanReplicationRef.isRepublishingEnabled());
        assertEquals("PassThroughMergePolicy", wanReplicationRef.getMergePolicyClassName());
        assertEquals(1, wanReplicationRef.getFilters().size());
        assertEquals(lowerCaseInternal("com.example.SampleFilter"), lowerCaseInternal(wanReplicationRef.getFilters().get(0)));
    }

    @Override
    public void testMapCustomEvictionPolicy() {
        String comparatorClassName = "com.my.custom.eviction.policy.class";

        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    mappy:\n"
                + "      eviction:\n"
                + "         comparator-class-name: " + comparatorClassName + "\n";

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("mappy");

        assertEquals(comparatorClassName, mapConfig.getEvictionConfig().getComparatorClassName());
    }

    @Override
    @Test
    public void testIndexesConfig() {
        String yaml = """
                hazelcast:
                  map:
                    people:
                      indexes:
                        - type: HASH
                          attributes:
                            - "name"
                        - attributes:
                          - "age"
                        - type: SORTED
                          attributes:
                            - "age"
                          btree-index:
                            page-size:
                              value: 1337
                              unit: BYTES
                            memory-tier:\s
                              capacity:\s
                                value: 1138
                                unit: BYTES
                """;

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("people");

        List<IndexConfig> indexConfigs = mapConfig.getIndexConfigs();
        assertFalse(indexConfigs.isEmpty());
        assertIndexEqual("name", false, indexConfigs.get(0));
        assertIndexEqual("age", true, indexConfigs.get(1));
        assertIndexEqual("age", true, indexConfigs.get(2));
        BTreeIndexConfig bTreeIndexConfig = indexConfigs.get(2).getBTreeIndexConfig();
        assertEquals(Capacity.of(1337, MemoryUnit.BYTES), bTreeIndexConfig.getPageSize());
        assertEquals(Capacity.of(1138, MemoryUnit.BYTES), bTreeIndexConfig.getMemoryTierConfig().getCapacity());
    }

    @Override
    @Test
    public void testAttributeConfig() {
        String yaml = """
                hazelcast:
                  map:
                    people:
                      attributes:
                        power:
                          extractor-class-name: com.car.PowerExtractor
                        weight:
                          extractor-class-name: com.car.WeightExtractor
                """;

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("people");

        assertFalse(mapConfig.getAttributeConfigs().isEmpty());
        assertAttributeEqual("power", "com.car.PowerExtractor", mapConfig.getAttributeConfigs().get(0));
        assertAttributeEqual("weight", "com.car.WeightExtractor", mapConfig.getAttributeConfigs().get(1));
    }

    @Override
    @Test(expected = SchemaViolationConfigurationException.class)
    public void testAttributeConfig_noName_emptyTag() {
        String yaml = """
                hazelcast:
                  map:
                    people:
                      attributes:
                        - extractor-class-name: com.car.WeightExtractor
                """;

        buildConfig(yaml);
    }

    private static void assertAttributeEqual(String expectedName, String expectedExtractor, AttributeConfig attributeConfig) {
        assertEquals(expectedName, attributeConfig.getName());
        assertEquals(expectedExtractor, attributeConfig.getExtractorClassName());
    }

    @Override
    @Test(expected = SchemaViolationConfigurationException.class)
    public void testAttributeConfig_noName_singleTag() {
        String yaml = """
                hazelcast:
                  map:
                   people:
                     attributes:
                       - extractor-class-name: com.car.WeightExtractor
                """;
        buildConfig(yaml);
    }

    @Override
    @Test(expected = SchemaViolationConfigurationException.class)
    public void testAttributeConfig_noExtractor() {
        String yaml = """
                hazelcast:
                  map:
                    people:
                      attributes:
                        weight: {}
                """;
        buildConfig(yaml);
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void testAttributeConfig_emptyExtractor() {
        String yaml = """
                hazelcast:
                  map:
                    people:
                      attributes:
                        weight:
                          extractor-class-name: ""
                """;
        buildConfig(yaml);
    }

    @Override
    @Test
    public void testQueryCacheFullConfig() {
        String yaml = """
                hazelcast:
                  map:
                    test:
                      query-caches:
                        cache-name:
                          entry-listeners:
                            - class-name: com.hazelcast.examples.EntryListener
                              include-value: true
                              local: false
                          include-value: true
                          batch-size: 1
                          buffer-size: 16
                          delay-seconds: 0
                          in-memory-format: BINARY
                          coalesce: false
                          populate: true
                          serialize-keys: true
                          indexes:
                            - type: HASH
                              attributes:
                                - "name"
                          predicate:
                            class-name: com.hazelcast.examples.SimplePredicate
                          eviction:
                            eviction-policy: LRU
                            max-size-policy: ENTRY_COUNT
                            size: 133
                """;

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
        assertTrue(queryCacheConfig.isSerializeKeys());
        assertIndexesEqual(queryCacheConfig);
        assertEquals("com.hazelcast.examples.SimplePredicate", queryCacheConfig.getPredicateConfig().getClassName());
        assertEquals(LRU, queryCacheConfig.getEvictionConfig().getEvictionPolicy());
        assertEquals(ENTRY_COUNT, queryCacheConfig.getEvictionConfig().getMaxSizePolicy());
        assertEquals(133, queryCacheConfig.getEvictionConfig().getSize());
    }

    private void assertIndexesEqual(QueryCacheConfig queryCacheConfig) {
        for (IndexConfig indexConfig : queryCacheConfig.getIndexConfigs()) {
            assertEquals("name", indexConfig.getAttributes().get(0));
            assertNotSame(indexConfig.getType(), IndexType.SORTED);
        }
    }

    @Override
    @Test
    public void testMapQueryCachePredicate() {
        String yaml = """
                hazelcast:
                  map:
                    test:
                      query-caches:
                        cache-class-name:
                          predicate:
                            class-name: com.hazelcast.examples.SimplePredicate
                        cache-sql:
                          predicate:
                            sql: "%age=40"
                """;

        Config config = buildConfig(yaml);
        QueryCacheConfig queryCacheClassNameConfig = config.getMapConfig("test").getQueryCacheConfigs().get(0);
        assertEquals("com.hazelcast.examples.SimplePredicate", queryCacheClassNameConfig.getPredicateConfig().getClassName());

        QueryCacheConfig queryCacheSqlConfig = config.getMapConfig("test").getQueryCacheConfigs().get(1);
        assertEquals("%age=40", queryCacheSqlConfig.getPredicateConfig().getSql());
    }

    @Override
    @Test
    public void testLiteMemberConfig() {
        String yaml = """
                hazelcast:
                  lite-member:
                    enabled: true
                """;

        Config config = buildConfig(yaml);

        assertTrue(config.isLiteMember());
    }

    @Override
    @Test
    public void testNonLiteMemberConfig() {
        String yaml = """
                hazelcast:
                  lite-member:
                    enabled: false
                """;

        Config config = buildConfig(yaml);

        assertFalse(config.isLiteMember());
    }

    @Override
    @Test(expected = SchemaViolationConfigurationException.class)
    public void testNonLiteMemberConfigWithoutEnabledField() {
        String yaml = """
                hazelcast:
                  lite-member: {}
                """;

        buildConfig(yaml);
    }

    @Override
    @Test(expected = SchemaViolationConfigurationException.class)
    public void testInvalidLiteMemberConfig() {
        String yaml = """
                hazelcast:
                  lite-member:
                    enabled: dummytext
                """;

        buildConfig(yaml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testDuplicateLiteMemberConfig() {
        String yaml = """
                hazelcast:
                  lite-member:
                    enabled: true
                  lite-member:
                    enabled: true
                """;

        buildConfig(yaml);
        fail();
    }

    private static void assertIndexEqual(String expectedAttribute, boolean expectedOrdered, IndexConfig indexConfig) {
        assertEquals(expectedAttribute, indexConfig.getAttributes().get(0));
        assertEquals(expectedOrdered, indexConfig.getType() == IndexType.SORTED);
    }

    @Override
    @Test
    public void testMapNativeMaxSizePolicy() {
        String yamlFormat = """
                hazelcast:
                  map:
                    mymap:
                      in-memory-format: NATIVE
                      eviction:
                        max-size-policy: "{0}"
                        size: 9991
                """;

        MessageFormat messageFormat = new MessageFormat(yamlFormat);

        MaxSizePolicy[] maxSizePolicies = MaxSizePolicy.values();
        for (MaxSizePolicy maxSizePolicy : maxSizePolicies) {
            if (maxSizePolicy == ENTRY_COUNT) {
                // imap does not support ENTRY_COUNT
                continue;
            }
            Object[] objects = {maxSizePolicy.toString()};
            String yaml = messageFormat.format(objects);
            Config config = buildConfig(yaml);
            MapConfig mapConfig = config.getMapConfig("mymap");
            EvictionConfig evictionConfig = mapConfig.getEvictionConfig();

            assertEquals(9991, evictionConfig.getSize());
            assertEquals(maxSizePolicy, evictionConfig.getMaxSizePolicy());
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
        String yaml = """
                hazelcast:
                  user-code-deployment:
                    enabled: true
                    class-cache-mode: OFF
                    provider-mode: LOCAL_CLASSES_ONLY
                    blacklist-prefixes: com.blacklisted,com.other.blacklisted
                    whitelist-prefixes: com.whitelisted,com.other.whitelisted
                    provider-filter: HAS_ATTRIBUTE:foo
                """;

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
    public void testEmptyUserCodeDeployment() {
        String yaml = """
                hazelcast:
                  user-code-deployment:
                    enabled: true
                """;

        Config config = buildConfig(yaml);
        UserCodeDeploymentConfig userCodeDeploymentConfig = config.getUserCodeDeploymentConfig();
        assertTrue(userCodeDeploymentConfig.isEnabled());
        assertEquals(UserCodeDeploymentConfig.ClassCacheMode.ETERNAL, userCodeDeploymentConfig.getClassCacheMode());
        assertEquals(UserCodeDeploymentConfig.ProviderMode.LOCAL_AND_CACHED_CLASSES, userCodeDeploymentConfig.getProviderMode());
        assertNull(userCodeDeploymentConfig.getBlacklistedPrefixes());
        assertNull(userCodeDeploymentConfig.getWhitelistedPrefixes());
        assertNull(userCodeDeploymentConfig.getProviderFilter());
    }

    @Override
    @Test
    public void testCRDTReplicationConfig() {
        final String yaml = """
                hazelcast:
                  crdt-replication:
                    max-concurrent-replication-targets: 10
                    replication-period-millis: 2000
                """;

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
        String yaml = """
                hazelcast:
                  serialization:
                    java-serialization-filter:
                      defaults-disabled: true
                      whitelist:
                        class:
                          - java.lang.String
                          - example.Foo
                        package:
                          - com.acme.app
                          - com.acme.app.subpkg
                        prefix:
                          - java
                          - com.hazelcast.
                          - "["
                      blacklist:
                        class:
                          - com.acme.app.BeanComparator
                """;

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
    public void testJavaReflectionFilter() {
        String yaml = """
                hazelcast:
                  sql:
                    java-reflection-filter:
                      defaults-disabled: true
                      whitelist:
                        class:
                          - java.lang.String
                          - example.Foo
                        package:
                          - com.acme.app
                          - com.acme.app.subpkg
                        prefix:
                          - java
                          - com.hazelcast.
                          - "["
                      blacklist:
                        class:
                          - com.acme.app.BeanComparator
                """;

        Config config = new InMemoryYamlConfig(yaml);
        JavaSerializationFilterConfig javaReflectionFilterConfig
                = config.getSqlConfig().getJavaReflectionFilterConfig();
        assertNotNull(javaReflectionFilterConfig);
        ClassFilter blackList = javaReflectionFilterConfig.getBlacklist();
        assertNotNull(blackList);
        ClassFilter whiteList = javaReflectionFilterConfig.getWhitelist();
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
    public void testCompactSerialization_serializerRegistration() {
        String yaml = """
                hazelcast:
                    serialization:
                        compact-serialization:
                            serializers:
                                - serializer: example.serialization.SerializableEmployeeDTOSerializer
                """;

        Config config = buildConfig(yaml);
        CompactTestUtil.verifyExplicitSerializerIsUsed(config.getSerializationConfig());
    }

    @Override
    public void testCompactSerialization_classRegistration() {
        String yaml = """
                hazelcast:
                    serialization:
                        compact-serialization:
                            classes:
                                - class: example.serialization.ExternalizableEmployeeDTO
                """;

        Config config = buildConfig(yaml);
        CompactTestUtil.verifyReflectiveSerializerIsUsed(config.getSerializationConfig());
    }

    @Override
    public void testCompactSerialization_serializerAndClassRegistration() {
        String yaml = """
                hazelcast:
                    serialization:
                        compact-serialization:
                            serializers:
                                - serializer: example.serialization.SerializableEmployeeDTOSerializer
                            classes:
                                - class: example.serialization.ExternalizableEmployeeDTO
                """;

        SerializationConfig config = buildConfig(yaml).getSerializationConfig();
        CompactTestUtil.verifyExplicitSerializerIsUsed(config);
        CompactTestUtil.verifyReflectiveSerializerIsUsed(config);
    }

    @Override
    public void testCompactSerialization_duplicateSerializerRegistration() {
        String yaml = """
                hazelcast:
                    serialization:
                        compact-serialization:
                            serializers:
                                - serializer: example.serialization.EmployeeDTOSerializer
                                - serializer: example.serialization.EmployeeDTOSerializer
                """;

        SerializationConfig config = buildConfig(yaml).getSerializationConfig();
        assertThatThrownBy(() -> CompactTestUtil.verifySerializationServiceBuilds(config))
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("Duplicate");
    }

    @Override
    public void testCompactSerialization_duplicateClassRegistration() {
        String yaml = """
                hazelcast:
                    serialization:
                        compact-serialization:
                            classes:
                                - class: example.serialization.ExternalizableEmployeeDTO
                                - class: example.serialization.ExternalizableEmployeeDTO
                """;

        SerializationConfig config = buildConfig(yaml).getSerializationConfig();
        assertThatThrownBy(() -> CompactTestUtil.verifySerializationServiceBuilds(config))
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("Duplicate");
    }

    @Override
    public void testCompactSerialization_registrationsWithDuplicateClasses() {
        String yaml = """
                hazelcast:
                    serialization:
                        compact-serialization:
                            serializers:
                                - serializer: example.serialization.EmployeeDTOSerializer
                                - serializer: example.serialization.SameClassEmployeeDTOSerializer
                """;

        SerializationConfig config = buildConfig(yaml).getSerializationConfig();
        assertThatThrownBy(() -> CompactTestUtil.verifySerializationServiceBuilds(config))
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("Duplicate")
                .hasMessageContaining("class");
    }

    @Override
    public void testCompactSerialization_registrationsWithDuplicateTypeNames() {
        String yaml = """
                hazelcast:
                    serialization:
                        compact-serialization:
                            serializers:
                                - serializer: example.serialization.EmployeeDTOSerializer
                                - serializer: example.serialization.SameTypeNameEmployeeDTOSerializer
                """;

        SerializationConfig config = buildConfig(yaml).getSerializationConfig();
        assertThatThrownBy(() -> CompactTestUtil.verifySerializationServiceBuilds(config))
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("Duplicate")
                .hasMessageContaining("type name");
    }

    @Override
    public void testCompactSerialization_withInvalidSerializer() {
        String yaml = """
                hazelcast:
                    serialization:
                        compact-serialization:
                            serializers:
                                - serializer: does.not.exist.FooSerializer
                """;

        SerializationConfig config = buildConfig(yaml).getSerializationConfig();
        assertThatThrownBy(() -> CompactTestUtil.verifySerializationServiceBuilds(config))
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("Cannot create an instance");
    }

    @Override
    public void testCompactSerialization_withInvalidCompactSerializableClass() {
        String yaml = """
                hazelcast:
                    serialization:
                        compact-serialization:
                            classes:
                                - class: does.not.exist.Foo
                """;

        SerializationConfig config = buildConfig(yaml).getSerializationConfig();
        assertThatThrownBy(() -> CompactTestUtil.verifySerializationServiceBuilds(config))
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("Cannot load");
    }

    @Override
    @Test
    public void testAllowOverrideDefaultSerializers() {
        final String yaml = """
                hazelcast:
                  serialization:
                    allow-override-default-serializers: true
                """;

        final Config config = new InMemoryYamlConfig(yaml);
        final boolean isAllowOverrideDefaultSerializers
                = config.getSerializationConfig().isAllowOverrideDefaultSerializers();
        assertTrue(isAllowOverrideDefaultSerializers);
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
                + "    auto-remove-stale-data: true\n"
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
        assertTrue(hotRestartPersistenceConfig.isAutoRemoveStaleData());
        assertEquals(new File(dir).getAbsolutePath(), hotRestartPersistenceConfig.getBaseDir().getAbsolutePath());
        assertEquals(new File(backupDir).getAbsolutePath(), hotRestartPersistenceConfig.getBackupDir().getAbsolutePath());
        assertEquals(parallelism, hotRestartPersistenceConfig.getParallelism());
        assertEquals(validationTimeout, hotRestartPersistenceConfig.getValidationTimeoutSeconds());
        assertEquals(dataLoadTimeout, hotRestartPersistenceConfig.getDataLoadTimeoutSeconds());
        assertEquals(policy, hotRestartPersistenceConfig.getClusterDataRecoveryPolicy());
    }

    @Override
    @Test
    public void testPersistence() {
        String dir = "/mnt/persistence-root/";
        String backupDir = "/mnt/persistence-backup/";
        int parallelism = 3;
        int validationTimeout = 13131;
        int dataLoadTimeout = 45454;
        int rebalanceDelaySeconds = 240;
        PersistenceClusterDataRecoveryPolicy policy = PersistenceClusterDataRecoveryPolicy.PARTIAL_RECOVERY_MOST_RECENT;
        String yaml = ""
                + "hazelcast:\n"
                + "  persistence:\n"
                + "    enabled: true\n"
                + "    auto-remove-stale-data: true\n"
                + "    base-dir: " + dir + "\n"
                + "    backup-dir: " + backupDir + "\n"
                + "    parallelism: " + parallelism + "\n"
                + "    validation-timeout-seconds: " + validationTimeout + "\n"
                + "    data-load-timeout-seconds: " + dataLoadTimeout + "\n"
                + "    cluster-data-recovery-policy: " + policy + "\n"
                + "    rebalance-delay-seconds: " + rebalanceDelaySeconds + "\n";

        Config config = new InMemoryYamlConfig(yaml);
        PersistenceConfig persistenceConfig = config.getPersistenceConfig();

        assertTrue(persistenceConfig.isEnabled());
        assertTrue(persistenceConfig.isAutoRemoveStaleData());
        assertEquals(new File(dir).getAbsolutePath(), persistenceConfig.getBaseDir().getAbsolutePath());
        assertEquals(new File(backupDir).getAbsolutePath(), persistenceConfig.getBackupDir().getAbsolutePath());
        assertEquals(parallelism, persistenceConfig.getParallelism());
        assertEquals(validationTimeout, persistenceConfig.getValidationTimeoutSeconds());
        assertEquals(dataLoadTimeout, persistenceConfig.getDataLoadTimeoutSeconds());
        assertEquals(policy, persistenceConfig.getClusterDataRecoveryPolicy());
        assertEquals(rebalanceDelaySeconds, persistenceConfig.getRebalanceDelaySeconds());
    }

    @Override
    @Test
    public void testDynamicConfig() {
        boolean persistenceEnabled = true;
        String backupDir = "/mnt/dynamic-configuration/backup-dir";
        int backupCount = 7;

        String yaml = ""
                + "hazelcast:\n"
                + "  dynamic-configuration:\n"
                + "    persistence-enabled: " + persistenceEnabled + "\n"
                + "    backup-dir: " + backupDir + "\n"
                + "    backup-count: " + backupCount + "\n";

        Config config = new InMemoryYamlConfig(yaml);
        DynamicConfigurationConfig dynamicConfigurationConfig = config.getDynamicConfigurationConfig();

        assertEquals(persistenceEnabled, dynamicConfigurationConfig.isPersistenceEnabled());
        assertEquals(new File(backupDir).getAbsolutePath(), dynamicConfigurationConfig.getBackupDir().getAbsolutePath());
        assertEquals(backupCount, dynamicConfigurationConfig.getBackupCount());

        yaml = ""
                + "hazelcast:\n"
                + "  dynamic-configuration:\n"
                + "    persistence-enabled: " + persistenceEnabled + "\n";

        config = new InMemoryYamlConfig(yaml);
        dynamicConfigurationConfig = config.getDynamicConfigurationConfig();

        assertEquals(persistenceEnabled, dynamicConfigurationConfig.isPersistenceEnabled());
        assertEquals(new File(DEFAULT_BACKUP_DIR).getAbsolutePath(), dynamicConfigurationConfig.getBackupDir().getAbsolutePath());
        assertEquals(DEFAULT_BACKUP_COUNT, dynamicConfigurationConfig.getBackupCount());
    }

    @Override
    @Test
    public void testLocalDevice() {
        String baseDir = "base-directory";
        int blockSize = 2048;
        int readIOThreadCount = 16;
        int writeIOThreadCount = 1;

        String yaml = ""
                + "hazelcast:\n";
        Config config = new InMemoryYamlConfig(yaml);

        // default device
        LocalDeviceConfig localDeviceConfig = config.getDeviceConfig(DEFAULT_DEVICE_NAME);
        assertEquals(DEFAULT_DEVICE_NAME, localDeviceConfig.getName());
        assertEquals(new File(DEFAULT_DEVICE_BASE_DIR).getAbsoluteFile(), localDeviceConfig.getBaseDir());
        assertEquals(DEFAULT_BLOCK_SIZE_IN_BYTES, localDeviceConfig.getBlockSize());
        assertEquals(DEFAULT_READ_IO_THREAD_COUNT, localDeviceConfig.getReadIOThreadCount());
        assertEquals(DEFAULT_WRITE_IO_THREAD_COUNT, localDeviceConfig.getWriteIOThreadCount());
        assertEquals(LocalDeviceConfig.DEFAULT_CAPACITY, localDeviceConfig.getCapacity());


        yaml = ""
                + "hazelcast:\n"
                + "  local-device:\n"
                + "    my-device:\n"
                + "      base-dir: " + baseDir + "\n"
                + "      capacity:\n"
                + "        unit: GIGABYTES\n"
                + "        value: 100\n"
                + "      block-size: " + blockSize + "\n"
                + "      read-io-thread-count: " + readIOThreadCount + "\n"
                + "      write-io-thread-count: " + writeIOThreadCount + "\n";

        config = new InMemoryYamlConfig(yaml);
        localDeviceConfig = config.getDeviceConfig("my-device");

        assertEquals("my-device", localDeviceConfig.getName());
        assertEquals(new File(baseDir).getAbsolutePath(), localDeviceConfig.getBaseDir().getAbsolutePath());
        assertEquals(blockSize, localDeviceConfig.getBlockSize());
        assertEquals(new Capacity(100, MemoryUnit.GIGABYTES), localDeviceConfig.getCapacity());
        assertEquals(readIOThreadCount, localDeviceConfig.getReadIOThreadCount());
        assertEquals(writeIOThreadCount, localDeviceConfig.getWriteIOThreadCount());

        int device0Multiplier = 2;
        int device1Multiplier = 4;
        yaml = ""
                + "hazelcast:\n"
                + "  local-device:\n"
                + "    device0:\n"
                + "      capacity:\n"
                + "        unit: MEGABYTES\n"
                + "        value: 1234567890\n"
                + "      block-size: " + (blockSize * device0Multiplier) + "\n"
                + "      read-io-thread-count: " + (readIOThreadCount * device0Multiplier) + "\n"
                + "      write-io-thread-count: " + (writeIOThreadCount * device0Multiplier) + "\n"
                + "    device1:\n"
                + "      block-size: " + (blockSize * device1Multiplier) + "\n"
                + "      read-io-thread-count: " + (readIOThreadCount * device1Multiplier) + "\n"
                + "      write-io-thread-count: " + (writeIOThreadCount * device1Multiplier) + "\n";

        config = new InMemoryYamlConfig(yaml);
        // default device is removed.
        assertEquals(2, config.getDeviceConfigs().size());
        assertNull(config.getDeviceConfig(DEFAULT_DEVICE_NAME));

        localDeviceConfig = config.getDeviceConfig("device0");
        assertEquals(blockSize * device0Multiplier, localDeviceConfig.getBlockSize());
        assertEquals(readIOThreadCount * device0Multiplier, localDeviceConfig.getReadIOThreadCount());
        assertEquals(writeIOThreadCount * device0Multiplier, localDeviceConfig.getWriteIOThreadCount());
        assertEquals(new Capacity(1234567890, MemoryUnit.MEGABYTES), localDeviceConfig.getCapacity());

        localDeviceConfig = config.getDeviceConfig("device1");
        assertEquals(blockSize * device1Multiplier, localDeviceConfig.getBlockSize());
        assertEquals(readIOThreadCount * device1Multiplier, localDeviceConfig.getReadIOThreadCount());
        assertEquals(writeIOThreadCount * device1Multiplier, localDeviceConfig.getWriteIOThreadCount());

        // override the default device config
        String newBaseDir = "/some/random/base/dir/for/tiered/store";
        yaml = ""
                + "hazelcast:\n"
                + "  local-device:\n"
                + "    " + DEFAULT_DEVICE_NAME + ":\n"
                + "      base-dir: " + newBaseDir + "\n"
                + "      block-size: " + (DEFAULT_BLOCK_SIZE_IN_BYTES * 2) + "\n"
                + "      read-io-thread-count: " + (DEFAULT_READ_IO_THREAD_COUNT * 2) + "\n";

        config = new InMemoryYamlConfig(yaml);
        assertEquals(1, config.getDeviceConfigs().size());

        localDeviceConfig = config.getDeviceConfig(DEFAULT_DEVICE_NAME);
        assertEquals(DEFAULT_DEVICE_NAME, localDeviceConfig.getName());
        assertEquals(new File(newBaseDir).getAbsoluteFile(), localDeviceConfig.getBaseDir());
        assertEquals(2 * DEFAULT_BLOCK_SIZE_IN_BYTES, localDeviceConfig.getBlockSize());
        assertEquals(2 * DEFAULT_READ_IO_THREAD_COUNT, localDeviceConfig.getReadIOThreadCount());
        assertEquals(DEFAULT_WRITE_IO_THREAD_COUNT, localDeviceConfig.getWriteIOThreadCount());
    }

    @Override
    @Test
    public void testTieredStore() {
        String yaml = """
                hazelcast:
                  map:
                    map0:
                      tiered-store:
                        enabled: true
                        memory-tier:
                          capacity:
                            unit: MEGABYTES
                            value: 1024
                        disk-tier:
                          enabled: true
                          device-name: local-device
                    map1:
                      tiered-store:
                        enabled: true
                        disk-tier:
                          enabled: true
                    map2:
                      tiered-store:
                        enabled: true
                        memory-tier:
                          capacity:
                            unit: GIGABYTES
                            value: 1
                    map3:
                      tiered-store:
                        enabled: true
                """;

        Config config = new InMemoryYamlConfig(yaml);
        TieredStoreConfig tieredStoreConfig = config.getMapConfig("map0").getTieredStoreConfig();
        assertTrue(tieredStoreConfig.isEnabled());

        MemoryTierConfig memoryTierConfig = tieredStoreConfig.getMemoryTierConfig();
        assertEquals(MemoryUnit.MEGABYTES, memoryTierConfig.getCapacity().getUnit());
        assertEquals(1024, memoryTierConfig.getCapacity().getValue());

        DiskTierConfig diskTierConfig = tieredStoreConfig.getDiskTierConfig();
        assertTrue(tieredStoreConfig.getDiskTierConfig().isEnabled());
        assertEquals("local-device", diskTierConfig.getDeviceName());

        assertEquals(DEFAULT_DEVICE_NAME,
                config.getMapConfig("map1").getTieredStoreConfig().getDiskTierConfig().getDeviceName());
        assertNotNull(config.getDeviceConfig(DEFAULT_DEVICE_NAME));

        tieredStoreConfig = config.getMapConfig("map2").getTieredStoreConfig();
        assertTrue(tieredStoreConfig.isEnabled());

        memoryTierConfig = tieredStoreConfig.getMemoryTierConfig();
        assertEquals(MemoryUnit.GIGABYTES, memoryTierConfig.getCapacity().getUnit());
        assertEquals(1L, memoryTierConfig.getCapacity().getValue());

        assertFalse(tieredStoreConfig.getDiskTierConfig().isEnabled());

        tieredStoreConfig = config.getMapConfig("map3").getTieredStoreConfig();
        memoryTierConfig = tieredStoreConfig.getMemoryTierConfig();
        assertEquals(DEFAULT_CAPACITY, memoryTierConfig.getCapacity());

        diskTierConfig = tieredStoreConfig.getDiskTierConfig();
        assertFalse(diskTierConfig.isEnabled());
        assertEquals(DEFAULT_DEVICE_NAME, diskTierConfig.getDeviceName());

        yaml = """
                hazelcast:
                  map:
                    some-map:
                      tiered-store:
                        enabled: true
                """;

        config = new InMemoryYamlConfig(yaml);
        assertEquals(1, config.getDeviceConfigs().size());
        assertEquals(1, config.getDeviceConfigs().size());
        assertEquals(new LocalDeviceConfig(), config.getDeviceConfig(DEFAULT_DEVICE_NAME));
        assertEquals(DEFAULT_DEVICE_NAME,
                config.getMapConfig("some-map").getTieredStoreConfig().getDiskTierConfig().getDeviceName());
    }

    @Override
    @Test
    public void testHotRestartEncryptionAtRest_whenJavaKeyStore() {
        int keySize = 16;
        String keyStorePath = "/tmp/keystore.p12";
        String keyStoreType = "PKCS12";
        String keyStorePassword = "password";
        int pollingInterval = 60;
        String currentKeyAlias = "current";
        String yaml = ""
                + "hazelcast:\n"
                + "  hot-restart-persistence:\n"
                + "    enabled: true\n"
                + "    encryption-at-rest:\n"
                + "      enabled: true\n"
                + "      algorithm: AES\n"
                + "      salt: some-salt\n"
                + "      key-size: " + keySize + "\n"
                + "      secure-store:\n"
                + "        keystore:\n"
                + "          path: " + keyStorePath + "\n"
                + "          type: " + keyStoreType + "\n"
                + "          password: " + keyStorePassword + "\n"
                + "          polling-interval: " + pollingInterval + "\n"
                + "          current-key-alias: " + currentKeyAlias + "\n";

        Config config = new InMemoryYamlConfig(yaml);
        HotRestartPersistenceConfig hotRestartPersistenceConfig = config.getHotRestartPersistenceConfig();
        assertTrue(hotRestartPersistenceConfig.isEnabled());

        EncryptionAtRestConfig encryptionAtRestConfig = hotRestartPersistenceConfig.getEncryptionAtRestConfig();
        assertTrue(encryptionAtRestConfig.isEnabled());
        assertEquals("AES", encryptionAtRestConfig.getAlgorithm());
        assertEquals("some-salt", encryptionAtRestConfig.getSalt());
        assertEquals(keySize, encryptionAtRestConfig.getKeySize());
        SecureStoreConfig secureStoreConfig = encryptionAtRestConfig.getSecureStoreConfig();
        assertTrue(secureStoreConfig instanceof JavaKeyStoreSecureStoreConfig);
        JavaKeyStoreSecureStoreConfig keyStoreConfig = (JavaKeyStoreSecureStoreConfig) secureStoreConfig;
        assertEquals(new File(keyStorePath).getAbsolutePath(), keyStoreConfig.getPath().getAbsolutePath());
        assertEquals(keyStoreType, keyStoreConfig.getType());
        assertEquals(keyStorePassword, keyStoreConfig.getPassword());
        assertEquals(pollingInterval, keyStoreConfig.getPollingInterval());
        assertEquals(currentKeyAlias, keyStoreConfig.getCurrentKeyAlias());
    }

    @Override
    @Test
    public void testPersistenceEncryptionAtRest_whenJavaKeyStore() {
        int keySize = 16;
        String keyStorePath = "/tmp/keystore.p12";
        String keyStoreType = "PKCS12";
        String keyStorePassword = "password";
        int pollingInterval = 60;
        String currentKeyAlias = "current";
        String yaml = ""
                + "hazelcast:\n"
                + "  persistence:\n"
                + "    enabled: true\n"
                + "    encryption-at-rest:\n"
                + "      enabled: true\n"
                + "      algorithm: AES\n"
                + "      salt: some-salt\n"
                + "      key-size: " + keySize + "\n"
                + "      secure-store:\n"
                + "        keystore:\n"
                + "          path: " + keyStorePath + "\n"
                + "          type: " + keyStoreType + "\n"
                + "          password: " + keyStorePassword + "\n"
                + "          polling-interval: " + pollingInterval + "\n"
                + "          current-key-alias: " + currentKeyAlias + "\n";

        Config config = new InMemoryYamlConfig(yaml);
        PersistenceConfig persistenceConfig = config.getPersistenceConfig();
        assertTrue(persistenceConfig.isEnabled());

        EncryptionAtRestConfig encryptionAtRestConfig = persistenceConfig.getEncryptionAtRestConfig();
        assertTrue(encryptionAtRestConfig.isEnabled());
        assertEquals("AES", encryptionAtRestConfig.getAlgorithm());
        assertEquals("some-salt", encryptionAtRestConfig.getSalt());
        assertEquals(keySize, encryptionAtRestConfig.getKeySize());
        SecureStoreConfig secureStoreConfig = encryptionAtRestConfig.getSecureStoreConfig();
        assertTrue(secureStoreConfig instanceof JavaKeyStoreSecureStoreConfig);
        JavaKeyStoreSecureStoreConfig keyStoreConfig = (JavaKeyStoreSecureStoreConfig) secureStoreConfig;
        assertEquals(new File(keyStorePath).getAbsolutePath(), keyStoreConfig.getPath().getAbsolutePath());
        assertEquals(keyStoreType, keyStoreConfig.getType());
        assertEquals(keyStorePassword, keyStoreConfig.getPassword());
        assertEquals(pollingInterval, keyStoreConfig.getPollingInterval());
        assertEquals(currentKeyAlias, keyStoreConfig.getCurrentKeyAlias());
    }

    @Override
    @Test
    public void testHotRestartEncryptionAtRest_whenVault() {
        int keySize = 16;
        String address = "https://localhost:1234";
        String secretPath = "secret/path";
        String token = "token";
        int pollingInterval = 60;
        String yaml = ""
                + "hazelcast:\n"
                + "  hot-restart-persistence:\n"
                + "    enabled: true\n"
                + "    encryption-at-rest:\n"
                + "      enabled: true\n"
                + "      algorithm: AES\n"
                + "      salt: some-salt\n"
                + "      key-size: " + keySize + "\n"
                + "      secure-store:\n"
                + "        vault:\n"
                + "          address: " + address + "\n"
                + "          secret-path: " + secretPath + "\n"
                + "          token: " + token + "\n"
                + "          polling-interval: " + pollingInterval + "\n"
                + "          ssl:\n"
                + "            enabled: true\n"
                + "            factory-class-name: com.hazelcast.nio.ssl.BasicSSLContextFactory\n"
                + "            properties:\n"
                + "              protocol: TLS\n";

        Config config = new InMemoryYamlConfig(yaml);
        HotRestartPersistenceConfig hotRestartPersistenceConfig = config.getHotRestartPersistenceConfig();
        assertTrue(hotRestartPersistenceConfig.isEnabled());

        EncryptionAtRestConfig encryptionAtRestConfig = hotRestartPersistenceConfig.getEncryptionAtRestConfig();
        assertTrue(encryptionAtRestConfig.isEnabled());
        assertEquals("AES", encryptionAtRestConfig.getAlgorithm());
        assertEquals("some-salt", encryptionAtRestConfig.getSalt());
        assertEquals(keySize, encryptionAtRestConfig.getKeySize());
        SecureStoreConfig secureStoreConfig = encryptionAtRestConfig.getSecureStoreConfig();
        assertTrue(secureStoreConfig instanceof VaultSecureStoreConfig);
        VaultSecureStoreConfig vaultConfig = (VaultSecureStoreConfig) secureStoreConfig;
        assertEquals(address, vaultConfig.getAddress());
        assertEquals(secretPath, vaultConfig.getSecretPath());
        assertEquals(token, vaultConfig.getToken());
        assertEquals(pollingInterval, vaultConfig.getPollingInterval());
        SSLConfig sslConfig = vaultConfig.getSSLConfig();
        assertTrue(sslConfig.isEnabled());
        assertEquals("com.hazelcast.nio.ssl.BasicSSLContextFactory", sslConfig.getFactoryClassName());
        assertEquals(1, sslConfig.getProperties().size());
        assertEquals("TLS", sslConfig.getProperties().get("protocol"));
    }

    @Override
    @Test
    public void testPersistenceEncryptionAtRest_whenVault() {
        int keySize = 16;
        String address = "https://localhost:1234";
        String secretPath = "secret/path";
        String token = "token";
        int pollingInterval = 60;
        String yaml = ""
                + "hazelcast:\n"
                + "  persistence:\n"
                + "    enabled: true\n"
                + "    encryption-at-rest:\n"
                + "      enabled: true\n"
                + "      algorithm: AES\n"
                + "      salt: some-salt\n"
                + "      key-size: " + keySize + "\n"
                + "      secure-store:\n"
                + "        vault:\n"
                + "          address: " + address + "\n"
                + "          secret-path: " + secretPath + "\n"
                + "          token: " + token + "\n"
                + "          polling-interval: " + pollingInterval + "\n"
                + "          ssl:\n"
                + "            enabled: true\n"
                + "            factory-class-name: com.hazelcast.nio.ssl.BasicSSLContextFactory\n"
                + "            properties:\n"
                + "              protocol: TLS\n";

        Config config = new InMemoryYamlConfig(yaml);
        PersistenceConfig persistenceConfig = config.getPersistenceConfig();
        assertTrue(persistenceConfig.isEnabled());

        EncryptionAtRestConfig encryptionAtRestConfig = persistenceConfig.getEncryptionAtRestConfig();
        assertTrue(encryptionAtRestConfig.isEnabled());
        assertEquals("AES", encryptionAtRestConfig.getAlgorithm());
        assertEquals("some-salt", encryptionAtRestConfig.getSalt());
        assertEquals(keySize, encryptionAtRestConfig.getKeySize());
        SecureStoreConfig secureStoreConfig = encryptionAtRestConfig.getSecureStoreConfig();
        assertTrue(secureStoreConfig instanceof VaultSecureStoreConfig);
        VaultSecureStoreConfig vaultConfig = (VaultSecureStoreConfig) secureStoreConfig;
        assertEquals(address, vaultConfig.getAddress());
        assertEquals(secretPath, vaultConfig.getSecretPath());
        assertEquals(token, vaultConfig.getToken());
        assertEquals(pollingInterval, vaultConfig.getPollingInterval());
        SSLConfig sslConfig = vaultConfig.getSSLConfig();
        assertTrue(sslConfig.isEnabled());
        assertEquals("com.hazelcast.nio.ssl.BasicSSLContextFactory", sslConfig.getFactoryClassName());
        assertEquals(1, sslConfig.getProperties().size());
        assertEquals("TLS", sslConfig.getProperties().get("protocol"));
    }

    @Override
    @Test
    public void testCachePermission() {
        String yaml = """
                hazelcast:
                  security:
                    enabled: true
                    client-permissions:
                      cache:
                        - name: /hz/cachemanager1/cache1
                          principal: dev
                          actions:
                            - create
                            - destroy
                            - add
                            - remove
                """;

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
        String yaml = """
                hazelcast:
                  security:
                    enabled: true
                    client-permissions:
                      priority-grant: true
                      config:
                        deny: true
                        principal: dev
                        endpoints:
                          - 127.0.0.1""";

        Config config = buildConfig(yaml);
        PermissionConfig expected = new PermissionConfig(CONFIG, null, "dev").setDeny(true);
        expected.getEndpoints().add("127.0.0.1");
        assertPermissionConfig(expected, config);
        assertTrue(config.getSecurityConfig().isPermissionPriorityGrant());
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testCacheConfig_withNativeInMemoryFormat_failsFastInOSS() {
        String yaml = """
                hazelcast:
                  cache:
                    cache:
                      eviction:
                        size: 10000000
                        max-size-policy: ENTRY_COUNT
                        eviction-policy: LFU
                      in-memory-format: NATIVE
                """;

        buildConfig(yaml);
    }

    @Override
    @Test
    public void testAllPermissionsCovered() throws IOException {
        URL yamlResource = YamlConfigBuilderTest.class.getClassLoader().getResource("hazelcast-fullconfig.yaml");
        Config config = new YamlConfigBuilder(yamlResource).build();
        Set<PermissionType> allPermissionTypes = Set.of(PermissionType.values());
        Set<PermissionType> foundPermissionTypes = config.getSecurityConfig().getClientPermissionConfigs().stream()
                .map(PermissionConfig::getType).collect(Collectors.toSet());
        Collection<PermissionType> difference = Sets.difference(allPermissionTypes, foundPermissionTypes);

        assertTrue(String.format("All permission types should be listed in %s. Not found ones: %s", yamlResource, difference),
                difference.isEmpty());
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    @Ignore("Schema validation is supposed to fail with missing mandatory field: class-name")
    public void testMemberAddressProvider_classNameIsMandatory() {
        String yaml = """
                hazelcast:
                  network:
                    member-address-provider:
                      enabled: true
                """;

        buildConfig(yaml);
    }

    @Override
    @Test
    public void testMemberAddressProviderEnabled() {
        String yaml = """
                hazelcast:
                  network:
                    member-address-provider:
                      enabled: true
                      class-name: foo.bar.Clazz
                """;

        Config config = buildConfig(yaml);
        MemberAddressProviderConfig memberAddressProviderConfig = config.getNetworkConfig().getMemberAddressProviderConfig();

        assertTrue(memberAddressProviderConfig.isEnabled());
        assertEquals("foo.bar.Clazz", memberAddressProviderConfig.getClassName());
    }

    @Override
    @Test
    public void testMemberAddressProviderEnabled_withProperties() {
        String yaml = """
                hazelcast:
                  network:
                    member-address-provider:
                      enabled: true
                      class-name: foo.bar.Clazz
                      properties:
                        propName1: propValue1
                """;

        Config config = buildConfig(yaml);
        MemberAddressProviderConfig memberAddressProviderConfig = config.getNetworkConfig().getMemberAddressProviderConfig();

        Properties properties = memberAddressProviderConfig.getProperties();
        assertEquals(1, properties.size());
        assertEquals("propValue1", properties.get("propName1"));
    }

    @Override
    @Test
    public void testFailureDetector_withProperties() {
        String yaml = """
                hazelcast:
                  network:
                    failure-detector:
                      icmp:
                        enabled: true
                        timeout-milliseconds: 42
                        fail-fast-on-startup: true
                        interval-milliseconds: 4200
                        max-attempts: 42
                        parallel-mode: true
                        ttl: 255
                """;

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
        String yaml = """
                hazelcast:
                  member-attributes:
                    IDENTIFIER:
                      type: string
                      value: ID
                """;

        Config config = buildConfig(yaml);
        MemberAttributeConfig memberAttributeConfig = config.getMemberAttributeConfig();
        assertNotNull(memberAttributeConfig);
        assertEquals("ID", memberAttributeConfig.getAttribute("IDENTIFIER"));
    }

    @Override
    @Test
    public void testMemcacheProtocolEnabled() {
        String yaml = """
                hazelcast:
                  network:
                    memcache-protocol:
                      enabled: true
                """;
        Config config = buildConfig(yaml);
        MemcacheProtocolConfig memcacheProtocolConfig = config.getNetworkConfig().getMemcacheProtocolConfig();
        assertNotNull(memcacheProtocolConfig);
        assertTrue(memcacheProtocolConfig.isEnabled());
    }

    @Override
    @Test
    public void testRestApiDefaults() {
        String yaml = """
                hazelcast:
                  network:
                    rest-api:
                      enabled: false""";
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
        String yaml = """
                hazelcast:
                  network:
                    rest-api:
                      enabled: true
                      endpoint-groups:
                        HEALTH_CHECK:
                          enabled: true
                        DATA:
                          enabled: true
                        CLUSTER_READ:
                          enabled: false""";
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
        String yaml = """
                hazelcast:
                  network:
                    rest-api:
                      enabled: true
                      endpoint-groups:
                        TEST:
                          enabled: true""";
        buildConfig(yaml);
    }

    @Override
    @Test
    public void testDefaultAdvancedNetworkConfig() {
        String yaml = """
                hazelcast:
                  advanced-network: {}
                """;

        Config config = buildConfig(yaml);
        AdvancedNetworkConfig advancedNetworkConfig = config.getAdvancedNetworkConfig();
        JoinConfig joinConfig = advancedNetworkConfig.getJoin();
        IcmpFailureDetectorConfig fdConfig = advancedNetworkConfig.getIcmpFailureDetectorConfig();
        MemberAddressProviderConfig providerConfig = advancedNetworkConfig.getMemberAddressProviderConfig();

        assertFalse(advancedNetworkConfig.isEnabled());
        assertTrue(joinConfig.getAutoDetectionConfig().isEnabled());
        assertNull(fdConfig);
        assertFalse(providerConfig.isEnabled());

        assertTrue(advancedNetworkConfig.getEndpointConfigs().containsKey(EndpointQualifier.MEMBER));
        assertEquals(1, advancedNetworkConfig.getEndpointConfigs().size());
    }

    @Override
    @Test
    public void testAdvancedNetworkConfig_whenInvalidSocketKeepIdleSeconds() {
        String invalid1 = getAdvancedNetworkConfigWithSocketOption("keep-idle-seconds", -1);
        Assert.assertThrows(IllegalArgumentException.class, () -> buildConfig(invalid1));

        String invalid2 = getAdvancedNetworkConfigWithSocketOption("keep-idle-seconds", 0);
        Assert.assertThrows(IllegalArgumentException.class, () -> buildConfig(invalid2));

        String invalid3 = getAdvancedNetworkConfigWithSocketOption("keep-idle-seconds", 32768);
        Assert.assertThrows(IllegalArgumentException.class, () -> buildConfig(invalid3));
    }

    @Override
    @Test
    public void testAdvancedNetworkConfig_whenInvalidSocketKeepIntervalSeconds() {
        String invalid1 = getAdvancedNetworkConfigWithSocketOption("keep-interval-seconds", -1);
        Assert.assertThrows(IllegalArgumentException.class, () -> buildConfig(invalid1));

        String invalid2 = getAdvancedNetworkConfigWithSocketOption("keep-interval-seconds", 0);
        Assert.assertThrows(IllegalArgumentException.class, () -> buildConfig(invalid2));

        String invalid3 = getAdvancedNetworkConfigWithSocketOption("keep-interval-seconds", 32768);
        Assert.assertThrows(IllegalArgumentException.class, () -> buildConfig(invalid3));
    }

    @Override
    @Test
    public void testAdvancedNetworkConfig_whenInvalidSocketKeepCount() {
        String invalid1 = getAdvancedNetworkConfigWithSocketOption("keep-count", -1);
        Assert.assertThrows(IllegalArgumentException.class, () -> buildConfig(invalid1));

        String invalid2 = getAdvancedNetworkConfigWithSocketOption("keep-count", 0);
        Assert.assertThrows(IllegalArgumentException.class, () -> buildConfig(invalid2));

        String invalid3 = getAdvancedNetworkConfigWithSocketOption("keep-count", 128);
        Assert.assertThrows(IllegalArgumentException.class, () -> buildConfig(invalid3));
    }

    @Override
    @Test
    public void testAmbiguousNetworkConfig_throwsException() {
        String yaml = """
                hazelcast:
                  advanced-network:
                    enabled: true
                  network:
                    port: 9999""";

        expected.expect(InvalidConfigurationException.class);
        buildConfig(yaml);
    }

    @Override
    @Test
    public void testNetworkConfigUnambiguous_whenAdvancedNetworkDisabled() {
        String yaml = """
                hazelcast:
                  advanced-network: {}
                  network:
                    port:
                      port: 9999
                """;

        Config config = buildConfig(yaml);
        assertFalse(config.getAdvancedNetworkConfig().isEnabled());
        assertEquals(9999, config.getNetworkConfig().getPort());
    }

    @Override
    @Test
    public void testMultipleMemberEndpointConfigs_throwsException() {
        String yaml = """
                hazelcast:
                advanced-network:
                  member-server-socket-endpoint-config: {}
                  member-server-socket-endpoint-config: {}""";

        expected.expect(InvalidConfigurationException.class);
        buildConfig(yaml);

    }

    @Override
    @Test
    public void testMultipleClientEndpointConfigs_throwsException() {
        String yaml = """
            hazelcast:
            advanced-network:
              client-server-socket-endpoint-config: {}
              client-server-socket-endpoint-config: {}""";

        expected.expect(InvalidConfigurationException.class);
        buildConfig(yaml);
    }

    @Override
    @Test
    public void testMultipleRestEndpointConfigs_throwsException() {
        String yaml = """
            hazelcast:
            advanced-network:
              rest-server-socket-endpoint-config: {}
              rest-server-socket-endpoint-config: {}""";

        expected.expect(InvalidConfigurationException.class);
        buildConfig(yaml);
    }

    @Override
    @Test
    public void testMultipleMemcacheEndpointConfigs_throwsException() {
        String yaml = """
            hazelcast:
            advanced-network:
              memcache-server-socket-endpoint-config: {}
              memcache-server-socket-endpoint-config: {}""";

        expected.expect(InvalidConfigurationException.class);
        buildConfig(yaml);
    }

    @Override
    @Test
    public void testMultipleJoinElements_throwsException() {
        String yaml = """
            hazelcast:
            advanced-network:
              join: {}
              join: {}""";

        expected.expect(InvalidConfigurationException.class);
        buildConfig(yaml);
    }

    @Override
    @Test
    public void testMultipleFailureDetectorElements_throwsException() {
        String yaml = """
            hazelcast:
            advanced-network:
              failure-detector: {}
              failure-detector: {}""";

        expected.expect(InvalidConfigurationException.class);
        buildConfig(yaml);
    }

    @Override
    @Test
    public void testMultipleMemberAddressProviderElements_throwsException() {
        String yaml = """
            hazelcast:
            advanced-network:
              member-address-provider: {}
              member-address-provider: {}""";

        expected.expect(InvalidConfigurationException.class);
        buildConfig(yaml);
    }

    @Test
    public void outboundPorts_asObject_ParsingTest() {
        String yaml = """
                hazelcast:
                  network:
                    outbound-ports:
                      ports: 2500-3000
                      more-ports: 2600-3500
                """;
        Config actual = buildConfig(yaml);
        assertEquals(Set.of("2500-3000", "2600-3500"), actual.getNetworkConfig().getOutboundPortDefinitions());
    }

    @Test
    public void outboundPorts_asSequence_ParsingTest() {
        String yaml = """
                hazelcast:
                  network:
                    outbound-ports:
                      - 1234-1999
                      - 2500
                """;
        Config actual = buildConfig(yaml);
        assertEquals(Set.of("2500", "1234-1999"), actual.getNetworkConfig().getOutboundPortDefinitions());
    }

    @Override
    protected Config buildCompleteAdvancedNetworkConfig() {
        String yaml = """
                hazelcast:
                  advanced-network:
                    enabled: true
                    join:
                      multicast:
                        enabled: false
                      tcp-ip:
                        enabled: true
                        required-member: 10.10.1.10
                        member-list:
                          - 10.10.1.11
                          - 10.10.1.12
                    failure-detector:
                      icmp:
                        enabled: true
                        timeout-milliseconds: 42
                        fail-fast-on-startup: true
                        interval-milliseconds: 4200
                        max-attempts: 42
                        parallel-mode: true
                        ttl: 255
                    member-address-provider:
                      class-name: com.hazelcast.test.Provider
                    member-server-socket-endpoint-config:
                      name: member-server-socket
                      outbound-ports:
                        ports: 33000-33100
                      interfaces:
                        enabled: true
                        interfaces:
                          - 10.10.0.1
                      ssl:
                        enabled: true
                        factory-class-name: com.hazelcast.examples.MySSLContextFactory
                        properties:
                          foo: bar
                      socket-interceptor:
                        enabled: true
                        class-name: com.hazelcast.examples.MySocketInterceptor
                        properties:
                          foo: baz
                      socket-options:
                        buffer-direct: true
                        tcp-no-delay: true
                        keep-alive: true
                        connect-timeout-seconds: 33
                        send-buffer-size-kb: 34
                        receive-buffer-size-kb: 67
                        linger-seconds: 11
                        keep-count: 12
                        keep-interval-seconds: 13
                        keep-idle-seconds: 14
                      symmetric-encryption:
                        enabled: true
                        algorithm: Algorithm
                        salt: thesalt
                        password: thepassword
                        iteration-count: 1000
                      port:
                        port-count: 93
                        auto-increment: false
                        port: 9191
                      public-address: 10.20.10.10
                      reuse-address: true
                    rest-server-socket-endpoint-config:
                      name: REST
                      port:
                        port: 8080
                      endpoint-groups:
                        WAN:
                          enabled: true
                        CLUSTER_READ:
                          enabled: true
                        CLUSTER_WRITE:
                          enabled: false
                        HEALTH_CHECK:
                          enabled: true
                    memcache-server-socket-endpoint-config:
                      name: MEMCACHE
                      outbound-ports:
                        ports: 42000-42100
                    wan-server-socket-endpoint-config:
                      WAN_SERVER1:
                        outbound-ports:
                          ports: 52000-52100
                      WAN_SERVER2:
                        outbound-ports:
                          ports: 53000-53100
                    wan-endpoint-config:
                      WAN_ENDPOINT1:
                        outbound-ports:
                          ports: 62000-62100
                      WAN_ENDPOINT2:
                        outbound-ports:
                          ports: 63000-63100
                    client-server-socket-endpoint-config:
                      name: CLIENT
                      outbound-ports:
                        ports: 72000-72100
                """;

        return buildConfig(yaml);
    }

    @Override
    @Test
    public void testCPSubsystemConfig() {
        String yaml = """
                hazelcast:
                  cp-subsystem:
                    cp-member-count: 10
                    group-size: 5
                    session-time-to-live-seconds: 15
                    session-heartbeat-interval-seconds: 3
                    missing-cp-member-auto-removal-seconds: 120
                    fail-on-indeterminate-operation-state: true
                    persistence-enabled: true
                    base-dir: /mnt/cp-data
                    data-load-timeout-seconds: 30
                    cp-member-priority: -1
                    map-limit: 25
                    raft-algorithm:
                      leader-election-timeout-in-millis: 500
                      leader-heartbeat-period-in-millis: 100
                      max-missed-leader-heartbeat-count: 3
                      append-request-max-entry-count: 25
                      commit-index-advance-count-to-snapshot: 250
                      uncommitted-entry-count-to-reject-new-appends: 75
                      append-request-backoff-timeout-in-millis: 50
                    semaphores:
                      sem1:
                        jdk-compatible: true
                        initial-permits: 1
                      sem2:
                        jdk-compatible: false
                        initial-permits: 2
                    locks:
                      lock1:
                        lock-acquire-limit: 1
                      lock2:
                        lock-acquire-limit: 2
                    maps:
                      map1:
                        max-size-mb: 1
                      map2:
                        max-size-mb: 2
                """;
        Config config = buildConfig(yaml);
        CPSubsystemConfig cpSubsystemConfig = config.getCPSubsystemConfig();
        assertEquals(10, cpSubsystemConfig.getCPMemberCount());
        assertEquals(5, cpSubsystemConfig.getGroupSize());
        assertEquals(15, cpSubsystemConfig.getSessionTimeToLiveSeconds());
        assertEquals(3, cpSubsystemConfig.getSessionHeartbeatIntervalSeconds());
        assertEquals(120, cpSubsystemConfig.getMissingCPMemberAutoRemovalSeconds());
        assertTrue(cpSubsystemConfig.isFailOnIndeterminateOperationState());
        assertTrue(cpSubsystemConfig.isPersistenceEnabled());
        assertEquals(new File("/mnt/cp-data").getAbsoluteFile(), cpSubsystemConfig.getBaseDir().getAbsoluteFile());
        assertEquals(30, cpSubsystemConfig.getDataLoadTimeoutSeconds());
        assertEquals(-1, cpSubsystemConfig.getCPMemberPriority());
        RaftAlgorithmConfig raftAlgorithmConfig = cpSubsystemConfig.getRaftAlgorithmConfig();
        assertEquals(500, raftAlgorithmConfig.getLeaderElectionTimeoutInMillis());
        assertEquals(100, raftAlgorithmConfig.getLeaderHeartbeatPeriodInMillis());
        assertEquals(3, raftAlgorithmConfig.getMaxMissedLeaderHeartbeatCount());
        assertEquals(25, raftAlgorithmConfig.getAppendRequestMaxEntryCount());
        assertEquals(250, raftAlgorithmConfig.getCommitIndexAdvanceCountToSnapshot());
        assertEquals(75, raftAlgorithmConfig.getUncommittedEntryCountToRejectNewAppends());
        assertEquals(50, raftAlgorithmConfig.getAppendRequestBackoffTimeoutInMillis());
        SemaphoreConfig semaphoreConfig1 = cpSubsystemConfig.findSemaphoreConfig("sem1");
        SemaphoreConfig semaphoreConfig2 = cpSubsystemConfig.findSemaphoreConfig("sem2");
        assertNotNull(semaphoreConfig1);
        assertNotNull(semaphoreConfig2);
        assertTrue(semaphoreConfig1.isJDKCompatible());
        assertFalse(semaphoreConfig2.isJDKCompatible());
        assertEquals(1, semaphoreConfig1.getInitialPermits());
        assertEquals(2, semaphoreConfig2.getInitialPermits());
        FencedLockConfig lockConfig1 = cpSubsystemConfig.findLockConfig("lock1");
        FencedLockConfig lockConfig2 = cpSubsystemConfig.findLockConfig("lock2");
        assertNotNull(lockConfig1);
        assertNotNull(lockConfig2);
        assertEquals(1, lockConfig1.getLockAcquireLimit());
        assertEquals(2, lockConfig2.getLockAcquireLimit());
        CPMapConfig mapConfig1 = cpSubsystemConfig.findCPMapConfig("map1");
        CPMapConfig mapConfig2 = cpSubsystemConfig.findCPMapConfig("map2");
        assertNotNull(mapConfig1);
        assertNotNull(mapConfig2);
        assertEquals("map1", mapConfig1.getName());
        assertEquals(1, mapConfig1.getMaxSizeMb());
        assertEquals("map2", mapConfig2.getName());
        assertEquals(2, mapConfig2.getMaxSizeMb());
        assertEquals(25, cpSubsystemConfig.getCPMapLimit());
    }

    @Override
    @Test
    public void testSqlConfig() {
        String yaml = """
                hazelcast:
                  sql:
                    statement-timeout-millis: 30
                    catalog-persistence-enabled: true
                """;
        Config config = buildConfig(yaml);
        SqlConfig sqlConfig = config.getSqlConfig();
        assertEquals(30L, sqlConfig.getStatementTimeoutMillis());
        assertTrue(sqlConfig.isCatalogPersistenceEnabled());
    }

    @Override
    @Test
    public void testWhitespaceInNonSpaceStrings() {
        String yaml = """
                hazelcast:
                  split-brain-protection:
                    name-of-split-brain-protection:
                      enabled: true
                      protect-on:   WRITE  \s
                """;

        buildConfig(yaml);
    }

    @Override
    @Test
    public void testPersistentMemoryDirectoryConfiguration() {
        String yaml = """
                hazelcast:
                  native-memory:
                    persistent-memory:
                      directories:
                        - directory: /mnt/pmem0
                          numa-node: 0
                        - directory: /mnt/pmem1
                          numa-node: 1
                """;

        Config yamlConfig = new InMemoryYamlConfig(yaml);

        PersistentMemoryConfig pmemConfig = yamlConfig.getNativeMemoryConfig()
                .getPersistentMemoryConfig();
        List<PersistentMemoryDirectoryConfig> directoryConfigs = pmemConfig
                .getDirectoryConfigs();
        assertFalse(pmemConfig.isEnabled());
        assertEquals(MOUNTED, pmemConfig.getMode());
        assertEquals(2, directoryConfigs.size());
        PersistentMemoryDirectoryConfig dir0Config = directoryConfigs.get(0);
        PersistentMemoryDirectoryConfig dir1Config = directoryConfigs.get(1);
        assertEquals("/mnt/pmem0", dir0Config.getDirectory());
        assertEquals(0, dir0Config.getNumaNode());
        assertEquals("/mnt/pmem1", dir1Config.getDirectory());
        assertEquals(1, dir1Config.getNumaNode());
    }

    @Test
    public void cacheEntryListenerConfigParsing() {
        String yaml = """
                hazelcast:
                  cache:
                    my-cache:
                      cache-entry-listeners:
                        - old-value-required: true
                          synchronous: true
                          cache-entry-listener-factory:
                            class-name: com.example.cache.MyEntryListenerFactory
                          cache-entry-event-filter-factory:
                            class-name: com.example.cache.MyEntryEventFilterFactory""";
        Config actual = buildConfig(yaml);
        CacheSimpleEntryListenerConfig expected = new CacheSimpleEntryListenerConfig()
                .setOldValueRequired(true)
                .setSynchronous(true)
                .setCacheEntryListenerFactory("com.example.cache.MyEntryListenerFactory")
                .setCacheEntryEventFilterFactory("com.example.cache.MyEntryEventFilterFactory");

        List<CacheSimpleEntryListenerConfig> actualListeners = actual.findCacheConfig("my-cache").getCacheEntryListeners();
        assertEquals(singletonList(expected), actualListeners);
    }

    @Override
    @Test
    public void testPersistentMemoryDirectoryConfigurationSimple() {
        String yaml = """
                hazelcast:
                  native-memory:
                    persistent-memory-directory: /mnt/pmem0""";

        Config config = buildConfig(yaml);
        PersistentMemoryConfig pmemConfig = config.getNativeMemoryConfig().getPersistentMemoryConfig();
        assertTrue(pmemConfig.isEnabled());

        List<PersistentMemoryDirectoryConfig> directoryConfigs = pmemConfig.getDirectoryConfigs();
        assertEquals(1, directoryConfigs.size());
        PersistentMemoryDirectoryConfig dir0Config = directoryConfigs.get(0);
        assertEquals("/mnt/pmem0", dir0Config.getDirectory());
        assertFalse(dir0Config.isNumaNodeSet());
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testPersistentMemoryDirectoryConfiguration_uniqueDirViolationThrows() {
        String yaml = """
                hazelcast:
                  native-memory:
                    persistent-memory:
                      directories:
                        - directory: /mnt/pmem0
                          numa-node: 0
                        - directory: /mnt/pmem0
                          numa-node: 1
                """;

        buildConfig(yaml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testPersistentMemoryDirectoryConfiguration_uniqueNumaNodeViolationThrows() {
        String yaml = """
                hazelcast:
                  native-memory:
                    persistent-memory:
                      directories:
                        - directory: /mnt/pmem0
                          numa-node: 0
                        - directory: /mnt/pmem1
                          numa-node: 0
                """;

        buildConfig(yaml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testPersistentMemoryDirectoryConfiguration_numaNodeConsistencyViolationThrows() {
        String yaml = """
                hazelcast:
                  native-memory:
                    persistent-memory:
                      directories:
                        - directory: /mnt/pmem0
                          numa-node: 0
                        - directory: /mnt/pmem1
                """;

        buildConfig(yaml);
    }

    @Override
    @Test
    public void testPersistentMemoryDirectoryConfiguration_simpleAndAdvancedPasses() {
        String yaml = """
                hazelcast:
                  native-memory:
                    persistent-memory-directory: /mnt/optane
                    persistent-memory:
                      directories:
                        - directory: /mnt/pmem0
                        - directory: /mnt/pmem1
                """;

        Config config = buildConfig(yaml);

        PersistentMemoryConfig pmemConfig = config.getNativeMemoryConfig().getPersistentMemoryConfig();
        assertTrue(pmemConfig.isEnabled());
        assertEquals(MOUNTED, pmemConfig.getMode());

        List<PersistentMemoryDirectoryConfig> directoryConfigs = pmemConfig.getDirectoryConfigs();
        assertEquals(3, directoryConfigs.size());
        PersistentMemoryDirectoryConfig dir0Config = directoryConfigs.get(0);
        PersistentMemoryDirectoryConfig dir1Config = directoryConfigs.get(1);
        PersistentMemoryDirectoryConfig dir2Config = directoryConfigs.get(2);
        assertEquals("/mnt/optane", dir0Config.getDirectory());
        assertFalse(dir0Config.isNumaNodeSet());
        assertEquals("/mnt/pmem0", dir1Config.getDirectory());
        assertFalse(dir1Config.isNumaNodeSet());
        assertEquals("/mnt/pmem1", dir2Config.getDirectory());
        assertFalse(dir2Config.isNumaNodeSet());
    }

    @Override
    @Test
    public void testPersistentMemoryConfiguration_SystemMemoryMode() {
        String yaml = """
                hazelcast:
                  native-memory:
                    persistent-memory:
                      enabled: true
                      mode: SYSTEM_MEMORY
                """;

        Config config = buildConfig(yaml);
        PersistentMemoryConfig pmemConfig = config.getNativeMemoryConfig().getPersistentMemoryConfig();
        assertTrue(pmemConfig.isEnabled());
        assertEquals(SYSTEM_MEMORY, pmemConfig.getMode());
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testPersistentMemoryConfiguration_NotExistingModeThrows() {
        String yaml = """
                hazelcast:
                  native-memory:
                    persistent-memory:
                      mode: NOT_EXISTING_MODE
                """;

        buildConfig(yaml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testPersistentMemoryDirectoryConfiguration_SystemMemoryModeThrows() {
        String yaml = """
                hazelcast:
                  native-memory:
                    persistent-memory:
                      mode: SYSTEM_MEMORY
                      directories:
                        - directory: /mnt/pmem0
                """;

        buildConfig(yaml);
    }

    @Override
    @Test
    public void testMetricsConfig() {
        String yaml = """
                hazelcast:
                  metrics:
                    enabled: false
                    management-center:
                      enabled: false
                      retention-seconds: 11
                    jmx:
                      enabled: false
                    collection-frequency-seconds: 10""";
        Config config = new InMemoryYamlConfig(yaml);
        MetricsConfig metricsConfig = config.getMetricsConfig();
        assertFalse(metricsConfig.isEnabled());
        MetricsManagementCenterConfig metricsMcConfig = metricsConfig.getManagementCenterConfig();
        assertFalse(metricsMcConfig.isEnabled());
        assertFalse(metricsConfig.getJmxConfig().isEnabled());
        assertEquals(10, metricsConfig.getCollectionFrequencySeconds());
        assertEquals(11, metricsMcConfig.getRetentionSeconds());
    }

    @Override
    @Test
    public void testInstanceTrackingConfig() {
        String yaml = """
                hazelcast:
                  instance-tracking:
                    enabled: true
                    file-name: /dummy/file
                    format-pattern: dummy-pattern with $HZ_INSTANCE_TRACKING{placeholder} and $RND{placeholder}""";
        Config config = new InMemoryYamlConfig(yaml);
        InstanceTrackingConfig trackingConfig = config.getInstanceTrackingConfig();
        assertTrue(trackingConfig.isEnabled());
        assertEquals("/dummy/file", trackingConfig.getFileName());
        assertEquals("dummy-pattern with $HZ_INSTANCE_TRACKING{placeholder} and $RND{placeholder}",
                trackingConfig.getFormatPattern());
    }

    @Override
    @Test
    public void testMetricsConfigMasterSwitchDisabled() {
        String yaml = """
                hazelcast:
                  metrics:
                    enabled: false""";
        Config config = new InMemoryYamlConfig(yaml);
        MetricsConfig metricsConfig = config.getMetricsConfig();
        assertFalse(metricsConfig.isEnabled());
        assertTrue(metricsConfig.getManagementCenterConfig().isEnabled());
        assertTrue(metricsConfig.getJmxConfig().isEnabled());
    }

    @Override
    @Test
    public void testMetricsConfigMcDisabled() {
        String yaml = """
                hazelcast:
                  metrics:
                    management-center:
                      enabled: false""";
        Config config = new InMemoryYamlConfig(yaml);
        MetricsConfig metricsConfig = config.getMetricsConfig();
        assertTrue(metricsConfig.isEnabled());
        assertFalse(metricsConfig.getManagementCenterConfig().isEnabled());
        assertTrue(metricsConfig.getJmxConfig().isEnabled());
    }

    @Override
    @Test
    public void testMetricsConfigJmxDisabled() {
        String yaml = """
                hazelcast:
                  metrics:
                    jmx:
                      enabled: false""";
        Config config = new InMemoryYamlConfig(yaml);
        MetricsConfig metricsConfig = config.getMetricsConfig();
        assertTrue(metricsConfig.isEnabled());
        assertTrue(metricsConfig.getManagementCenterConfig().isEnabled());
        assertFalse(metricsConfig.getJmxConfig().isEnabled());
    }

    @Override
    protected Config buildAuditlogConfig() {
        String yaml = """
                hazelcast:
                  auditlog:
                    enabled: true
                    factory-class-name: com.acme.auditlog.AuditlogToSyslogFactory
                    properties:
                      host: syslogserver.acme.com
                      port: 514
                      type: tcp
                """;
        return new InMemoryYamlConfig(yaml);
    }

    @Override
    protected Config buildMapWildcardConfig() {
        String yaml = """
                hazelcast:
                  map:
                    map*:
                      attributes:
                        name:
                          extractor-class-name: usercodedeployment.CapitalizingFirstNameExtractor
                    mapBackup2*:
                      backup-count: 2
                      attributes:
                        name:
                          extractor-class-name: usercodedeployment.CapitalizingFirstNameExtractor
                """;

        return new InMemoryYamlConfig(yaml);
    }

    @Override
    @Test
    public void testIntegrityCheckerConfig() {
        String yaml = """
                hazelcast:
                  integrity-checker:
                    enabled: false
                """;

        Config config = buildConfig(yaml);

        assertFalse(config.getIntegrityCheckerConfig().isEnabled());
    }

    @Override
    public void testDataConnectionConfigs() {
        String yaml = """
                hazelcast:
                  data-connection:
                    mysql-database:
                      type: jdbc
                      properties:
                        jdbcUrl: jdbc:mysql://dummy:3306
                        some.property: dummy-value
                      shared: true
                    other-database:
                      type: other
                """;

        Config config = new InMemoryYamlConfig(yaml);

        Map<String, DataConnectionConfig> dataConnectionConfigs = config.getDataConnectionConfigs();

        assertThat(dataConnectionConfigs).hasSize(2);
        assertThat(dataConnectionConfigs).containsKey("mysql-database");
        DataConnectionConfig mysqlDataConnectionConfig = dataConnectionConfigs.get("mysql-database");
        assertThat(mysqlDataConnectionConfig.getType()).isEqualTo("jdbc");
        assertThat(mysqlDataConnectionConfig.getName()).isEqualTo("mysql-database");
        assertThat(mysqlDataConnectionConfig.isShared()).isTrue();
        assertThat(mysqlDataConnectionConfig.getProperty("jdbcUrl")).isEqualTo("jdbc:mysql://dummy:3306");
        assertThat(mysqlDataConnectionConfig.getProperty("some.property")).isEqualTo("dummy-value");

        assertThat(dataConnectionConfigs).containsKey("other-database");
        DataConnectionConfig otherDataConnectionConfig = dataConnectionConfigs.get("other-database");
        assertThat(otherDataConnectionConfig.getType()).isEqualTo("other");
    }

    @Override
    @Test
    public void testPartitioningAttributeConfigs() {
        String yaml = """
                hazelcast:
                  map:
                    test:
                      partition-attributes:
                        - name: attr1
                        - name: attr2
                """;

        final MapConfig mapConfig = buildConfig(yaml).getMapConfig("test");
        assertThat(mapConfig.getPartitioningAttributeConfigs()).containsExactly(
                new PartitioningAttributeConfig("attr1"),
                new PartitioningAttributeConfig("attr2")
        );
    }

    @Override
    public void testNamespaceConfigs() throws IOException {

        File tempJar = tempFolder.newFile("tempJar.jar");
        try (FileOutputStream out = new FileOutputStream(tempJar)) {
            out.write(new byte[]{0x50, 0x4B, 0x03, 0x04});
        }
        File tempJarZip = tempFolder.newFile("tempZip.zip");
        File tempClass = tempFolder.newFile("TempClass.class");

        String yamlTestString = "hazelcast:\n"
                + "  user-code-namespaces:\n"
                + "    enabled: true\n"
                + "    class-filter:\n"
                + "      defaults-disabled: false\n"
                + "      blacklist:\n"
                + "        class:\n"
                + "          - com.acme.app.BeanComparator\n"
                + "      whitelist:\n"
                + "        package:\n"
                + "          - com.acme.app\n"
                + "        prefix:\n"
                + "          - com.hazelcast.\n"
                + "    ns1:\n"
                + "      - id: \"jarId\"\n"
                + "        resource-type: \"jar\"\n"
                + "        url: " + tempJar.toURI().toURL() + "\n"
                + "      - id: \"zipId\"\n"
                + "        resource-type: \"jars_in_zip\"\n"
                + "        url: " + tempJarZip.toURI().toURL() + "\n"
                + "      - id: \"classId\"\n"
                + "        resource-type: \"class\"\n"
                + "        url: " + tempClass.toURI().toURL() + "\n"
                + "    ns2:\n"
                + "      - id: \"jarId2\"\n"
                + "        resource-type: \"jar\"\n"
                + "        url: " + tempJar.toURI().toURL() + "\n";

        final UserCodeNamespacesConfig userCodeNamespacesConfig = buildConfig(yamlTestString).getNamespacesConfig();
        assertThat(userCodeNamespacesConfig.isEnabled()).isTrue();
        assertThat(userCodeNamespacesConfig.getNamespaceConfigs()).hasSize(2);
        final UserCodeNamespaceConfig userCodeNamespaceConfig = userCodeNamespacesConfig.getNamespaceConfigs().get("ns1");

        assertNotNull(userCodeNamespaceConfig);

        assertThat(userCodeNamespaceConfig.getName()).isEqualTo("ns1");
        assertThat(userCodeNamespaceConfig.getResourceConfigs()).hasSize(3);

        // validate NS1 ResourceDefinition contents.
        Collection<ResourceDefinition> ns1Resources = userCodeNamespaceConfig.getResourceConfigs();
        Optional<ResourceDefinition> jarIdResource = ns1Resources.stream().filter(r -> r.id().equals("jarId")).findFirst();
        assertThat(jarIdResource).isPresent();
        assertThat(jarIdResource.get().url()).isEqualTo(tempJar.toURI().toURL().toString());
        assertEquals(ResourceType.JAR, jarIdResource.get().type());
        // check the bytes[] are equal
        assertArrayEquals(getTestFileBytes(tempJar), jarIdResource.get().payload());
        Optional<ResourceDefinition> zipId = ns1Resources.stream().filter(r -> r.id().equals("zipId")).findFirst();
        Optional<ResourceDefinition> classId = ns1Resources.stream().filter(r -> r.id().equals("classId")).findFirst();
        assertThat(zipId).isPresent();
        assertThat(zipId.get().url()).isEqualTo(tempJarZip.toURI().toURL().toString());
        assertEquals(ResourceType.JARS_IN_ZIP, zipId.get().type());
        assertThat(classId).isPresent();
        assertThat(classId.get().url()).isEqualTo(tempClass.toURI().toURL().toString());
        assertEquals(ResourceType.CLASS, classId.get().type());
        // check the bytes[] are equal
        assertArrayEquals(getTestFileBytes(tempJarZip), zipId.get().payload());
        // validate NS2 ResourceDefinition contents.
        final UserCodeNamespaceConfig userCodeNamespaceConfig2 = userCodeNamespacesConfig.getNamespaceConfigs().get("ns2");
        assertNotNull(userCodeNamespaceConfig2);
        assertThat(userCodeNamespaceConfig2.getName()).isEqualTo("ns2");
        assertThat(userCodeNamespaceConfig2.getResourceConfigs()).hasSize(1);
        Collection<ResourceDefinition> ns2Resources = userCodeNamespaceConfig2.getResourceConfigs();
        assertThat(ns2Resources).hasSize(1);
        Optional<ResourceDefinition> jarId2Resource = ns2Resources.stream().filter(r -> r.id().equals("jarId2")).findFirst();
        assertThat(jarId2Resource).isPresent();
        assertThat(jarId2Resource.get().url()).isEqualTo(tempJar.toURI().toURL().toString());
        assertEquals(ResourceType.JAR, jarId2Resource.get().type());
        // check the bytes[] are equal
        assertArrayEquals(getTestFileBytes(tempJar), jarId2Resource.get().payload());

        // Validate filtering config
        assertNotNull(userCodeNamespacesConfig.getClassFilterConfig());
        JavaSerializationFilterConfig filterConfig = userCodeNamespacesConfig.getClassFilterConfig();
        assertFalse(filterConfig.isDefaultsDisabled());
        assertTrue(filterConfig.getWhitelist().isListed("com.acme.app.FakeClass"));
        assertTrue(filterConfig.getWhitelist().isListed("com.hazelcast.fake.place.MagicClass"));
        assertFalse(filterConfig.getWhitelist().isListed("not.in.the.whitelist.ClassName"));
        assertTrue(filterConfig.getBlacklist().isListed("com.acme.app.BeanComparator"));
        assertFalse(filterConfig.getBlacklist().isListed("not.in.the.blacklist.ClassName"));
    }

    @Test
    public void testNamespaceConfigs_newStyle() throws IOException {

        File tempJar = tempFolder.newFile("tempJar.jar");
        try (FileOutputStream out = new FileOutputStream(tempJar)) {
            out.write(new byte[]{0x50, 0x4B, 0x03, 0x04});
        }
        File tempJarZip = tempFolder.newFile("tempZip.zip");
        File tempClass = tempFolder.newFile("TempClass.class");

        String yamlTestString = "hazelcast:\n"
                + "  user-code-namespaces:\n"
                + "    enabled: true\n"
                + "    class-filter:\n"
                + "      defaults-disabled: false\n"
                + "      blacklist:\n"
                + "        class:\n"
                + "          - com.acme.app.BeanComparator\n"
                + "      whitelist:\n"
                + "        package:\n"
                + "          - com.acme.app\n"
                + "        prefix:\n"
                + "          - com.hazelcast.\n"
                + "    name-spaces:\n"
                + "      ns1:\n"
                + "        - id: \"jarId\"\n"
                + "          resource-type: \"jar\"\n"
                + "          url: " + tempJar.toURI().toURL() + "\n"
                + "        - id: \"zipId\"\n"
                + "          resource-type: \"jars_in_zip\"\n"
                + "          url: " + tempJarZip.toURI().toURL() + "\n"
                + "        - id: \"classId\"\n"
                + "          resource-type: \"class\"\n"
                + "          url: " + tempClass.toURI().toURL() + "\n"
                + "      ns2:\n"
                + "        - id: \"jarId2\"\n"
                + "          resource-type: \"jar\"\n"
                + "          url: " + tempJar.toURI().toURL() + "\n";

        final UserCodeNamespacesConfig userCodeNamespacesConfig = buildConfig(yamlTestString).getNamespacesConfig();
        assertThat(userCodeNamespacesConfig.isEnabled()).isTrue();
        assertThat(userCodeNamespacesConfig.getNamespaceConfigs()).hasSize(2);
        final UserCodeNamespaceConfig userCodeNamespaceConfig = userCodeNamespacesConfig.getNamespaceConfigs().get("ns1");

        assertNotNull(userCodeNamespaceConfig);

        assertThat(userCodeNamespaceConfig.getName()).isEqualTo("ns1");
        assertThat(userCodeNamespaceConfig.getResourceConfigs()).hasSize(3);

        // validate NS1 ResourceDefinition contents.
        Collection<ResourceDefinition> ns1Resources = userCodeNamespaceConfig.getResourceConfigs();
        Optional<ResourceDefinition> jarIdResource = ns1Resources.stream().filter(r -> r.id().equals("jarId")).findFirst();
        assertThat(jarIdResource).isPresent();
        assertThat(jarIdResource.get().url()).isEqualTo(tempJar.toURI().toURL().toString());
        assertEquals(ResourceType.JAR, jarIdResource.get().type());
        // check the bytes[] are equal
        assertArrayEquals(getTestFileBytes(tempJar), jarIdResource.get().payload());
        Optional<ResourceDefinition> zipId = ns1Resources.stream().filter(r -> r.id().equals("zipId")).findFirst();
        Optional<ResourceDefinition> classId = ns1Resources.stream().filter(r -> r.id().equals("classId")).findFirst();
        assertThat(zipId).isPresent();
        assertThat(zipId.get().url()).isEqualTo(tempJarZip.toURI().toURL().toString());
        assertEquals(ResourceType.JARS_IN_ZIP, zipId.get().type());
        assertThat(classId).isPresent();
        assertThat(classId.get().url()).isEqualTo(tempClass.toURI().toURL().toString());
        assertEquals(ResourceType.CLASS, classId.get().type());
        // check the bytes[] are equal
        assertArrayEquals(getTestFileBytes(tempJarZip), zipId.get().payload());
        // validate NS2 ResourceDefinition contents.
        final UserCodeNamespaceConfig userCodeNamespaceConfig2 = userCodeNamespacesConfig.getNamespaceConfigs().get("ns2");
        assertNotNull(userCodeNamespaceConfig2);
        assertThat(userCodeNamespaceConfig2.getName()).isEqualTo("ns2");
        assertThat(userCodeNamespaceConfig2.getResourceConfigs()).hasSize(1);
        Collection<ResourceDefinition> ns2Resources = userCodeNamespaceConfig2.getResourceConfigs();
        assertThat(ns2Resources).hasSize(1);
        Optional<ResourceDefinition> jarId2Resource = ns2Resources.stream().filter(r -> r.id().equals("jarId2")).findFirst();
        assertThat(jarId2Resource).isPresent();
        assertThat(jarId2Resource.get().url()).isEqualTo(tempJar.toURI().toURL().toString());
        assertEquals(ResourceType.JAR, jarId2Resource.get().type());
        // check the bytes[] are equal
        assertArrayEquals(getTestFileBytes(tempJar), jarId2Resource.get().payload());

        // Validate filtering config
        assertNotNull(userCodeNamespacesConfig.getClassFilterConfig());
        JavaSerializationFilterConfig filterConfig = userCodeNamespacesConfig.getClassFilterConfig();
        assertFalse(filterConfig.isDefaultsDisabled());
        assertTrue(filterConfig.getWhitelist().isListed("com.acme.app.FakeClass"));
        assertTrue(filterConfig.getWhitelist().isListed("com.hazelcast.fake.place.MagicClass"));
        assertFalse(filterConfig.getWhitelist().isListed("not.in.the.whitelist.ClassName"));
        assertTrue(filterConfig.getBlacklist().isListed("com.acme.app.BeanComparator"));
        assertFalse(filterConfig.getBlacklist().isListed("not.in.the.blacklist.ClassName"));
    }

    /**
     * Unit test for {@link YamlMemberDomConfigProcessor}. It is placed under {@link com.hazelcast.config} package,
     * because we need access to the {@link UserCodeNamespacesConfig#getNamespaceConfigs()} package-private method.
     */
    @Test
    public void unitTestNamespaceConfigs_oldStyle() throws IOException {
        File tempJar = tempFolder.newFile("tempJar.jar");

        String yamlTestString = "hazelcast:\n"
                + "  user-code-namespaces:\n"
                + "    enabled: true\n"
                + "    ns1:\n"
                + "      - id: \"jarId1\"\n"
                + "        resource-type: \"jar\"\n"
                + "        url: " + tempJar.toURI().toURL() + "\n"
                + "      - id: \"jarId2\"\n"
                + "        resource-type: \"jar\"\n"
                + "        url: " + tempJar.toURI().toURL() + "\n"
                + "    ns2:\n"
                + "      - id: \"jarId3\"\n"
                + "        resource-type: \"jar\"\n"
                + "        url: " + tempJar.toURI().toURL() + "\n"
                + "      - id: \"jarId4\"\n"
                + "        resource-type: \"jar\"\n"
                + "        url: " + tempJar.toURI().toURL() + "\n";

        assertNamespaceConfig(yamlTestString);
    }

    /**
     * Unit test for {@link YamlMemberDomConfigProcessor}. It is placed under {@link com.hazelcast.config} package,
     * because we need access to the {@link UserCodeNamespacesConfig#getNamespaceConfigs()} package-private method.
     */
    @Test
    public void unitTestNamespaceConfigs_newStyle() throws IOException {
        File tempJar = tempFolder.newFile("tempJar.jar");

        String yamlTestString = "hazelcast:\n"
                + "  user-code-namespaces:\n"
                + "    enabled: true\n"
                + "    name-spaces:\n"
                + "      ns1:\n"
                + "        - id: \"jarId1\"\n"
                + "          resource-type: \"jar\"\n"
                + "          url: " + tempJar.toURI().toURL() + "\n"
                + "        - id: \"jarId2\"\n"
                + "          resource-type: \"jar\"\n"
                + "          url: " + tempJar.toURI().toURL() + "\n"
                + "      ns2:\n"
                + "        - id: \"jarId3\"\n"
                + "          resource-type: \"jar\"\n"
                + "          url: " + tempJar.toURI().toURL() + "\n"
                + "        - id: \"jarId4\"\n"
                + "          resource-type: \"jar\"\n"
                + "          url: " + tempJar.toURI().toURL() + "\n";

        assertNamespaceConfig(yamlTestString);
    }

    /**
     * Unit test for {@link YamlMemberDomConfigProcessor}. It is placed under {@link com.hazelcast.config} package,
     * because we need access to the {@link UserCodeNamespacesConfig#getNamespaceConfigs()} package-private method.
     */
    @Test
    public void unitTestDuplicatedNamespaceConfigs_mixedStyle_throws() throws IOException {
        File tempJar = tempFolder.newFile("tempJar.jar");

        /*
           name-spaces:
              ns1
           ns1
         */
        final String yamlTestString = "hazelcast:\n"
                + "  user-code-namespaces:\n"
                + "    enabled: true\n"
                + "    name-spaces:\n"
                + "      ns1:\n"
                + "        - id: \"jarId1\"\n"
                + "          resource-type: \"jar\"\n"
                + "          url: " + tempJar.toURI().toURL() + "\n"
                + "        - id: \"jarId2\"\n"
                + "          resource-type: \"jar\"\n"
                + "          url: " + tempJar.toURI().toURL() + "\n"
                + "    ns1:\n"
                + "      - id: \"jarId3\"\n"
                + "        resource-type: \"jar\"\n"
                + "        url: " + tempJar.toURI().toURL() + "\n"
                + "      - id: \"jarId4\"\n"
                + "        resource-type: \"jar\"\n"
                + "        url: " + tempJar.toURI().toURL() + "\n";

        assertThatThrownBy(() -> buildConfig(yamlTestString).getNamespacesConfig())
                .isInstanceOf(InvalidConfigurationException.class);

        /*
           ns1
           name-spaces:
              ns1
         */
        final String _yamlTestString = "hazelcast:\n"
                + "  user-code-namespaces:\n"
                + "    enabled: true\n"
                + "    ns1:\n"
                + "      - id: \"jarId3\"\n"
                + "        resource-type: \"jar\"\n"
                + "        url: " + tempJar.toURI().toURL() + "\n"
                + "      - id: \"jarId4\"\n"
                + "        resource-type: \"jar\"\n"
                + "        url: " + tempJar.toURI().toURL() + "\n"
                + "    name-spaces:\n"
                + "      ns1:\n"
                + "        - id: \"jarId1\"\n"
                + "          resource-type: \"jar\"\n"
                + "          url: " + tempJar.toURI().toURL() + "\n"
                + "        - id: \"jarId2\"\n"
                + "          resource-type: \"jar\"\n"
                + "          url: " + tempJar.toURI().toURL() + "\n";

        assertThatThrownBy(() -> buildConfig(_yamlTestString).getNamespacesConfig())
                .isInstanceOf(InvalidConfigurationException.class);
    }

    @Test
    public void unitTestNamespaceConfigs_throws() throws IOException {
        File tempJar = tempFolder.newFile("tempJar.jar");

        String yamlTestString = "hazelcast:\n"
                + "  user-code-namespaces:\n"
                + "    enabled: true\n"
                + "    name-spaces:\n"
                + "      ns1:\n"
                + "        - id: \"jarId1\"\n"
                + "          resource-type: \"jars\"\n"
                + "          url: " + tempJar.toURI().toURL() + "\n";

        assertThatThrownBy(() -> buildConfig(yamlTestString))
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("was configured with invalid resource type");
    }

    private void assertNamespaceConfig(String yamlTestString) {
        final UserCodeNamespacesConfig ucnConfig = buildConfig(yamlTestString).getNamespacesConfig();
        assertNotNull(ucnConfig);
        assertTrue(ucnConfig.isEnabled());
        Map<String, UserCodeNamespaceConfig> namespaceConfigs = ucnConfig.getNamespaceConfigs();
        assertEquals(2, namespaceConfigs.size());
        assertTrue(namespaceConfigs.keySet().containsAll(asList("ns1", "ns2")));
        namespaceConfigs.values().forEach(namespaceConfig ->
                assertEquals(2, namespaceConfig.getResourceConfigs().size()));
    }

    @Override
    public void testRestConfig() throws IOException {
        String yaml = """
                hazelcast:
                  rest:
                    enabled: true
                    port: 8080
                    security-realm: realmName
                    token-validity-seconds: 500
                    max-login-attempts: 10
                    lockout-duration-seconds: 10
                    ssl:
                      enabled: true
                      client-auth: NEED
                      ciphers: TLS_RSA_WITH_AES_128_CBC_SHA, TLS_RSA_WITH_AES_128_CBC_SHA256
                      enabled-protocols: TLSv1.2, TLSv1.3
                      key-alias: myKeyAlias
                      key-password: myKeyPassword
                      key-store: /path/to/keystore
                      key-store-password: myKeyStorePassword
                      key-store-type: JKS
                      key-store-provider: SUN
                      trust-store: /path/to/truststore
                      trust-store-password: myTrustStorePassword
                      trust-store-type: JKS
                      trust-store-provider: SUN
                      protocol: TLS
                      certificate: /path/to/certificate
                      certificate-key: /path/to/certificate-key
                      trust-certificate: /path/to/trust-certificate
                      trust-certificate-key: /path/to/trust-certificate-key
                """;

        Config config = buildConfig(yaml);
        validateRestConfig(config);
    }

    @Override
    public void testMapExpiryConfig() {
        String yaml = """
                hazelcast:
                  map:
                    expiry:
                      time-to-live-seconds: 2147483647
                      max-idle-seconds: 2147483647
                """;

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("expiry");

        assertEquals(Integer.MAX_VALUE, mapConfig.getTimeToLiveSeconds());
        assertEquals(Integer.MAX_VALUE, mapConfig.getMaxIdleSeconds());
    }

    @Override
    @Test
    public void testTpcConfig() {
        String yaml = """
                hazelcast:
                  tpc:
                    enabled: true
                    eventloop-count: 12
                """;

        TpcConfig tpcConfig = buildConfig(yaml).getTpcConfig();

        assertThat(tpcConfig.isEnabled()).isTrue();
        assertThat(tpcConfig.getEventloopCount()).isEqualTo(12);
    }

    @Override
    @Test
    public void testTpcSocketConfig() {
        String yaml = """
                hazelcast:
                  network:
                    tpc-socket:
                      port-range: 14000-16000
                      receive-buffer-size-kb: 256
                      send-buffer-size-kb: 256
                """;

        TpcSocketConfig tpcSocketConfig = buildConfig(yaml).getNetworkConfig().getTpcSocketConfig();

        assertThat(tpcSocketConfig.getPortRange()).isEqualTo("14000-16000");
        assertThat(tpcSocketConfig.getReceiveBufferSizeKB()).isEqualTo(256);
        assertThat(tpcSocketConfig.getSendBufferSizeKB()).isEqualTo(256);
    }

    @Override
    @Test
    public void testTpcSocketConfigAdvanced() {
        String yaml = """
                hazelcast:
                  advanced-network:
                    enabled: true
                    member-server-socket-endpoint-config:\s
                      tpc-socket:\s
                        port-range: 14000-16000
                        receive-buffer-size-kb: 256
                        send-buffer-size-kb: 256
                    client-server-socket-endpoint-config:
                      tpc-socket:
                        port-range: 14000-16000
                        receive-buffer-size-kb: 256
                        send-buffer-size-kb: 256
                    memcache-server-socket-endpoint-config:
                      tpc-socket:
                        port-range: 14000-16000
                        receive-buffer-size-kb: 256
                        send-buffer-size-kb: 256
                    rest-server-socket-endpoint-config:
                      tpc-socket:
                        port-range: 14000-16000
                        receive-buffer-size-kb: 256
                        send-buffer-size-kb: 256
                    wan-endpoint-config:\s
                      tokyo:
                        tpc-socket:
                          port-range: 14000-16000
                          receive-buffer-size-kb: 256
                          send-buffer-size-kb: 256
                    wan-server-socket-endpoint-config:\s
                      london:
                        tpc-socket:
                          port-range: 14000-16000
                          receive-buffer-size-kb: 256
                          send-buffer-size-kb: 256
                """;

        Map<EndpointQualifier, EndpointConfig> endpointConfigs = buildConfig(yaml)
                .getAdvancedNetworkConfig()
                .getEndpointConfigs();

        assertThat(endpointConfigs).hasSize(6);

        endpointConfigs.forEach((endpointQualifier, endpointConfig) -> {
            TpcSocketConfig tpcSocketConfig = endpointConfig.getTpcSocketConfig();

            assertThat(tpcSocketConfig.getPortRange()).isEqualTo("14000-16000");
            assertThat(tpcSocketConfig.getReceiveBufferSizeKB()).isEqualTo(256);
            assertThat(tpcSocketConfig.getSendBufferSizeKB()).isEqualTo(256);
        });
    }

    @Override
    public void testVectorCollectionConfig() {
        String yaml = """
                hazelcast:
                  vector-collection:
                    vector-1:
                      indexes:
                        - name: index-1-1
                          dimension: 2
                          metric: DOT
                          max-degree: 10
                          ef-construction: 10
                          use-deduplication: true
                        - name: index-1-2
                          dimension: 3
                          metric: EUCLIDEAN
                    vector-2:
                      backup-count: 2
                      async-backup-count: 1
                      split-brain-protection-ref: splitBrainProtectionName
                      user-code-namespace: ns1
                      merge-policy:
                        batch-size: 132
                        class-name: CustomMergePolicy
                      indexes:
                        - dimension: 4
                          metric: COSINE
                          use-deduplication: false
                """;

        Config config = buildConfig(yaml);
        validateVectorCollectionConfig(config);
    }

    @Override
    @Test
    public void testVectorCollectionConfig_backupCount_max() {
        int backupCount = 6;
        String yaml = simpleVectorCollectionBackupCountConfig(backupCount);
        Config config = buildConfig(yaml);
        assertThat(config.getVectorCollectionConfigs().get("vector-1").getBackupCount()).isEqualTo(backupCount);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testVectorCollectionConfig_backupCount_moreThanMax() {
        String yaml = simpleVectorCollectionBackupCountConfig(7);
        buildConfig(yaml);
    }

    @Override
    @Test
    public void testVectorCollectionConfig_backupCount_min() {
        int backupCount = 0;
        String yaml = simpleVectorCollectionBackupCountConfig(backupCount);
        Config config = buildConfig(yaml);
        assertThat(config.getVectorCollectionConfigs().get("vector-1").getBackupCount()).isEqualTo(backupCount);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testVectorCollectionConfig_backupCount_lessThanMin() {
        String yaml = simpleVectorCollectionBackupCountConfig(-1);
        buildConfig(yaml);
    }

    @Override
    @Test
    public void testVectorCollectionConfig_asyncBackupCount_max() {
        int asyncBackupCount = 6;
        // we need to set backup-count=0 since default is 1
        // and backup count + async backup count must not exceed 6
        String yaml = simpleVectorCollectionBackupCountAndAsyncBackupCountConfig(0, asyncBackupCount);
        Config config = buildConfig(yaml);
        assertThat(config.getVectorCollectionConfigs().get("vector-1").getAsyncBackupCount()).isEqualTo(asyncBackupCount);
        assertThat(config.getVectorCollectionConfigs().get("vector-1").getBackupCount()).isEqualTo(0);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testVectorCollectionConfig_asyncBackupCount_moreThanMax() {
        // we need to set backup-count=0 since default is 1
        // and backup count + async backup count must not exceed 6
        String yaml = simpleVectorCollectionBackupCountAndAsyncBackupCountConfig(0, 7);
        buildConfig(yaml);
    }

    @Override
    @Test
    public void testVectorCollectionConfig_asyncBackupCount_min() {
        int asyncBackupCount = 0;
        String yaml = simpleVectorCollectionAsyncBackupCountConfig(asyncBackupCount);
        Config config = buildConfig(yaml);
        assertThat(config.getVectorCollectionConfigs().get("vector-1").getAsyncBackupCount()).isEqualTo(asyncBackupCount);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testVectorCollectionConfig_asyncBackupCount_lessThanMin() {
        String yaml = simpleVectorCollectionAsyncBackupCountConfig(-1);
        buildConfig(yaml);
    }

    @Override
    @Test
    public void testVectorCollectionConfig_backupSyncAndAsyncCount_max() {
        int backupCount = 4;
        int asyncBackupCount = 2;
        String yaml = simpleVectorCollectionBackupCountAndAsyncBackupCountConfig(backupCount, asyncBackupCount);
        Config config = buildConfig(yaml);
        assertThat(config.getVectorCollectionConfigs().get("vector-1").getBackupCount()).isEqualTo(backupCount);
        assertThat(config.getVectorCollectionConfigs().get("vector-1").getAsyncBackupCount()).isEqualTo(asyncBackupCount);
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void testVectorCollectionConfig_backupSyncAndAsyncCount_moreThanMax() {
        int backupCount = 4;
        int asyncBackupCount = 3;
        String yaml = simpleVectorCollectionBackupCountAndAsyncBackupCountConfig(backupCount, asyncBackupCount);
        buildConfig(yaml);
    }

    @Override
    public void testVectorCollectionConfig_multipleIndexesWithTheSameName_fail() {
        String yaml = """
                hazelcast:
                  vector-collection:
                    vector-1:
                      indexes:
                        - name: index-1
                          dimension: 2
                          metric: DOT
                          max-degree: 10
                          ef-construction: 10
                          use-deduplication: true
                        - name: index-1
                          dimension: 3
                          metric: EUCLIDEAN
                """;

        Config config = buildConfig(yaml);
    }

    private String simpleVectorCollectionBackupCountConfig(int count) {
        return simpleVectorCollectionBackupCountConfig("backup-count", count);
    }

    private String simpleVectorCollectionAsyncBackupCountConfig(int count) {
        return simpleVectorCollectionBackupCountConfig("async-backup-count", count);
    }

    private String simpleVectorCollectionBackupCountConfig(String tagName, int count) {
        return String.format("""
                hazelcast:
                  vector-collection:
                    vector-1:
                      %s: %d
                      indexes:
                        - dimension: 4
                          metric: COSINE
                """, tagName, count);
    }

    private String simpleVectorCollectionBackupCountAndAsyncBackupCountConfig(
            int backupCount, int asyncBackupCount) {
        return String.format("""
                hazelcast:
                  vector-collection:
                    vector-1:
                      backup-count: %d
                      async-backup-count: %d
                      indexes:
                        - dimension: 4
                          metric: COSINE
                """, backupCount, asyncBackupCount);
    }

    public String getAdvancedNetworkConfigWithSocketOption(String socketOption, int value) {
        return "hazelcast:\n"
                + "  advanced-network:\n"
                + "    enabled: true\n"
                + "    member-server-socket-endpoint-config: \n"
                + "      socket-options: \n"
                + "        " + socketOption + ": " + value + "\n";
    }
}
