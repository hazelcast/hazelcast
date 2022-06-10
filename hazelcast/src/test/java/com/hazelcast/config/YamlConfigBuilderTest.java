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

package com.hazelcast.config;

import com.google.common.collect.ImmutableSet;
import com.hazelcast.config.LoginModuleConfig.LoginModuleUsage;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.config.cp.FencedLockConfig;
import com.hazelcast.config.cp.RaftAlgorithmConfig;
import com.hazelcast.config.cp.SemaphoreConfig;
import com.hazelcast.config.security.KerberosAuthenticationConfig;
import com.hazelcast.config.security.KerberosIdentityConfig;
import com.hazelcast.config.security.LdapAuthenticationConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.config.security.SimpleAuthenticationConfig;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.config.SchemaViolationConfigurationException;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.impl.compact.CompactTestUtil;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionOn;
import com.hazelcast.splitbrainprotection.impl.ProbabilisticSplitBrainProtectionFunction;
import com.hazelcast.splitbrainprotection.impl.RecentlyActiveSplitBrainProtectionFunction;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.TopicOverloadPolicy;
import com.hazelcast.wan.WanPublisherState;
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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.config.DynamicConfigurationConfig.DEFAULT_BACKUP_COUNT;
import static com.hazelcast.config.DynamicConfigurationConfig.DEFAULT_BACKUP_DIR;
import static com.hazelcast.config.LocalDeviceConfig.DEFAULT_BLOCK_SIZE_IN_BYTES;
import static com.hazelcast.config.LocalDeviceConfig.DEFAULT_DEVICE_BASE_DIR;
import static com.hazelcast.config.LocalDeviceConfig.DEFAULT_DEVICE_NAME;
import static com.hazelcast.config.LocalDeviceConfig.DEFAULT_READ_IO_THREAD_COUNT;
import static com.hazelcast.config.LocalDeviceConfig.DEFAULT_WRITE_IO_THREAD_COUNT;
import static com.hazelcast.config.EvictionPolicy.LRU;
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
public class YamlConfigBuilderTest
        extends AbstractConfigBuilderTest {

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
        String yaml = ""
                + "hazelcast:\n"
                + "  cluster-name: my-cluster\n";

        Config config = buildConfig(yaml);
        assertEquals("my-cluster", config.getClusterName());
    }

    @Override
    @Test
    public void testConfigurationWithFileName()
            throws Exception {
        assumeThatNotZingJDK6(); // https://github.com/hazelcast/hazelcast/issues/9044

        File file = createTempFile("foo", "bar");
        file.deleteOnExit();

        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    my-map:\n"
                + "      backup-count: 1";
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
    public void testSecurityConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  security:\n"
                + "    enabled: true\n"
                + "    security-interceptors:\n"
                + "      - foo\n"
                + "      - bar\n"
                + "    client-block-unmapped-actions: false\n"
                + "    member-authentication:\n"
                + "      realm: mr\n"
                + "    client-authentication:\n"
                + "      realm: cr\n"
                + "    realms:\n"
                + "      - name: mr\n"
                + "        authentication:\n"
                + "          jaas:\n"
                + "            - class-name: MyRequiredLoginModule\n"
                + "              usage: REQUIRED\n"
                + "              properties:\n"
                + "                login-property: login-value\n"
                + "            - class-name: MyRequiredLoginModule2\n"
                + "              usage: SUFFICIENT\n"
                + "              properties:\n"
                + "                login-property2: login-value2\n"
                + "        identity:\n"
                + "          credentials-factory:\n"
                + "            class-name: MyCredentialsFactory\n"
                + "            properties:\n"
                + "              property: value\n"
                + "      - name: cr\n"
                + "        authentication:\n"
                + "          jaas:\n"
                + "            - class-name: MyOptionalLoginModule\n"
                + "              usage: OPTIONAL\n"
                + "              properties:\n"
                + "                client-property: client-value\n"
                + "            - class-name: MyRequiredLoginModule\n"
                + "              usage: REQUIRED\n"
                + "              properties:\n"
                + "                client-property2: client-value2\n"
                + "      - name: kerberos\n"
                + "        authentication:\n"
                + "          kerberos:\n"
                + "            skip-role: false\n"
                + "            relax-flags-check: true\n"
                + "            use-name-without-realm: true\n"
                + "            security-realm: krb5Acceptor\n"
                + "            principal: jduke@HAZELCAST.COM\n"
                + "            keytab-file: /opt/jduke.keytab\n"
                + "            ldap:\n"
                + "              url: ldap://127.0.0.1\n"
                + "        identity:\n"
                + "          kerberos:\n"
                + "            realm: HAZELCAST.COM\n"
                + "            security-realm: krb5Initializer\n"
                + "            principal: jduke@HAZELCAST.COM\n"
                + "            keytab-file: /opt/jduke.keytab\n"
                + "            use-canonical-hostname: true\n"
                + "      - name: simple\n"
                + "        authentication:\n"
                + "          simple:\n"
                + "            skip-role: true\n"
                + "            users:\n"
                + "              - username: test\n"
                + "                password: 'a1234'\n"
                + "                roles:\n"
                + "                  - monitor\n"
                + "                  - hazelcast\n"
                + "              - username: dev\n"
                + "                password: secret\n"
                + "                roles:\n"
                + "                  - root\n"
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

        RealmConfig memberRealm = securityConfig.getRealmConfig(securityConfig.getMemberRealm());
        CredentialsFactoryConfig memberCredentialsConfig = memberRealm.getCredentialsFactoryConfig();
        assertEquals("MyCredentialsFactory", memberCredentialsConfig.getClassName());
        assertEquals(1, memberCredentialsConfig.getProperties().size());
        assertEquals("value", memberCredentialsConfig.getProperties().getProperty("property"));

        List<LoginModuleConfig> memberLoginModuleConfigs = memberRealm.getJaasAuthenticationConfig().getLoginModuleConfigs();
        assertEquals(2, memberLoginModuleConfigs.size());
        Iterator<LoginModuleConfig> memberLoginIterator = memberLoginModuleConfigs.iterator();

        LoginModuleConfig memberLoginModuleCfg1 = memberLoginIterator.next();
        assertEquals("MyRequiredLoginModule", memberLoginModuleCfg1.getClassName());
        assertEquals(LoginModuleUsage.REQUIRED, memberLoginModuleCfg1.getUsage());
        assertEquals(1, memberLoginModuleCfg1.getProperties().size());
        assertEquals("login-value", memberLoginModuleCfg1.getProperties().getProperty("login-property"));

        LoginModuleConfig memberLoginModuleCfg2 = memberLoginIterator.next();
        assertEquals("MyRequiredLoginModule2", memberLoginModuleCfg2.getClassName());
        assertEquals(LoginModuleUsage.SUFFICIENT, memberLoginModuleCfg2.getUsage());
        assertEquals(1, memberLoginModuleCfg2.getProperties().size());
        assertEquals("login-value2", memberLoginModuleCfg2.getProperties().getProperty("login-property2"));

        RealmConfig clientRealm = securityConfig.getRealmConfig(securityConfig.getClientRealm());
        List<LoginModuleConfig> clientLoginModuleConfigs = clientRealm.getJaasAuthenticationConfig().getLoginModuleConfigs();
        assertEquals(2, clientLoginModuleConfigs.size());
        Iterator<LoginModuleConfig> clientLoginIterator = clientLoginModuleConfigs.iterator();

        LoginModuleConfig clientLoginModuleCfg1 = clientLoginIterator.next();
        assertEquals("MyOptionalLoginModule", clientLoginModuleCfg1.getClassName());
        assertEquals(LoginModuleUsage.OPTIONAL, clientLoginModuleCfg1.getUsage());
        assertEquals(1, clientLoginModuleCfg1.getProperties().size());
        assertEquals("client-value", clientLoginModuleCfg1.getProperties().getProperty("client-property"));

        LoginModuleConfig clientLoginModuleCfg2 = clientLoginIterator.next();
        assertEquals("MyRequiredLoginModule", clientLoginModuleCfg2.getClassName());
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
                + "        shouldUseDns: false\n"
                + "        serviceUrl.default: http://localhost:8082/eureka\n"
                + "        namespace: hazelcast\n";

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
                + "              key-boolean: true\n";

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
                + "    port:\n"
                + "      port: 5703\n");
        assertEquals(100, config.getNetworkConfig().getPortCount());
        assertEquals(5703, config.getNetworkConfig().getPort());
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
                + "    port: \n"
                + "      port: 5801\n");
        assertTrue(config.getNetworkConfig().isPortAutoIncrement());
        assertEquals(5801, config.getNetworkConfig().getPort());
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
    public void readQueueConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  queue:\n"
                + "    custom:\n"
                + "      priority-comparator-class-name: com.hazelcast.collection.impl.queue.model.PriorityElementComparator\n"
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
                + "      split-brain-protection-ref: customSplitBrainProtectionRule\n"
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
        assertEquals("com.hazelcast.collection.impl.queue.model.PriorityElementComparator",
                customQueueConfig.getPriorityComparatorClassName());

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
                + "      split-brain-protection-ref: customSplitBrainProtectionRule\n"
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
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    testCaseInsensitivity:\n"
                + "      in-memory-format: BINARY\n"
                + "      backup-count: 1\n"
                + "      async-backup-count: 0\n"
                + "      time-to-live-seconds: 0\n"
                + "      max-idle-seconds: 0\n"
                + "      eviction:\n"
                + "         eviction-policy: NONE\n"
                + "         max-size-policy: per_partition\n"
                + "         size: 0\n"
                + "      merge-policy:\n"
                + "        class-name: CustomMergePolicy\n"
                + "        batch-size: 2342\n";

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
        String yaml = ""
                + "hazelcast:\n"
                + "  management-center:\n"
                + "    scripting-enabled: true\n"
                + "    console-enabled: true\n"
                + "    data-access-enabled: false\n"
                + "    trusted-interfaces:\n"
                + "      - 127.0.0.1\n"
                + "      - 192.168.1.*\n";

        Config config = buildConfig(yaml);
        ManagementCenterConfig mcConfig = config.getManagementCenterConfig();

        assertTrue(mcConfig.isScriptingEnabled());
        assertTrue(mcConfig.isConsoleEnabled());
        assertFalse(mcConfig.isDataAccessEnabled());
        assertEquals(2, mcConfig.getTrustedInterfaces().size());
        assertTrue(mcConfig.getTrustedInterfaces().containsAll(ImmutableSet.of("127.0.0.1", "192.168.1.*")));
    }

    @Override
    @Test
    public void testNullManagementCenterConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  management-center: {}";

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
    public void testMapConfig_statisticsEnable() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    mymap:\n"
                + "      statistics-enabled: false";

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertFalse(mapConfig.isStatisticsEnabled());
    }

    @Override
    public void testMapConfig_perEntryStatsEnabled() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    mymap:\n"
                + "      per-entry-stats-enabled: true";

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertTrue(mapConfig.isStatisticsEnabled());
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
                + "         eviction:\n"
                + "             eviction-policy: LRU\n"
                + ""
                + "    lfuMap:\n"
                + "          eviction:\n"
                + "             eviction-policy: LFU\n"
                + ""
                + "    noneMap:\n"
                + "         eviction:\n"
                + "             eviction-policy: NONE\n"
                + ""
                + "    randomMap:\n"
                + "        eviction:\n"
                + "             eviction-policy: RANDOM\n";

        Config config = buildConfig(yaml);

        assertEquals(EvictionPolicy.LRU, config.getMapConfig("lruMap").getEvictionConfig().getEvictionPolicy());
        assertEquals(EvictionPolicy.LFU, config.getMapConfig("lfuMap").getEvictionConfig().getEvictionPolicy());
        assertEquals(EvictionPolicy.NONE, config.getMapConfig("noneMap").getEvictionConfig().getEvictionPolicy());
        assertEquals(EvictionPolicy.RANDOM, config.getMapConfig("randomMap").getEvictionConfig().getEvictionPolicy());
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
    public void testMapStoreEnabled() {
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
    }

    @Override
    @Test
    public void testMapStoreEnabledIfNotDisabled() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    mymap:\n"
                + "      map-store:\n"
                + "        initial-mode: EAGER\n";

        Config config = buildConfig(yaml);
        MapStoreConfig mapStoreConfig = config.getMapConfig("mymap").getMapStoreConfig();

        assertTrue(mapStoreConfig.isEnabled());
    }

    @Override
    @Test
    public void testMapStoreDisabled() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    mymap:\n"
                + "      map-store:\n"
                + "        enabled: false\n"
                + "        initial-mode: EAGER\n";

        Config config = buildConfig(yaml);
        MapStoreConfig mapStoreConfig = config.getMapConfig("mymap").getMapStoreConfig();

        assertFalse(mapStoreConfig.isEnabled());
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
                + (useDefault ? " {}" : "\n        write-coalescing: " + String.valueOf(value) + "\n");
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
    public void testPartitionGroupNodeAware() {
        String yaml = ""
                + "hazelcast:\n"
                + "  partition-group:\n"
                + "    enabled: true\n"
                + "    group-type: NODE_AWARE\n";

        Config config = buildConfig(yaml);
        PartitionGroupConfig partitionGroupConfig = config.getPartitionGroupConfig();
        assertTrue(partitionGroupConfig.isEnabled());
        assertEquals(PartitionGroupConfig.MemberGroupType.NODE_AWARE, partitionGroupConfig.getGroupType());
    }

    @Override
    @Test
    public void testPartitionGroupPlacementAware() {
        String yaml = ""
                + "hazelcast:\n"
                + "  partition-group:\n"
                + "    enabled: true\n"
                + "    group-type: PLACEMENT_AWARE\n";

        Config config = buildConfig(yaml);
        PartitionGroupConfig partitionGroupConfig = config.getPartitionGroupConfig();
        assertTrue(partitionGroupConfig.isEnabled());
        assertEquals(PartitionGroupConfig.MemberGroupType.PLACEMENT_AWARE, partitionGroupConfig.getGroupType());
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
        String yaml = ""
                + "hazelcast:\n"
                + "  flake-id-generator:\n"
                + "    gen:\n"
                + "      prefetch-count: 3\n"
                + "      prefetch-validity-millis: 10\n"
                + "      epoch-start: 1514764800001\n"
                + "      node-id-offset: 30\n"
                + "      bits-sequence: 22\n"
                + "      bits-node-id: 33\n"
                + "      allowed-future-millis: 20000\n"
                + "      statistics-enabled: false\n"
                + "    gen2:\n"
                + "      statistics-enabled: true";

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
        assertEquals(Boolean.TRUE, multicastConfig.getLoopbackModeEnabled());
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
                + "      batch-publisher:\n"
                + "        istanbulPublisherId:\n"
                + "          cluster-name: istanbul\n"
                + "          batch-size: 100\n"
                + "          batch-max-delay-millis: 200\n"
                + "          response-timeout-millis: 300\n"
                + "          acknowledge-type: ACK_ON_RECEIPT\n"
                + "          initial-publisher-state: STOPPED\n"
                + "          snapshot-enabled: true\n"
                + "          idle-min-park-ns: 400\n"
                + "          idle-max-park-ns: 500\n"
                + "          max-concurrent-invocations: 600\n"
                + "          discovery-period-seconds: 700\n"
                + "          use-endpoint-private-address: true\n"
                + "          queue-full-behavior: THROW_EXCEPTION\n"
                + "          max-target-endpoints: 800\n"
                + "          queue-capacity: 21\n"
                + "          target-endpoints: a,b,c,d\n"
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
                + "          properties:\n"
                + "            custom.prop.publisher: prop.publisher\n"
                + "        ankara:\n"
                + "          queue-full-behavior: THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE\n"
                + "          initial-publisher-state: STOPPED\n"
                + "      consumer:\n"
                + "        class-name: com.hazelcast.wan.custom.WanConsumer\n"
                + "        properties:\n"
                + "          custom.prop.consumer: prop.consumer\n"
                + "        persist-wan-replicated-data: false\n";

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
        String yaml = ""
                + "hazelcast:\n"
                + "  split-brain-protection:\n"
                + "    mySplitBrainProtection:\n"
                + "      enabled: true\n"
                + "      minimum-cluster-size: 3\n"
                + "      function-class-name: com.my.splitbrainprotection.function\n"
                + "      protect-on: READ\n";

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
        String yaml = ""
                + "hazelcast:\n"
                + "  split-brain-protection:\n"
                + "    mySplitBrainProtection:\n"
                + "      enabled: true\n"
                + "      minimum-cluster-size: 3\n"
                + "      listeners:\n"
                + "         - com.abc.my.splitbrainprotection.listener\n"
                + "         - com.abc.my.second.listener\n"
                + "      function-class-name: com.hazelcast.SomeSplitBrainProtectionFunction\n";

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
        String yaml = ""
                + "hazelcast:\n"
                + "  split-brain-protection:\n"
                + "    mysplit-brain-protection:\n"
                + "      enabled: true\n"
                + "      minimum-cluster-size: 3\n"
                + "      function-class-name: com.hazelcast.SomeSplitBrainProtectionFunction\n"
                + "      recently-active-split-brain-protection: {}";

        buildConfig(yaml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testConfig_whenClassNameAndProbabilisticSplitBrainProtectionDefined_exceptionIsThrown() {
        String yaml = ""
                + "hazelcast:\n"
                + "  split-brain-protection:\n"
                + "    mysplit-brain-protection:\n"
                + "      enabled: true\n"
                + "      minimum-cluster-size: 3\n"
                + "      function-class-name: com.hazelcast.SomeSplitBrainProtectionFunction\n"
                + "      probabilistic-split-brain-protection: {}";

        buildConfig(yaml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    @Ignore("Schema validation is supposed to fail, two split brain protection implementation is defined")
    public void testConfig_whenBothBuiltinSplitBrainProtectionsDefined_exceptionIsThrown() {
        String yaml = ""
                + "hazelcast:\n"
                + "  split-brain-protection:\n"
                + "    mysplit-brain-protection:\n"
                + "      enabled: true\n"
                + "      minimum-cluster-size: 3\n"
                + "      probabilistic-split-brain-protection: {}\n"
                + "      recently-active-split-brain-protection: {}\n";

        buildConfig(yaml);
    }

    @Override
    @Test
    public void testConfig_whenRecentlyActiveSplitBrainProtection_withDefaultValues() {
        String yaml = ""
                + "hazelcast:\n"
                + "  split-brain-protection:\n"
                + "    mySplitBrainProtection:\n"
                + "      enabled: true\n"
                + "      minimum-cluster-size: 3\n"
                + "      recently-active-split-brain-protection: {}";

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
        String yaml = ""
                + "hazelcast:\n"
                + "  split-brain-protection:\n"
                + "    mySplitBrainProtection:\n"
                + "      enabled: true\n"
                + "      minimum-cluster-size: 3\n"
                + "      recently-active-split-brain-protection:\n"
                + "        heartbeat-tolerance-millis: 13000\n";

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
        String yaml = ""
                + "hazelcast:\n"
                + "  split-brain-protection:\n"
                + "    mySplitBrainProtection:\n"
                + "      enabled: true\n"
                + "      minimum-cluster-size: 3\n"
                + "      probabilistic-split-brain-protection: {}";

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
        String yaml = ""
                + "hazelcast:\n"
                + "  split-brain-protection:\n"
                + "    mySplitBrainProtection:\n"
                + "      enabled: true\n"
                + "      minimum-cluster-size: 3\n"
                + "      probabilistic-split-brain-protection:\n"
                + "        acceptable-heartbeat-pause-millis: 37400\n"
                + "        suspicion-threshold: 3.14592\n"
                + "        max-sample-size: 42\n"
                + "        min-std-deviation-millis: 1234\n"
                + "        heartbeat-interval-millis: 4321";

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
        String yaml = ""
                + "hazelcast:\n"
                + "  cache:\n"
                + "    foobar:\n"
                + "      split-brain-protection-ref: customSplitBrainProtectionRule\n"
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
                + "      merge-policy:\n"
                + "         batch-size: 100\n"
                + "         class-name: LatestAccessMergePolicy\n"
                + "      disable-per-entry-invalidation-events: true\n"
                + "      merkle-tree:\n"
                + "        enabled: true\n"
                + "        depth: 20\n"
                + "      hot-restart:\n"
                + "        enabled: false\n"
                + "        fsync: false\n"
                + "      data-persistence:\n"
                + "        enabled: true\n"
                + "        fsync: true\n"
                + "      event-journal:\n"
                + "        enabled: true\n"
                + "        capacity: 120\n"
                + "        time-to-live-seconds: 20\n"
                + "      partition-lost-listeners:\n"
                + "        - com.your-package.YourPartitionLostListener\n"
                + "      cache-entry-listeners:\n"
                + "        - old-value-required: false\n"
                + "          synchronous: false\n"
                + "          cache-entry-listener-factory:\n"
                + "            class-name: com.example.cache.MyEntryListenerFactory\n"
                + "          cache-entry-event-filter-factory:\n"
                + "            class-name: com.example.cache.MyEntryEventFilterFactory\n";

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
        String yaml = ""
                + "hazelcast:\n"
                + "  executor-service:\n"
                + "    foobar:\n"
                + "      pool-size: 2\n"
                + "      split-brain-protection-ref: customSplitBrainProtectionRule\n"
                + "      statistics-enabled: false\n"
                + "      queue-capacity: 0\n";

        Config config = buildConfig(yaml);
        ExecutorConfig executorConfig = config.getExecutorConfig("foobar");

        assertFalse(config.getExecutorConfigs().isEmpty());
        assertEquals(2, executorConfig.getPoolSize());
        assertEquals("customSplitBrainProtectionRule", executorConfig.getSplitBrainProtectionName());
        assertFalse(executorConfig.isStatisticsEnabled());
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
                + "      split-brain-protection-ref: customSplitBrainProtectionRule\n"
                + "      statistics-enabled: false\n";

        Config config = buildConfig(yaml);
        DurableExecutorConfig durableExecutorConfig = config.getDurableExecutorConfig("foobar");

        assertFalse(config.getDurableExecutorConfigs().isEmpty());
        assertEquals(2, durableExecutorConfig.getPoolSize());
        assertEquals(3, durableExecutorConfig.getDurability());
        assertEquals(4, durableExecutorConfig.getCapacity());
        assertEquals("customSplitBrainProtectionRule", durableExecutorConfig.getSplitBrainProtectionName());
        assertFalse(durableExecutorConfig.isStatisticsEnabled());
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
                + "      split-brain-protection-ref: customSplitBrainProtectionRule\n"
                + "      statistics-enabled: false\n"
                + "      merge-policy:\n"
                + "        batch-size: 99\n"
                + "        class-name: PutIfAbsent";

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
                + "      split-brain-protection-ref: customSplitBrainProtectionRule\n"
                + "      merge-policy:\n"
                + "        class-name: com.hazelcast.spi.merge.HyperLogLogMergePolicy";

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
        String yaml = ""
                + "hazelcast:\n"
                + "  cardinality-estimator:\n"
                + "    foobar:\n"
                + "      backup-count: 2\n"
                + "      async-backup-count: 3\n"
                + "      split-brain-protection-ref: customSplitBrainProtectionRule\n"
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
                + "      split-brain-protection-ref: splitBrainProtectionRuleWithThreeMembers\n"
                + "      statistics-enabled: false\n";

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
        String yaml = ""
                + "hazelcast:\n"
                + "  multimap:\n"
                + "    myMultiMap:\n"
                + "      backup-count: 2\n"
                + "      async-backup-count: 3\n"
                + "      binary: false\n"
                + "      value-collection-type: SET\n"
                + "      split-brain-protection-ref: customSplitBrainProtectionRule\n"
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
        assertEquals("customSplitBrainProtectionRule", multiMapConfig.getSplitBrainProtectionName());
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
                + "      split-brain-protection-ref: customSplitBrainProtectionRule\n"
                + "      merge-policy:\n"
                + "        batch-size: 2342\n"
                + "        class-name: CustomMergePolicy\n";

        Config config = buildConfig(yaml);
        ReplicatedMapConfig replicatedMapConfig = config.getReplicatedMapConfig("foobar");

        assertFalse(config.getReplicatedMapConfigs().isEmpty());
        assertEquals(InMemoryFormat.BINARY, replicatedMapConfig.getInMemoryFormat());
        assertFalse(replicatedMapConfig.isAsyncFillup());
        assertFalse(replicatedMapConfig.isStatisticsEnabled());
        assertEquals("customSplitBrainProtectionRule", replicatedMapConfig.getSplitBrainProtectionName());

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
                + "      split-brain-protection-ref: customSplitBrainProtectionRule\n"
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
        assertEquals("customSplitBrainProtectionRule", listConfig.getSplitBrainProtectionName());
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
                + "     split-brain-protection-ref: customSplitBrainProtectionRule\n"
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
        assertEquals("customSplitBrainProtectionRule", setConfig.getSplitBrainProtectionName());
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
                + "      split-brain-protection-ref: customSplitBrainProtectionRule\n"
                + "      in-memory-format: BINARY\n"
                + "      statistics-enabled: true\n"
                + "      cache-deserialized-values: INDEX-ONLY\n"
                + "      backup-count: 2\n"
                + "      async-backup-count: 1\n"
                + "      time-to-live-seconds: 42\n"
                + "      max-idle-seconds: 42\n"
                + "      eviction:\n"
                + "         eviction-policy: RANDOM\n"
                + "         max-size-policy: PER_NODE\n"
                + "         size: 42\n"
                + "      read-backup-data: true\n"
                + "      merkle-tree:\n"
                + "        enabled: true\n"
                + "        depth: 20\n"
                + "      event-journal:\n"
                + "        enabled: true\n"
                + "        capacity: 120\n"
                + "        time-to-live-seconds: 20\n"
                + "      hot-restart:\n"
                + "        enabled: false\n"
                + "        fsync: false\n"
                + "      data-persistence:\n"
                + "        enabled: true\n"
                + "        fsync: true\n"
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
                + "        time-to-live-seconds: 42\n"
                + "        max-idle-seconds: 42\n"
                + "        invalidate-on-change: true\n"
                + "        in-memory-format: BINARY\n"
                + "        cache-local-entries: false\n"
                + "        eviction:\n"
                + "          size: 1000\n"
                + "          max-size-policy: ENTRY_COUNT\n"
                + "          eviction-policy: LFU\n"
                + "      wan-replication-ref:\n"
                + "        my-wan-cluster-batch:\n"
                + "          merge-policy-class-name: PassThroughMergePolicy\n"
                + "          filters:\n"
                + "            - com.example.SampleFilter\n"
                + "          republishing-enabled: false\n"
                + "      indexes:\n"
                + "        - attributes:\n"
                + "          - \"age\"\n"
                + "      attributes:\n"
                + "        currency:\n"
                + "          extractor-class-name: com.bank.CurrencyExtractor\n"
                + "      partition-lost-listeners:\n"
                + "         - com.your-package.YourPartitionLostListener\n"
                + "      entry-listeners:\n"
                + "         - class-name: com.your-package.MyEntryListener\n"
                + "           include-value: false\n"
                + "           local: false\n";

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
        assertEquals(1, mapConfig.getIndexConfigs().size());
        assertEquals("age", mapConfig.getIndexConfigs().get(0).getAttributes().get(0));
        assertTrue(mapConfig.getIndexConfigs().get(0).getType() == IndexType.SORTED);
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
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    people:\n"
                + "      indexes:\n"
                + "        - type: HASH\n"
                + "          attributes:\n"
                + "            - \"name\"\n"
                + "        - attributes:\n"
                + "          - \"age\"\n";

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("people");

        assertFalse(mapConfig.getIndexConfigs().isEmpty());
        assertIndexEqual("name", false, mapConfig.getIndexConfigs().get(0));
        assertIndexEqual("age", true, mapConfig.getIndexConfigs().get(1));
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
                + "          extractor-class-name: com.car.PowerExtractor\n"
                + "        weight:\n"
                + "          extractor-class-name: com.car.WeightExtractor\n";

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("people");

        assertFalse(mapConfig.getAttributeConfigs().isEmpty());
        assertAttributeEqual("power", "com.car.PowerExtractor", mapConfig.getAttributeConfigs().get(0));
        assertAttributeEqual("weight", "com.car.WeightExtractor", mapConfig.getAttributeConfigs().get(1));
    }

    @Override
    @Test(expected = SchemaViolationConfigurationException.class)
    public void testAttributeConfig_noName_emptyTag() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    people:\n"
                + "      attributes:\n"
                + "        - extractor-class-name: com.car.WeightExtractor\n";

        buildConfig(yaml);
    }

    private static void assertAttributeEqual(String expectedName, String expectedExtractor, AttributeConfig attributeConfig) {
        assertEquals(expectedName, attributeConfig.getName());
        assertEquals(expectedExtractor, attributeConfig.getExtractorClassName());
    }

    @Override
    @Test(expected = SchemaViolationConfigurationException.class)
    public void testAttributeConfig_noName_singleTag() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "   people:\n"
                + "     attributes:\n"
                + "       - extractor-class-name: com.car.WeightExtractor\n";
        buildConfig(yaml);
    }

    @Override
    @Test(expected = SchemaViolationConfigurationException.class)
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
                + "          extractor-class-name: \"\"\n";
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
                + "          serialize-keys: true\n"
                + "          indexes:\n"
                + "            - type: HASH\n"
                + "              attributes:\n"
                + "                - \"name\"\n"
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
            assertFalse(indexConfig.getType() == IndexType.SORTED);
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
    @Test(expected = SchemaViolationConfigurationException.class)
    public void testNonLiteMemberConfigWithoutEnabledField() {
        String yaml = ""
                + "hazelcast:\n"
                + "  lite-member: {}\n";

        buildConfig(yaml);
    }

    @Override
    @Test(expected = SchemaViolationConfigurationException.class)
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

    private static void assertIndexEqual(String expectedAttribute, boolean expectedOrdered, IndexConfig indexConfig) {
        assertEquals(expectedAttribute, indexConfig.getAttributes().get(0));
        assertEquals(expectedOrdered, indexConfig.getType() == IndexType.SORTED);
    }

    @Override
    @Test
    public void testMapNativeMaxSizePolicy() {
        String yamlFormat = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    mymap:\n"
                + "      in-memory-format: NATIVE\n"
                + "      eviction:\n"
                + "        max-size-policy: \"{0}\"\n"
                + "        size: 9991\n";

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
    public void testEmptyUserCodeDeployment() {
        String yaml = ""
                + "hazelcast:\n"
                + "  user-code-deployment:\n"
                + "    enabled: true\n";

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
    public void testCompactSerialization() {
        String yaml = ""
                + "hazelcast:\n"
                + "    serialization:\n"
                + "        compact-serialization:\n"
                + "            enabled: true\n";

        Config config = buildConfig(yaml);
        assertTrue(config.getSerializationConfig().getCompactSerializationConfig().isEnabled());
    }

    @Override
    public void testCompactSerialization_explicitSerializationRegistration() {
        String yaml = ""
                + "hazelcast:\n"
                + "    serialization:\n"
                + "        compact-serialization:\n"
                + "            enabled: true\n"
                + "            registered-classes:\n"
                + "                - class: example.serialization.EmployeeDTO\n"
                + "                  type-name: obj\n"
                + "                  serializer: example.serialization.EmployeeDTOSerializer\n";

        Config config = buildConfig(yaml);
        CompactTestUtil.verifyExplicitSerializerIsUsed(config.getSerializationConfig());
    }

    @Override
    public void testCompactSerialization_reflectiveSerializerRegistration() {
        String yaml = ""
                + "hazelcast:\n"
                + "    serialization:\n"
                + "        compact-serialization:\n"
                + "            enabled: true\n"
                + "            registered-classes:\n"
                + "                - class: example.serialization.ExternalizableEmployeeDTO\n";

        Config config = buildConfig(yaml);
        CompactTestUtil.verifyReflectiveSerializerIsUsed(config.getSerializationConfig());
    }

    @Override
    public void testCompactSerialization_registrationWithJustTypeName() {
        String yaml = ""
                + "hazelcast:\n"
                + "    serialization:\n"
                + "        compact-serialization:\n"
                + "            enabled: true\n"
                + "            registered-classes:\n"
                + "                - class: example.serialization.EmployeeDTO\n"
                + "                  type-name: employee\n";

        buildConfig(yaml);
    }

    @Override
    public void testCompactSerialization_registrationWithJustSerializer() {
        String yaml = ""
                + "hazelcast:\n"
                + "    serialization:\n"
                + "        compact-serialization:\n"
                + "            enabled: true\n"
                + "            registered-classes:\n"
                + "                - class: example.serialization.EmployeeDTO\n"
                + "                  serializer: example.serialization.EmployeeDTOSerializer\n";

        buildConfig(yaml);
    }

    @Override
    @Test
    public void testAllowOverrideDefaultSerializers() {
        final String yaml = ""
                + "hazelcast:\n"
                + "  serialization:\n"
                + "    allow-override-default-serializers: true\n";

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

        Config config = new InMemoryYamlConfig(yaml);
        LocalDeviceConfig localDeviceConfig = config.getDeviceConfig("my-device");

        assertEquals("my-device", localDeviceConfig.getName());
        assertEquals(new File(baseDir).getAbsolutePath(), localDeviceConfig.getBaseDir().getAbsolutePath());
        assertEquals(blockSize, localDeviceConfig.getBlockSize());
        assertEquals(new MemorySize(100, MemoryUnit.GIGABYTES), localDeviceConfig.getCapacity());
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
        assertEquals(3, config.getDeviceConfigs().size());

        localDeviceConfig = config.getDeviceConfig("device0");
        assertEquals(blockSize * device0Multiplier, localDeviceConfig.getBlockSize());
        assertEquals(readIOThreadCount * device0Multiplier, localDeviceConfig.getReadIOThreadCount());
        assertEquals(writeIOThreadCount * device0Multiplier, localDeviceConfig.getWriteIOThreadCount());
        assertEquals(new MemorySize(1234567890, MemoryUnit.MEGABYTES), localDeviceConfig.getCapacity());

        localDeviceConfig = config.getDeviceConfig("device1");
        assertEquals(blockSize * device1Multiplier, localDeviceConfig.getBlockSize());
        assertEquals(readIOThreadCount * device1Multiplier, localDeviceConfig.getReadIOThreadCount());
        assertEquals(writeIOThreadCount * device1Multiplier, localDeviceConfig.getWriteIOThreadCount());

        // default device
        localDeviceConfig = config.getDeviceConfig(DEFAULT_DEVICE_NAME);
        assertEquals(DEFAULT_DEVICE_NAME, localDeviceConfig.getName());
        assertEquals(new File(DEFAULT_DEVICE_BASE_DIR).getAbsoluteFile(), localDeviceConfig.getBaseDir());
        assertEquals(DEFAULT_BLOCK_SIZE_IN_BYTES, localDeviceConfig.getBlockSize());
        assertEquals(DEFAULT_READ_IO_THREAD_COUNT, localDeviceConfig.getReadIOThreadCount());
        assertEquals(DEFAULT_WRITE_IO_THREAD_COUNT, localDeviceConfig.getWriteIOThreadCount());
        assertEquals(LocalDeviceConfig.DEFAULT_CAPACITY, localDeviceConfig.getCapacity());

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
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    map0:\n"
                + "      tiered-store:\n"
                + "        enabled: true\n"
                + "        memory-tier:\n"
                + "          capacity:\n"
                + "            unit: MEGABYTES\n"
                + "            value: 1024\n"
                + "        disk-tier:\n"
                + "          enabled: true\n"
                + "          device-name: local-device\n"
                + "    map1:\n"
                + "      tiered-store:\n"
                + "        enabled: true\n"
                + "        disk-tier:\n"
                + "          enabled: true\n"
                + "    map2:\n"
                + "      tiered-store:\n"
                + "        enabled: true\n"
                + "        memory-tier:\n"
                + "          capacity:\n"
                + "            unit: GIGABYTES\n"
                + "            value: 1\n"
                + "    map3:\n"
                + "      tiered-store:\n"
                + "        enabled: true\n";

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

        yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    some-map:\n"
                + "      tiered-store:\n"
                + "        enabled: true\n";

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
        PermissionConfig expected = new PermissionConfig(CONFIG, null, "dev");
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
        Config config;
        try {
            config = new YamlConfigBuilder(yamlResource).build();
        } finally {
            IOUtil.closeResource(yamlResource);
        }
        Set<PermissionConfig.PermissionType> permTypes = new HashSet<>(asList(PermissionConfig.PermissionType.values()));
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
        assertTrue(joinConfig.getAutoDetectionConfig().isEnabled());
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

    @Test
    public void outboundPorts_asObject_ParsingTest() {
        String yaml = ""
                + "hazelcast:\n"
                + "  network:\n"
                + "    outbound-ports:\n"
                + "      ports: 2500-3000\n"
                + "      more-ports: 2600-3500\n";
        Config actual = buildConfig(yaml);
        assertEquals(new HashSet<>(asList("2500-3000", "2600-3500")), actual.getNetworkConfig().getOutboundPortDefinitions());
    }

    @Test
    public void outboundPorts_asSequence_ParsingTest() {
        String yaml = ""
                + "hazelcast:\n"
                + "  network:\n"
                + "    outbound-ports:\n"
                + "      - 1234-1999\n"
                + "      - 2500\n";
        Config actual = buildConfig(yaml);
        assertEquals(new HashSet<>(asList("2500", "1234-1999")), actual.getNetworkConfig().getOutboundPortDefinitions());
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
    @Test
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
                + "    persistence-enabled: true\n"
                + "    base-dir: /mnt/cp-data\n"
                + "    data-load-timeout-seconds: 30\n"
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
                + "        initial-permits: 1\n"
                + "      sem2:\n"
                + "        jdk-compatible: false\n"
                + "        initial-permits: 2\n"
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
        assertTrue(cpSubsystemConfig.isPersistenceEnabled());
        assertEquals(new File("/mnt/cp-data").getAbsoluteFile(), cpSubsystemConfig.getBaseDir().getAbsoluteFile());
        assertEquals(30, cpSubsystemConfig.getDataLoadTimeoutSeconds());
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
    }

    @Override
    @Test
    public void testSqlConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  sql:\n"
                + "    statement-timeout-millis: 30\n";
        Config config = buildConfig(yaml);
        SqlConfig sqlConfig = config.getSqlConfig();
        assertEquals(30L, sqlConfig.getStatementTimeoutMillis());
    }

    @Override
    @Test
    public void testWhitespaceInNonSpaceStrings() {
        String yaml = ""
                + "hazelcast:\n"
                + "  split-brain-protection:\n"
                + "    name-of-split-brain-protection:\n"
                + "      enabled: true\n"
                + "      protect-on:   WRITE   \n";

        buildConfig(yaml);
    }

    @Override
    @Test
    public void testPersistentMemoryDirectoryConfiguration() {
        String yaml = ""
                + "hazelcast:\n"
                + "  native-memory:\n"
                + "    persistent-memory:\n"
                + "      directories:\n"
                + "        - directory: /mnt/pmem0\n"
                + "          numa-node: 0\n"
                + "        - directory: /mnt/pmem1\n"
                + "          numa-node: 1\n";

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
        String yaml = "hazelcast:\n"
                + "  cache:\n"
                + "    my-cache:\n"
                + "      cache-entry-listeners:\n"
                + "        - old-value-required: true\n"
                + "          synchronous: true\n"
                + "          cache-entry-listener-factory:\n"
                + "            class-name: com.example.cache.MyEntryListenerFactory\n"
                + "          cache-entry-event-filter-factory:\n"
                + "            class-name: com.example.cache.MyEntryEventFilterFactory";
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
        String yaml = ""
                + "hazelcast:\n"
                + "  native-memory:\n"
                + "    persistent-memory-directory: /mnt/pmem0";

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
        String yaml = ""
                + "hazelcast:\n"
                + "  native-memory:\n"
                + "    persistent-memory:\n"
                + "      directories:\n"
                + "        - directory: /mnt/pmem0\n"
                + "          numa-node: 0\n"
                + "        - directory: /mnt/pmem0\n"
                + "          numa-node: 1\n";

        buildConfig(yaml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testPersistentMemoryDirectoryConfiguration_uniqueNumaNodeViolationThrows() {
        String yaml = ""
                + "hazelcast:\n"
                + "  native-memory:\n"
                + "    persistent-memory:\n"
                + "      directories:\n"
                + "        - directory: /mnt/pmem0\n"
                + "          numa-node: 0\n"
                + "        - directory: /mnt/pmem1\n"
                + "          numa-node: 0\n";

        buildConfig(yaml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testPersistentMemoryDirectoryConfiguration_numaNodeConsistencyViolationThrows() {
        String yaml = ""
                + "hazelcast:\n"
                + "  native-memory:\n"
                + "    persistent-memory:\n"
                + "      directories:\n"
                + "        - directory: /mnt/pmem0\n"
                + "          numa-node: 0\n"
                + "        - directory: /mnt/pmem1\n";

        buildConfig(yaml);
    }

    @Override
    @Test
    public void testPersistentMemoryDirectoryConfiguration_simpleAndAdvancedPasses() {
        String yaml = ""
                + "hazelcast:\n"
                + "  native-memory:\n"
                + "    persistent-memory-directory: /mnt/optane\n"
                + "    persistent-memory:\n"
                + "      directories:\n"
                + "        - directory: /mnt/pmem0\n"
                + "        - directory: /mnt/pmem1\n";

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
        String yaml = ""
                + "hazelcast:\n"
                + "  native-memory:\n"
                + "    persistent-memory:\n"
                + "      enabled: true\n"
                + "      mode: SYSTEM_MEMORY\n";

        Config config = buildConfig(yaml);
        PersistentMemoryConfig pmemConfig = config.getNativeMemoryConfig().getPersistentMemoryConfig();
        assertTrue(pmemConfig.isEnabled());
        assertEquals(SYSTEM_MEMORY, pmemConfig.getMode());
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testPersistentMemoryConfiguration_NotExistingModeThrows() {
        String yaml = ""
                + "hazelcast:\n"
                + "  native-memory:\n"
                + "    persistent-memory:\n"
                + "      mode: NOT_EXISTING_MODE\n";

        buildConfig(yaml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testPersistentMemoryDirectoryConfiguration_SystemMemoryModeThrows() {
        String yaml = ""
                + "hazelcast:\n"
                + "  native-memory:\n"
                + "    persistent-memory:\n"
                + "      mode: SYSTEM_MEMORY\n"
                + "      directories:\n"
                + "        - directory: /mnt/pmem0\n";

        buildConfig(yaml);
    }

    @Override
    @Test
    public void testMetricsConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  metrics:\n"
                + "    enabled: false\n"
                + "    management-center:\n"
                + "      enabled: false\n"
                + "      retention-seconds: 11\n"
                + "    jmx:\n"
                + "      enabled: false\n"
                + "    collection-frequency-seconds: 10";
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
        String yaml = ""
                + "hazelcast:\n"
                + "  instance-tracking:\n"
                + "    enabled: true\n"
                + "    file-name: /dummy/file\n"
                + "    format-pattern: dummy-pattern with $HZ_INSTANCE_TRACKING{placeholder} and $RND{placeholder}";
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
        String yaml = ""
                + "hazelcast:\n"
                + "  metrics:\n"
                + "    enabled: false";
        Config config = new InMemoryYamlConfig(yaml);
        MetricsConfig metricsConfig = config.getMetricsConfig();
        assertFalse(metricsConfig.isEnabled());
        assertTrue(metricsConfig.getManagementCenterConfig().isEnabled());
        assertTrue(metricsConfig.getJmxConfig().isEnabled());
    }

    @Override
    @Test
    public void testMetricsConfigMcDisabled() {
        String yaml = ""
                + "hazelcast:\n"
                + "  metrics:\n"
                + "    management-center:\n"
                + "      enabled: false";
        Config config = new InMemoryYamlConfig(yaml);
        MetricsConfig metricsConfig = config.getMetricsConfig();
        assertTrue(metricsConfig.isEnabled());
        assertFalse(metricsConfig.getManagementCenterConfig().isEnabled());
        assertTrue(metricsConfig.getJmxConfig().isEnabled());
    }

    @Override
    @Test
    public void testMetricsConfigJmxDisabled() {
        String yaml = ""
                + "hazelcast:\n"
                + "  metrics:\n"
                + "    jmx:\n"
                + "      enabled: false";
        Config config = new InMemoryYamlConfig(yaml);
        MetricsConfig metricsConfig = config.getMetricsConfig();
        assertTrue(metricsConfig.isEnabled());
        assertTrue(metricsConfig.getManagementCenterConfig().isEnabled());
        assertFalse(metricsConfig.getJmxConfig().isEnabled());
    }

    @Override
    protected Config buildAuditlogConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  auditlog:\n"
                + "    enabled: true\n"
                + "    factory-class-name: com.acme.auditlog.AuditlogToSyslogFactory\n"
                + "    properties:\n"
                + "      host: syslogserver.acme.com\n"
                + "      port: 514\n"
                + "      type: tcp\n";
        return new InMemoryYamlConfig(yaml);
    }

    @Override
    protected Config buildMapWildcardConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    map*:\n"
                + "      attributes:\n"
                + "        name:\n"
                + "          extractor-class-name: usercodedeployment.CapitalizingFirstNameExtractor\n"
                + "    mapBackup2*:\n"
                + "      backup-count: 2\n"
                + "      attributes:\n"
                + "        name:\n"
                + "          extractor-class-name: usercodedeployment.CapitalizingFirstNameExtractor\n";

        return new InMemoryYamlConfig(yaml);
    }

    @Override
    @Test
    public void testIntegrityCheckerConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  integrity-checker:\n"
                + "    enabled: false\n";

        Config config = buildConfig(yaml);

        assertFalse(config.getIntegrityCheckerConfig().isEnabled());
    }

    @Override
    public void testMapExpiryConfig() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    expiry:\n"
                + "      time-to-live-seconds: 2147483647\n"
                + "      max-idle-seconds: 2147483647\n";

        Config config = buildConfig(yaml);
        MapConfig mapConfig = config.getMapConfig("expiry");

        assertEquals(Integer.MAX_VALUE, mapConfig.getTimeToLiveSeconds());
        assertEquals(Integer.MAX_VALUE, mapConfig.getMaxIdleSeconds());
    }
}
