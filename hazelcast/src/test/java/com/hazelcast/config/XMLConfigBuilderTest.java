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
import com.hazelcast.config.security.TokenEncoding;
import com.hazelcast.config.security.TokenIdentityConfig;
import com.hazelcast.config.tpc.TpcConfig;
import com.hazelcast.config.tpc.TpcSocketConfig;
import com.hazelcast.instance.EndpointQualifier;
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
import static com.hazelcast.config.RestEndpointGroup.CLUSTER_READ;
import static com.hazelcast.config.RestEndpointGroup.HEALTH_CHECK;
import static com.hazelcast.config.WanQueueFullBehavior.THROW_EXCEPTION;
import static com.hazelcast.internal.util.StringUtil.lowerCaseInternal;
import static java.io.File.createTempFile;
import static java.nio.charset.StandardCharsets.US_ASCII;
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

/**
 * XML specific implementation of the tests that should be
 * maintained in both XML and YAML configuration builder tests.
 * <p>
 * <p>
 * NOTE: This test class must not define test cases, it is meant only to
 * implement test cases defined in {@link AbstractConfigBuilderTest}.
 * <p>
 * <p>
 * NOTE2: Test cases specific to XML should be added to {@link XmlOnlyConfigBuilderTest}
 *
 * @see AbstractConfigBuilderTest
 * @see YamlConfigBuilderTest
 * @see XmlOnlyConfigBuilderTest
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("WeakerAccess")
public class XMLConfigBuilderTest extends AbstractConfigBuilderTest {

    static final String HAZELCAST_START_TAG = "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n";
    static final String HAZELCAST_END_TAG = "</hazelcast>\n";

    static final String SECURITY_START_TAG = "<security enabled=\"true\">\n";
    static final String SECURITY_END_TAG = "</security>\n";
    static final String ACTIONS_FRAGMENT = "<actions>"
            + "<action>create</action>"
            + "<action>destroy</action>"
            + "<action>add</action>"
            + "<action>remove</action>"
            + "</actions>";

    @Override
    @Test
    public void testConfigurationURL() throws Exception {
        URL configURL = getClass().getClassLoader().getResource("hazelcast-default.xml");
        Config config = new XmlConfigBuilder(configURL).build();
        assertEquals(configURL, config.getConfigurationUrl());
        assertNull(config.getConfigurationFile());
    }

    @Override
    @Test
    public void testClusterName() {
        String xml = HAZELCAST_START_TAG
                + "  <cluster-name>my-cluster</cluster-name>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);

        assertEquals("my-cluster", config.getClusterName());
    }

    @Override
    @Test
    public void testConfigurationWithFileName() throws Exception {
        File file = createTempFile("foo", "bar");
        file.deleteOnExit();

        String xml = HAZELCAST_START_TAG
                + "  <cluster-name>foobar</cluster-name>\n"
                + HAZELCAST_END_TAG;
        Writer writer = new PrintWriter(file, StandardCharsets.UTF_8);
        writer.write(xml);
        writer.close();

        String path = file.getAbsolutePath();
        Config config = new XmlConfigBuilder(path).build();
        assertEquals(path, config.getConfigurationFile().getAbsolutePath());
        assertNull(config.getConfigurationUrl());
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void testConfiguration_withNullInputStream() {
        new XmlConfigBuilder((InputStream) null);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testJoinValidation() {
        String xml = HAZELCAST_START_TAG
                + "    <network>\n"
                + "        <join>\n"
                + "            <multicast enabled=\"true\"/>\n"
                + "            <tcp-ip enabled=\"true\"/>\n"
                + "        </join>\n"
                + "    </network>\n"
                + HAZELCAST_END_TAG;
        buildConfig(xml);
    }

    @Override
    @Test
    public void testSecurityConfig() {
        String xml = HAZELCAST_START_TAG
                + "<security enabled=\"true\">"
                + "  <security-interceptors>"
                + "    <interceptor class-name=\"foo\"/>"
                + "    <interceptor class-name=\"bar\"/>"
                + "  </security-interceptors>"
                + "  <client-block-unmapped-actions>false</client-block-unmapped-actions>"
                + "  <realms>"
                + "    <realm name='mr'>"
                + "      <authentication>"
                + "        <jaas>"
                + "          <login-module class-name=\"MyRequiredLoginModule\" usage=\"REQUIRED\">\n"
                + "            <properties>\n"
                + "              <property name=\"login-property\">login-value</property>\n"
                + "            </properties>\n"
                + "          </login-module>\n"
                + "          <login-module class-name=\"MyRequiredLoginModule2\" usage=\"SUFFICIENT\">\n"
                + "            <properties>\n"
                + "              <property name=\"login-property2\">login-value2</property>\n"
                + "            </properties>\n"
                + "          </login-module>\n"
                + "        </jaas>"
                + "      </authentication>"
                + "      <identity>"
                + "        <credentials-factory class-name=\"MyCredentialsFactory\">\n"
                + "          <properties>\n"
                + "            <property name=\"property\">value</property>\n"
                + "          </properties>\n"
                + "        </credentials-factory>\n"
                + "      </identity>"
                + "    </realm>"
                + "    <realm name='cr'>"
                + "      <authentication>"
                + "        <jaas>"
                + "          <login-module class-name=\"MyOptionalLoginModule\" usage=\"OPTIONAL\">\n"
                + "            <properties>\n"
                + "              <property name=\"client-property\">client-value</property>\n"
                + "            </properties>\n"
                + "          </login-module>\n"
                + "          <login-module class-name=\"MyRequiredLoginModule\" usage=\"REQUIRED\">\n"
                + "            <properties>\n"
                + "              <property name=\"client-property2\">client-value2</property>\n"
                + "            </properties>\n"
                + "          </login-module>\n"
                + "        </jaas>"
                + "      </authentication>"
                + "      <identity>"
                + "        <token encoding=\"base64\">****</token>"
                + "      </identity>"
                + "    </realm>"
                + "    <realm name='kerberos'>"
                + "      <authentication>"
                + "        <kerberos>"
                + "          <skip-role>false</skip-role>"
                + "          <relax-flags-check>true</relax-flags-check>"
                + "          <use-name-without-realm>true</use-name-without-realm>"
                + "          <security-realm>krb5Acceptor</security-realm>"
                + "          <principal>jduke@HAZELCAST.COM</principal>"
                + "          <keytab-file>/opt/jduke.keytab</keytab-file>"
                + "          <ldap>"
                + "            <url>ldap://127.0.0.1</url>"
                + "          </ldap>"
                + "        </kerberos>"
                + "      </authentication>"
                + "      <identity>"
                + "        <kerberos>"
                + "          <realm>HAZELCAST.COM</realm>"
                + "          <security-realm>krb5Initializer</security-realm>"
                + "          <principal>jduke@HAZELCAST.COM</principal>"
                + "          <keytab-file>/opt/jduke.keytab</keytab-file>"
                + "          <use-canonical-hostname>true</use-canonical-hostname>"
                + "        </kerberos>"
                + "      </identity>"
                + "    </realm>"
                + "    <realm name='simple'>"
                + "      <authentication>"
                + "        <simple>"
                + "          <skip-role>true</skip-role>"
                + "          <role-separator>:</role-separator>"
                + "          <user username='test' password='a1234'>"
                + "            <role>monitor</role>"
                + "            <role>hazelcast</role>"
                + "          </user>"
                + "          <user username='dev' password='secret'>"
                + "            <role>root</role>"
                + "          </user>"
                + "        </simple>"
                + "      </authentication>"
                + "      <access-control-service>"
                + "        <factory-class-name>"
                + "            com.acme.access.AccessControlServiceFactory"
                + "        </factory-class-name>"
                + "        <properties>"
                + "            <property name='decisionFile'>/opt/acl.xml</property>"
                + "        </properties>"
                + "      </access-control-service>"
                + "    </realm>"
                + "  </realms>"
                + "  <member-authentication realm='mr'/>\n"
                + "  <client-authentication realm='cr'/>\n"
                + "  <client-permission-policy class-name=\"MyPermissionPolicy\">\n"
                + "    <properties>\n"
                + "      <property name=\"permission-property\">permission-value</property>\n"
                + "    </properties>\n"
                + "  </client-permission-policy>"
                + "</security>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
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

        TokenIdentityConfig tokenIdentityConfig = clientRealm.getTokenIdentityConfig();
        assertEquals(TokenEncoding.BASE64, tokenIdentityConfig.getEncoding());
        assertArrayEquals(ConfigXmlGenerator.MASK_FOR_SENSITIVE_DATA.getBytes(US_ASCII), tokenIdentityConfig.getToken());

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
        assertEquals(":", simpleAuthnCfg.getRoleSeparator());
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
        String xml = HAZELCAST_START_TAG
                + "    <cluster-name>dev</cluster-name>\n"
                + "    <network>\n"
                + "        <port auto-increment=\"true\">5701</port>\n"
                + "        <join>\n"
                + "            <multicast enabled=\"false\">\n"
                + "                <multicast-group>224.2.2.3</multicast-group>\n"
                + "                <multicast-port>54327</multicast-port>\n"
                + "            </multicast>\n"
                + "            <tcp-ip enabled=\"false\">\n"
                + "                <interface>127.0.0.1</interface>\n"
                + "            </tcp-ip>\n"
                + "            <aws enabled=\"true\" connection-timeout-seconds=\"10\" >\n"
                + "                <access-key>sample-access-key</access-key>\n"
                + "                <secret-key>sample-secret-key</secret-key>\n"
                + "                <iam-role>sample-role</iam-role>\n"
                + "                <region>sample-region</region>\n"
                + "                <host-header>sample-header</host-header>\n"
                + "                <security-group-name>sample-group</security-group-name>\n"
                + "                <tag-key>sample-tag-key</tag-key>\n"
                + "                <tag-value>sample-tag-value</tag-value>\n"
                + "            </aws>\n"
                + "        </join>\n"
                + "        <interfaces enabled=\"false\">\n"
                + "            <interface>10.10.1.*</interface>\n"
                + "        </interfaces>\n"
                + "    </network>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);

        AwsConfig awsConfig = config.getNetworkConfig().getJoin().getAwsConfig();
        assertTrue(awsConfig.isEnabled());
        assertAwsConfig(awsConfig);
    }

    @Override
    @Test
    public void readGcpConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <network>\n"
                + "        <join>\n"
                + "            <multicast enabled=\"false\"/>\n"
                + "            <tcp-ip enabled=\"false\"/>\n"
                + "            <gcp enabled=\"true\">\n"
                + "                <use-public-ip>true</use-public-ip>\n"
                + "                <zones>us-east1-b</zones>\n"
                + "            </gcp>\n"
                + "        </join>\n"
                + "    </network>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);

        GcpConfig gcpConfig = config.getNetworkConfig().getJoin().getGcpConfig();

        assertTrue(gcpConfig.isEnabled());
        assertTrue(gcpConfig.isUsePublicIp());
        assertEquals("us-east1-b", gcpConfig.getProperty("zones"));
    }

    @Override
    @Test
    public void readAzureConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <network>\n"
                + "        <join>\n"
                + "            <multicast enabled=\"false\"/>\n"
                + "            <tcp-ip enabled=\"false\"/>\n"
                + "            <azure enabled=\"true\">\n"
                + "                <client-id>123456789!</client-id>\n"
                + "                <use-public-ip>true</use-public-ip>\n"
                + "            </azure>\n"
                + "        </join>\n"
                + "    </network>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);

        AzureConfig azureConfig = config.getNetworkConfig().getJoin().getAzureConfig();

        assertTrue(azureConfig.isEnabled());
        assertTrue(azureConfig.isUsePublicIp());
        assertEquals("123456789!", azureConfig.getProperty("client-id"));
    }

    @Override
    @Test
    public void readKubernetesConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <network>\n"
                + "        <join>\n"
                + "            <multicast enabled=\"false\"/>\n"
                + "            <tcp-ip enabled=\"false\"/>\n"
                + "            <kubernetes enabled=\"true\">\n"
                + "                <use-public-ip>true</use-public-ip>\n"
                + "                <namespace>hazelcast</namespace>\n"
                + "            </kubernetes>\n"
                + "        </join>\n"
                + "    </network>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);

        KubernetesConfig kubernetesConfig = config.getNetworkConfig().getJoin().getKubernetesConfig();

        assertTrue(kubernetesConfig.isEnabled());
        assertTrue(kubernetesConfig.isUsePublicIp());
        assertEquals("hazelcast", kubernetesConfig.getProperty("namespace"));
    }

    @Override
    @Test
    public void readEurekaConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <network>\n"
                + "        <join>\n"
                + "            <multicast enabled=\"false\"/>\n"
                + "            <tcp-ip enabled=\"false\"/>\n"
                + "            <eureka enabled=\"true\">\n"
                + "                <use-public-ip>true</use-public-ip>\n"
                + "                <namespace>hazelcast</namespace>\n"
                + "                <shouldUseDns>false</shouldUseDns>\n"
                + "                <serviceUrl.default>http://localhost:8082/eureka</serviceUrl.default>\n"
                + "            </eureka>\n"
                + "        </join>\n"
                + "    </network>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);

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
        String xml = HAZELCAST_START_TAG
                + "    <cluster-name>dev</cluster-name>\n"
                + "    <network>\n"
                + "        <port auto-increment=\"true\">5701</port>\n"
                + "        <join>\n"
                + "            <multicast enabled=\"false\">\n"
                + "                <multicast-group>224.2.2.3</multicast-group>\n"
                + "                <multicast-port>54327</multicast-port>\n"
                + "            </multicast>\n"
                + "            <tcp-ip enabled=\"false\">\n"
                + "                <interface>127.0.0.1</interface>\n"
                + "            </tcp-ip>\n"
                + "            <discovery-strategies>\n"
                + "                <node-filter class=\"DummyFilterClass\" />\n"
                + "                <discovery-strategy class=\"DummyDiscoveryStrategy1\" enabled=\"true\">\n"
                + "                    <properties>\n"
                + "                        <property name=\"key-string\">foo</property>\n"
                + "                        <property name=\"key-int\">123</property>\n"
                + "                        <property name=\"key-boolean\">true</property>\n"
                + "                    </properties>\n"
                + "                </discovery-strategy>\n"
                + "            </discovery-strategies>\n"
                + "        </join>\n"
                + "        <interfaces enabled=\"false\">\n"
                + "            <interface>10.10.1.*</interface>\n"
                + "        </interfaces>\n"
                + "    </network>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        DiscoveryConfig discoveryConfig = config.getNetworkConfig().getJoin().getDiscoveryConfig();
        assertTrue(discoveryConfig.isEnabled());
        assertDiscoveryConfig(discoveryConfig);
    }

    @Override
    @Test
    public void testSSLConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <network>\n"
                + "        <ssl enabled=\"true\">\r\n"
                + "          <factory-class-name>\r\n"
                + "              com.hazelcast.nio.ssl.BasicSSLContextFactory\r\n"
                + "          </factory-class-name>\r\n"
                + "          <properties>\r\n"
                + "            <property name=\"protocol\">TLS</property>\r\n"
                + "          </properties>\r\n"
                + "        </ssl>\r\n"
                + "    </network>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        SSLConfig sslConfig = config.getNetworkConfig().getSSLConfig();
        assertTrue(sslConfig.isEnabled());
        assertEquals("com.hazelcast.nio.ssl.BasicSSLContextFactory", sslConfig.getFactoryClassName());
        assertEquals(1, sslConfig.getProperties().size());
        assertEquals("TLS", sslConfig.getProperties().get("protocol"));
    }

    @Override
    @Test
    public void testSymmetricEncryptionConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <network>\n"
                + "      <symmetric-encryption enabled=\"true\">\n"
                + "        <algorithm>AES</algorithm>\n"
                + "        <salt>some-salt</salt>\n"
                + "        <password>some-pass</password>\n"
                + "        <iteration-count>7531</iteration-count>\n"
                + "      </symmetric-encryption>"
                + "    </network>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
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
        Config config = buildConfig(HAZELCAST_START_TAG
                + "    <network>\n"
                + "        <port port-count=\"200\">5702</port>\n"
                + "    </network>\n"
                + HAZELCAST_END_TAG);
        assertEquals(200, config.getNetworkConfig().getPortCount());
        assertEquals(5702, config.getNetworkConfig().getPort());

        // check if the default is passed in correctly
        config = buildConfig(HAZELCAST_START_TAG
                + "    <network>\n"
                + "        <port>5701</port>\n"
                + "    </network>\n"
                + HAZELCAST_END_TAG);
        assertEquals(100, config.getNetworkConfig().getPortCount());
    }

    @Override
    @Test
    public void readPortAutoIncrement() {
        // explicitly set
        Config config = buildConfig(HAZELCAST_START_TAG
                + "    <network>\n"
                + "        <port auto-increment=\"false\">5701</port>\n"
                + "    </network>\n"
                + HAZELCAST_END_TAG);
        assertFalse(config.getNetworkConfig().isPortAutoIncrement());

        // check if the default is picked up correctly
        config = buildConfig(HAZELCAST_START_TAG
                + "    <network>\n"
                + "        <port>5701</port>\n"
                + "    </network>\n"
                + HAZELCAST_END_TAG);
        assertTrue(config.getNetworkConfig().isPortAutoIncrement());
    }

    @Override
    @Test
    public void networkReuseAddress() {
        Config config = buildConfig(HAZELCAST_START_TAG
                + "    <network>\n"
                + "        <reuse-address>true</reuse-address>\n"
                + "    </network>\n"
                + HAZELCAST_END_TAG);
        assertTrue(config.getNetworkConfig().isReuseAddress());
    }

    @Override
    @Test
    public void readQueueConfig() {
        String xml = HAZELCAST_START_TAG
                + "      <queue name=\"custom\">"
                + "        <priority-comparator-class-name>com.hazelcast.collection.impl.queue.model.PriorityElementComparator</priority-comparator-class-name>"
                + "        <statistics-enabled>false</statistics-enabled>"
                + "        <max-size>100</max-size>"
                + "        <backup-count>2</backup-count>"
                + "        <async-backup-count>1</async-backup-count>"
                + "        <empty-queue-ttl>1</empty-queue-ttl>"
                + "        <item-listeners>"
                + "            <item-listener include-value=\"false\">com.hazelcast.examples.ItemListener</item-listener>"
                + "        </item-listeners>"
                + "        <queue-store enabled=\"false\">"
                + "            <class-name>com.hazelcast.QueueStoreImpl</class-name>"
                + "            <properties>"
                + "                <property name=\"binary\">false</property>"
                + "                <property name=\"memory-limit\">1000</property>"
                + "                <property name=\"bulk-load\">500</property>"
                + "            </properties>"
                + "        </queue-store>"
                + "        <split-brain-protection-ref>customSplitBrainProtectionRule</split-brain-protection-ref>"
                + "        <merge-policy batch-size=\"23\">CustomMergePolicy</merge-policy>"
                + "        <user-code-namespace>ns1</user-code-namespace>"
                + "    </queue>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        QueueConfig queueConfig = config.getQueueConfig("custom");
        assertEquals("com.hazelcast.collection.impl.queue.model.PriorityElementComparator",
                queueConfig.getPriorityComparatorClassName());
        assertFalse(queueConfig.isStatisticsEnabled());
        assertEquals(100, queueConfig.getMaxSize());
        assertEquals(2, queueConfig.getBackupCount());
        assertEquals(1, queueConfig.getAsyncBackupCount());
        assertEquals(1, queueConfig.getEmptyQueueTtl());
        assertEquals("ns1", queueConfig.getUserCodeNamespace());

        MergePolicyConfig mergePolicyConfig = queueConfig.getMergePolicyConfig();
        assertEquals("CustomMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(23, mergePolicyConfig.getBatchSize());

        assertEquals(1, queueConfig.getItemListenerConfigs().size());
        ItemListenerConfig listenerConfig = queueConfig.getItemListenerConfigs().iterator().next();
        assertEquals("com.hazelcast.examples.ItemListener", listenerConfig.getClassName());
        assertFalse(listenerConfig.isIncludeValue());

        QueueStoreConfig storeConfig = queueConfig.getQueueStoreConfig();
        assertNotNull(storeConfig);
        assertFalse(storeConfig.isEnabled());
        assertEquals("com.hazelcast.QueueStoreImpl", storeConfig.getClassName());

        Properties storeConfigProperties = storeConfig.getProperties();
        assertEquals(3, storeConfigProperties.size());
        assertEquals("500", storeConfigProperties.getProperty("bulk-load"));
        assertEquals("1000", storeConfigProperties.getProperty("memory-limit"));
        assertEquals("false", storeConfigProperties.getProperty("binary"));

        assertEquals("customSplitBrainProtectionRule", queueConfig.getSplitBrainProtectionName());
    }

    @Override
    @Test
    public void readListConfig() {
        String xml = HAZELCAST_START_TAG
                + "      <list name=\"myList\">"
                + "        <statistics-enabled>false</statistics-enabled>"
                + "        <max-size>100</max-size>"
                + "        <backup-count>2</backup-count>"
                + "        <async-backup-count>1</async-backup-count>"
                + "        <item-listeners>"
                + "            <item-listener include-value=\"false\">com.hazelcast.examples.ItemListener</item-listener>"
                + "        </item-listeners>"
                + "        <merge-policy batch-size=\"4223\">PassThroughMergePolicy</merge-policy>"
                + "        <user-code-namespace>ns1</user-code-namespace>"
                + "    </list>"
                + HAZELCAST_END_TAG;
        Config config = buildConfig(xml);
        ListConfig listConfig = config.getListConfig("myList");

        assertEquals("myList", listConfig.getName());
        assertFalse(listConfig.isStatisticsEnabled());
        assertEquals(100, listConfig.getMaxSize());
        assertEquals(2, listConfig.getBackupCount());
        assertEquals(1, listConfig.getAsyncBackupCount());
        assertEquals(1, listConfig.getItemListenerConfigs().size());
        assertEquals("ns1", listConfig.getUserCodeNamespace());

        ItemListenerConfig listenerConfig = listConfig.getItemListenerConfigs().iterator().next();
        assertEquals("com.hazelcast.examples.ItemListener", listenerConfig.getClassName());
        assertFalse(listenerConfig.isIncludeValue());

        MergePolicyConfig mergePolicyConfig = listConfig.getMergePolicyConfig();
        assertEquals("PassThroughMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(4223, mergePolicyConfig.getBatchSize());
    }

    @Override
    @Test
    public void readSetConfig() {
        String xml = HAZELCAST_START_TAG
                + "      <set name=\"mySet\">"
                + "        <statistics-enabled>false</statistics-enabled>"
                + "        <max-size>100</max-size>"
                + "        <backup-count>2</backup-count>"
                + "        <async-backup-count>1</async-backup-count>"
                + "        <item-listeners>"
                + "            <item-listener>com.hazelcast.examples.ItemListener</item-listener>"
                + "        </item-listeners>"
                + "        <merge-policy batch-size=\"4223\">PassThroughMergePolicy</merge-policy>"
                + "        <user-code-namespace>ns1</user-code-namespace>"
                + "    </set>"
                + HAZELCAST_END_TAG;
        Config config = buildConfig(xml);
        SetConfig setConfig = config.getSetConfig("mySet");

        assertEquals("mySet", setConfig.getName());
        assertFalse(setConfig.isStatisticsEnabled());
        assertEquals(100, setConfig.getMaxSize());
        assertEquals(2, setConfig.getBackupCount());
        assertEquals(1, setConfig.getAsyncBackupCount());
        assertEquals(1, setConfig.getItemListenerConfigs().size());
        assertEquals("ns1", setConfig.getUserCodeNamespace());

        ItemListenerConfig listenerConfig = setConfig.getItemListenerConfigs().iterator().next();
        assertEquals("com.hazelcast.examples.ItemListener", listenerConfig.getClassName());

        MergePolicyConfig mergePolicyConfig = setConfig.getMergePolicyConfig();
        assertEquals("PassThroughMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(4223, mergePolicyConfig.getBatchSize());
    }

    @Override
    @Test
    public void readReliableTopicConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <reliable-topic name=\"custom\">"
                + "           <read-batch-size>35</read-batch-size>"
                + "           <statistics-enabled>false</statistics-enabled>"
                + "           <topic-overload-policy>DISCARD_OLDEST</topic-overload-policy>"
                + "           <message-listeners>"
                + "               <message-listener>MessageListenerImpl</message-listener>"
                + "           </message-listeners>"
                + "           <user-code-namespace>ns1</user-code-namespace>"
                + "    </reliable-topic>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);

        ReliableTopicConfig topicConfig = config.getReliableTopicConfig("custom");

        assertEquals(35, topicConfig.getReadBatchSize());
        assertFalse(topicConfig.isStatisticsEnabled());
        assertEquals(TopicOverloadPolicy.DISCARD_OLDEST, topicConfig.getTopicOverloadPolicy());
        assertEquals("ns1", topicConfig.getUserCodeNamespace());

        // checking listener configuration
        assertEquals(1, topicConfig.getMessageListenerConfigs().size());
        ListenerConfig listenerConfig = topicConfig.getMessageListenerConfigs().get(0);
        assertEquals("MessageListenerImpl", listenerConfig.getClassName());
        assertNull(listenerConfig.getImplementation());
    }

    @Override
    @Test
    public void readTopicConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <topic name=\"custom\">"
                + "           <statistics-enabled>false</statistics-enabled>"
                + "           <global-ordering-enabled>true</global-ordering-enabled>"
                + "           <multi-threading-enabled>false</multi-threading-enabled>"
                + "           <message-listeners>"
                + "               <message-listener>MessageListenerImpl</message-listener>"
                + "           </message-listeners>"
                + "           <user-code-namespace>ns1</user-code-namespace>"
                + "    </topic>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);

        TopicConfig topicConfig = config.getTopicConfig("custom");

        assertTrue(topicConfig.isGlobalOrderingEnabled());
        assertFalse(topicConfig.isMultiThreadingEnabled());
        assertFalse(topicConfig.isStatisticsEnabled());
        assertEquals("ns1", topicConfig.getUserCodeNamespace());

        // checking listener configuration
        assertEquals(1, topicConfig.getMessageListenerConfigs().size());
        ListenerConfig listenerConfig = topicConfig.getMessageListenerConfigs().get(0);
        assertEquals("MessageListenerImpl", listenerConfig.getClassName());
        assertNull(listenerConfig.getImplementation());
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testCaseInsensitivityOfSettings() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"testCaseInsensitivity\">"
                + "    <in-memory-format>BINARY</in-memory-format>"
                + "    <backup-count>1</backup-count>"
                + "    <async-backup-count>0</async-backup-count>"
                + "    <time-to-live-seconds>0</time-to-live-seconds>"
                + "    <max-idle-seconds>0</max-idle-seconds>    "
                + "    <eviction eviction-policy=\"NONE\" max-size-policy=\"per_partition\" size=\"0\"/>"
                + "    <merge-policy batch-size=\"2342\">CustomMergePolicy</merge-policy>"
                + "</map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
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
    public void testMapExpiryConfig() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"expiry\">"
                + "    <time-to-live-seconds>2147483647</time-to-live-seconds>"
                + "    <max-idle-seconds>2147483647</max-idle-seconds>    "
                + "</map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig("expiry");

        assertEquals(Integer.MAX_VALUE, mapConfig.getTimeToLiveSeconds());
        assertEquals(Integer.MAX_VALUE, mapConfig.getMaxIdleSeconds());
    }

    @Override
    @Test
    public void readRingbuffer() {
        String xml = HAZELCAST_START_TAG
                + "    <ringbuffer name=\"custom\">"
                + "        <capacity>10</capacity>"
                + "        <backup-count>2</backup-count>"
                + "        <async-backup-count>1</async-backup-count>"
                + "        <time-to-live-seconds>9</time-to-live-seconds>"
                + "        <in-memory-format>OBJECT</in-memory-format>"
                + "        <ringbuffer-store enabled=\"false\">"
                + "            <class-name>com.hazelcast.RingbufferStoreImpl</class-name>"
                + "            <properties>"
                + "                <property name=\"store-path\">.//tmp//bufferstore</property>"
                + "            </properties>"
                + "        </ringbuffer-store>"
                + "        <split-brain-protection-ref>customSplitBrainProtectionRule</split-brain-protection-ref>"
                + "        <merge-policy batch-size=\"2342\">CustomMergePolicy</merge-policy>"
                + "        <user-code-namespace>ns1</user-code-namespace>"
                + "    </ringbuffer>"
                + HAZELCAST_END_TAG;
        Config config = buildConfig(xml);
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
    }

    @Override
    @Test
    public void testManagementCenterConfig() {
        String xml = HAZELCAST_START_TAG
                + "<management-center scripting-enabled='true' console-enabled='true' data-access-enabled='false'>"
                + "  <trusted-interfaces>\n"
                + "    <interface>127.0.0.1</interface>\n"
                + "    <interface>192.168.1.*</interface>\n"
                + "  </trusted-interfaces>\n"
                + "</management-center>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
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
        String xml = HAZELCAST_START_TAG
                + "<management-center>"
                + "</management-center>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        ManagementCenterConfig mcConfig = config.getManagementCenterConfig();

        assertFalse(mcConfig.isScriptingEnabled());
        assertFalse(mcConfig.isConsoleEnabled());
        assertTrue(mcConfig.isDataAccessEnabled());
    }

    @Override
    @Test
    public void testEmptyManagementCenterConfig() {
        String xml = HAZELCAST_START_TAG + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        ManagementCenterConfig mcConfig = config.getManagementCenterConfig();

        assertFalse(mcConfig.isScriptingEnabled());
        assertFalse(mcConfig.isConsoleEnabled());
        assertTrue(mcConfig.isDataAccessEnabled());
    }

    @Override
    @Test
    public void testMapStoreInitialModeLazy() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"mymap\">"
                + "<map-store enabled=\"true\" initial-mode=\"LAZY\"></map-store>"
                + "</map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapStoreConfig mapStoreConfig = config.getMapConfig("mymap").getMapStoreConfig();

        assertTrue(mapStoreConfig.isEnabled());
        assertEquals(MapStoreConfig.InitialLoadMode.LAZY, mapStoreConfig.getInitialLoadMode());
    }

    @Override
    @Test
    public void testMapConfig_metadataPolicy() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"mymap\">"
                + "<metadata-policy>CREATE_ON_UPDATE</metadata-policy>"
                + "</map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertEquals(MetadataPolicy.CREATE_ON_UPDATE, mapConfig.getMetadataPolicy());
    }

    @Override
    public void testMapConfig_statisticsEnable() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"mymap\">"
                + "<statistics-enabled>false</statistics-enabled>"
                + "</map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertFalse(mapConfig.isStatisticsEnabled());
    }

    @Override
    public void testMapConfig_perEntryStatsEnabled() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"mymap\">"
                + "<per-entry-stats-enabled>true</per-entry-stats-enabled>"
                + "</map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertTrue(mapConfig.isPerEntryStatsEnabled());
    }

    @Override
    @Test
    public void testMapConfig_metadataPolicy_defaultValue() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"mymap\">"
                + "</map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertEquals(MetadataPolicy.CREATE_ON_UPDATE, mapConfig.getMetadataPolicy());
    }

    @Override
    @Test
    public void testMapConfig_evictions() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"lruMap\">"
                + "        <eviction eviction-policy=\"LRU\"/>\n"
                + "    </map>\n"
                + "<map name=\"lfuMap\">"
                + "         <eviction eviction-policy=\"LFU\"/>\n"
                + "    </map>\n"
                + "<map name=\"noneMap\">"
                + "         <eviction eviction-policy=\"NONE\"/>\n"
                + "    </map>\n"
                + "<map name=\"randomMap\">"
                + "       <eviction eviction-policy=\"RANDOM\"/>\n"
                + "    </map>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);

        assertEquals(EvictionPolicy.LRU, config.getMapConfig("lruMap").getEvictionConfig().getEvictionPolicy());
        assertEquals(EvictionPolicy.LFU, config.getMapConfig("lfuMap").getEvictionConfig().getEvictionPolicy());
        assertEquals(EvictionPolicy.NONE, config.getMapConfig("noneMap").getEvictionConfig().getEvictionPolicy());
        assertEquals(EvictionPolicy.RANDOM, config.getMapConfig("randomMap").getEvictionConfig().getEvictionPolicy());
    }

    @Override
    @Test
    public void testMapConfig_cacheValueConfig_defaultValue() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"mymap\">"
                + "</map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertEquals(CacheDeserializedValues.INDEX_ONLY, mapConfig.getCacheDeserializedValues());
    }

    @Override
    @Test
    public void testMapConfig_cacheValueConfig_never() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"mymap\">"
                + "<cache-deserialized-values>NEVER</cache-deserialized-values>"
                + "</map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertEquals(CacheDeserializedValues.NEVER, mapConfig.getCacheDeserializedValues());
    }

    @Override
    @Test
    public void testMapConfig_cacheValueConfig_always() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"mymap\">"
                + "<cache-deserialized-values>ALWAYS</cache-deserialized-values>"
                + "</map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertEquals(CacheDeserializedValues.ALWAYS, mapConfig.getCacheDeserializedValues());
    }

    @Override
    @Test
    public void testMapConfig_cacheValueConfig_indexOnly() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"mymap\">"
                + "<cache-deserialized-values>INDEX-ONLY</cache-deserialized-values>"
                + "</map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertEquals(CacheDeserializedValues.INDEX_ONLY, mapConfig.getCacheDeserializedValues());
    }

    @Override
    @Test
    public void testMapStoreInitialModeEager() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"mymap\">"
                + "<map-store enabled=\"true\" initial-mode=\"EAGER\"></map-store>"
                + "</map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapStoreConfig mapStoreConfig = config.getMapConfig("mymap").getMapStoreConfig();

        assertTrue(mapStoreConfig.isEnabled());
        assertEquals(MapStoreConfig.InitialLoadMode.EAGER, mapStoreConfig.getInitialLoadMode());
    }

    @Override
    @Test
    public void testMapStoreWriteBatchSize() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"mymap\">"
                + "<map-store >"
                + "<write-batch-size>23</write-batch-size>"
                + "</map-store>"
                + "</map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapStoreConfig mapStoreConfig = config.getMapConfig("mymap").getMapStoreConfig();

        assertEquals(23, mapStoreConfig.getWriteBatchSize());
    }

    @Override
    @Test
    public void testMapStoreEnabled() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"mymap\">"
                + "<map-store enabled=\"true\" />"
                + "</map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapStoreConfig mapStoreConfig = config.getMapConfig("mymap").getMapStoreConfig();

        assertTrue(mapStoreConfig.isEnabled());
    }

    @Override
    @Test
    public void testMapStoreEnabledIfNotDisabled() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"mymap\">"
                + "<map-store />"
                + "</map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapStoreConfig mapStoreConfig = config.getMapConfig("mymap").getMapStoreConfig();

        assertTrue(mapStoreConfig.isEnabled());
    }

    @Override
    @Test
    public void testMapStoreDisabled() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"mymap\">"
                + "<map-store enabled=\"false\" />"
                + "</map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
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
        String xml = getWriteCoalescingConfigXml(writeCoalescing, useDefault);
        Config config = buildConfig(xml);
        return config.getMapConfig("mymap").getMapStoreConfig();
    }

    private String getWriteCoalescingConfigXml(boolean value, boolean useDefault) {
        return HAZELCAST_START_TAG
                + "<map name=\"mymap\">"
                + "<map-store >"
                + (useDefault ? "" : "<write-coalescing>" + value + "</write-coalescing>")
                + "</map-store>"
                + "</map>"
                + HAZELCAST_END_TAG;
    }

    private MapStoreConfig getOffloadMapStoreConfig(boolean offload, boolean useDefault) {
        String xml = getOffloadConfigXml(offload, useDefault);
        Config config = buildConfig(xml);
        return config.getMapConfig("mymap").getMapStoreConfig();
    }

    private String getOffloadConfigXml(boolean value, boolean useDefault) {
        return HAZELCAST_START_TAG
                + "<map name=\"mymap\">"
                + "<map-store >"
                + (useDefault ? "" : "<offload>" + value + "</offload>")
                + "</map-store>"
                + "</map>"
                + HAZELCAST_END_TAG;
    }

    @Override
    @Test
    public void testNearCacheInMemoryFormat() {
        String mapName = "testMapNearCacheInMemoryFormat";
        String xml = HAZELCAST_START_TAG
                + "  <map name=\"" + mapName + "\">\n"
                + "    <near-cache>\n"
                + "      <in-memory-format>OBJECT</in-memory-format>\n"
                + "    </near-cache>\n"
                + "  </map>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig(mapName);
        NearCacheConfig ncConfig = mapConfig.getNearCacheConfig();

        assertEquals(InMemoryFormat.OBJECT, ncConfig.getInMemoryFormat());
    }

    @Override
    @Test
    public void testNearCacheInMemoryFormatNative_withKeysByReference() {
        String mapName = "testMapNearCacheInMemoryFormatNative";
        String xml = HAZELCAST_START_TAG
                + "  <map name=\"" + mapName + "\">\n"
                + "    <near-cache>\n"
                + "      <in-memory-format>NATIVE</in-memory-format>\n"
                + "      <serialize-keys>false</serialize-keys>\n"
                + "    </near-cache>\n"
                + "  </map>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig(mapName);
        NearCacheConfig ncConfig = mapConfig.getNearCacheConfig();

        assertEquals(InMemoryFormat.NATIVE, ncConfig.getInMemoryFormat());
        assertTrue(ncConfig.isSerializeKeys());
    }

    @Override
    @Test
    public void testNearCacheEvictionPolicy() {
        String xml = HAZELCAST_START_TAG
                + "  <map name=\"lfuNearCache\">"
                + "    <near-cache>"
                + "      <eviction eviction-policy=\"LFU\"/>"
                + "    </near-cache>"
                + "  </map>"
                + "  <map name=\"lruNearCache\">"
                + "    <near-cache>"
                + "      <eviction eviction-policy=\"LRU\"/>"
                + "    </near-cache>"
                + "  </map>"
                + "  <map name=\"noneNearCache\">"
                + "    <near-cache>"
                + "      <eviction eviction-policy=\"NONE\"/>"
                + "    </near-cache>"
                + "  </map>"
                + "  <map name=\"randomNearCache\">"
                + "    <near-cache>"
                + "      <eviction eviction-policy=\"RANDOM\"/>"
                + "    </near-cache>"
                + "  </map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
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
        String xml = HAZELCAST_START_TAG
                + "<partition-group enabled=\"true\" group-type=\"ZONE_AWARE\" />"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        PartitionGroupConfig partitionGroupConfig = config.getPartitionGroupConfig();
        assertTrue(partitionGroupConfig.isEnabled());
        assertEquals(PartitionGroupConfig.MemberGroupType.ZONE_AWARE, partitionGroupConfig.getGroupType());
    }

    @Override
    @Test
    public void testPartitionGroupNodeAware() {
        String xml = HAZELCAST_START_TAG
                + "<partition-group enabled=\"true\" group-type=\"NODE_AWARE\" />"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        PartitionGroupConfig partitionGroupConfig = config.getPartitionGroupConfig();
        assertTrue(partitionGroupConfig.isEnabled());
        assertEquals(PartitionGroupConfig.MemberGroupType.NODE_AWARE, partitionGroupConfig.getGroupType());
    }

    @Override
    @Test
    public void testPartitionGroupPlacementAware() {
        String xml = HAZELCAST_START_TAG
                + "<partition-group enabled=\"true\" group-type=\"PLACEMENT_AWARE\" />"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        PartitionGroupConfig partitionGroupConfig = config.getPartitionGroupConfig();
        assertTrue(partitionGroupConfig.isEnabled());
        assertEquals(PartitionGroupConfig.MemberGroupType.PLACEMENT_AWARE, partitionGroupConfig.getGroupType());
    }

    @Override
    @Test
    public void testPartitionGroupSPI() {
        String xml = HAZELCAST_START_TAG
                + "<partition-group enabled=\"true\" group-type=\"SPI\" />"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        assertEquals(PartitionGroupConfig.MemberGroupType.SPI, config.getPartitionGroupConfig().getGroupType());
    }

    @Override
    @Test
    public void testPartitionGroupMemberGroups() {
        String xml = HAZELCAST_START_TAG
                + "<partition-group enabled=\"true\" group-type=\"SPI\">"
                + "  <member-group>"
                + "    <interface>10.10.1.1</interface>"
                + "    <interface>10.10.1.2</interface>"
                + "  </member-group>"
                + "  <member-group>"
                + "    <interface>10.10.1.3</interface>"
                + "    <interface>10.10.1.4</interface>"
                + "  </member-group>"
                + "</partition-group>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
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
        String xml = HAZELCAST_START_TAG
                + "  <map name=\"" + mapName + "\">\n"
                + "    <near-cache name=\"test\">\n"
                + "      <in-memory-format>OBJECT</in-memory-format>\n"
                + "      <serialize-keys>false</serialize-keys>\n"
                + "      <time-to-live-seconds>77</time-to-live-seconds>\n"
                + "      <max-idle-seconds>92</max-idle-seconds>\n"
                + "      <invalidate-on-change>false</invalidate-on-change>\n"
                + "      <cache-local-entries>false</cache-local-entries>\n"
                + "      <eviction eviction-policy=\"LRU\" max-size-policy=\"ENTRY_COUNT\" size=\"3333\"/>\n"
                + "    </near-cache>\n"
                + "  </map>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
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
        String xml = HAZELCAST_START_TAG
                + "  <map name=\"" + mapName + "\">\n"
                + "    <wan-replication-ref name=\"test\">\n"
                + "      <merge-policy-class-name>TestMergePolicy</merge-policy-class-name>\n"
                + "      <filters>\n"
                + "        <filter-impl>com.example.SampleFilter</filter-impl>\n"
                + "      </filters>\n"
                + "    </wan-replication-ref>\n"
                + "  </map>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
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
        String xml = HAZELCAST_START_TAG
                + "  <wan-replication name=\"" + configName + "\">\n"
                + "        <batch-publisher>\n"
                + "                <cluster-name>nyc</cluster-name>\n"
                + "                <publisher-id>publisherId</publisher-id>\n"
                + "                <batch-size>100</batch-size>\n"
                + "                <batch-max-delay-millis>200</batch-max-delay-millis>\n"
                + "                <response-timeout-millis>300</response-timeout-millis>\n"
                + "                <acknowledge-type>ACK_ON_RECEIPT</acknowledge-type>\n"
                + "                <initial-publisher-state>STOPPED</initial-publisher-state>\n"
                + "                <snapshot-enabled>true</snapshot-enabled>\n"
                + "                <idle-min-park-ns>400</idle-min-park-ns>\n"
                + "                <idle-max-park-ns>500</idle-max-park-ns>\n"
                + "                <max-concurrent-invocations>600</max-concurrent-invocations>\n"
                + "                <discovery-period-seconds>700</discovery-period-seconds>\n"
                + "                <use-endpoint-private-address>true</use-endpoint-private-address>\n"
                + "                <queue-full-behavior>THROW_EXCEPTION</queue-full-behavior>\n"
                + "                <max-target-endpoints>800</max-target-endpoints>\n"
                + "                <queue-capacity>21</queue-capacity>\n"
                + "            <target-endpoints>a,b,c,d</target-endpoints>"
                + "            <sync>\n"
                + "                <consistency-check-strategy>MERKLE_TREES</consistency-check-strategy>\n"
                + "            </sync>\n"
                + "            <properties>\n"
                + "                <property name=\"propName1\">propValue1</property>\n"
                + "            </properties>\n"
                + "            <endpoint>nyc-endpoint</endpoint>\n"
                + "        </batch-publisher>"
                + "        <custom-publisher>\n"
                + "                <class-name>PublisherClassName</class-name>\n"
                + "                <publisher-id>customPublisherId</publisher-id>\n"
                + "            <properties>\n"
                + "                <property name=\"propName1\">propValue1</property>\n"
                + "            </properties>\n"
                + "        </custom-publisher>\n"
                + "        <consumer>\n"
                + "            <class-name>ConsumerClassName</class-name>\n"
                + "            <properties>\n"
                + "                <property name=\"propName1\">propValue1</property>\n"
                + "            </properties>\n"
                + "        </consumer>\n"
                + "    </wan-replication>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        WanReplicationConfig wanReplicationConfig = config.getWanReplicationConfig(configName);

        assertEquals(configName, wanReplicationConfig.getName());

        WanConsumerConfig consumerConfig = wanReplicationConfig.getConsumerConfig();
        assertNotNull(consumerConfig);
        assertEquals("ConsumerClassName", consumerConfig.getClassName());

        Map<String, Comparable> properties = consumerConfig.getProperties();
        assertNotNull(properties);
        assertEquals(1, properties.size());
        assertEquals("propValue1", properties.get("propName1"));

        List<WanBatchPublisherConfig> publishers = wanReplicationConfig.getBatchPublisherConfigs();
        assertNotNull(publishers);
        assertEquals(1, publishers.size());
        WanBatchPublisherConfig pc = publishers.get(0);

        assertEquals("nyc", pc.getClusterName());
        assertEquals("publisherId", pc.getPublisherId());
        assertEquals(100, pc.getBatchSize());
        assertEquals(200, pc.getBatchMaxDelayMillis());
        assertEquals(300, pc.getResponseTimeoutMillis());
        assertEquals(WanAcknowledgeType.ACK_ON_RECEIPT, pc.getAcknowledgeType());
        assertEquals(WanPublisherState.STOPPED, pc.getInitialPublisherState());
        assertTrue(pc.isSnapshotEnabled());
        assertEquals(400, pc.getIdleMinParkNs());
        assertEquals(500, pc.getIdleMaxParkNs());
        assertEquals(600, pc.getMaxConcurrentInvocations());
        assertEquals(700, pc.getDiscoveryPeriodSeconds());
        assertTrue(pc.isUseEndpointPrivateAddress());
        assertEquals(THROW_EXCEPTION, pc.getQueueFullBehavior());
        assertEquals(800, pc.getMaxTargetEndpoints());
        assertEquals(21, pc.getQueueCapacity());
        assertEquals("a,b,c,d", pc.getTargetEndpoints());
        assertEquals("nyc-endpoint", pc.getEndpoint());
        assertEquals(ConsistencyCheckStrategy.MERKLE_TREES, pc.getSyncConfig().getConsistencyCheckStrategy());

        properties = pc.getProperties();
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
        String xml = HAZELCAST_START_TAG
                + "  <wan-replication name=\"" + configName + "\">\n"
                + "        <consumer>\n"
                + "        </consumer>\n"
                + "    </wan-replication>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        WanReplicationConfig wanReplicationConfig = config.getWanReplicationConfig(configName);
        WanConsumerConfig consumerConfig = wanReplicationConfig.getConsumerConfig();
        assertFalse(consumerConfig.isPersistWanReplicatedData());
    }

    @Override
    @Test
    public void testWanReplicationSyncConfig() {
        String configName = "test";
        String xml = HAZELCAST_START_TAG
                + "  <wan-replication name=\"" + configName + "\">\n"
                + "        <batch-publisher>\n"
                + "                <cluster-name>nyc</cluster-name>\n"
                + "                <sync>\n"
                + "                    <consistency-check-strategy>MERKLE_TREES</consistency-check-strategy>\n"
                + "                </sync>\n"
                + "            </batch-publisher>\n"
                + "    </wan-replication>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        WanReplicationConfig wanReplicationConfig = config.getWanReplicationConfig(configName);

        assertEquals(configName, wanReplicationConfig.getName());

        List<WanBatchPublisherConfig> publishers = wanReplicationConfig.getBatchPublisherConfigs();
        assertNotNull(publishers);
        assertEquals(1, publishers.size());
        WanBatchPublisherConfig pc = publishers.get(0);
        assertEquals(ConsistencyCheckStrategy.MERKLE_TREES, pc.getSyncConfig().getConsistencyCheckStrategy());
    }

    @Override
    @Test
    public void testFlakeIdGeneratorConfig() {
        String xml = HAZELCAST_START_TAG
                + "<flake-id-generator name='gen'>"
                + "  <prefetch-count>3</prefetch-count>"
                + "  <prefetch-validity-millis>10</prefetch-validity-millis>"
                + "  <epoch-start>1514764800001</epoch-start>"
                + "  <node-id-offset>30</node-id-offset>"
                + "  <bits-sequence>22</bits-sequence>"
                + "  <bits-node-id>33</bits-node-id>"
                + "  <allowed-future-millis>20000</allowed-future-millis>"
                + "  <statistics-enabled>false</statistics-enabled>"
                + "</flake-id-generator>"
                + HAZELCAST_END_TAG;
        Config config = buildConfig(xml);
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
    }

    @Override
    @Test
    public void testParseExceptionIsNotSwallowed() {
        String invalidXml = HAZELCAST_START_TAG + "</hazelcast";
        expected.expect(InvalidConfigurationException.class);
        buildConfig(invalidXml);
    }

    @Override
    @Test
    public void testMapPartitionLostListenerConfig() {
        String mapName = "map1";
        String listenerName = "DummyMapPartitionLostListenerImpl";
        String xml = createMapPartitionLostListenerConfiguredXml(mapName, listenerName);

        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig("map1");
        assertMapPartitionLostListener(listenerName, mapConfig);
    }

    @Override
    @Test
    public void testMapPartitionLostListenerConfigReadOnly() {
        String mapName = "map1";
        String listenerName = "DummyMapPartitionLostListenerImpl";
        String xml = createMapPartitionLostListenerConfiguredXml(mapName, listenerName);

        Config config = buildConfig(xml);
        MapConfig mapConfig = config.findMapConfig("map1");
        assertMapPartitionLostListener(listenerName, mapConfig);
    }

    private void assertMapPartitionLostListener(String listenerName, MapConfig mapConfig) {
        assertFalse(mapConfig.getPartitionLostListenerConfigs().isEmpty());
        assertEquals(listenerName, mapConfig.getPartitionLostListenerConfigs().get(0).getClassName());
    }

    private String createMapPartitionLostListenerConfiguredXml(String mapName, String listenerName) {
        return HAZELCAST_START_TAG
                + "<map name=\"" + mapName + "\">\n"
                + "    <partition-lost-listeners>\n"
                + "        <partition-lost-listener>" + listenerName + "</partition-lost-listener>\n"
                + "    </partition-lost-listeners>\n"
                + "</map>\n"
                + HAZELCAST_END_TAG;
    }

    @Override
    @Test
    public void testCachePartitionLostListenerConfig() {
        String cacheName = "cache1";
        String listenerName = "DummyCachePartitionLostListenerImpl";
        String xml = createCachePartitionLostListenerConfiguredXml(cacheName, listenerName);

        Config config = buildConfig(xml);
        CacheSimpleConfig cacheConfig = config.getCacheConfig("cache1");
        assertCachePartitionLostListener(listenerName, cacheConfig);
    }

    @Override
    @Test
    public void testCachePartitionLostListenerConfigReadOnly() {
        String cacheName = "cache1";
        String listenerName = "DummyCachePartitionLostListenerImpl";
        String xml = createCachePartitionLostListenerConfiguredXml(cacheName, listenerName);

        Config config = buildConfig(xml);
        CacheSimpleConfig cacheConfig = config.findCacheConfig("cache1");
        assertCachePartitionLostListener(listenerName, cacheConfig);
    }

    private void assertCachePartitionLostListener(String listenerName, CacheSimpleConfig cacheConfig) {
        assertFalse(cacheConfig.getPartitionLostListenerConfigs().isEmpty());
        assertEquals(listenerName, cacheConfig.getPartitionLostListenerConfigs().get(0).getClassName());
    }

    private String createCachePartitionLostListenerConfiguredXml(String cacheName, String listenerName) {
        return HAZELCAST_START_TAG
                + "<cache name=\"" + cacheName + "\">\n"
                + "    <partition-lost-listeners>\n"
                + "        <partition-lost-listener>" + listenerName + "</partition-lost-listener>\n"
                + "    </partition-lost-listeners>\n"
                + "</cache>\n"
                + HAZELCAST_END_TAG;
    }

    protected static Config buildConfig(String xml) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        return configBuilder.build();
    }

    @Override
    @Test
    public void readMulticastConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <network>\n"
                + "        <join>\n"
                + "            <multicast enabled=\"false\" loopbackModeEnabled=\"true\">\n"
                + "                <multicast-group>224.2.2.4</multicast-group>\n"
                + "                <multicast-port>65438</multicast-port>\n"
                + "                <multicast-timeout-seconds>4</multicast-timeout-seconds>\n"
                + "                <multicast-time-to-live>42</multicast-time-to-live>\n"
                + "                <trusted-interfaces>\n"
                + "                  <interface>127.0.0.1</interface>\n"
                + "                  <interface>0.0.0.0</interface>\n"
                + "                </trusted-interfaces>\n"
                + "            </multicast>\n"
                + "        </join>\n"
                + "    </network>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
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
        String xml = HAZELCAST_START_TAG
                + "   <wan-replication name=\"my-wan-cluster\">\n"
                + "        <batch-publisher>\n"
                + "                <cluster-name>istanbul</cluster-name>\n"
                + "                <publisher-id>istanbulPublisherId</publisher-id>\n"
                + "                <batch-size>100</batch-size>\n"
                + "                <batch-max-delay-millis>200</batch-max-delay-millis>\n"
                + "                <response-timeout-millis>300</response-timeout-millis>\n"
                + "                <acknowledge-type>ACK_ON_RECEIPT</acknowledge-type>\n"
                + "                <initial-publisher-state>STOPPED</initial-publisher-state>\n"
                + "                <snapshot-enabled>true</snapshot-enabled>\n"
                + "                <idle-min-park-ns>400</idle-min-park-ns>\n"
                + "                <idle-max-park-ns>500</idle-max-park-ns>\n"
                + "                <max-concurrent-invocations>600</max-concurrent-invocations>\n"
                + "                <discovery-period-seconds>700</discovery-period-seconds>\n"
                + "                <use-endpoint-private-address>true</use-endpoint-private-address>\n"
                + "                <queue-full-behavior>THROW_EXCEPTION</queue-full-behavior>\n"
                + "                <max-target-endpoints>800</max-target-endpoints>\n"
                + "                <queue-capacity>21</queue-capacity>\n"
                + "                <target-endpoints>a,b,c,d</target-endpoints>\n"
                + "            <sync>\n"
                + "                <consistency-check-strategy>MERKLE_TREES</consistency-check-strategy>\n"
                + "            </sync>\n"
                + "            <aws enabled=\"false\" connection-timeout-seconds=\"10\" >\n"
                + "               <access-key>sample-access-key</access-key>\n"
                + "               <secret-key>sample-secret-key</secret-key>\n"
                + "               <iam-role>sample-role</iam-role>\n"
                + "               <region>sample-region</region>\n"
                + "               <host-header>sample-header</host-header>\n"
                + "               <security-group-name>sample-group</security-group-name>\n"
                + "               <tag-key>sample-tag-key</tag-key>\n"
                + "               <tag-value>sample-tag-value</tag-value>\n"
                + "            </aws>\n"
                + "            <discovery-strategies>\n"
                + "               <node-filter class=\"DummyFilterClass\" />\n"
                + "               <discovery-strategy class=\"DummyDiscoveryStrategy1\" enabled=\"true\">\n"
                + "                  <properties>\n"
                + "                     <property name=\"key-string\">foo</property>\n"
                + "                     <property name=\"key-int\">123</property>\n"
                + "                     <property name=\"key-boolean\">true</property>\n"
                + "                  </properties>\n"
                + "               </discovery-strategy>\n"
                + "            </discovery-strategies>\n"
                + "            <properties>\n"
                + "                <property name=\"custom.prop.publisher\">prop.publisher</property>\n"
                + "            </properties>\n"
                + "            <endpoint>nyc-endpoint</endpoint>\n"
                + "        </batch-publisher>"
                + "        <batch-publisher>\n"
                + "                <cluster-name>ankara</cluster-name>\n"
                + "                <initial-publisher-state>STOPPED</initial-publisher-state>\n"
                + "                <queue-full-behavior>THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE</queue-full-behavior>\n"
                + "        </batch-publisher>\n"
                + "        <custom-publisher>\n"
                + "                <publisher-id>customPublisherId</publisher-id>\n"
                + "                <class-name>PublisherClassName</class-name>\n"
                + "            <properties>\n"
                + "                <property name=\"propName1\">propValue1</property>\n"
                + "            </properties>\n"
                + "        </custom-publisher>\n"
                + "        <consumer>\n"
                + "            <class-name>com.hazelcast.wan.custom.WanConsumer</class-name>\n"
                + "            <properties>\n"
                + "                <property name=\"custom.prop.consumer\">prop.consumer</property>\n"
                + "            </properties>\n"
                + "            <persist-wan-replicated-data>true</persist-wan-replicated-data>\n"
                + "        </consumer>\n"
                + "   </wan-replication>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
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
        assertEquals("nyc-endpoint", pc1.getEndpoint());
        assertEquals(ConsistencyCheckStrategy.MERKLE_TREES, pc1.getSyncConfig().getConsistencyCheckStrategy());
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
        assertEquals("", pc2.getPublisherId());
        assertEquals(WanQueueFullBehavior.THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE, pc2.getQueueFullBehavior());
        assertEquals(WanPublisherState.STOPPED, pc2.getInitialPublisherState());

        List<WanCustomPublisherConfig> customPublishers = wanReplicationConfig.getCustomPublisherConfigs();
        assertNotNull(customPublishers);
        assertEquals(1, customPublishers.size());
        WanCustomPublisherConfig customPublisher = customPublishers.get(0);
        assertEquals("customPublisherId", customPublisher.getPublisherId());
        assertEquals("PublisherClassName", customPublisher.getClassName());
        Map<String, Comparable> properties = customPublisher.getProperties();
        assertNotNull(properties);
        assertEquals(1, properties.size());
        assertEquals("propValue1", properties.get("propName1"));

        WanConsumerConfig consumerConfig = wanReplicationConfig.getConsumerConfig();
        assertEquals("com.hazelcast.wan.custom.WanConsumer", consumerConfig.getClassName());
        Map<String, Comparable> consProperties = consumerConfig.getProperties();
        assertEquals("prop.consumer", consProperties.get("custom.prop.consumer"));
        assertTrue(consumerConfig.isPersistWanReplicatedData());
    }

    private void assertDiscoveryConfig(DiscoveryConfig c) {
        assertEquals("DummyFilterClass", c.getNodeFilterClass());
        assertEquals(1, c.getDiscoveryStrategyConfigs().size());

        DiscoveryStrategyConfig config = c.getDiscoveryStrategyConfigs().iterator().next();
        assertEquals("DummyDiscoveryStrategy1", config.getClassName());

        Map<String, Comparable> props = config.getProperties();
        assertEquals("foo", props.get("key-string"));
        assertEquals("123", props.get("key-int"));
        assertEquals("true", props.get("key-boolean"));
    }

    @Override
    @Test
    public void testSplitBrainProtectionConfig() {
        String xml = HAZELCAST_START_TAG
                + "      <split-brain-protection enabled=\"true\" name=\"mySplitBrainProtection\">\n"
                + "        <minimum-cluster-size>3</minimum-cluster-size>\n"
                + "        <function-class-name>com.my.splitbrainprotection.function</function-class-name>\n"
                + "        <protect-on>READ</protect-on>\n"
                + "      </split-brain-protection>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
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
        String xml = HAZELCAST_START_TAG
                + "      <split-brain-protection enabled=\"true\" name=\"mySplitBrainProtection\">\n"
                + "        <minimum-cluster-size>3</minimum-cluster-size>\n"
                + "        <listeners>"
                + "           <listener>com.abc.my.splitbrainprotection.listener</listener>"
                + "           <listener>com.abc.my.second.listener</listener>"
                + "       </listeners> "
                + "        <function-class-name>com.hazelcast.SomesplitBrainProtectionFunction</function-class-name>"
                + "    </split-brain-protection>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        SplitBrainProtectionConfig splitBrainProtectionConfig = config.getSplitBrainProtectionConfig("mySplitBrainProtection");

        assertFalse(splitBrainProtectionConfig.getListenerConfigs().isEmpty());
        assertEquals("com.abc.my.splitbrainprotection.listener", splitBrainProtectionConfig.getListenerConfigs().get(0).getClassName());
        assertEquals("com.abc.my.second.listener", splitBrainProtectionConfig.getListenerConfigs().get(1).getClassName());
        assertEquals("com.hazelcast.SomesplitBrainProtectionFunction", splitBrainProtectionConfig.getFunctionClassName());
    }

    @Override
    @Test
    public void testConfig_whenClassNameAndRecentlyActiveSplitBrainProtectionDefined_exceptionIsThrown() {
        String xml = HAZELCAST_START_TAG
                + "      <split-brain-protection enabled=\"true\" name=\"mySplitBrainProtection\">\n"
                + "        <minimum-cluster-size>3</minimum-cluster-size>\n"
                + "        <function-class-name>com.hazelcast.SomesplitBrainProtectionFunction</function-class-name>"
                + "        <recently-active-split-brain-protection />"
                + "    </split-brain-protection>\n"
                + HAZELCAST_END_TAG;

        expected.expect(InvalidConfigurationException.class);
        buildConfig(xml);
    }

    @Override
    @Test
    public void testConfig_whenClassNameAndProbabilisticSplitBrainProtectionDefined_exceptionIsThrown() {
        String xml = HAZELCAST_START_TAG
                + "      <split-brain-protection enabled=\"true\" name=\"mySplitBrainProtection\">\n"
                + "        <minimum-cluster-size>3</minimum-cluster-size>\n"
                + "        <function-class-name>com.hazelcast.SomesplitBrainProtectionFunction</function-class-name>"
                + "        <probabilistic-split-brain-protection />"
                + "    </split-brain-protection>\n"
                + HAZELCAST_END_TAG;

        expected.expect(InvalidConfigurationException.class);
        buildConfig(xml);
    }

    @Override
    @Test
    public void testConfig_whenBothBuiltinSplitBrainProtectionsDefined_exceptionIsThrown() {
        String xml = HAZELCAST_START_TAG
                + "      <split-brain-protection enabled=\"true\" name=\"mySplitBrainProtection\">\n"
                + "        <minimum-cluster-size>3</minimum-cluster-size>\n"
                + "        <probabilistic-split-brain-protection />"
                + "        <recently-active-split-brain-protection />"
                + "    </split-brain-protection>\n"
                + HAZELCAST_END_TAG;

        expected.expect(InvalidConfigurationException.class);
        buildConfig(xml);
    }

    @Override
    @Test
    public void testConfig_whenRecentlyActiveSplitBrainProtection_withDefaultValues() {
        String xml = HAZELCAST_START_TAG
                + "      <split-brain-protection enabled=\"true\" name=\"mySplitBrainProtection\">\n"
                + "        <minimum-cluster-size>3</minimum-cluster-size>\n"
                + "        <recently-active-split-brain-protection />"
                + "    </split-brain-protection>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        SplitBrainProtectionConfig splitBrainProtectionConfig = config.getSplitBrainProtectionConfig("mySplitBrainProtection");
        assertInstanceOf(RecentlyActiveSplitBrainProtectionFunction.class, splitBrainProtectionConfig.getFunctionImplementation());
        RecentlyActiveSplitBrainProtectionFunction splitBrainProtectionFunction = (RecentlyActiveSplitBrainProtectionFunction) splitBrainProtectionConfig
                .getFunctionImplementation();
        assertEquals(RecentlyActiveSplitBrainProtectionConfigBuilder.DEFAULT_HEARTBEAT_TOLERANCE_MILLIS,
                splitBrainProtectionFunction.getHeartbeatToleranceMillis());
    }

    @Override
    @Test
    public void testConfig_whenRecentlyActiveSplitBrainProtection_withCustomValues() {
        String xml = HAZELCAST_START_TAG
                + "      <split-brain-protection enabled=\"true\" name=\"mySplitBrainProtection\">\n"
                + "        <minimum-cluster-size>3</minimum-cluster-size>\n"
                + "        <recently-active-split-brain-protection heartbeat-tolerance-millis=\"13000\" />"
                + "    </split-brain-protection>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        SplitBrainProtectionConfig splitBrainProtectionConfig = config.getSplitBrainProtectionConfig("mySplitBrainProtection");
        assertEquals(3, splitBrainProtectionConfig.getMinimumClusterSize());
        assertInstanceOf(RecentlyActiveSplitBrainProtectionFunction.class, splitBrainProtectionConfig.getFunctionImplementation());
        RecentlyActiveSplitBrainProtectionFunction splitBrainProtectionFunction = (RecentlyActiveSplitBrainProtectionFunction) splitBrainProtectionConfig
                .getFunctionImplementation();
        assertEquals(13000, splitBrainProtectionFunction.getHeartbeatToleranceMillis());
    }

    @Override
    @Test
    public void testConfig_whenProbabilisticSplitBrainProtection_withDefaultValues() {
        String xml = HAZELCAST_START_TAG
                + "      <split-brain-protection enabled=\"true\" name=\"mySplitBrainProtection\">\n"
                + "        <minimum-cluster-size>3</minimum-cluster-size>\n"
                + "        <probabilistic-split-brain-protection />"
                + "    </split-brain-protection>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        SplitBrainProtectionConfig splitBrainProtectionConfig = config.getSplitBrainProtectionConfig("mySplitBrainProtection");
        assertInstanceOf(ProbabilisticSplitBrainProtectionFunction.class, splitBrainProtectionConfig.getFunctionImplementation());
        ProbabilisticSplitBrainProtectionFunction splitBrainProtectionFunction = (ProbabilisticSplitBrainProtectionFunction) splitBrainProtectionConfig.getFunctionImplementation();
        assertEquals(ProbabilisticSplitBrainProtectionConfigBuilder.DEFAULT_HEARTBEAT_INTERVAL_MILLIS,
                splitBrainProtectionFunction.getHeartbeatIntervalMillis());
        assertEquals(ProbabilisticSplitBrainProtectionConfigBuilder.DEFAULT_HEARTBEAT_PAUSE_MILLIS,
                splitBrainProtectionFunction.getAcceptableHeartbeatPauseMillis());
        assertEquals(ProbabilisticSplitBrainProtectionConfigBuilder.DEFAULT_MIN_STD_DEVIATION,
                splitBrainProtectionFunction.getMinStdDeviationMillis());
        assertEquals(ProbabilisticSplitBrainProtectionConfigBuilder.DEFAULT_PHI_THRESHOLD, splitBrainProtectionFunction.getSuspicionThreshold(), 0.01);
        assertEquals(ProbabilisticSplitBrainProtectionConfigBuilder.DEFAULT_SAMPLE_SIZE, splitBrainProtectionFunction.getMaxSampleSize());
    }

    @Override
    @Test
    public void testConfig_whenProbabilisticSplitBrainProtection_withCustomValues() {
        String xml = HAZELCAST_START_TAG
                + "      <split-brain-protection enabled=\"true\" name=\"mySplitBrainProtection\">\n"
                + "        <minimum-cluster-size>3</minimum-cluster-size>\n"
                + "        <probabilistic-split-brain-protection acceptable-heartbeat-pause-millis=\"37400\" suspicion-threshold=\"3.14592\" "
                + "                 max-sample-size=\"42\" min-std-deviation-millis=\"1234\""
                + "                 heartbeat-interval-millis=\"4321\" />"
                + "    </split-brain-protection>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
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
        String xml = HAZELCAST_START_TAG
                + "    <cache name=\"foobar\">\n"
                + "        <split-brain-protection-ref>customSplitBrainProtectionRule</split-brain-protection-ref>\n"
                + "        <key-type class-name=\"java.lang.Object\"/>\n"
                + "        <value-type class-name=\"java.lang.Object\"/>\n"
                + "        <statistics-enabled>false</statistics-enabled>\n"
                + "        <management-enabled>false</management-enabled>\n"
                + "        <read-through>true</read-through>\n"
                + "        <write-through>true</write-through>\n"
                + "        <cache-loader-factory class-name=\"com.example.cache.MyCacheLoaderFactory\"/>\n"
                + "        <cache-writer-factory class-name=\"com.example.cache.MyCacheWriterFactory\"/>\n"
                + "        <expiry-policy-factory class-name=\"com.example.cache.MyExpirePolicyFactory\"/>\n"
                + "        <in-memory-format>BINARY</in-memory-format>\n"
                + "        <backup-count>1</backup-count>\n"
                + "        <async-backup-count>0</async-backup-count>\n"
                + "        <eviction size=\"1000\" max-size-policy=\"ENTRY_COUNT\" eviction-policy=\"LFU\"/>\n"
                + "        <merge-policy batch-size=\"111\">LatestAccessMergePolicy</merge-policy>\n"
                + "        <disable-per-entry-invalidation-events>true</disable-per-entry-invalidation-events>\n"
                + "        <user-code-namespace>ns1</user-code-namespace>\n"
                + "        <event-journal enabled=\"true\">\n"
                + "            <capacity>120</capacity>\n"
                + "            <time-to-live-seconds>20</time-to-live-seconds>\n"
                + "          </event-journal>"
                + "        <merkle-tree enabled=\"true\">\n"
                + "            <depth>20</depth>\n"
                + "          </merkle-tree>"
                + "        <hot-restart enabled=\"false\">\n"
                + "            <fsync>false</fsync>\n"
                + "          </hot-restart>"
                + "        <data-persistence enabled=\"true\">\n"
                + "            <fsync>true</fsync>\n"
                + "          </data-persistence>"
                + "        <partition-lost-listeners>\n"
                + "            <partition-lost-listener>com.your-package.YourPartitionLostListener</partition-lost-listener>\n"
                + "          </partition-lost-listeners>"
                + "        <cache-entry-listeners>\n"
                + "            <cache-entry-listener old-value-required=\"false\" synchronous=\"false\">\n"
                + "                <cache-entry-listener-factory\n"
                + "                        class-name=\"com.example.cache.MyEntryListenerFactory\"/>\n"
                + "                <cache-entry-event-filter-factory\n"
                + "                        class-name=\"com.example.cache.MyEntryEventFilterFactory\"/>\n"
                + "            </cache-entry-listener>\n"
                + "        </cache-entry-listeners>"
                + "    </cache>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
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
        assertEquals(MaxSizePolicy.ENTRY_COUNT, cacheConfig.getEvictionConfig().getMaxSizePolicy());
        assertEquals(EvictionPolicy.LFU, cacheConfig.getEvictionConfig().getEvictionPolicy());
        assertEquals("LatestAccessMergePolicy", cacheConfig.getMergePolicyConfig().getPolicy());
        assertEquals(111, cacheConfig.getMergePolicyConfig().getBatchSize());
        assertTrue(cacheConfig.isDisablePerEntryInvalidationEvents());
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
        assertEquals("com.your-package.YourPartitionLostListener", cacheConfig.getPartitionLostListenerConfigs().get(0).getClassName());
        assertEquals(1, cacheConfig.getCacheEntryListeners().size());
        assertEquals("com.example.cache.MyEntryListenerFactory", cacheConfig.getCacheEntryListeners().get(0).getCacheEntryListenerFactory());
        assertEquals("com.example.cache.MyEntryEventFilterFactory", cacheConfig.getCacheEntryListeners().get(0).getCacheEntryEventFilterFactory());
        assertTrue(cacheConfig.getMerkleTreeConfig().isEnabled());
        assertEquals(20, cacheConfig.getMerkleTreeConfig().getDepth());
        assertEquals("ns1", cacheConfig.getUserCodeNamespace());
    }

    @Override
    @Test
    public void testExecutorConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <executor-service name=\"foobar\">\n"
                + "        <pool-size>2</pool-size>\n"
                + "        <split-brain-protection-ref>customSplitBrainProtectionRule</split-brain-protection-ref>"
                + "        <statistics-enabled>false</statistics-enabled>"
                + "        <queue-capacity>0</queue-capacity>"
                + "        <user-code-namespace>ns1</user-code-namespace>"
                + "    </executor-service>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
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
        String xml = HAZELCAST_START_TAG
                + "    <durable-executor-service name=\"foobar\">\n"
                + "        <statistics-enabled>false</statistics-enabled>"
                + "        <pool-size>2</pool-size>\n"
                + "        <durability>3</durability>\n"
                + "        <capacity>4</capacity>\n"
                + "        <split-brain-protection-ref>customSplitBrainProtectionRule</split-brain-protection-ref>"
                + "        <user-code-namespace>ns1</user-code-namespace>"
                + "    </durable-executor-service>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        DurableExecutorConfig durableExecutorConfig = config.getDurableExecutorConfig("foobar");

        assertFalse(config.getDurableExecutorConfigs().isEmpty());
        assertEquals(2, durableExecutorConfig.getPoolSize());
        assertEquals(3, durableExecutorConfig.getDurability());
        assertEquals(4, durableExecutorConfig.getCapacity());
        assertEquals("customSplitBrainProtectionRule", durableExecutorConfig.getSplitBrainProtectionName());
        assertEquals("ns1", durableExecutorConfig.getUserCodeNamespace());
        assertFalse(durableExecutorConfig.isStatisticsEnabled());
    }

    @Override
    @Test
    public void testScheduledExecutorConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <scheduled-executor-service name=\"foobar\">\n"
                + "        <statistics-enabled>false</statistics-enabled>"
                + "        <durability>4</durability>\n"
                + "        <pool-size>5</pool-size>\n"
                + "        <capacity>2</capacity>\n"
                + "        <split-brain-protection-ref>customSplitBrainProtectionRule</split-brain-protection-ref>"
                + "        <merge-policy batch-size='99'>PutIfAbsent</merge-policy>"
                + "        <user-code-namespace>ns1</user-code-namespace>"
                + "    </scheduled-executor-service>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
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
        String xml = HAZELCAST_START_TAG
                + "    <cardinality-estimator name=\"foobar\">\n"
                + "        <backup-count>2</backup-count>\n"
                + "        <async-backup-count>3</async-backup-count>\n"
                + "        <split-brain-protection-ref>customSplitBrainProtectionRule</split-brain-protection-ref>"
                + "        <merge-policy>com.hazelcast.spi.merge.HyperLogLogMergePolicy</merge-policy>"
                + "    </cardinality-estimator>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        CardinalityEstimatorConfig cardinalityEstimatorConfig = config.getCardinalityEstimatorConfig("foobar");

        assertFalse(config.getCardinalityEstimatorConfigs().isEmpty());
        assertEquals(2, cardinalityEstimatorConfig.getBackupCount());
        assertEquals(3, cardinalityEstimatorConfig.getAsyncBackupCount());
        assertEquals("com.hazelcast.spi.merge.HyperLogLogMergePolicy",
                cardinalityEstimatorConfig.getMergePolicyConfig().getPolicy());
        assertEquals("customSplitBrainProtectionRule", cardinalityEstimatorConfig.getSplitBrainProtectionName());
    }

    @Override
    @Test
    public void testCardinalityEstimatorConfigWithInvalidMergePolicy() {
        String xml = HAZELCAST_START_TAG
                + "    <cardinality-estimator name=\"foobar\">\n"
                + "        <backup-count>2</backup-count>\n"
                + "        <async-backup-count>3</async-backup-count>\n"
                + "        <split-brain-protection-ref>customSplitBrainProtectionRule</split-brain-protection-ref>"
                + "        <merge-policy>CustomMergePolicy</merge-policy>"
                + "    </cardinality-estimator>\n"
                + HAZELCAST_END_TAG;

        expected.expect(InvalidConfigurationException.class);
        buildConfig(xml);
    }

    @Override
    @Test
    public void testPNCounterConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <pn-counter name=\"pn-counter-1\">\n"
                + "        <replica-count>100</replica-count>\n"
                + "        <split-brain-protection-ref>splitBrainProtectionRuleWithThreeMembers</split-brain-protection-ref>\n"
                + "        <statistics-enabled>false</statistics-enabled>\n"
                + "    </pn-counter>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        PNCounterConfig pnCounterConfig = config.getPNCounterConfig("pn-counter-1");

        assertFalse(config.getPNCounterConfigs().isEmpty());
        assertEquals(100, pnCounterConfig.getReplicaCount());
        assertEquals("splitBrainProtectionRuleWithThreeMembers", pnCounterConfig.getSplitBrainProtectionName());
        assertFalse(pnCounterConfig.isStatisticsEnabled());
    }

    @Override
    @Test
    public void testMultiMapConfig() {
        String xml = HAZELCAST_START_TAG
                + "  <multimap name=\"myMultiMap\">"
                + "        <backup-count>2</backup-count>"
                + "        <async-backup-count>3</async-backup-count>"
                + "        <binary>false</binary>"
                + "        <value-collection-type>SET</value-collection-type>"
                + "        <split-brain-protection-ref>customSplitBrainProtectionRule</split-brain-protection-ref>"
                + "        <entry-listeners>\n"
                + "            <entry-listener include-value=\"true\" local=\"true\">com.hazelcast.examples.EntryListener</entry-listener>\n"
                + "          </entry-listeners>"
                + "        <merge-policy batch-size=\"23\">CustomMergePolicy</merge-policy>"
                + "        <user-code-namespace>ns1</user-code-namespace>"
                + "  </multimap>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
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
        String xml = HAZELCAST_START_TAG
                + "    <replicatedmap name=\"foobar\">\n"
                + "        <in-memory-format>BINARY</in-memory-format>\n"
                + "        <async-fillup>false</async-fillup>\n"
                + "        <statistics-enabled>false</statistics-enabled>\n"
                + "        <split-brain-protection-ref>customSplitBrainProtectionRule</split-brain-protection-ref>\n"
                + "        <merge-policy batch-size=\"2342\">CustomMergePolicy</merge-policy>\n"
                + "        <user-code-namespace>ns1</user-code-namespace>\n"
                + "    </replicatedmap>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
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
        String xml = HAZELCAST_START_TAG
                + "    <list name=\"foobar\">\n"
                + "        <split-brain-protection-ref>customSplitBrainProtectionRule</split-brain-protection-ref>"
                + "        <statistics-enabled>false</statistics-enabled>"
                + "        <max-size>42</max-size>"
                + "        <backup-count>2</backup-count>"
                + "        <async-backup-count>1</async-backup-count>"
                + "        <merge-policy batch-size=\"100\">SplitBrainMergePolicy</merge-policy>"
                + "        <item-listeners>\n"
                + "            <item-listener include-value=\"true\">com.hazelcast.examples.ItemListener</item-listener>\n"
                + "        </item-listeners>"
                + "    </list>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
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
        String xml = HAZELCAST_START_TAG
                + "    <set name=\"foobar\">\n"
                + "        <split-brain-protection-ref>customSplitBrainProtectionRule</split-brain-protection-ref>"
                + "        <backup-count>2</backup-count>"
                + "        <async-backup-count>1</async-backup-count>"
                + "        <max-size>42</max-size>"
                + "        <merge-policy batch-size=\"42\">SplitBrainMergePolicy</merge-policy>"
                + "        <item-listeners>\n"
                + "            <item-listener include-value=\"true\">com.hazelcast.examples.ItemListener</item-listener>\n"
                + "          </item-listeners>"
                + "    </set>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
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
        String xml = HAZELCAST_START_TAG
                + "    <map name=\"foobar\">\n"
                + "        <split-brain-protection-ref>customSplitBrainProtectionRule</split-brain-protection-ref>\n"
                + "        <in-memory-format>BINARY</in-memory-format>\n"
                + "        <statistics-enabled>true</statistics-enabled>\n"
                + "        <cache-deserialized-values>INDEX-ONLY</cache-deserialized-values>\n"
                + "        <backup-count>2</backup-count>\n"
                + "        <async-backup-count>1</async-backup-count>\n"
                + "        <time-to-live-seconds>42</time-to-live-seconds>\n"
                + "        <max-idle-seconds>42</max-idle-seconds>\n"
                + "        <eviction eviction-policy=\"RANDOM\" max-size-policy=\"PER_NODE\" size=\"42\"/>\n"
                + "        <read-backup-data>true</read-backup-data>\n"
                + "        <user-code-namespace>ns1</user-code-namespace>\n"
                + "        <merkle-tree enabled=\"true\">\n"
                + "            <depth>20</depth>\n"
                + "          </merkle-tree>"
                + "        <event-journal enabled=\"true\">\n"
                + "            <capacity>120</capacity>\n"
                + "            <time-to-live-seconds>20</time-to-live-seconds>\n"
                + "          </event-journal>"
                + "        <hot-restart enabled=\"false\">\n"
                + "            <fsync>false</fsync>\n"
                + "          </hot-restart>"
                + "        <data-persistence enabled=\"true\">\n"
                + "            <fsync>true</fsync>\n"
                + "          </data-persistence>"
                + "        <map-store enabled=\"true\" initial-mode=\"LAZY\">\n"
                + "            <class-name>com.hazelcast.examples.DummyStore</class-name>\n"
                + "            <write-delay-seconds>42</write-delay-seconds>\n"
                + "            <write-batch-size>42</write-batch-size>\n"
                + "            <write-coalescing>true</write-coalescing>\n"
                + "            <offload>false</offload>\n"
                + "            <properties>\n"
                + "                <property name=\"jdbc_url\">my.jdbc.com</property>\n"
                + "            </properties>\n"
                + "          </map-store>"
                + "        <near-cache>\n"
                + "            <time-to-live-seconds>42</time-to-live-seconds>\n"
                + "            <max-idle-seconds>42</max-idle-seconds>\n"
                + "            <invalidate-on-change>true</invalidate-on-change>\n"
                + "            <in-memory-format>BINARY</in-memory-format>\n"
                + "            <cache-local-entries>false</cache-local-entries>\n"
                + "            <eviction size=\"1000\" max-size-policy=\"ENTRY_COUNT\" eviction-policy=\"LFU\"/>\n"
                + "          </near-cache>"
                + "        <wan-replication-ref name=\"my-wan-cluster-batch\">\n"
                + "            <merge-policy-class-name>PassThroughMergePolicy</merge-policy-class-name>\n"
                + "            <filters>\n"
                + "                <filter-impl>com.example.SampleFilter</filter-impl>\n"
                + "            </filters>\n"
                + "            <republishing-enabled>false</republishing-enabled>\n"
                + "          </wan-replication-ref>"
                + "        <indexes>\n"
                + "          <index>\n"
                + "            <attributes>\n"
                + "              <attribute>age</attribute>\n"
                + "            </attributes>\n"
                + "          </index>\n"
                + "        </indexes>"
                + "        <attributes>\n"
                + "            <attribute extractor-class-name=\"com.bank.CurrencyExtractor\">currency</attribute>\n"
                + "           </attributes>"
                + "        <partition-lost-listeners>\n"
                + "            <partition-lost-listener>com.your-package.YourPartitionLostListener</partition-lost-listener>\n"
                + "          </partition-lost-listeners>"
                + "        <entry-listeners>\n"
                + "            <entry-listener include-value=\"false\" local=\"false\">com.your-package.MyEntryListener</entry-listener>\n"
                + "          </entry-listeners>"
                + "    </map>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
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
        assertSame(mapConfig.getIndexConfigs().get(0).getType(), IndexType.SORTED);
        assertEquals(1, mapConfig.getAttributeConfigs().size());
        assertEquals("com.bank.CurrencyExtractor", mapConfig.getAttributeConfigs().get(0).getExtractorClassName());
        assertEquals("currency", mapConfig.getAttributeConfigs().get(0).getName());
        assertEquals(1, mapConfig.getPartitionLostListenerConfigs().size());
        assertEquals("com.your-package.YourPartitionLostListener", mapConfig.getPartitionLostListenerConfigs().get(0).getClassName());
        assertEquals(1, mapConfig.getEntryListenerConfigs().size());
        assertFalse(mapConfig.getEntryListenerConfigs().get(0).isIncludeValue());
        assertFalse(mapConfig.getEntryListenerConfigs().get(0).isLocal());
        assertEquals("com.your-package.MyEntryListener", mapConfig.getEntryListenerConfigs().get(0).getClassName());
        assertTrue(mapConfig.getMerkleTreeConfig().isEnabled());
        assertEquals(20, mapConfig.getMerkleTreeConfig().getDepth());
        // because of conflict with dataPersistence, override will occur
        assertTrue(mapConfig.getHotRestartConfig().isEnabled());
        assertTrue(mapConfig.getHotRestartConfig().isFsync());
        assertTrue(mapConfig.getDataPersistenceConfig().isEnabled());
        assertTrue(mapConfig.getDataPersistenceConfig().isFsync());
        assertEquals("ns1", mapConfig.getUserCodeNamespace());

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
        assertFalse(mapStoreConfig.isOffload());
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
    @Test
    public void testMapCustomEvictionPolicy() {
        String comparatorClassName = "com.my.custom.eviction.policy.class";

        String xml = HAZELCAST_START_TAG
                + "   <map name=\"mappy\">\n"
                + "       <eviction comparator-class-name=\"" + comparatorClassName + "\" />"
                + "   </map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig("mappy");

        assertEquals(comparatorClassName, mapConfig.getEvictionConfig().getComparatorClassName());
    }

    @Override
    @Test
    public void testIndexesConfig() {
        String xml = HAZELCAST_START_TAG
                + "   <map name=\"people\">\n"
                + "       <indexes>\n"
                + "           <index type=\"HASH\">\n"
                + "               <attributes>\n"
                + "                   <attribute>name</attribute>\n"
                + "               </attributes>\n"
                + "           </index>\n"
                + "           <index>\n"
                + "               <attributes>\n"
                + "                   <attribute>age</attribute>\n"
                + "               </attributes>\n"
                + "           </index>\n"
                + "           <index type=\"SORTED\">\n"
                + "               <attributes>\n"
                + "                   <attribute>age</attribute>\n"
                + "               </attributes>\n"
                + "               <btree-index>"
                + "                   <page-size value=\"1337\" unit=\"BYTES\" />"
                + "                   <memory-tier>"
                + "                       <capacity value=\"1138\" unit=\"BYTES\" />"
                + "                   </memory-tier>"
                + "               </btree-index>"
                + "           </index>\n"
                + "       </indexes>"
                + "   </map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
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

    private static void assertIndexEqual(String expectedAttribute, boolean expectedOrdered, IndexConfig indexConfig) {
        assertEquals(expectedAttribute, indexConfig.getAttributes().get(0));
        assertEquals(expectedOrdered, indexConfig.getType() == IndexType.SORTED);
    }

    @Override
    @Test
    public void testAttributeConfig() {
        String xml = HAZELCAST_START_TAG
                + "   <map name=\"people\">\n"
                + "       <attributes>\n"
                + "           <attribute extractor-class-name=\"com.car.PowerExtractor\">power</attribute>\n"
                + "           <attribute extractor-class-name=\"com.car.WeightExtractor\">weight</attribute>\n"
                + "       </attributes>"
                + "   </map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig("people");

        assertFalse(mapConfig.getAttributeConfigs().isEmpty());
        assertAttributeEqual("power", "com.car.PowerExtractor", mapConfig.getAttributeConfigs().get(0));
        assertAttributeEqual("weight", "com.car.WeightExtractor", mapConfig.getAttributeConfigs().get(1));
    }

    @Override
    @Test
    public void testAttributeConfig_noName_emptyTag() {
        String xml = HAZELCAST_START_TAG
                + "   <map name=\"people\">\n"
                + "       <attributes>\n"
                + "           <attribute extractor-class-name=\"com.car.WeightExtractor\"></attribute>\n"
                + "       </attributes>"
                + "   </map>"
                + HAZELCAST_END_TAG;
        expected.expect(IllegalArgumentException.class);
        buildConfig(xml);
    }

    private static void assertAttributeEqual(String expectedName, String expectedExtractor, AttributeConfig attributeConfig) {
        assertEquals(expectedName, attributeConfig.getName());
        assertEquals(expectedExtractor, attributeConfig.getExtractorClassName());
    }

    @Override
    @Test
    public void testAttributeConfig_noName_singleTag() {
        String xml = HAZELCAST_START_TAG
                + "   <map name=\"people\">\n"
                + "       <attributes>\n"
                + "           <attribute extractor-class-name=\"com.car.WeightExtractor\"/>\n"
                + "       </attributes>"
                + "   </map>"
                + HAZELCAST_END_TAG;
        expected.expect(IllegalArgumentException.class);
        buildConfig(xml);
    }

    @Override
    @Test
    public void testAttributeConfig_noExtractor() {
        String xml = HAZELCAST_START_TAG
                + "   <map name=\"people\">\n"
                + "       <attributes>\n"
                + "           <attribute>weight</attribute>\n"
                + "       </attributes>"
                + "   </map>"
                + HAZELCAST_END_TAG;
        expected.expect(IllegalArgumentException.class);
        buildConfig(xml);
    }

    @Override
    @Test
    public void testAttributeConfig_emptyExtractor() {
        String xml = HAZELCAST_START_TAG
                + "   <map name=\"people\">\n"
                + "       <attributes>\n"
                + "           <attribute extractor-class-name=\"\">weight</attribute>\n"
                + "       </attributes>"
                + "   </map>"
                + HAZELCAST_END_TAG;
        expected.expect(IllegalArgumentException.class);
        buildConfig(xml);
    }

    @Override
    @Test
    public void testQueryCacheFullConfig() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"test\">"
                + "<query-caches>"
                + "<query-cache name=\"cache-name\">"
                + "<entry-listeners>"
                + "<entry-listener include-value=\"true\" local=\"false\">com.hazelcast.examples.EntryListener</entry-listener>"
                + "</entry-listeners>"
                + "<include-value>true</include-value>"
                + "<batch-size>1</batch-size>"
                + "<buffer-size>16</buffer-size>"
                + "<delay-seconds>0</delay-seconds>"
                + "<in-memory-format>BINARY</in-memory-format>"
                + "<coalesce>false</coalesce>"
                + "<populate>true</populate>"
                + "<serialize-keys>true</serialize-keys>"
                + "<indexes>"
                + "<index type=\"HASH\"><attributes><attribute>name</attribute></attributes></index>"
                + "</indexes>"
                + "<predicate type=\"class-name\"> "
                + "com.hazelcast.examples.SimplePredicate"
                + "</predicate>"
                + "<eviction eviction-policy=\"LRU\" max-size-policy=\"ENTRY_COUNT\" size=\"133\"/>"
                + "</query-cache>"
                + "</query-caches>"
                + "</map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
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

    @Override
    @Test
    public void testMapQueryCachePredicate() {
        String xml = HAZELCAST_START_TAG
                + "  <map name=\"test\">\n"
                + "    <query-caches>\n"
                + "      <query-cache name=\"cache-class-name\">\n"
                + "        <predicate type=\"class-name\">com.hazelcast.examples.SimplePredicate</predicate>\n"
                + "      </query-cache>\n"
                + "      <query-cache name=\"cache-sql\">\n"
                + "        <predicate type=\"sql\">%age=40</predicate>\n"
                + "      </query-cache>\n"
                + "    </query-caches>\n"
                + "  </map>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        QueryCacheConfig queryCacheClassNameConfig = config.getMapConfig("test").getQueryCacheConfigs().get(0);
        assertEquals("com.hazelcast.examples.SimplePredicate", queryCacheClassNameConfig.getPredicateConfig().getClassName());

        QueryCacheConfig queryCacheSqlConfig = config.getMapConfig("test").getQueryCacheConfigs().get(1);
        assertEquals("%age=40", queryCacheSqlConfig.getPredicateConfig().getSql());
    }

    @Override
    @Test
    public void testLiteMemberConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <lite-member enabled=\"true\"/>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);

        assertTrue(config.isLiteMember());
    }

    @Override
    @Test
    public void testNonLiteMemberConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <lite-member enabled=\"false\"/>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);

        assertFalse(config.isLiteMember());
    }

    @Override
    @Test
    public void testNonLiteMemberConfigWithoutEnabledField() {
        String xml = HAZELCAST_START_TAG
                + "    <lite-member/>\n"
                + HAZELCAST_END_TAG;

        expected.expect(InvalidConfigurationException.class);
        buildConfig(xml);
    }

    @Override
    @Test
    public void testInvalidLiteMemberConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <lite-member enabled=\"dummytext\"/>\n"
                + HAZELCAST_END_TAG;

        expected.expect(InvalidConfigurationException.class);
        buildConfig(xml);
    }

    @Override
    @Test
    public void testDuplicateLiteMemberConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <lite-member enabled=\"true\"/>\n"
                + "    <lite-member enabled=\"true\"/>\n"
                + HAZELCAST_END_TAG;

        expected.expect(InvalidConfigurationException.class);
        buildConfig(xml);
    }

    private void assertIndexesEqual(QueryCacheConfig queryCacheConfig) {
        for (IndexConfig indexConfig : queryCacheConfig.getIndexConfigs()) {
            assertEquals("name", indexConfig.getAttributes().get(0));
            assertNotSame(indexConfig.getType(), IndexType.SORTED);
        }
    }

    @Override
    @Test
    public void testMapNativeMaxSizePolicy() {
        String xmlFormat = HAZELCAST_START_TAG
                + "<map name=\"mymap\">"
                + "<in-memory-format>NATIVE</in-memory-format>"
                + "<eviction max-size-policy=\"{0}\" size=\"9991\"/>"
                + "</map>"
                + HAZELCAST_END_TAG;
        MessageFormat messageFormat = new MessageFormat(xmlFormat);

        MaxSizePolicy[] maxSizePolicies = MaxSizePolicy.values();
        for (MaxSizePolicy maxSizePolicy : maxSizePolicies) {
            if (maxSizePolicy == ENTRY_COUNT) {
                // imap does not support ENTRY_COUNT
                continue;
            }
            Object[] objects = {maxSizePolicy.toString()};
            String xml = messageFormat.format(objects);
            Config config = buildConfig(xml);
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
        String xml = HAZELCAST_START_TAG
                + "<instance-name>" + name + "</instance-name>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        assertEquals(name, config.getInstanceName());
    }

    @Override
    @Test
    public void testUserCodeDeployment() {
        String xml = HAZELCAST_START_TAG
                + "<user-code-deployment enabled=\"true\">"
                + "<class-cache-mode>OFF</class-cache-mode>"
                + "<provider-mode>LOCAL_CLASSES_ONLY</provider-mode>"
                + "<blacklist-prefixes>com.blacklisted,com.other.blacklisted</blacklist-prefixes>"
                + "<whitelist-prefixes>com.whitelisted,com.other.whitelisted</whitelist-prefixes>"
                + "<provider-filter>HAS_ATTRIBUTE:foo</provider-filter>"
                + "</user-code-deployment>"
                + HAZELCAST_END_TAG;
        Config config = new InMemoryXmlConfig(xml);
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
        String xml = HAZELCAST_START_TAG
                + "<user-code-deployment enabled=\"true\"/>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
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
        final String xml = HAZELCAST_START_TAG
                + "<crdt-replication>\n"
                + "        <max-concurrent-replication-targets>10</max-concurrent-replication-targets>\n"
                + "        <replication-period-millis>2000</replication-period-millis>\n"
                + "</crdt-replication>"
                + HAZELCAST_END_TAG;
        final Config config = new InMemoryXmlConfig(xml);
        final CRDTReplicationConfig replicationConfig = config.getCRDTReplicationConfig();
        assertEquals(10, replicationConfig.getMaxConcurrentReplicationTargets());
        assertEquals(2000, replicationConfig.getReplicationPeriodMillis());
    }

    @Override
    @Test
    public void testGlobalSerializer() {
        String name = randomName();
        String val = "true";
        String xml = HAZELCAST_START_TAG
                + "  <serialization>\n"
                + "      <serializers>\n"
                + "          <global-serializer override-java-serialization=\"" + val + "\">" + name + "</global-serializer>\n"
                + "      </serializers>\n"
                + "  </serialization>"
                + HAZELCAST_END_TAG;

        Config config = new InMemoryXmlConfig(xml);
        GlobalSerializerConfig globalSerializerConfig = config.getSerializationConfig().getGlobalSerializerConfig();
        assertEquals(name, globalSerializerConfig.getClassName());
        assertTrue(globalSerializerConfig.isOverrideJavaSerialization());
    }

    @Override
    @Test
    public void testJavaSerializationFilter() {
        String xml = HAZELCAST_START_TAG
                + "  <serialization>\n"
                + "      <java-serialization-filter defaults-disabled='true'>\n"
                + "          <whitelist>\n"
                + "              <class>java.lang.String</class>\n"
                + "              <class>example.Foo</class>\n"
                + "              <package>com.acme.app</package>\n"
                + "              <package>com.acme.app.subpkg</package>\n"
                + "              <prefix>java</prefix>\n"
                + "              <prefix>com.hazelcast.</prefix>\n"
                + "              <prefix>[</prefix>\n"
                + "          </whitelist>\n"
                + "          <blacklist>\n"
                + "              <class>com.acme.app.BeanComparator</class>\n"
                + "          </blacklist>\n"
                + "      </java-serialization-filter>\n"
                + "  </serialization>\n"
                + HAZELCAST_END_TAG;

        Config config = new InMemoryXmlConfig(xml);
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
        String xml = HAZELCAST_START_TAG
                + "  <sql>\n"
                + "      <java-reflection-filter defaults-disabled='true'>\n"
                + "          <whitelist>\n"
                + "              <class>java.lang.String</class>\n"
                + "              <class>example.Foo</class>\n"
                + "              <package>com.acme.app</package>\n"
                + "              <package>com.acme.app.subpkg</package>\n"
                + "              <prefix>java</prefix>\n"
                + "              <prefix>com.hazelcast.</prefix>\n"
                + "              <prefix>[</prefix>\n"
                + "          </whitelist>\n"
                + "          <blacklist>\n"
                + "              <class>com.acme.app.BeanComparator</class>\n"
                + "          </blacklist>\n"
                + "      </java-reflection-filter>\n"
                + "  </sql>\n"
                + HAZELCAST_END_TAG;

        Config config = new InMemoryXmlConfig(xml);
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
        String xml = HAZELCAST_START_TAG
                + "    <serialization>\n"
                + "        <compact-serialization>\n"
                + "            <serializers>\n"
                + "                <serializer>example.serialization.SerializableEmployeeDTOSerializer</serializer>\n"
                + "            </serializers>\n"
                + "        </compact-serialization>\n"
                + "    </serialization>\n"
                + HAZELCAST_END_TAG;

        SerializationConfig config = buildConfig(xml).getSerializationConfig();
        CompactTestUtil.verifyExplicitSerializerIsUsed(config);
    }

    @Override
    public void testCompactSerialization_classRegistration() {
        String xml = HAZELCAST_START_TAG
                + "    <serialization>\n"
                + "        <compact-serialization>\n"
                + "            <classes>\n"
                + "                <class>example.serialization.ExternalizableEmployeeDTO</class>\n"
                + "            </classes>\n"
                + "        </compact-serialization>\n"
                + "    </serialization>\n"
                + HAZELCAST_END_TAG;

        SerializationConfig config = buildConfig(xml).getSerializationConfig();
        CompactTestUtil.verifyReflectiveSerializerIsUsed(config);
    }

    @Override
    public void testCompactSerialization_serializerAndClassRegistration() {
        String xml = HAZELCAST_START_TAG
                + "    <serialization>\n"
                + "        <compact-serialization>\n"
                + "            <serializers>\n"
                + "                <serializer>example.serialization.SerializableEmployeeDTOSerializer</serializer>\n"
                + "            </serializers>\n"
                + "            <classes>\n"
                + "                <class>example.serialization.ExternalizableEmployeeDTO</class>\n"
                + "            </classes>\n"
                + "        </compact-serialization>\n"
                + "    </serialization>\n"
                + HAZELCAST_END_TAG;

        SerializationConfig config = buildConfig(xml).getSerializationConfig();
        CompactTestUtil.verifyExplicitSerializerIsUsed(config);
        CompactTestUtil.verifyReflectiveSerializerIsUsed(config);
    }

    @Override
    public void testCompactSerialization_duplicateSerializerRegistration() {
        String xml = HAZELCAST_START_TAG
                + "    <serialization>\n"
                + "        <compact-serialization>\n"
                + "            <serializers>\n"
                + "                <serializer>example.serialization.EmployeeDTOSerializer</serializer>\n"
                + "                <serializer>example.serialization.EmployeeDTOSerializer</serializer>\n"
                + "            </serializers>\n"
                + "        </compact-serialization>\n"
                + "    </serialization>\n"
                + HAZELCAST_END_TAG;

        SerializationConfig config = buildConfig(xml).getSerializationConfig();
        assertThatThrownBy(() -> CompactTestUtil.verifySerializationServiceBuilds(config))
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("Duplicate");
    }

    @Override
    public void testCompactSerialization_duplicateClassRegistration() {
        String xml = HAZELCAST_START_TAG
                + "    <serialization>\n"
                + "        <compact-serialization>\n"
                + "            <classes>\n"
                + "                <class>example.serialization.ExternalizableEmployeeDTO</class>\n"
                + "                <class>example.serialization.ExternalizableEmployeeDTO</class>\n"
                + "            </classes>\n"
                + "        </compact-serialization>\n"
                + "    </serialization>\n"
                + HAZELCAST_END_TAG;

        SerializationConfig config = buildConfig(xml).getSerializationConfig();
        assertThatThrownBy(() -> CompactTestUtil.verifySerializationServiceBuilds(config))
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("Duplicate");
    }

    @Override
    public void testCompactSerialization_registrationsWithDuplicateClasses() {
        String xml = HAZELCAST_START_TAG
                + "    <serialization>\n"
                + "        <compact-serialization>\n"
                + "            <serializers>\n"
                + "                <serializer>example.serialization.EmployeeDTOSerializer</serializer>\n"
                + "                <serializer>example.serialization.SameClassEmployeeDTOSerializer</serializer>\n"
                + "            </serializers>\n"
                + "        </compact-serialization>\n"
                + "    </serialization>\n"
                + HAZELCAST_END_TAG;

        SerializationConfig config = buildConfig(xml).getSerializationConfig();
        assertThatThrownBy(() -> CompactTestUtil.verifySerializationServiceBuilds(config))
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("Duplicate")
                .hasMessageContaining("class");
    }

    @Override
    public void testCompactSerialization_registrationsWithDuplicateTypeNames() {
        String xml = HAZELCAST_START_TAG
                + "    <serialization>\n"
                + "        <compact-serialization>\n"
                + "            <serializers>\n"
                + "                <serializer>example.serialization.EmployeeDTOSerializer</serializer>\n"
                + "                <serializer>example.serialization.SameTypeNameEmployeeDTOSerializer</serializer>\n"
                + "            </serializers>\n"
                + "        </compact-serialization>\n"
                + "    </serialization>\n"
                + HAZELCAST_END_TAG;

        SerializationConfig config = buildConfig(xml).getSerializationConfig();
        assertThatThrownBy(() -> CompactTestUtil.verifySerializationServiceBuilds(config))
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("Duplicate")
                .hasMessageContaining("type name");
    }

    @Override
    public void testCompactSerialization_withInvalidSerializer() {
        String xml = HAZELCAST_START_TAG
                + "    <serialization>\n"
                + "        <compact-serialization>\n"
                + "            <serializers>\n"
                + "                <serializer>does.not.exist.FooSerializer</serializer>\n"
                + "            </serializers>\n"
                + "        </compact-serialization>\n"
                + "    </serialization>\n"
                + HAZELCAST_END_TAG;

        SerializationConfig config = buildConfig(xml).getSerializationConfig();
        assertThatThrownBy(() -> CompactTestUtil.verifySerializationServiceBuilds(config))
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("Cannot create an instance");
    }

    @Override
    public void testCompactSerialization_withInvalidCompactSerializableClass() {
        String xml = HAZELCAST_START_TAG
                + "    <serialization>\n"
                + "        <compact-serialization>\n"
                + "            <classes>\n"
                + "                <class>does.not.exist.Foo</class>\n"
                + "            </classes>\n"
                + "        </compact-serialization>\n"
                + "    </serialization>\n"
                + HAZELCAST_END_TAG;

        SerializationConfig config = buildConfig(xml).getSerializationConfig();
        assertThatThrownBy(() -> CompactTestUtil.verifySerializationServiceBuilds(config))
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("Cannot load");
    }

    @Override
    @Test
    public void testAllowOverrideDefaultSerializers() {
        String xml = HAZELCAST_START_TAG
                + "  <serialization>\n"
                + "      <allow-override-default-serializers>true</allow-override-default-serializers>\n"
                + "  </serialization>\n"
                + HAZELCAST_END_TAG;

        final Config config = new InMemoryXmlConfig(xml);
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
        String xml = HAZELCAST_START_TAG
                + "<hot-restart-persistence enabled=\"true\">"
                + "    <base-dir>" + dir + "</base-dir>"
                + "    <backup-dir>" + backupDir + "</backup-dir>"
                + "    <parallelism>" + parallelism + "</parallelism>"
                + "    <validation-timeout-seconds>" + validationTimeout + "</validation-timeout-seconds>"
                + "    <data-load-timeout-seconds>" + dataLoadTimeout + "</data-load-timeout-seconds>"
                + "    <cluster-data-recovery-policy>" + policy + "</cluster-data-recovery-policy>"
                + "    <auto-remove-stale-data>false</auto-remove-stale-data>"
                + "</hot-restart-persistence>\n"
                + HAZELCAST_END_TAG;

        Config config = new InMemoryXmlConfig(xml);
        HotRestartPersistenceConfig hotRestartPersistenceConfig = config.getHotRestartPersistenceConfig();

        assertTrue(hotRestartPersistenceConfig.isEnabled());
        assertEquals(new File(dir).getAbsolutePath(), hotRestartPersistenceConfig.getBaseDir().getAbsolutePath());
        assertEquals(new File(backupDir).getAbsolutePath(), hotRestartPersistenceConfig.getBackupDir().getAbsolutePath());
        assertEquals(parallelism, hotRestartPersistenceConfig.getParallelism());
        assertEquals(validationTimeout, hotRestartPersistenceConfig.getValidationTimeoutSeconds());
        assertEquals(dataLoadTimeout, hotRestartPersistenceConfig.getDataLoadTimeoutSeconds());
        assertEquals(policy, hotRestartPersistenceConfig.getClusterDataRecoveryPolicy());
        assertFalse(hotRestartPersistenceConfig.isAutoRemoveStaleData());
    }

    @Override
    @Test
    public void testPersistence() {
        String dir = "/mnt/persistence-root/";
        String backupDir = "/mnt/persistence-backup/";
        int parallelism = 3;
        int validationTimeout = 13131;
        int dataLoadTimeout = 45454;
        PersistenceClusterDataRecoveryPolicy policy = PersistenceClusterDataRecoveryPolicy.PARTIAL_RECOVERY_MOST_RECENT;
        String xml = HAZELCAST_START_TAG
                + "<persistence enabled=\"true\">"
                + "    <base-dir>" + dir + "</base-dir>"
                + "    <backup-dir>" + backupDir + "</backup-dir>"
                + "    <parallelism>" + parallelism + "</parallelism>"
                + "    <validation-timeout-seconds>" + validationTimeout + "</validation-timeout-seconds>"
                + "    <data-load-timeout-seconds>" + dataLoadTimeout + "</data-load-timeout-seconds>"
                + "    <cluster-data-recovery-policy>" + policy + "</cluster-data-recovery-policy>"
                + "    <auto-remove-stale-data>false</auto-remove-stale-data>"
                + "    <rebalance-delay-seconds>240</rebalance-delay-seconds>"
                + "</persistence>\n"
                + HAZELCAST_END_TAG;

        Config config = new InMemoryXmlConfig(xml);
        PersistenceConfig persistenceConfig = config.getPersistenceConfig();

        assertTrue(persistenceConfig.isEnabled());
        assertEquals(new File(dir).getAbsolutePath(), persistenceConfig.getBaseDir().getAbsolutePath());
        assertEquals(new File(backupDir).getAbsolutePath(), persistenceConfig.getBackupDir().getAbsolutePath());
        assertEquals(parallelism, persistenceConfig.getParallelism());
        assertEquals(validationTimeout, persistenceConfig.getValidationTimeoutSeconds());
        assertEquals(dataLoadTimeout, persistenceConfig.getDataLoadTimeoutSeconds());
        assertEquals(policy, persistenceConfig.getClusterDataRecoveryPolicy());
        assertFalse(persistenceConfig.isAutoRemoveStaleData());
        assertEquals(240, persistenceConfig.getRebalanceDelaySeconds());
    }

    @Override
    @Test
    public void testDynamicConfig() {
        boolean persistenceEnabled = true;
        String backupDir = "/mnt/dynamic-configuration/backup-dir";
        int backupCount = 7;

        String xml = HAZELCAST_START_TAG
                + "<dynamic-configuration>"
                + "    <persistence-enabled>" + persistenceEnabled + "</persistence-enabled>"
                + "    <backup-dir>" + backupDir + "</backup-dir>"
                + "    <backup-count>" + backupCount + "</backup-count>"
                + "</dynamic-configuration>\n"
                + HAZELCAST_END_TAG;

        Config config = new InMemoryXmlConfig(xml);
        DynamicConfigurationConfig dynamicConfigurationConfig = config.getDynamicConfigurationConfig();

        assertEquals(persistenceEnabled, dynamicConfigurationConfig.isPersistenceEnabled());
        assertEquals(new File(backupDir).getAbsolutePath(), dynamicConfigurationConfig.getBackupDir().getAbsolutePath());
        assertEquals(backupCount, dynamicConfigurationConfig.getBackupCount());

        xml = HAZELCAST_START_TAG
                + "<dynamic-configuration>"
                + "    <persistence-enabled>" + persistenceEnabled + "</persistence-enabled>"
                + "</dynamic-configuration>\n"
                + HAZELCAST_END_TAG;

        config = new InMemoryXmlConfig(xml);
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

        String xml = HAZELCAST_START_TAG + HAZELCAST_END_TAG;
        Config config = new InMemoryXmlConfig(xml);

        // default device
        LocalDeviceConfig localDeviceConfig = config.getDeviceConfig(DEFAULT_DEVICE_NAME);
        System.out.println(localDeviceConfig);
        assertEquals(DEFAULT_DEVICE_NAME, localDeviceConfig.getName());
        assertEquals(new File(DEFAULT_DEVICE_BASE_DIR).getAbsoluteFile(), localDeviceConfig.getBaseDir());
        assertEquals(DEFAULT_BLOCK_SIZE_IN_BYTES, localDeviceConfig.getBlockSize());
        assertEquals(DEFAULT_READ_IO_THREAD_COUNT, localDeviceConfig.getReadIOThreadCount());
        assertEquals(DEFAULT_WRITE_IO_THREAD_COUNT, localDeviceConfig.getWriteIOThreadCount());
        assertEquals(LocalDeviceConfig.DEFAULT_CAPACITY, localDeviceConfig.getCapacity());


        xml = HAZELCAST_START_TAG
                + "<local-device name=\"my-device\">"
                + "    <base-dir>" + baseDir + "</base-dir>"
                + "    <block-size>" + blockSize + "</block-size>"
                + "    <capacity value=\"100\" unit=\"GIGABYTES\"/>"
                + "    <read-io-thread-count>" + readIOThreadCount + "</read-io-thread-count>"
                + "    <write-io-thread-count>" + writeIOThreadCount + "</write-io-thread-count>"
                + "</local-device>\n"
                + HAZELCAST_END_TAG;

        config = new InMemoryXmlConfig(xml);
        localDeviceConfig = config.getDeviceConfig("my-device");

        assertNotNull(localDeviceConfig);
        assertEquals("my-device", localDeviceConfig.getName());
        assertEquals(new File(baseDir).getAbsolutePath(), localDeviceConfig.getBaseDir().getAbsolutePath());
        assertEquals(blockSize, localDeviceConfig.getBlockSize());
        assertEquals(new Capacity(100, MemoryUnit.GIGABYTES), localDeviceConfig.getCapacity());
        assertEquals(readIOThreadCount, localDeviceConfig.getReadIOThreadCount());
        assertEquals(writeIOThreadCount, localDeviceConfig.getWriteIOThreadCount());

        int device0Multiplier = 2;
        int device1Multiplier = 4;
        xml = HAZELCAST_START_TAG
                + "<local-device name=\"device0\">"
                + "    <capacity value=\"1234567890\" unit=\"MEGABYTES\"/>"
                + "    <block-size>" + (blockSize * device0Multiplier) + "</block-size>"
                + "    <read-io-thread-count>" + (readIOThreadCount * device0Multiplier) + "</read-io-thread-count>"
                + "    <write-io-thread-count>" + (writeIOThreadCount * device0Multiplier) + "</write-io-thread-count>"
                + "</local-device>\n"
                + "<local-device name=\"device1\">"
                + "    <block-size>" + (blockSize * device1Multiplier) + "</block-size>"
                + "    <read-io-thread-count>" + (readIOThreadCount * device1Multiplier) + "</read-io-thread-count>"
                + "    <write-io-thread-count>" + (writeIOThreadCount * device1Multiplier) + "</write-io-thread-count>"
                + "</local-device>\n"
                + HAZELCAST_END_TAG;

        config = new InMemoryXmlConfig(xml);
        // default device removed
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
        xml = HAZELCAST_START_TAG
                + "<local-device name=\"" + DEFAULT_DEVICE_NAME + "\">"
                + "    <base-dir>" + newBaseDir + "</base-dir>"
                + "    <block-size>" + (DEFAULT_BLOCK_SIZE_IN_BYTES * 2) + "</block-size>"
                + "    <read-io-thread-count>" + (DEFAULT_READ_IO_THREAD_COUNT * 2) + "</read-io-thread-count>"
                + "</local-device>\n"
                + HAZELCAST_END_TAG;

        config = new InMemoryXmlConfig(xml);
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
        String xml = HAZELCAST_START_TAG
                + "<map name=\"map0\">"
                + "    <tiered-store enabled=\"true\">"
                + "        <memory-tier>"
                + "            <capacity value=\"1024\" unit=\"MEGABYTES\"/>"
                + "        </memory-tier>"
                + "        <disk-tier enabled=\"true\" device-name=\"local-device\"/>"
                + "    </tiered-store>"
                + "</map>\n"
                + "<map name=\"map1\">"
                + "    <tiered-store enabled=\"true\">"
                + "        <disk-tier enabled=\"true\"/>"
                + "    </tiered-store>"
                + "</map>\n"
                + "<map name=\"map2\">"
                + "    <tiered-store enabled=\"true\">"
                + "        <memory-tier>"
                + "            <capacity value=\"1\" unit=\"GIGABYTES\"/>"
                + "        </memory-tier>"
                + "    </tiered-store>"
                + "</map>\n"
                + "<map name=\"map3\">"
                + "    <tiered-store enabled=\"true\"/>"
                + "</map>\n"
                + HAZELCAST_END_TAG;

        Config config = new InMemoryXmlConfig(xml);
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

        xml = HAZELCAST_START_TAG
                + "<map name=\"some-map\">"
                + "    <tiered-store enabled=\"true\"/>"
                + "</map>\n"
                + HAZELCAST_END_TAG;

        config = new InMemoryXmlConfig(xml);
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
        String xml = HAZELCAST_START_TAG
                + "<hot-restart-persistence enabled=\"true\">"
                + "    <encryption-at-rest enabled=\"true\">\n"
                + "        <algorithm>AES</algorithm>\n"
                + "        <salt>some-salt</salt>\n"
                + "        <key-size>" + keySize + "</key-size>\n"
                + "        <secure-store>\n"
                + "            <keystore>\n"
                + "                <path>" + keyStorePath + "</path>\n"
                + "                <type>" + keyStoreType + "</type>\n"
                + "                <password>" + keyStorePassword + "</password>\n"
                + "                <polling-interval>" + pollingInterval + "</polling-interval>\n"
                + "                <current-key-alias>" + currentKeyAlias + "</current-key-alias>\n"
                + "            </keystore>\n"
                + "        </secure-store>\n"
                + "    </encryption-at-rest>"
                + "</hot-restart-persistence>\n"
                + HAZELCAST_END_TAG;

        Config config = new InMemoryXmlConfig(xml);
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
        String xml = HAZELCAST_START_TAG
                + "<persistence enabled=\"true\">"
                + "    <encryption-at-rest enabled=\"true\">\n"
                + "        <algorithm>AES</algorithm>\n"
                + "        <salt>some-salt</salt>\n"
                + "        <key-size>" + keySize + "</key-size>\n"
                + "        <secure-store>\n"
                + "            <keystore>\n"
                + "                <path>" + keyStorePath + "</path>\n"
                + "                <type>" + keyStoreType + "</type>\n"
                + "                <password>" + keyStorePassword + "</password>\n"
                + "                <polling-interval>" + pollingInterval + "</polling-interval>\n"
                + "                <current-key-alias>" + currentKeyAlias + "</current-key-alias>\n"
                + "            </keystore>\n"
                + "        </secure-store>\n"
                + "    </encryption-at-rest>"
                + "</persistence>\n"
                + HAZELCAST_END_TAG;

        Config config = new InMemoryXmlConfig(xml);
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
        String xml = HAZELCAST_START_TAG
                + "<hot-restart-persistence enabled=\"true\">"
                + "    <encryption-at-rest enabled=\"true\">\n"
                + "        <algorithm>AES</algorithm>\n"
                + "        <salt>some-salt</salt>\n"
                + "        <key-size>" + keySize + "</key-size>\n"
                + "        <secure-store>\n"
                + "            <vault>\n"
                + "                <address>" + address + "</address>\n"
                + "                <secret-path>" + secretPath + "</secret-path>\n"
                + "                <token>" + token + "</token>\n"
                + "                <polling-interval>" + pollingInterval + "</polling-interval>\n"
                + "                <ssl enabled=\"true\">\n"
                + "                  <factory-class-name>\n"
                + "                      com.hazelcast.nio.ssl.BasicSSLContextFactory\n"
                + "                  </factory-class-name>\n"
                + "                  <properties>\n"
                + "                    <property name=\"protocol\">TLS</property>\n"
                + "                  </properties>\n"
                + "                </ssl>\n"
                + "            </vault>\n"
                + "        </secure-store>\n"
                + "    </encryption-at-rest>"
                + "</hot-restart-persistence>\n"
                + HAZELCAST_END_TAG;

        Config config = new InMemoryXmlConfig(xml);
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
        String xml = HAZELCAST_START_TAG
                + "<persistence enabled=\"true\">"
                + "    <encryption-at-rest enabled=\"true\">\n"
                + "        <algorithm>AES</algorithm>\n"
                + "        <salt>some-salt</salt>\n"
                + "        <key-size>" + keySize + "</key-size>\n"
                + "        <secure-store>\n"
                + "            <vault>\n"
                + "                <address>" + address + "</address>\n"
                + "                <secret-path>" + secretPath + "</secret-path>\n"
                + "                <token>" + token + "</token>\n"
                + "                <polling-interval>" + pollingInterval + "</polling-interval>\n"
                + "                <ssl enabled=\"true\">\n"
                + "                  <factory-class-name>\n"
                + "                      com.hazelcast.nio.ssl.BasicSSLContextFactory\n"
                + "                  </factory-class-name>\n"
                + "                  <properties>\n"
                + "                    <property name=\"protocol\">TLS</property>\n"
                + "                  </properties>\n"
                + "                </ssl>\n"
                + "            </vault>\n"
                + "        </secure-store>\n"
                + "    </encryption-at-rest>"
                + "</persistence>\n"
                + HAZELCAST_END_TAG;

        Config config = new InMemoryXmlConfig(xml);
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
    public void testOnJoinPermissionOperation() {
        for (OnJoinPermissionOperationName onJoinOp : OnJoinPermissionOperationName.values()) {
            String xml = HAZELCAST_START_TAG + SECURITY_START_TAG
                    + "  <client-permissions on-join-operation='" + onJoinOp.name() + "'/>"
                    + SECURITY_END_TAG + HAZELCAST_END_TAG;
            Config config = buildConfig(xml);
            assertSame(onJoinOp, config.getSecurityConfig().getOnJoinPermissionOperation());
        }
    }

    @Override
    @Test
    public void testCachePermission() {
        String xml = HAZELCAST_START_TAG + SECURITY_START_TAG
                + "  <client-permissions>"
                + "    <cache-permission name=\"/hz/cachemanager1/cache1\" principal=\"dev\">"
                + ACTIONS_FRAGMENT
                + "    </cache-permission>\n"
                + "  </client-permissions>"
                + SECURITY_END_TAG + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        assertSame("Receive is expected to be default on-join-operation", OnJoinPermissionOperationName.RECEIVE,
                config.getSecurityConfig().getOnJoinPermissionOperation());
        PermissionConfig expected = new PermissionConfig(CACHE, "/hz/cachemanager1/cache1", "dev");
        expected.addAction("create").addAction("destroy").addAction("add").addAction("remove");
        assertPermissionConfig(expected, config);
        assertFalse(config.getSecurityConfig().isPermissionPriorityGrant());
    }

    @Override
    @Test
    public void testConfigPermission() {
        String xml = HAZELCAST_START_TAG + SECURITY_START_TAG
                + "  <client-permissions priority-grant='true'>"
                + "    <config-permission principal=\"dev\" deny='true'>"
                + "       <endpoints><endpoint>127.0.0.1</endpoint></endpoints>"
                + "    </config-permission>\n"
                + "  </client-permissions>"
                + SECURITY_END_TAG + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        PermissionConfig expected = new PermissionConfig(CONFIG, null, "dev").setDeny(true);
        expected.getEndpoints().add("127.0.0.1");
        assertPermissionConfig(expected, config);
        assertTrue(config.getSecurityConfig().isPermissionPriorityGrant());
    }

    @Override
    @Test
    public void testAllPermissionsCovered() throws IOException {
        URL xmlResource = XMLConfigBuilderTest.class.getClassLoader().getResource("hazelcast-fullconfig.xml");
        Config config = new XmlConfigBuilder(xmlResource).build();
        Set<PermissionType> allPermissionTypes = Set.of(PermissionType.values());
        Set<PermissionType> foundPermissionTypes = config.getSecurityConfig().getClientPermissionConfigs().stream()
                .map(PermissionConfig::getType).collect(Collectors.toSet());
        Collection<PermissionType> difference = Sets.difference(allPermissionTypes, foundPermissionTypes);

        assertTrue(String.format("All permission types should be listed in %s. Not found ones: %s", xmlResource, difference),
                difference.isEmpty());
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testCacheConfig_withNativeInMemoryFormat_failsFastInOSS() {
        String xml = HAZELCAST_START_TAG
                + "    <cache name=\"cache\">"
                + "        <eviction size=\"10000000\" max-size-policy=\"ENTRY_COUNT\" eviction-policy=\"LFU\"/>"
                + "        <in-memory-format>NATIVE</in-memory-format>\n"
                + "    </cache>"
                + HAZELCAST_END_TAG;

        buildConfig(xml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testMemberAddressProvider_classNameIsMandatory() {
        String xml = HAZELCAST_START_TAG
                + "<network> "
                + "  <member-address-provider enabled=\"true\">"
                + "  </member-address-provider>"
                + "</network> "
                + HAZELCAST_END_TAG;

        buildConfig(xml);
    }

    @Override
    @Test
    public void testMemberAddressProviderEnabled() {
        String xml = HAZELCAST_START_TAG
                + "<network> "
                + "  <member-address-provider enabled=\"true\">"
                + "    <class-name>foo.bar.Clazz</class-name>"
                + "  </member-address-provider>"
                + "</network> "
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);

        MemberAddressProviderConfig memberAddressProviderConfig = config.getNetworkConfig().getMemberAddressProviderConfig();

        assertTrue(memberAddressProviderConfig.isEnabled());
        assertEquals("foo.bar.Clazz", memberAddressProviderConfig.getClassName());
    }

    @Override
    @Test
    public void testMemberAddressProviderEnabled_withProperties() {
        String xml = HAZELCAST_START_TAG
                + "<network> "
                + "  <member-address-provider enabled=\"true\">"
                + "    <class-name>foo.bar.Clazz</class-name>"
                + "    <properties>"
                + "       <property name=\"propName1\">propValue1</property>"
                + "    </properties>"
                + "  </member-address-provider>"
                + "</network> "
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MemberAddressProviderConfig memberAddressProviderConfig = config.getNetworkConfig().getMemberAddressProviderConfig();

        Properties properties = memberAddressProviderConfig.getProperties();
        assertEquals(1, properties.size());
        assertEquals("propValue1", properties.get("propName1"));
    }

    @Override
    @Test
    public void testFailureDetector_withProperties() {
        String xml = HAZELCAST_START_TAG
                + "<network>"
                + "  <failure-detector>\n"
                + "            <icmp enabled=\"true\">\n"
                + "                <timeout-milliseconds>42</timeout-milliseconds>\n"
                + "                <fail-fast-on-startup>true</fail-fast-on-startup>\n"
                + "                <interval-milliseconds>4200</interval-milliseconds>\n"
                + "                <max-attempts>42</max-attempts>\n"
                + "                <parallel-mode>true</parallel-mode>\n"
                + "                <ttl>255</ttl>\n"
                + "            </icmp>\n"
                + "  </failure-detector>"
                + "</network>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
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
    public void testCPSubsystemConfig() {
        String xml = HAZELCAST_START_TAG
                + "<cp-subsystem>\n"
                + "  <cp-member-count>10</cp-member-count>\n"
                + "  <group-size>5</group-size>\n"
                + "  <session-time-to-live-seconds>15</session-time-to-live-seconds>\n"
                + "  <session-heartbeat-interval-seconds>3</session-heartbeat-interval-seconds>\n"
                + "  <missing-cp-member-auto-removal-seconds>120</missing-cp-member-auto-removal-seconds>\n"
                + "  <fail-on-indeterminate-operation-state>true</fail-on-indeterminate-operation-state>\n"
                + "  <persistence-enabled>true</persistence-enabled>\n"
                + "  <base-dir>/mnt/cp-data</base-dir>\n"
                + "  <data-load-timeout-seconds>30</data-load-timeout-seconds>\n"
                + "  <cp-member-priority>-1</cp-member-priority>\n"
                + "  <raft-algorithm>\n"
                + "    <leader-election-timeout-in-millis>500</leader-election-timeout-in-millis>\n"
                + "    <leader-heartbeat-period-in-millis>100</leader-heartbeat-period-in-millis>\n"
                + "    <max-missed-leader-heartbeat-count>3</max-missed-leader-heartbeat-count>\n"
                + "    <append-request-max-entry-count>25</append-request-max-entry-count>\n"
                + "    <commit-index-advance-count-to-snapshot>250</commit-index-advance-count-to-snapshot>\n"
                + "    <uncommitted-entry-count-to-reject-new-appends>75</uncommitted-entry-count-to-reject-new-appends>\n"
                + "    <append-request-backoff-timeout-in-millis>50</append-request-backoff-timeout-in-millis>\n"
                + "  </raft-algorithm>\n"
                + "  <semaphores>\n"
                + "    <semaphore>\n"
                + "      <name>sem1</name>\n"
                + "      <jdk-compatible>true</jdk-compatible>\n"
                + "      <initial-permits>1</initial-permits>\n"
                + "    </semaphore>\n"
                + "    <semaphore>\n"
                + "      <name>sem2</name>\n"
                + "      <jdk-compatible>false</jdk-compatible>\n"
                + "      <initial-permits>2</initial-permits>\n"
                + "    </semaphore>\n"
                + "  </semaphores>\n"
                + "  <locks>\n"
                + "    <fenced-lock>\n"
                + "      <name>lock1</name>\n"
                + "      <lock-acquire-limit>1</lock-acquire-limit>\n"
                + "    </fenced-lock>\n"
                + "    <fenced-lock>\n"
                + "      <name>lock2</name>\n"
                + "      <lock-acquire-limit>2</lock-acquire-limit>\n"
                + "    </fenced-lock>\n"
                + "  </locks>\n"
                + "  <maps>\n"
                + "    <map>\n"
                + "      <name>map1</name>\n"
                + "      <max-size-mb>1</max-size-mb>\n"
                + "    </map>\n"
                + "    <map>\n"
                + "      <name>map2</name>\n"
                + "      <max-size-mb>2</max-size-mb>\n"
                + "    </map>\n"
                + "  </maps>\n"
                + "  <map-limit>20</map-limit>\n"
                + "</cp-subsystem>"
                + HAZELCAST_END_TAG;
        Config config = new InMemoryXmlConfig(xml);
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
        assertEquals(20, cpSubsystemConfig.getCPMapLimit());
    }

    @Override
    public void testHandleMemberAttributes() {
        String xml = HAZELCAST_START_TAG
                + "<member-attributes>\n"
                + "     <attribute name=\"IDENTIFIER\">ID</attribute>\n"
                + "</member-attributes>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MemberAttributeConfig memberAttributeConfig = config.getMemberAttributeConfig();
        assertNotNull(memberAttributeConfig);
        assertEquals("ID", memberAttributeConfig.getAttribute("IDENTIFIER"));
    }

    @Override
    @Test
    public void testMemcacheProtocolEnabled() {
        String xml = HAZELCAST_START_TAG
                + "<network><memcache-protocol enabled='true'/></network>\n"
                + HAZELCAST_END_TAG;
        Config config = buildConfig(xml);
        MemcacheProtocolConfig memcacheProtocolConfig = config.getNetworkConfig().getMemcacheProtocolConfig();
        assertNotNull(memcacheProtocolConfig);
        assertTrue(memcacheProtocolConfig.isEnabled());
    }

    @Override
    @Test
    public void testRestApiDefaults() {
        String xml = HAZELCAST_START_TAG
                + "<network><rest-api enabled='false'/></network>\n"
                + HAZELCAST_END_TAG;
        Config config = buildConfig(xml);
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
        String xml = HAZELCAST_START_TAG
                + "<network>\n"
                + "<rest-api enabled='true'>\n"
                + "  <endpoint-group name='HEALTH_CHECK' enabled='true'/>\n"
                + "  <endpoint-group name='DATA' enabled='true'/>\n"
                + "  <endpoint-group name='CLUSTER_READ' enabled='false'/>\n"
                + "</rest-api>\n"
                + "</network>\n"
                + HAZELCAST_END_TAG;
        Config config = buildConfig(xml);
        RestApiConfig restApiConfig = config.getNetworkConfig().getRestApiConfig();
        assertTrue(restApiConfig.isEnabled());
        assertTrue(restApiConfig.isGroupEnabled(HEALTH_CHECK));
        assertFalse(restApiConfig.isGroupEnabled(CLUSTER_READ));
        assertEquals(RestEndpointGroup.CLUSTER_WRITE.isEnabledByDefault(),
                restApiConfig.isGroupEnabled(RestEndpointGroup.CLUSTER_WRITE));
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testUnknownRestApiEndpointGroup() {
        String xml = HAZELCAST_START_TAG
                + "<rest-api enabled='true'>\n"
                + "  <endpoint-group name='TEST' enabled='true'/>\n"
                + "</rest-api>\n"
                + HAZELCAST_END_TAG;
        buildConfig(xml);
    }

    @Override
    @Test
    public void testDefaultAdvancedNetworkConfig() {
        String xml = HAZELCAST_START_TAG
                + "<advanced-network>"
                + "</advanced-network>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
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
        // -1 fails XSD validation
        String invalid1 = getAdvancedNetworkConfigWithSocketOption("keep-idle-seconds", -1);
        Assert.assertThrows(InvalidConfigurationException.class, () -> buildConfig(invalid1));
        // 0 fails XSD validation
        String invalid2 = getAdvancedNetworkConfigWithSocketOption("keep-idle-seconds", 0);
        Assert.assertThrows(InvalidConfigurationException.class, () -> buildConfig(invalid2));
        // 32768 fails range check in EndpointConfig
        String invalid3 = getAdvancedNetworkConfigWithSocketOption("keep-idle-seconds", 32768);
        Assert.assertThrows(IllegalArgumentException.class, () -> buildConfig(invalid3));
    }

    @Override
    @Test
    public void testAdvancedNetworkConfig_whenInvalidSocketKeepIntervalSeconds() {
        // -1 fails XSD validation
        String invalid1 = getAdvancedNetworkConfigWithSocketOption("keep-interval-seconds", -1);
        Assert.assertThrows(InvalidConfigurationException.class, () -> buildConfig(invalid1));
        // 0 fails XSD validation
        String invalid2 = getAdvancedNetworkConfigWithSocketOption("keep-interval-seconds", 0);
        Assert.assertThrows(InvalidConfigurationException.class, () -> buildConfig(invalid2));
        // 32768 fails range check in EndpointConfig
        String invalid3 = getAdvancedNetworkConfigWithSocketOption("keep-interval-seconds", 32768);
        Assert.assertThrows(IllegalArgumentException.class, () -> buildConfig(invalid3));
    }

    @Override
    @Test
    public void testAdvancedNetworkConfig_whenInvalidSocketKeepCount() {
        // -1 fails XSD validation
        String invalid1 = getAdvancedNetworkConfigWithSocketOption("keep-count", -1);
        Assert.assertThrows(InvalidConfigurationException.class, () -> buildConfig(invalid1));
        // 0 fails XSD validation
        String invalid2 = getAdvancedNetworkConfigWithSocketOption("keep-count", 0);
        Assert.assertThrows(InvalidConfigurationException.class, () -> buildConfig(invalid2));
        // 128 fails range check in EndpointConfig
        String invalid3 = getAdvancedNetworkConfigWithSocketOption("keep-count", 128);
        Assert.assertThrows(IllegalArgumentException.class, () -> buildConfig(invalid3));
    }

    @Override
    @Test
    public void testAmbiguousNetworkConfig_throwsException() {
        String xml = HAZELCAST_START_TAG
                + "  <advanced-network enabled=\"true\">\n"
                + "  </advanced-network>\n"
                + "  <network>\n"
                + "    <port>9999</port>\n"
                + "  </network>"
                + HAZELCAST_END_TAG;

        expected.expect(InvalidConfigurationException.class);
        buildConfig(xml);
    }

    @Override
    @Test
    public void testNetworkConfigUnambiguous_whenAdvancedNetworkDisabled() {
        String xml = HAZELCAST_START_TAG
                + "  <advanced-network>\n"
                + "  </advanced-network>\n"
                + "  <network>\n"
                + "    <port>9999</port>\n"
                + "  </network>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        assertFalse(config.getAdvancedNetworkConfig().isEnabled());
        assertEquals(9999, config.getNetworkConfig().getPort());
    }

    @Override
    @Test
    public void testMultipleClientEndpointConfigs_throwsException() {
        String xml = HAZELCAST_START_TAG
                + "<advanced-network enabled=\"true\">"
                + "  <client-server-socket-endpoint-config name=\"client-1\"/>"
                + "  <client-server-socket-endpoint-config name=\"client-2\"/>"
                + "</advanced-network>"
                + HAZELCAST_END_TAG;

        expected.expect(InvalidConfigurationException.class);
        buildConfig(xml);
    }

    @Override
    @Test
    public void testMultipleRestEndpointConfigs_throwsException() {
        String xml = HAZELCAST_START_TAG
                + "<advanced-network enabled=\"true\">"
                + "  <rest-server-socket-endpoint-config name=\"rest-1\"/>"
                + "  <rest-server-socket-endpoint-config name=\"rest-2\"/>"
                + "</advanced-network>"
                + HAZELCAST_END_TAG;

        expected.expect(InvalidConfigurationException.class);
        buildConfig(xml);
    }

    @Override
    @Test
    public void testMultipleMemcacheEndpointConfigs_throwsException() {
        String xml = HAZELCAST_START_TAG
                + "<advanced-network enabled=\"true\">"
                + "  <memcache-server-socket-endpoint-config name=\"mc-1\"/>"
                + "  <memcache-server-socket-endpoint-config name=\"mc-2\"/>"
                + "</advanced-network>"
                + HAZELCAST_END_TAG;

        expected.expect(InvalidConfigurationException.class);
        buildConfig(xml);
    }

    @Override
    @Test
    public void testMultipleJoinElements_throwsException() {
        String xml = HAZELCAST_START_TAG
                + "<advanced-network enabled=\"true\">"
                + "  <join><auto-detection enabled=\"true\"/></join>"
                + "  <join><auto-detection enabled=\"false\"/></join>"
                + "</advanced-network>"
                + HAZELCAST_END_TAG;

        expected.expect(InvalidConfigurationException.class);
        buildConfig(xml);
    }

    @Override
    @Test
    public void testMultipleFailureDetectorElements_throwsException() {
        String xml = HAZELCAST_START_TAG
                + "<advanced-network enabled=\"true\">"
                + "  <failure-detector enabled=\"true\"/>"
                + "  <failure-detector enabled=\"false\"/>"
                + "</advanced-network>"
                + HAZELCAST_END_TAG;

        expected.expect(InvalidConfigurationException.class);
        buildConfig(xml);
    }

    @Override
    @Test
    public void testMultipleMemberAddressProviderElements_throwsException() {
        String xml = HAZELCAST_START_TAG
                + "<advanced-network enabled=\"true\">"
                + "  <member-address-provider enabled=\"true\">"
                + "    <class-name>com.acme.DummyProvider</class-name>"
                + "  </member-address-provider>"
                + "  <member-address-provider enabled=\"true\">"
                + "    <class-name>com.acme.DummyProvider</class-name>"
                + "  </member-address-provider>"
                + "</advanced-network>"
                + HAZELCAST_END_TAG;

        expected.expect(InvalidConfigurationException.class);
        buildConfig(xml);
    }

    @Override
    @Test
    public void testMultipleMemberEndpointConfigs_throwsException() {
        String xml = HAZELCAST_START_TAG
                + "<advanced-network enabled=\"true\">"
                + "  <member-server-socket-endpoint-config name=\"member-server-socket\">\n"
                + "  </member-server-socket-endpoint-config>\n"
                + "  <member-server-socket-endpoint-config name=\"member-server-socket-2\">\n"
                + "  </member-server-socket-endpoint-config>\n"
                + "</advanced-network>"
                + HAZELCAST_END_TAG;

        expected.expect(InvalidConfigurationException.class);
        buildConfig(xml);
    }

    @Override
    public void testWhitespaceInNonSpaceStrings() {
        String xml = HAZELCAST_START_TAG
                + "<split-brain-protection enabled='true' name='q'><protect-on> \n WRITE \n </protect-on></split-brain-protection>"
                + HAZELCAST_END_TAG;
        buildConfig(xml);
    }

    @Override
    @Test
    public void testPersistentMemoryDirectoryConfiguration() {
        String xml = HAZELCAST_START_TAG
                + "<native-memory>\n"
                + "  <persistent-memory>\n"
                + "    <directories>\n"
                + "      <directory numa-node=\"0\">/mnt/pmem0</directory>\n"
                + "      <directory numa-node=\"1\">/mnt/pmem1</directory>\n"
                + "    </directories>\n"
                + "  </persistent-memory>\n"
                + "</native-memory>\n"
                + HAZELCAST_END_TAG;

        Config xmlConfig = new InMemoryXmlConfig(xml);

        PersistentMemoryConfig pmemConfig = xmlConfig.getNativeMemoryConfig()
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

    @Override
    @Test
    public void testPersistentMemoryDirectoryConfigurationSimple() {
        String xml = HAZELCAST_START_TAG
                + "<native-memory>\n"
                + "  <persistent-memory-directory>/mnt/pmem0</persistent-memory-directory>\n"
                + "</native-memory>\n"
                + HAZELCAST_END_TAG;

        Config xmlConfig = new InMemoryXmlConfig(xml);

        PersistentMemoryConfig pmemConfig = xmlConfig.getNativeMemoryConfig().getPersistentMemoryConfig();
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
        String xml = HAZELCAST_START_TAG
                + "<native-memory>\n"
                + "  <persistent-memory>\n"
                + "    <directories>\n"
                + "      <directory numa-node=\"0\">/mnt/pmem0</directory>\n"
                + "      <directory numa-node=\"1\">/mnt/pmem0</directory>\n"
                + "    </directories>\n"
                + "  </persistent-memory>\n"
                + "</native-memory>\n"
                + HAZELCAST_END_TAG;

        buildConfig(xml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testPersistentMemoryDirectoryConfiguration_uniqueNumaNodeViolationThrows() {
        String xml = HAZELCAST_START_TAG
                + "<native-memory>\n"
                + "  <persistent-memory>\n"
                + "    <directories>\n"
                + "      <directory numa-node=\"0\">/mnt/pmem0</directory>\n"
                + "      <directory numa-node=\"0\">/mnt/pmem1</directory>\n"
                + "    </directories>\n"
                + "  </persistent-memory>\n"
                + "</native-memory>\n"
                + HAZELCAST_END_TAG;

        buildConfig(xml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testPersistentMemoryDirectoryConfiguration_numaNodeConsistencyViolationThrows() {
        String xml = HAZELCAST_START_TAG
                + "<native-memory>\n"
                + "  <persistent-memory>\n"
                + "    <directories>\n"
                + "      <directory numa-node=\"0\">/mnt/pmem0</directory>\n"
                + "      <directory>/mnt/pmem1</directory>\n"
                + "    </directories>\n"
                + "  </persistent-memory>\n"
                + "</native-memory>\n"
                + HAZELCAST_END_TAG;

        buildConfig(xml);
    }

    @Override
    @Test
    public void testPersistentMemoryDirectoryConfiguration_simpleAndAdvancedPasses() {
        String xml = HAZELCAST_START_TAG
                + "<native-memory>\n"
                + "  <persistent-memory-directory>/mnt/optane</persistent-memory-directory>\n"
                + "  <persistent-memory mode=\"MOUNTED\">\n"
                + "    <directories>\n"
                + "      <directory>/mnt/pmem0</directory>\n"
                + "      <directory>/mnt/pmem1</directory>\n"
                + "    </directories>\n"
                + "  </persistent-memory>\n"
                + "</native-memory>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
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
        String xml = HAZELCAST_START_TAG
                + "<native-memory>\n"
                + "  <persistent-memory enabled=\"true\" mode=\"SYSTEM_MEMORY\" />\n"
                + "</native-memory>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        PersistentMemoryConfig pmemConfig = config.getNativeMemoryConfig().getPersistentMemoryConfig();
        assertTrue(pmemConfig.isEnabled());
        assertEquals(PersistentMemoryMode.SYSTEM_MEMORY, pmemConfig.getMode());
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testPersistentMemoryConfiguration_NotExistingModeThrows() {
        String xml = HAZELCAST_START_TAG
                + "<native-memory>\n"
                + "  <persistent-memory mode=\"NOT_EXISTING_MODE\" />\n"
                + "</native-memory>\n"
                + HAZELCAST_END_TAG;

        buildConfig(xml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testPersistentMemoryDirectoryConfiguration_SystemMemoryModeThrows() {
        String xml = HAZELCAST_START_TAG
                + "<native-memory>\n"
                + "  <persistent-memory mode=\"SYSTEM_MEMORY\">\n"
                + "    <directories>\n"
                + "      <directory>/mnt/pmem0</directory>\n"
                + "    </directories>\n"
                + "  </persistent-memory>\n"
                + "</native-memory>\n"
                + HAZELCAST_END_TAG;

        buildConfig(xml);
    }

    @Override
    protected Config buildCompleteAdvancedNetworkConfig() {
        String xml = HAZELCAST_START_TAG
                + "<advanced-network enabled=\"true\">"
                + "  <join>\n"
                + "      <multicast enabled=\"false\"/>\n"
                + "      <tcp-ip enabled=\"true\">\n"
                + "        <required-member>10.10.1.10</required-member>\n"
                + "        <member>10.10.1.11</member>\n"
                + "        <member>10.10.1.12</member>\n"
                + "      </tcp-ip>\n"
                + "  </join>\n"
                + "  <failure-detector>\n"
                + "            <icmp enabled=\"true\">\n"
                + "                <timeout-milliseconds>42</timeout-milliseconds>\n"
                + "                <fail-fast-on-startup>true</fail-fast-on-startup>\n"
                + "                <interval-milliseconds>4200</interval-milliseconds>\n"
                + "                <max-attempts>42</max-attempts>\n"
                + "                <parallel-mode>true</parallel-mode>\n"
                + "                <ttl>255</ttl>\n"
                + "            </icmp>\n"
                + "  </failure-detector>\n"
                + "  <member-address-provider>\n"
                + "    <class-name>com.hazelcast.test.Provider</class-name>\n"
                + "  </member-address-provider>\n"
                + "  <member-server-socket-endpoint-config name=\"member-server-socket\">\n"
                + "    <outbound-ports><ports>33000-33100</ports></outbound-ports>\n"
                + "    <interfaces enabled=\"true\">\n"
                + "      <interface>10.10.0.1</interface>\n"
                + "    </interfaces>\n"
                + "    <ssl enabled=\"true\">\n"
                + "      <factory-class-name>com.hazelcast.examples.MySSLContextFactory</factory-class-name>\n"
                + "      <properties>\n"
                + "        <property name=\"foo\">bar</property>\n"
                + "      </properties>\n"
                + "    </ssl>\n"
                + "    <socket-interceptor enabled=\"true\">\n"
                + "      <class-name>com.hazelcast.examples.MySocketInterceptor</class-name>\n"
                + "      <properties>\n"
                + "         <property name=\"foo\">baz</property>\n"
                + "      </properties>\n"
                + "    </socket-interceptor>\n"
                + "    <socket-options>\n"
                + "      <buffer-direct>true</buffer-direct>\n"
                + "      <tcp-no-delay>true</tcp-no-delay>\n"
                + "      <keep-alive>true</keep-alive>\n"
                + "      <connect-timeout-seconds>33</connect-timeout-seconds>\n"
                + "      <send-buffer-size-kb>34</send-buffer-size-kb>\n"
                + "      <receive-buffer-size-kb>67</receive-buffer-size-kb>\n"
                + "      <linger-seconds>11</linger-seconds>\n"
                + "      <keep-count>12</keep-count>\n"
                + "      <keep-interval-seconds>13</keep-interval-seconds>\n"
                + "      <keep-idle-seconds>14</keep-idle-seconds>\n"
                + "    </socket-options>\n"
                + "    <symmetric-encryption enabled=\"true\">\n"
                + "      <algorithm>Algorithm</algorithm>\n"
                + "      <salt>thesalt</salt>\n"
                + "      <password>thepassword</password>\n"
                + "      <iteration-count>1000</iteration-count>\n"
                + "    </symmetric-encryption>\n"
                + "    <port port-count=\"93\" auto-increment=\"false\">9191</port>\n"
                + "    <public-address>10.20.10.10</public-address>\n"
                + "    <reuse-address>true</reuse-address>\n"
                + "  </member-server-socket-endpoint-config>\n"
                + "  <rest-server-socket-endpoint-config name=\"REST\">\n"
                + "    <port>8080</port>\n"
                + "    <endpoint-groups>\n"
                + "      <endpoint-group name=\"WAN\" enabled=\"true\"/>\n"
                + "      <endpoint-group name=\"CLUSTER_READ\" enabled=\"true\"/>\n"
                + "      <endpoint-group name=\"CLUSTER_WRITE\" enabled=\"false\"/>\n"
                + "      <endpoint-group name=\"HEALTH_CHECK\" enabled=\"true\"/>\n"
                + "    </endpoint-groups>\n"
                + "  </rest-server-socket-endpoint-config>\n"
                + "  <memcache-server-socket-endpoint-config name=\"MEMCACHE\">\n"
                + "    <outbound-ports><ports>42000-42100</ports></outbound-ports>\n"
                + "  </memcache-server-socket-endpoint-config>\n"
                + "  <wan-server-socket-endpoint-config name=\"WAN_SERVER1\">\n"
                + "    <outbound-ports><ports>52000-52100</ports></outbound-ports>\n"
                + "  </wan-server-socket-endpoint-config>\n"
                + "  <wan-server-socket-endpoint-config name=\"WAN_SERVER2\">\n"
                + "    <outbound-ports><ports>53000-53100</ports></outbound-ports>\n"
                + "  </wan-server-socket-endpoint-config>\n"
                + "  <wan-endpoint-config name=\"WAN_ENDPOINT1\">\n"
                + "    <outbound-ports><ports>62000-62100</ports></outbound-ports>\n"
                + "  </wan-endpoint-config>\n"
                + "  <wan-endpoint-config name=\"WAN_ENDPOINT2\">\n"
                + "    <outbound-ports><ports>63000-63100</ports></outbound-ports>\n"
                + "  </wan-endpoint-config>\n"
                + "  <client-server-socket-endpoint-config name=\"CLIENT\">\n"
                + "    <outbound-ports><ports>72000-72100</ports></outbound-ports>\n"
                + "  </client-server-socket-endpoint-config>\n"
                + "</advanced-network>\n"
                + HAZELCAST_END_TAG;

        return buildConfig(xml);
    }

    @Override
    @Test
    public void testMetricsConfig() {
        String xml = HAZELCAST_START_TAG
                + "<metrics enabled=\"false\">"
                + "  <management-center enabled=\"false\">"
                + "    <retention-seconds>11</retention-seconds>"
                + "  </management-center>"
                + "  <jmx enabled=\"false\" />"
                + "  <collection-frequency-seconds>10</collection-frequency-seconds>\n"
                + "</metrics>"
                + HAZELCAST_END_TAG;
        Config config = new InMemoryXmlConfig(xml);
        MetricsConfig metricsConfig = config.getMetricsConfig();
        MetricsManagementCenterConfig metricsMcConfig = metricsConfig.getManagementCenterConfig();
        assertFalse(metricsConfig.isEnabled());
        assertFalse(metricsMcConfig.isEnabled());
        assertFalse(metricsConfig.getJmxConfig().isEnabled());
        assertEquals(10, metricsConfig.getCollectionFrequencySeconds());
        assertEquals(11, metricsMcConfig.getRetentionSeconds());
    }

    @Override
    @Test
    public void testInstanceTrackingConfig() {
        String xml = HAZELCAST_START_TAG
                + "<instance-tracking enabled=\"true\">"
                + "  <file-name>/dummy/file</file-name>"
                + "  <format-pattern>dummy-pattern with $HZ_INSTANCE_TRACKING{placeholder} and $RND{placeholder}</format-pattern>"
                + "</instance-tracking>"
                + HAZELCAST_END_TAG;
        Config config = new InMemoryXmlConfig(xml);
        InstanceTrackingConfig trackingConfig = config.getInstanceTrackingConfig();
        assertTrue(trackingConfig.isEnabled());
        assertEquals("/dummy/file", trackingConfig.getFileName());
        assertEquals("dummy-pattern with $HZ_INSTANCE_TRACKING{placeholder} and $RND{placeholder}",
                trackingConfig.getFormatPattern());
    }

    @Override
    @Test
    public void testMetricsConfigMasterSwitchDisabled() {
        String xml = HAZELCAST_START_TAG
                + "<metrics enabled=\"false\"/>"
                + HAZELCAST_END_TAG;
        Config config = new InMemoryXmlConfig(xml);
        MetricsConfig metricsConfig = config.getMetricsConfig();
        assertFalse(metricsConfig.isEnabled());
        assertTrue(metricsConfig.getManagementCenterConfig().isEnabled());
        assertTrue(metricsConfig.getJmxConfig().isEnabled());
    }

    @Override
    @Test
    public void testMetricsConfigMcDisabled() {
        String xml = HAZELCAST_START_TAG
                + "<metrics>"
                + "  <management-center enabled=\"false\" />"
                + "</metrics>"
                + HAZELCAST_END_TAG;
        Config config = new InMemoryXmlConfig(xml);
        MetricsConfig metricsConfig = config.getMetricsConfig();
        assertTrue(metricsConfig.isEnabled());
        assertFalse(metricsConfig.getManagementCenterConfig().isEnabled());
        assertTrue(metricsConfig.getJmxConfig().isEnabled());
    }

    @Override
    @Test
    public void testMetricsConfigJmxDisabled() {
        String xml = HAZELCAST_START_TAG
                + "<metrics>"
                + "  <jmx enabled=\"false\" />"
                + "</metrics>"
                + HAZELCAST_END_TAG;
        Config config = new InMemoryXmlConfig(xml);
        MetricsConfig metricsConfig = config.getMetricsConfig();
        assertTrue(metricsConfig.isEnabled());
        assertTrue(metricsConfig.getManagementCenterConfig().isEnabled());
        assertFalse(metricsConfig.getJmxConfig().isEnabled());
    }

    @Override
    protected Config buildAuditlogConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <auditlog enabled='true'>\n"
                + "        <factory-class-name>\n"
                + "            com.acme.auditlog.AuditlogToSyslogFactory\n"
                + "        </factory-class-name>\n"
                + "        <properties>\n"
                + "            <property name='host'>syslogserver.acme.com</property>\n"
                + "            <property name='port'>514</property>\n"
                + "            <property name='type'>tcp</property>\n"
                + "        </properties>\n"
                + "    </auditlog>"
                + HAZELCAST_END_TAG;
        return new InMemoryXmlConfig(xml);
    }

    @Override
    @Test
    public void testSqlConfig() {
        String xml = HAZELCAST_START_TAG
                + "<sql>\n"
                + "  <statement-timeout-millis>30</statement-timeout-millis>\n"
                + "  <catalog-persistence-enabled>true</catalog-persistence-enabled>\n"
                + "</sql>"
                + HAZELCAST_END_TAG;
        Config config = new InMemoryXmlConfig(xml);
        SqlConfig sqlConfig = config.getSqlConfig();
        assertEquals(30L, sqlConfig.getStatementTimeoutMillis());
        assertTrue(sqlConfig.isCatalogPersistenceEnabled());
    }

    @Override
    @Test
    public void testIntegrityCheckerConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <integrity-checker enabled=\"false\"/>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);

        assertFalse(config.getIntegrityCheckerConfig().isEnabled());
    }

    @Override
    @Test
    public void testDataConnectionConfigs() {
        String xml = HAZELCAST_START_TAG
                + "    <data-connection name=\"mysql-database\">\n"
                + "        <type>jdbc</type>\n"
                + "        <properties>\n"
                + "            <property name=\"jdbcUrl\">jdbc:mysql://dummy:3306</property>\n"
                + "            <property name=\"some.property\">dummy-value</property>\n"
                + "        </properties>\n"
                + "      <shared>true</shared>\n"
                + "    </data-connection>"
                + "    <data-connection name=\"other-database\">\n"
                + "        <type>other</type>\n"
                + "    </data-connection>"
                + HAZELCAST_END_TAG;

        Map<String, DataConnectionConfig> dataConnectionConfigs = buildConfig(xml).getDataConnectionConfigs();

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
    public void testTpcConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <tpc enabled=\"true\">\n"
                + "        <eventloop-count>12</eventloop-count>\n"
                + "    </tpc>"
                + HAZELCAST_END_TAG;

        TpcConfig tpcConfig = buildConfig(xml).getTpcConfig();

        assertThat(tpcConfig.isEnabled()).isTrue();
        assertThat(tpcConfig.getEventloopCount()).isEqualTo(12);
    }

    @Override
    @Test
    public void testTpcSocketConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <network>"
                + "        <tpc-socket>\n"
                + "            <port-range>14000-16000</port-range>\n"
                + "            <receive-buffer-size-kb>256</receive-buffer-size-kb>\n"
                + "            <send-buffer-size-kb>256</send-buffer-size-kb>\n"
                + "        </tpc-socket>\n"
                + "    </network>"
                + HAZELCAST_END_TAG;

        TpcSocketConfig tpcSocketConfig = buildConfig(xml).getNetworkConfig().getTpcSocketConfig();

        assertThat(tpcSocketConfig.getPortRange()).isEqualTo("14000-16000");
        assertThat(tpcSocketConfig.getReceiveBufferSizeKB()).isEqualTo(256);
        assertThat(tpcSocketConfig.getSendBufferSizeKB()).isEqualTo(256);
    }

    @Override
    @Test
    public void testTpcSocketConfigAdvanced() {
        String xml = HAZELCAST_START_TAG
                + "    <advanced-network enabled=\"true\">\n"
                + "        <member-server-socket-endpoint-config>\n"
                + "            <tpc-socket>\n"
                + "                <port-range>14000-16000</port-range>\n"
                + "                <receive-buffer-size-kb>256</receive-buffer-size-kb>\n"
                + "                <send-buffer-size-kb>256</send-buffer-size-kb>\n"
                + "            </tpc-socket>\n"
                + "        </member-server-socket-endpoint-config>\n"
                + "        <client-server-socket-endpoint-config>\n"
                + "            <tpc-socket>\n"
                + "                <port-range>14000-16000</port-range>\n"
                + "                <receive-buffer-size-kb>256</receive-buffer-size-kb>\n"
                + "                <send-buffer-size-kb>256</send-buffer-size-kb>\n"
                + "            </tpc-socket>\n"
                + "        </client-server-socket-endpoint-config>\n"
                + "        <memcache-server-socket-endpoint-config>\n"
                + "            <tpc-socket>\n"
                + "                <port-range>14000-16000</port-range>\n"
                + "                <receive-buffer-size-kb>256</receive-buffer-size-kb>\n"
                + "                <send-buffer-size-kb>256</send-buffer-size-kb>\n"
                + "            </tpc-socket>\n"
                + "        </memcache-server-socket-endpoint-config>\n"
                + "        <rest-server-socket-endpoint-config>\n"
                + "            <tpc-socket>\n"
                + "                <port-range>14000-16000</port-range>\n"
                + "                <receive-buffer-size-kb>256</receive-buffer-size-kb>\n"
                + "                <send-buffer-size-kb>256</send-buffer-size-kb>\n"
                + "            </tpc-socket>\n"
                + "        </rest-server-socket-endpoint-config>\n"
                + "        <wan-endpoint-config name=\"tokyo\">\n"
                + "            <tpc-socket>\n"
                + "                <port-range>14000-16000</port-range>\n"
                + "                <receive-buffer-size-kb>256</receive-buffer-size-kb>\n"
                + "                <send-buffer-size-kb>256</send-buffer-size-kb>\n"
                + "            </tpc-socket>\n"
                + "        </wan-endpoint-config>\n"
                + "        <wan-server-socket-endpoint-config name=\"london\">\n"
                + "            <tpc-socket>\n"
                + "                <port-range>14000-16000</port-range>\n"
                + "                <receive-buffer-size-kb>256</receive-buffer-size-kb>\n"
                + "                <send-buffer-size-kb>256</send-buffer-size-kb>\n"
                + "            </tpc-socket>\n"
                + "        </wan-server-socket-endpoint-config>\n"
                + "    </advanced-network>"
                + HAZELCAST_END_TAG;

        Map<EndpointQualifier, EndpointConfig> endpointConfigs = buildConfig(xml)
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
    @Test
    public void testPartitioningAttributeConfigs() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"test\">"
                + "     <partition-attributes>"
                + "         <attribute>attr1</attribute>"
                + "         <attribute>attr2</attribute>"
                + "     </partition-attributes>"
                + "</map>"
                + HAZELCAST_END_TAG;

        final MapConfig mapConfig = buildConfig(xml).getMapConfig("test");
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
        File tempClassFile = tempFolder.newFile("TempClass.class");

        String xml = HAZELCAST_START_TAG
                + "<user-code-namespaces enabled=\"true\">"
                + "    <class-filter defaults-disabled=\"true\">\n"
                + "        <blacklist>\n"
                + "            <class>com.acme.app.BeanComparator</class>\n"
                + "        </blacklist>\n"
                + "        <whitelist>\n"
                + "            <package>com.acme.app</package>\n"
                + "            <prefix>com.hazelcast.</prefix>\n"
                + "        </whitelist>\n"
                + "    </class-filter>"
                + "    <namespace name=\"ns1\">"
                + "        <jar id=\"jarId\">"
                + "            <url>" + tempJar.toURI().toURL() + "</url>"
                + "        </jar>"
                + "        <jars-in-zip id=\"zipId\">"
                + "            <url>" + tempJarZip.toURI().toURL() + "</url>"
                + "        </jars-in-zip>"
                + "        <class id=\"classId\">"
                + "            <url>" + tempClassFile.toURI().toURL() + "</url>"
                + "        </class>"
                + "    </namespace>"
                + "    <namespace name=\"ns2\">"
                + "        <jar id=\"jarId2\">"
                + "            <url>" + tempJar.toURI().toURL() + "</url>"
                + "        </jar>"
                + "    </namespace>"
                + "</user-code-namespaces>"
                + HAZELCAST_END_TAG;


        final UserCodeNamespacesConfig userCodeNamespacesConfig = buildConfig(xml).getNamespacesConfig();
        assertThat(userCodeNamespacesConfig.isEnabled()).isTrue();
        assertThat(userCodeNamespacesConfig.getNamespaceConfigs()).hasSize(2);
        final UserCodeNamespaceConfig userCodeNamespaceConfig = userCodeNamespacesConfig.getNamespaceConfigs().get("ns1");

        assertNotNull(userCodeNamespaceConfig);

        assertThat(userCodeNamespaceConfig.getName()).isEqualTo("ns1");
        assertThat(userCodeNamespaceConfig.getResourceConfigs()).hasSize(3);

        // validate NS1 ResourceDefinition contents.
        Collection<ResourceDefinition> ns1Resources = userCodeNamespaceConfig.getResourceConfigs();
        Optional<ResourceDefinition> jarIdResource = ns1Resources.stream().filter(r -> r.id().equals("jarId")).findFirst();
        Optional<ResourceDefinition> classIdResource = ns1Resources.stream().filter(r -> r.id().equals("classId")).findFirst();

        assertThat(jarIdResource).isPresent();
        assertThat(jarIdResource.get().url()).isEqualTo(tempJar.toURI().toURL().toString());
        assertEquals(ResourceType.JAR, jarIdResource.get().type());
        assertThat(classIdResource).isPresent();
        assertThat(classIdResource.get().url()).isEqualTo(tempClassFile.toURI().toURL().toString());
        assertEquals(ResourceType.CLASS, classIdResource.get().type());

        // check the bytes[] are equal
        assertArrayEquals(getTestFileBytes(tempJar), jarIdResource.get().payload());
        Optional<ResourceDefinition> zipId = ns1Resources.stream().filter(r -> r.id().equals("zipId")).findFirst();
        assertThat(zipId).isPresent();
        assertThat(zipId.get().url()).isEqualTo(tempJarZip.toURI().toURL().toString());
        assertEquals(ResourceType.JARS_IN_ZIP, zipId.get().type());
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
        assertTrue(filterConfig.isDefaultsDisabled());
        assertTrue(filterConfig.getWhitelist().isListed("com.acme.app.FakeClass"));
        assertTrue(filterConfig.getWhitelist().isListed("com.hazelcast.fake.place.MagicClass"));
        assertFalse(filterConfig.getWhitelist().isListed("not.in.the.whitelist.ClassName"));
        assertTrue(filterConfig.getBlacklist().isListed("com.acme.app.BeanComparator"));
        assertFalse(filterConfig.getBlacklist().isListed("not.in.the.blacklist.ClassName"));
    }

    @Override
    public void testRestConfig() throws IOException {
        final Config config = buildRestConfigFromXmlString();
        validateRestConfig(config);
    }

    @Override
    public void testVectorCollectionConfig() {
        String xml = HAZELCAST_START_TAG
                + """
                    <vector-collection name="vector-1">
                      <indexes>
                        <index name="index-1-1">
                            <dimension>2</dimension>
                            <metric>DOT</metric>
                            <max-degree>10</max-degree>
                            <ef-construction>10</ef-construction>
                            <use-deduplication>true</use-deduplication>
                        </index>
                        <index name="index-1-2">
                            <dimension>3</dimension>
                            <metric>EUCLIDEAN</metric>
                        </index>
                      </indexes>
                    </vector-collection>
                    <vector-collection name="vector-2">
                      <backup-count>2</backup-count>
                      <async-backup-count>1</async-backup-count>
                      <split-brain-protection-ref>splitBrainProtectionName</split-brain-protection-ref>
                      <merge-policy batch-size="132">CustomMergePolicy</merge-policy>
                      <user-code-namespace>ns1</user-code-namespace>
                      <indexes>
                        <index>
                            <dimension>4</dimension>
                            <metric>COSINE</metric>
                            <use-deduplication>false</use-deduplication>
                        </index>
                      </indexes>
                    </vector-collection>
                """
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        validateVectorCollectionConfig(config);
    }

    @Override
    @Test
    public void testVectorCollectionConfig_backupCount_max() {
        int backupCount = 6;
        String xml = simpleVectorCollectionBackupCountConfig(backupCount);
        Config config = buildConfig(xml);
        assertThat(config.getVectorCollectionConfigs().get("vector-1").getBackupCount()).isEqualTo(backupCount);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testVectorCollectionConfig_backupCount_moreThanMax() {
        String xml = simpleVectorCollectionBackupCountConfig(7);
        buildConfig(xml);
    }

    @Override
    @Test
    public void testVectorCollectionConfig_backupCount_min() {
        int backupCount = 0;
        String xml = simpleVectorCollectionBackupCountConfig(backupCount);
        Config config = buildConfig(xml);
        assertThat(config.getVectorCollectionConfigs().get("vector-1").getBackupCount()).isEqualTo(backupCount);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testVectorCollectionConfig_backupCount_lessThanMin() {
        String xml = simpleVectorCollectionBackupCountConfig(-1);
        buildConfig(xml);
    }

    @Override
    @Test
    public void testVectorCollectionConfig_asyncBackupCount_max() {
        int asyncBackupCount = 6;
        // we need to set backup-count=0 since default is 1
        // and backup count + async backup count must not exceed 6
        String xml = simpleVectorCollectionBackupCountAndAsyncBackupCountConfig(0, asyncBackupCount);
        Config config = buildConfig(xml);
        assertThat(config.getVectorCollectionConfigs().get("vector-1").getAsyncBackupCount()).isEqualTo(asyncBackupCount);
        assertThat(config.getVectorCollectionConfigs().get("vector-1").getBackupCount()).isEqualTo(0);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testVectorCollectionConfig_asyncBackupCount_moreThanMax() {
        // we need to set backup-count=0 since default is 1
        // and backup count + async backup count must not exceed 6
        String xml = simpleVectorCollectionBackupCountAndAsyncBackupCountConfig(0, 7);
        buildConfig(xml);
    }

    @Override
    @Test
    public void testVectorCollectionConfig_asyncBackupCount_min() {
        int asyncBackupCount = 0;
        String xml = simpleVectorCollectionAsyncBackupCountConfig(asyncBackupCount);
        Config config = buildConfig(xml);
        assertThat(config.getVectorCollectionConfigs().get("vector-1").getAsyncBackupCount()).isEqualTo(asyncBackupCount);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testVectorCollectionConfig_asyncBackupCount_lessThanMin() {
        String xml = simpleVectorCollectionAsyncBackupCountConfig(-1);
        buildConfig(xml);
    }

    @Override
    @Test
    public void testVectorCollectionConfig_backupSyncAndAsyncCount_max() {
        int backupCount = 4;
        int asyncBackupCount = 2;
        String xml = simpleVectorCollectionBackupCountAndAsyncBackupCountConfig(backupCount, asyncBackupCount);
        Config config = buildConfig(xml);
        assertThat(config.getVectorCollectionConfigs().get("vector-1").getBackupCount()).isEqualTo(backupCount);
        assertThat(config.getVectorCollectionConfigs().get("vector-1").getAsyncBackupCount()).isEqualTo(asyncBackupCount);
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void testVectorCollectionConfig_backupSyncAndAsyncCount_moreThanMax() {
        int backupCount = 4;
        int asyncBackupCount = 3;
        String xml = simpleVectorCollectionBackupCountAndAsyncBackupCountConfig(backupCount, asyncBackupCount);
        buildConfig(xml);
    }

    @Override
    public void testVectorCollectionConfig_multipleIndexesWithTheSameName_fail() {
        String xml = HAZELCAST_START_TAG
                + """
                    <vector-collection name="vector-1">
                      <indexes>
                        <index name="index-1">
                            <dimension>2</dimension>
                            <metric>DOT</metric>
                            <max-degree>10</max-degree>
                            <ef-construction>10</ef-construction>
                            <use-deduplication>true</use-deduplication>
                        </index>
                        <index name="index-1">
                            <dimension>3</dimension>
                            <metric>EUCLIDEAN</metric>
                        </index>
                      </indexes>
                    </vector-collection>
                """
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
    }

    private String simpleVectorCollectionBackupCountConfig(int count) {
        return simpleVectorCollectionBackupCountConfig("backup-count", count);
    }

    private String simpleVectorCollectionAsyncBackupCountConfig(int count) {
        return simpleVectorCollectionBackupCountConfig("async-backup-count", count);
    }

    private String simpleVectorCollectionBackupCountConfig(String tagName, int count) {
        return String.format(HAZELCAST_START_TAG
                + """
                    <vector-collection name="vector-1">
                      <%s>%d</%s>
                      <indexes>
                        <index>
                            <dimension>4</dimension>
                            <metric>COSINE</metric>
                        </index>
                      </indexes>
                    </vector-collection>
                """
                + HAZELCAST_END_TAG, tagName, count, tagName);
    }

    private String simpleVectorCollectionBackupCountAndAsyncBackupCountConfig(
            int backupCount, int asyncBackupCount) {
        return String.format(HAZELCAST_START_TAG
                + """
                    <vector-collection name="vector-1">
                      <backup-count>%d</backup-count>
                      <async-backup-count>%d</async-backup-count>
                      <indexes>
                        <index>
                            <dimension>4</dimension>
                            <metric>COSINE</metric>
                        </index>
                      </indexes>
                    </vector-collection>
                """
                + HAZELCAST_END_TAG, backupCount, asyncBackupCount);
    }

    static Config buildRestConfigFromXmlString() {
        String xml = HAZELCAST_START_TAG
                + "    <rest enabled=\"true\">\n"
                + "        <port>8080</port>\n"
                + "        <security-realm>realmName</security-realm>\n"
                + "        <token-validity-seconds>500</token-validity-seconds>\n"
                + "        <max-login-attempts>10</max-login-attempts>\n"
                + "        <lockout-duration-seconds>10</lockout-duration-seconds>\n"
                + "        <ssl enabled=\"true\">\n"
                + "            <client-auth>NEED</client-auth>\n"
                + "            <ciphers>TLS_RSA_WITH_AES_128_CBC_SHA, TLS_RSA_WITH_AES_128_CBC_SHA256</ciphers>\n"
                + "            <enabled-protocols>TLSv1.2, TLSv1.3</enabled-protocols>\n"
                + "            <key-alias>myKeyAlias</key-alias>\n"
                + "            <key-password>myKeyPassword</key-password>\n"
                + "            <key-store>/path/to/keystore</key-store>\n"
                + "            <key-store-password>myKeyStorePassword</key-store-password>\n"
                + "            <key-store-type>JKS</key-store-type>\n"
                + "            <key-store-provider>SUN</key-store-provider>\n"
                + "            <trust-store>/path/to/truststore</trust-store>\n"
                + "            <trust-store-password>myTrustStorePassword</trust-store-password>\n"
                + "            <trust-store-type>JKS</trust-store-type>\n"
                + "            <trust-store-provider>SUN</trust-store-provider>\n"
                + "            <protocol>TLS</protocol>\n"
                + "            <certificate>/path/to/certificate</certificate>\n"
                + "            <certificate-key>/path/to/certificate-key</certificate-key>\n"
                + "            <trust-certificate>/path/to/trust-certificate</trust-certificate>\n"
                + "            <trust-certificate-key>/path/to/trust-certificate-key</trust-certificate-key>\n"
                + "        </ssl>\n"
                + "    </rest>\n"
                + HAZELCAST_END_TAG;

        final Config config = buildConfig(xml);
        return config;
    }

    @Override
    protected Config buildMapWildcardConfig() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"map*\">\n"
                + "  <attributes>\n"
                + "    <attribute extractor-class-name=\"usercodedeployment.CapitalizingFirstNameExtractor\">name</attribute>\n"
                + "  </attributes>\n"
                + "</map>\n"
                + "<map name=\"mapBackup2*\">\n"
                + "  <backup-count>2</backup-count>"
                + "  <attributes>\n"
                + "    <attribute extractor-class-name=\"usercodedeployment.CapitalizingFirstNameExtractor\">name</attribute>\n"
                + "  </attributes>\n"
                + "</map>\n"
                + HAZELCAST_END_TAG;

        return new InMemoryXmlConfig(xml);
    }

    public String getAdvancedNetworkConfigWithSocketOption(String socketOption, int value) {
        String xml = HAZELCAST_START_TAG
                + "<advanced-network enabled=\"true\">"
                + "  <member-server-socket-endpoint-config name=\"member-server-socket\">\n"
                + "    <socket-options>"
                + "      <" + socketOption + ">" + value + "</" + socketOption + ">"
                + "    </socket-options>"
                + "  </member-server-socket-endpoint-config>\n"
                + "</advanced-network>"
                + HAZELCAST_END_TAG;
        return xml;
    }
}
