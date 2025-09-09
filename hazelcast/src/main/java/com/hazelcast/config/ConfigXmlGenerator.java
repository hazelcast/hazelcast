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

import com.hazelcast.config.cp.CPMapConfig;
import com.hazelcast.config.rest.RestConfig;
import com.hazelcast.config.tpc.TpcConfig;
import com.hazelcast.config.tpc.TpcSocketConfig;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.config.cp.FencedLockConfig;
import com.hazelcast.config.cp.RaftAlgorithmConfig;
import com.hazelcast.config.cp.SemaphoreConfig;
import com.hazelcast.config.security.AbstractClusterLoginConfig;
import com.hazelcast.config.security.AccessControlServiceConfig;
import com.hazelcast.config.security.JaasAuthenticationConfig;
import com.hazelcast.config.security.KerberosAuthenticationConfig;
import com.hazelcast.config.security.KerberosIdentityConfig;
import com.hazelcast.config.security.LdapAuthenticationConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.config.security.SimpleAuthenticationConfig;
import com.hazelcast.config.security.TlsAuthenticationConfig;
import com.hazelcast.config.security.TokenIdentityConfig;
import com.hazelcast.config.security.UsernamePasswordIdentityConfig;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.config.ConfigXmlGeneratorHelper;
import com.hazelcast.internal.config.PersistenceAndHotRestartPersistenceMerger;
import com.hazelcast.internal.namespace.ResourceDefinition;
import com.hazelcast.internal.util.CollectionUtil;
import com.hazelcast.internal.util.MapUtil;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.ResourceType;
import com.hazelcast.memory.Capacity;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.splitbrainprotection.impl.ProbabilisticSplitBrainProtectionFunction;
import com.hazelcast.splitbrainprotection.impl.RecentlyActiveSplitBrainProtectionFunction;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.config.PermissionConfig.PermissionType.ALL;
import static com.hazelcast.config.PermissionConfig.PermissionType.CONFIG;
import static com.hazelcast.config.PermissionConfig.PermissionType.TRANSACTION;
import static com.hazelcast.internal.config.AliasedDiscoveryConfigUtils.aliasedDiscoveryConfigsFrom;
import static com.hazelcast.internal.dynamicconfig.DynamicConfigXmlGenerator.aliasedDiscoveryConfigsGenerator;
import static com.hazelcast.internal.dynamicconfig.DynamicConfigXmlGenerator.cacheXmlGenerator;
import static com.hazelcast.internal.dynamicconfig.DynamicConfigXmlGenerator.cardinalityEstimatorXmlGenerator;
import static com.hazelcast.internal.dynamicconfig.DynamicConfigXmlGenerator.classNameOrImplClass;
import static com.hazelcast.internal.dynamicconfig.DynamicConfigXmlGenerator.discoveryStrategyConfigXmlGenerator;
import static com.hazelcast.internal.dynamicconfig.DynamicConfigXmlGenerator.durableExecutorXmlGenerator;
import static com.hazelcast.internal.dynamicconfig.DynamicConfigXmlGenerator.executorXmlGenerator;
import static com.hazelcast.internal.dynamicconfig.DynamicConfigXmlGenerator.flakeIdGeneratorXmlGenerator;
import static com.hazelcast.internal.dynamicconfig.DynamicConfigXmlGenerator.listXmlGenerator;
import static com.hazelcast.internal.dynamicconfig.DynamicConfigXmlGenerator.mapXmlGenerator;
import static com.hazelcast.internal.dynamicconfig.DynamicConfigXmlGenerator.multiMapXmlGenerator;
import static com.hazelcast.internal.dynamicconfig.DynamicConfigXmlGenerator.pnCounterXmlGenerator;
import static com.hazelcast.internal.dynamicconfig.DynamicConfigXmlGenerator.queueXmlGenerator;
import static com.hazelcast.internal.dynamicconfig.DynamicConfigXmlGenerator.reliableTopicXmlGenerator;
import static com.hazelcast.internal.dynamicconfig.DynamicConfigXmlGenerator.replicatedMapXmlGenerator;
import static com.hazelcast.internal.dynamicconfig.DynamicConfigXmlGenerator.ringbufferXmlGenerator;
import static com.hazelcast.internal.dynamicconfig.DynamicConfigXmlGenerator.scheduledExecutorXmlGenerator;
import static com.hazelcast.internal.dynamicconfig.DynamicConfigXmlGenerator.setXmlGenerator;
import static com.hazelcast.internal.dynamicconfig.DynamicConfigXmlGenerator.topicXmlGenerator;
import static com.hazelcast.internal.dynamicconfig.DynamicConfigXmlGenerator.vectorCollectionXmlGenerator;
import static com.hazelcast.internal.dynamicconfig.DynamicConfigXmlGenerator.wanReplicationXmlGenerator;
import static com.hazelcast.internal.util.Preconditions.isNotNull;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;
import static com.hazelcast.internal.util.XmlUtil.format;
import static java.util.Arrays.asList;

/**
 * The ConfigXmlGenerator is responsible for transforming a {@link Config} to a Hazelcast XML string.
 */
@SuppressWarnings({"checkstyle:methodcount", "ClassFanOutComplexity"})
public class ConfigXmlGenerator {

    /**
     * Mask to hide the sensitive values in configuration.
     */
    public static final String MASK_FOR_SENSITIVE_DATA = "****";

    private static final int INDENT = 5;

    private final boolean formatted;
    private final boolean maskSensitiveFields;

    /**
     * Creates a ConfigXmlGenerator that will format the code.
     */
    public ConfigXmlGenerator() {
        this(true);
    }

    /**
     * Creates a ConfigXmlGenerator.
     *
     * @param formatted {@code true} if the XML should be formatted, {@code false} otherwise
     */
    public ConfigXmlGenerator(boolean formatted) {
        this(formatted, true);
    }

    /**
     * Creates a ConfigXmlGenerator.
     *
     * @param formatted           {@code true} if the XML should be formatted, {@code false} otherwise
     * @param maskSensitiveFields {@code true} if the sensitive fields (like passwords) should be masked in the
     *                            output XML, {@code false} otherwise
     */
    public ConfigXmlGenerator(boolean formatted, boolean maskSensitiveFields) {
        this.formatted = formatted;
        this.maskSensitiveFields = maskSensitiveFields;
    }

    /**
     * Generates the XML string based on some Config.
     *
     * @param config the configuration
     * @return the XML string
     */
    @SuppressWarnings("checkstyle:MethodLength")
    public String generate(Config config) {
        isNotNull(config, "Config");

        StringBuilder xml = new StringBuilder();
        XmlGenerator gen = new XmlGenerator(xml);

        PersistenceAndHotRestartPersistenceMerger.merge(config.getHotRestartPersistenceConfig(),
                config.getPersistenceConfig());

        xml.append("<hazelcast ")
                .append("xmlns=\"http://www.hazelcast.com/schema/config\"\n")
                .append("xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n")
                .append("xsi:schemaLocation=\"http://www.hazelcast.com/schema/config ")
                .append("http://www.hazelcast.com/schema/config/hazelcast-config-")
                .append(Versions.CURRENT_CLUSTER_VERSION)
                .append(".xsd\">");
        gen.node("license-key", getOrMaskValue(config.getLicenseKey()))
                .node("instance-name", config.getInstanceName())
                .node("cluster-name", config.getClusterName())
        ;

        managementCenterXmlGenerator(gen, config);
        gen.appendProperties(config.getProperties());
        securityXmlGenerator(gen, config);
        wanReplicationXmlGenerator(gen, config);
        networkConfigXmlGenerator(gen, config);
        advancedNetworkConfigXmlGenerator(gen, config);
        replicatedMapXmlGenerator(gen, config);
        mapXmlGenerator(gen, config);
        cacheXmlGenerator(gen, config);
        queueXmlGenerator(gen, config);
        multiMapXmlGenerator(gen, config);
        listXmlGenerator(gen, config);
        setXmlGenerator(gen, config);
        topicXmlGenerator(gen, config);
        ringbufferXmlGenerator(gen, config);
        executorXmlGenerator(gen, config);
        durableExecutorXmlGenerator(gen, config);
        scheduledExecutorXmlGenerator(gen, config);
        partitionGroupXmlGenerator(gen, config);
        cardinalityEstimatorXmlGenerator(gen, config);
        listenerXmlGenerator(gen, config);
        serializationXmlGenerator(gen, config);
        reliableTopicXmlGenerator(gen, config);
        liteMemberXmlGenerator(gen, config);
        nativeMemoryXmlGenerator(gen, config);
        persistenceXmlGenerator(gen, config);
        dynamicConfigurationXmlGenerator(gen, config);
        localDeviceConfigXmlGenerator(gen, config);
        flakeIdGeneratorXmlGenerator(gen, config);
        crdtReplicationXmlGenerator(gen, config);
        pnCounterXmlGenerator(gen, config);
        splitBrainProtectionXmlGenerator(gen, config);
        cpSubsystemConfig(gen, config);
        metricsConfig(gen, config);
        instanceTrackingConfig(gen, config);
        sqlConfig(gen, config);
        jetConfig(gen, config);
        factoryWithPropertiesXmlGenerator(gen, "auditlog", config.getAuditlogConfig());
        userCodeDeploymentConfig(gen, config);
        integrityCheckerXmlGenerator(gen, config);
        dataConnectionConfiguration(gen, config);
        tpcConfiguration(gen, config);
        namespacesConfiguration(gen, config);
        restServerConfiguration(gen, config);
        vectorCollectionXmlGenerator(gen, config);
        memberAttributesXmlGenerator(gen, config);
        xml.append("</hazelcast>");

        String xmlString = xml.toString();
        return formatted ? format(xmlString, INDENT) : xmlString;
    }

    private String getOrMaskValue(String value) {
        if (value == null) {
            return null;
        }
        return maskSensitiveFields ? MASK_FOR_SENSITIVE_DATA : value;
    }

    private void managementCenterXmlGenerator(XmlGenerator gen, Config config) {
        ManagementCenterConfig mcConfig = config.getManagementCenterConfig();
        if (mcConfig != null) {
            gen.open("management-center",
                    "scripting-enabled", mcConfig.isScriptingEnabled(),
                    "console-enabled", mcConfig.isConsoleEnabled(),
                    "data-access-enabled", mcConfig.isDataAccessEnabled());
            trustedInterfacesXmlGenerator(gen, mcConfig.getTrustedInterfaces());
            gen.close();
        }
    }

    private static void listenerXmlGenerator(XmlGenerator gen, Config config) {
        if (config.getListenerConfigs().isEmpty()) {
            return;
        }
        gen.open("listeners");
        for (ListenerConfig lc : config.getListenerConfigs()) {
            gen.node("listener", classNameOrImplClass(lc.getClassName(), lc.getImplementation()));
        }
        gen.close();
    }

    private void securityXmlGenerator(XmlGenerator gen, Config config) {
        SecurityConfig c = config.getSecurityConfig();
        if (c == null) {
            return;
        }

        gen.open("security", "enabled", c.isEnabled())
                .node("client-block-unmapped-actions", c.getClientBlockUnmappedActions());

        PermissionPolicyConfig ppc = c.getClientPolicyConfig();
        if (ppc.getClassName() != null) {
            gen.open("client-permission-policy", "class-name", ppc.getClassName())
                    .appendProperties(ppc.getProperties())
                    .close();
        }

        Map<String, RealmConfig> realms = c.getRealmConfigs();
        if (realms != null && !realms.isEmpty()) {
            gen.open("realms");
            for (Map.Entry<String, RealmConfig> realmEntry : realms.entrySet()) {
                securityRealmGenerator(gen, realmEntry.getKey(), realmEntry.getValue());
            }
            gen.close();
        }
        addRealmReference(gen, "member-authentication", c.getMemberRealm());
        addRealmReference(gen, "client-authentication", c.getClientRealm());

        List<SecurityInterceptorConfig> sic = c.getSecurityInterceptorConfigs();
        if (!sic.isEmpty()) {
            gen.open("security-interceptors");
            for (SecurityInterceptorConfig s : sic) {
                gen.open("interceptor", "class-name", s.getClassName())
                        .close();
            }
            gen.close();
        }

        appendSecurityPermissions(gen, "client-permissions", c.getClientPermissionConfigs(),
                "on-join-operation", c.getOnJoinPermissionOperation(),
                "priority-grant", c.isPermissionPriorityGrant());
        gen.close();
    }

    private static void addRealmReference(XmlGenerator gen, String refName, String realmName) {
        if (realmName != null) {
            gen.node(refName, null, "realm", realmName);
        }
    }

    protected void securityRealmGenerator(XmlGenerator gen, String name, RealmConfig c) {
        gen.open("realm", "name", name);
        if (c.isAuthenticationConfigured()) {
            gen.open("authentication");
            jaasAuthenticationGenerator(gen, c.getJaasAuthenticationConfig());
            tlsAuthenticationGenerator(gen, c.getTlsAuthenticationConfig());
            ldapAuthenticationGenerator(gen, c.getLdapAuthenticationConfig());
            kerberosAuthenticationGenerator(gen, c.getKerberosAuthenticationConfig());
            simpleAuthenticationGenerator(gen, c.getSimpleAuthenticationConfig());
            gen.close();
        }
        if (c.isIdentityConfigured()) {
            gen.open("identity");
            CredentialsFactoryConfig cf = c.getCredentialsFactoryConfig();
            if (cf != null) {
                gen.open("credentials-factory", "class-name", cf.getClassName()).appendProperties(cf.getProperties()).close();
            }
            UsernamePasswordIdentityConfig upi = c.getUsernamePasswordIdentityConfig();
            if (upi != null) {
                gen.node("username-password", null, "username", upi.getUsername(), "password", getOrMaskValue(upi.getPassword()));
            }
            TokenIdentityConfig ti = c.getTokenIdentityConfig();
            if (ti != null) {
                gen.node("token", getOrMaskValue(ti.getTokenEncoded()), "encoding", ti.getEncoding().toString());
            }
            kerberosIdentityGenerator(gen, c.getKerberosIdentityConfig());
            gen.close();
        }
        AccessControlServiceConfig acs = c.getAccessControlServiceConfig();
        if (acs != null) {
            factoryWithPropertiesXmlGenerator(gen, "access-control-service", acs);
        }
        gen.close();
    }

    private static void tlsAuthenticationGenerator(XmlGenerator gen, TlsAuthenticationConfig c) {
        if (c == null) {
            return;
        }
        XmlGenerator tlsGen = gen.open("tls", "roleAttribute", c.getRoleAttribute());
        addClusterLoginElements(tlsGen, c)
                .close();
    }

    private void ldapAuthenticationGenerator(XmlGenerator gen, LdapAuthenticationConfig c) {
        if (c == null) {
            return;
        }
        addClusterLoginElements(gen.open("ldap"), c)
                .node("url", c.getUrl())
                .nodeIfContents("socket-factory-class-name", c.getSocketFactoryClassName())
                .nodeIfContents("parse-dn", c.getParseDn())
                .nodeIfContents("role-context", c.getRoleContext())
                .nodeIfContents("role-filter", c.getRoleFilter())
                .nodeIfContents("role-mapping-attribute", c.getRoleMappingAttribute())
                .nodeIfContents("role-mapping-mode", c.getRoleMappingMode())
                .nodeIfContents("role-name-attribute", c.getRoleNameAttribute())
                .nodeIfContents("role-recursion-max-depth", c.getRoleRecursionMaxDepth())
                .nodeIfContents("role-search-scope", c.getRoleSearchScope())
                .nodeIfContents("user-name-attribute", c.getUserNameAttribute())
                .nodeIfContents("system-user-dn", c.getSystemUserDn())
                .nodeIfContents("system-user-password", getOrMaskValue(c.getSystemUserPassword()))
                .nodeIfContents("system-authentication", c.getSystemAuthentication())
                .nodeIfContents("security-realm", c.getSecurityRealm())
                .nodeIfContents("password-attribute", c.getPasswordAttribute())
                .nodeIfContents("user-context", c.getUserContext())
                .nodeIfContents("user-filter", c.getUserFilter())
                .nodeIfContents("user-search-scope", c.getUserSearchScope())
                .nodeIfContents("skip-authentication", c.getSkipAuthentication())
                .close();
    }

    private void kerberosAuthenticationGenerator(XmlGenerator gen, KerberosAuthenticationConfig c) {
        if (c == null) {
            return;
        }
        XmlGenerator kerberosGen = gen.open("kerberos");
        addClusterLoginElements(kerberosGen, c)
                .nodeIfContents("relax-flags-check", c.getRelaxFlagsCheck())
                .nodeIfContents("use-name-without-realm", c.getUseNameWithoutRealm())
                .nodeIfContents("security-realm", c.getSecurityRealm())
                .nodeIfContents("keytab-file", c.getKeytabFile())
                .nodeIfContents("principal", c.getPrincipal());
        ldapAuthenticationGenerator(kerberosGen, c.getLdapAuthenticationConfig());
        kerberosGen.close();
    }

    private void simpleAuthenticationGenerator(XmlGenerator gen, SimpleAuthenticationConfig c) {
        if (c == null) {
            return;
        }
        XmlGenerator simpleGen = gen.open("simple");
        addClusterLoginElements(simpleGen, c).nodeIfContents("role-separator", c.getRoleSeparator());
        for (String username : c.getUsernames()) {
            simpleGen.open("user", "username", username, "password", getOrMaskValue(c.getPassword(username)));
            for (String role : c.getRoles(username)) {
                simpleGen.node("role", role);
            }
            // close <user> node
            simpleGen.close();
        }
        simpleGen.close();
    }

    private static void kerberosIdentityGenerator(XmlGenerator gen, KerberosIdentityConfig c) {
        if (c == null) {
            return;
        }
        gen.open("kerberos")
                .nodeIfContents("realm", c.getRealm())
                .nodeIfContents("security-realm", c.getSecurityRealm())
                .nodeIfContents("keytab-file", c.getKeytabFile())
                .nodeIfContents("principal", c.getPrincipal())
                .nodeIfContents("service-name-prefix", c.getServiceNamePrefix())
                .nodeIfContents("spn", c.getSpn())
                .nodeIfContents("use-canonical-hostname", c.getUseCanonicalHostname())
                .close();
    }

    private static XmlGenerator addClusterLoginElements(XmlGenerator gen, AbstractClusterLoginConfig<?> c) {
        gen.nodeIfContents("skip-identity", c.getSkipIdentity());
        gen.nodeIfContents("skip-endpoint", c.getSkipEndpoint());
        gen.nodeIfContents("skip-role", c.getSkipRole());
        return gen;
    }

    private static void jaasAuthenticationGenerator(XmlGenerator gen, JaasAuthenticationConfig c) {
        if (c == null) {
            return;
        }
        appendLoginModules(gen, "jaas", c.getLoginModuleConfigs());
    }

    private static void appendSecurityPermissions(XmlGenerator gen, String tag, Set<PermissionConfig> cpc, Object... attributes) {
        final List<PermissionConfig.PermissionType> clusterPermTypes = asList(ALL, CONFIG, TRANSACTION);

        gen.open(tag, attributes);
        for (PermissionConfig p : cpc) {
            if (clusterPermTypes.contains(p.getType())) {
                gen.open(p.getType().getNodeName(), "principal", p.getPrincipal(), "deny", p.isDeny());
            } else {
                gen.open(p.getType().getNodeName(), "principal", p.getPrincipal(), "name", p.getName(), "deny", p.isDeny());
            }

            if (!p.getEndpoints().isEmpty()) {
                gen.open("endpoints");
                for (String endpoint : p.getEndpoints()) {
                    gen.node("endpoint", endpoint);
                }
                gen.close();
            }

            if (!p.getActions().isEmpty()) {
                gen.open("actions");
                for (String action : p.getActions()) {
                    gen.node("action", action);
                }
                gen.close();
            }
            gen.close();
        }
        gen.close();
    }

    private static void appendLoginModules(XmlGenerator gen, String tag, List<LoginModuleConfig> loginModuleConfigs) {
        gen.open(tag);
        for (LoginModuleConfig lm : loginModuleConfigs) {
            List<String> attrs = new ArrayList<>();
            attrs.add("class-name");
            attrs.add(lm.getClassName());

            if (lm.getUsage() != null) {
                attrs.add("usage");
                attrs.add(lm.getUsage().name());
            }
            gen.open("login-module", attrs.toArray())
                    .appendProperties(lm.getProperties())
                    .close();
        }
        gen.close();
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    private static void serializationXmlGenerator(XmlGenerator gen, Config config) {
        SerializationConfig c = config.getSerializationConfig();
        if (c == null) {
            return;
        }
        gen.open("serialization")
                .node("portable-version", c.getPortableVersion())
                .node("use-native-byte-order", c.isUseNativeByteOrder())
                .node("byte-order", c.getByteOrder())
                .node("enable-compression", c.isEnableCompression())
                .node("enable-shared-object", c.isEnableSharedObject())
                .node("allow-unsafe", c.isAllowUnsafe())
                .node("allow-override-default-serializers", c.isAllowOverrideDefaultSerializers());

        Map<Integer, String> dsfClasses = c.getDataSerializableFactoryClasses();
        Map<Integer, DataSerializableFactory> dsfImpls = c.getDataSerializableFactories();
        if (!MapUtil.isNullOrEmpty(dsfClasses) || !MapUtil.isNullOrEmpty(dsfImpls)) {
            gen.open("data-serializable-factories");
            appendSerializationFactory(gen, "data-serializable-factory", dsfClasses);
            appendSerializationFactory(gen, "data-serializable-factory", dsfImpls);
            gen.close();
        }

        Map<Integer, String> portableClasses = c.getPortableFactoryClasses();
        Map<Integer, PortableFactory> portableImpls = c.getPortableFactories();
        if (!MapUtil.isNullOrEmpty(portableClasses) || !MapUtil.isNullOrEmpty(portableImpls)) {
            gen.open("portable-factories");
            appendSerializationFactory(gen, "portable-factory", portableClasses);
            appendSerializationFactory(gen, "portable-factory", portableImpls);
            gen.close();
        }

        Collection<SerializerConfig> serializers = c.getSerializerConfigs();
        GlobalSerializerConfig globalSerializerConfig = c.getGlobalSerializerConfig();
        if (CollectionUtil.isNotEmpty(serializers) || globalSerializerConfig != null) {
            gen.open("serializers");

            if (globalSerializerConfig != null) {
                gen.node("global-serializer",
                        classNameOrImplClass(
                                globalSerializerConfig.getClassName(), globalSerializerConfig.getImplementation()),
                        "override-java-serialization", globalSerializerConfig.isOverrideJavaSerialization());
            }

            if (CollectionUtil.isNotEmpty(serializers)) {
                for (SerializerConfig serializer : serializers) {
                    gen.node("serializer", null,
                            "type-class", classNameOrClass(serializer.getTypeClassName(), serializer.getTypeClass()),
                            "class-name", classNameOrImplClass(serializer.getClassName(), serializer.getImplementation()));
                }
            }
            gen.close();
        }
        gen.node("check-class-def-errors", c.isCheckClassDefErrors());
        JavaSerializationFilterConfig javaSerializationFilterConfig = c.getJavaSerializationFilterConfig();
        if (javaSerializationFilterConfig != null) {
            gen.open("java-serialization-filter", "defaults-disabled", javaSerializationFilterConfig.isDefaultsDisabled());
            appendFilterList(gen, "blacklist", javaSerializationFilterConfig.getBlacklist());
            appendFilterList(gen, "whitelist", javaSerializationFilterConfig.getWhitelist());
            gen.close();
        }

        ConfigXmlGeneratorHelper.compactSerialization(gen, c.getCompactSerializationConfig());

        // close serialization
        gen.close();
    }

    private static String classNameOrClass(String className, Class<?> clazz) {
        return !isNullOrEmpty(className) ? className
                : clazz != null ? clazz.getName()
                : null;
    }

    private static void partitionGroupXmlGenerator(XmlGenerator gen, Config config) {
        PartitionGroupConfig pg = config.getPartitionGroupConfig();
        if (pg == null) {
            return;
        }
        gen.open("partition-group", "enabled", pg.isEnabled(), "group-type", pg.getGroupType());

        Collection<MemberGroupConfig> configs = pg.getMemberGroupConfigs();
        if (CollectionUtil.isNotEmpty(configs)) {
            for (MemberGroupConfig mgConfig : configs) {
                gen.open("member-group");
                for (String iface : mgConfig.getInterfaces()) {
                    gen.node("interface", iface);
                }
                gen.close();
            }
        }
        gen.close();
    }

    private void networkConfigXmlGenerator(XmlGenerator gen, Config config) {
        if (config.getAdvancedNetworkConfig().isEnabled()) {
            return;
        }

        NetworkConfig netCfg = config.getNetworkConfig();
        gen.open("network")
                .node("public-address", netCfg.getPublicAddress())
                .node("port", netCfg.getPort(),
                        "port-count", netCfg.getPortCount(),
                        "auto-increment", netCfg.isPortAutoIncrement())
                .node("reuse-address", netCfg.isReuseAddress());

        Collection<String> outboundPortDefinitions = netCfg.getOutboundPortDefinitions();
        if (CollectionUtil.isNotEmpty(outboundPortDefinitions)) {
            gen.open("outbound-ports");
            for (String def : outboundPortDefinitions) {
                gen.node("ports", def);
            }
            gen.close();
        }

        JoinConfig join = netCfg.getJoin();
        gen.open("join");
        autoDetectionConfigXmlGenerator(gen, join);
        multicastConfigXmlGenerator(gen, join);
        tcpIpConfigXmlGenerator(gen, join);
        aliasedDiscoveryConfigsGenerator(gen, aliasedDiscoveryConfigsFrom(join));
        discoveryStrategyConfigXmlGenerator(gen, join.getDiscoveryConfig());
        gen.close();

        interfacesConfigXmlGenerator(gen, netCfg.getInterfaces());
        sslConfigXmlGenerator(gen, netCfg.getSSLConfig());
        socketInterceptorConfigXmlGenerator(gen, netCfg.getSocketInterceptorConfig());
        symmetricEncInterceptorConfigXmlGenerator(gen, netCfg.getSymmetricEncryptionConfig());
        memberAddressProviderConfigXmlGenerator(gen, netCfg.getMemberAddressProviderConfig());
        failureDetectorConfigXmlGenerator(gen, netCfg.getIcmpFailureDetectorConfig());
        restApiXmlGenerator(gen, netCfg);
        memcacheProtocolXmlGenerator(gen, netCfg);
        tpcSocketConfigXmlGenerator(gen, netCfg.getTpcSocketConfig());
        gen.close();
    }

    private void advancedNetworkConfigXmlGenerator(XmlGenerator gen, Config config) {
        AdvancedNetworkConfig netCfg = config.getAdvancedNetworkConfig();
        if (!netCfg.isEnabled()) {
            return;
        }

        gen.open("advanced-network", "enabled", netCfg.isEnabled());

        JoinConfig join = netCfg.getJoin();
        gen.open("join");
        autoDetectionConfigXmlGenerator(gen, join);
        multicastConfigXmlGenerator(gen, join);
        tcpIpConfigXmlGenerator(gen, join);
        aliasedDiscoveryConfigsGenerator(gen, aliasedDiscoveryConfigsFrom(join));
        discoveryStrategyConfigXmlGenerator(gen, join.getDiscoveryConfig());
        gen.close();

        failureDetectorConfigXmlGenerator(gen, netCfg.getIcmpFailureDetectorConfig());
        memberAddressProviderConfigXmlGenerator(gen, netCfg.getMemberAddressProviderConfig());
        for (EndpointConfig endpointConfig : netCfg.getEndpointConfigs().values()) {
            endpointConfigXmlGenerator(gen, endpointConfig);
        }
        gen.close();
    }

    private void endpointConfigXmlGenerator(XmlGenerator gen, EndpointConfig endpointConfig) {
        if (endpointConfig.getName() != null) {
            gen.open(endpointConfigElementName(endpointConfig), "name", endpointConfig.getName());
        } else {
            gen.open(endpointConfigElementName(endpointConfig));
        }

        Collection<String> outboundPortDefinitions = endpointConfig.getOutboundPortDefinitions();
        if (CollectionUtil.isNotEmpty(outboundPortDefinitions)) {
            gen.open("outbound-ports");
            for (String def : outboundPortDefinitions) {
                gen.node("ports", def);
            }
            gen.close();
        }

        interfacesConfigXmlGenerator(gen, endpointConfig.getInterfaces());
        sslConfigXmlGenerator(gen, endpointConfig.getSSLConfig());
        socketInterceptorConfigXmlGenerator(gen, endpointConfig.getSocketInterceptorConfig());
        symmetricEncInterceptorConfigXmlGenerator(gen, endpointConfig.getSymmetricEncryptionConfig());

        if (endpointConfig instanceof RestServerEndpointConfig rsec) {
            gen.open("endpoint-groups");
            for (RestEndpointGroup group : RestEndpointGroup.values()) {
                gen.node("endpoint-group", null, "name", group.name(),
                        "enabled", rsec.isGroupEnabled(group));
            }
            gen.close();
        }

        // socket-options
        gen.open("socket-options");
        gen.node("buffer-direct", endpointConfig.isSocketBufferDirect());
        gen.node("tcp-no-delay", endpointConfig.isSocketTcpNoDelay());
        gen.node("keep-alive", endpointConfig.isSocketKeepAlive());
        gen.node("connect-timeout-seconds", endpointConfig.getSocketConnectTimeoutSeconds());
        gen.node("send-buffer-size-kb", endpointConfig.getSocketSendBufferSizeKb());
        gen.node("receive-buffer-size-kb", endpointConfig.getSocketRcvBufferSizeKb());
        gen.node("linger-seconds", endpointConfig.getSocketLingerSeconds());
        gen.node("keep-idle-seconds", endpointConfig.getSocketKeepIdleSeconds());
        gen.node("keep-interval-seconds", endpointConfig.getSocketKeepIntervalSeconds());
        gen.node("keep-count", endpointConfig.getSocketKeepCount());
        gen.close();

        if (endpointConfig instanceof ServerSocketEndpointConfig serverSocketEndpointConfig) {
            gen.node("port", serverSocketEndpointConfig.getPort(),
                    "port-count", serverSocketEndpointConfig.getPortCount(),
                    "auto-increment", serverSocketEndpointConfig.isPortAutoIncrement())
                    .node("public-address", serverSocketEndpointConfig.getPublicAddress())
                    .node("reuse-address", serverSocketEndpointConfig.isReuseAddress());
        }

        tpcSocketConfigXmlGenerator(gen, endpointConfig.getTpcSocketConfig());
        gen.close();
    }

    public static String endpointConfigElementName(EndpointConfig endpointConfig) {
        if (endpointConfig instanceof ServerSocketEndpointConfig) {
            switch (endpointConfig.getProtocolType()) {
                case REST:
                    return "rest-server-socket-endpoint-config";
                case WAN:
                    return "wan-server-socket-endpoint-config";
                case CLIENT:
                    return "client-server-socket-endpoint-config";
                case MEMBER:
                    return "member-server-socket-endpoint-config";
                case MEMCACHE:
                    return "memcache-server-socket-endpoint-config";
                default:
                    throw new IllegalStateException("Not recognised protocol type");
            }
        }

        return "wan-endpoint-config";
    }

    private static void localDeviceConfigXmlGenerator(XmlGenerator gen, Config config) {
        config.getDeviceConfigs().values().stream()
                .filter(DeviceConfig::isLocal)
                .forEach(deviceConfig -> {
                    LocalDeviceConfig localDeviceConfig = (LocalDeviceConfig) deviceConfig;
                    Capacity capacity = localDeviceConfig.getCapacity();
                    gen.open("local-device", "name", localDeviceConfig.getName())
                            .node("base-dir", localDeviceConfig.getBaseDir().getAbsolutePath())
                            .node("capacity", null,
                                    "unit", capacity.getUnit(), "value", capacity.getValue())
                            .node("block-size", localDeviceConfig.getBlockSize())
                            .node("read-io-thread-count", localDeviceConfig.getReadIOThreadCount())
                            .node("write-io-thread-count", localDeviceConfig.getWriteIOThreadCount())
                            .close();
                });
    }

    private static void autoDetectionConfigXmlGenerator(XmlGenerator gen, JoinConfig join) {
        gen.open("auto-detection", "enabled", join.getAutoDetectionConfig().isEnabled()).close();
    }

    private static void multicastConfigXmlGenerator(XmlGenerator gen, JoinConfig join) {
        MulticastConfig mcConfig = join.getMulticastConfig();
        gen.open("multicast", "enabled", mcConfig.isEnabled(), "loopbackModeEnabled", mcConfig.getLoopbackModeEnabled())
                .node("multicast-group", mcConfig.getMulticastGroup())
                .node("multicast-port", mcConfig.getMulticastPort())
                .node("multicast-timeout-seconds", mcConfig.getMulticastTimeoutSeconds())
                .node("multicast-time-to-live", mcConfig.getMulticastTimeToLive());

        trustedInterfacesXmlGenerator(gen, mcConfig.getTrustedInterfaces());
        gen.close();
    }

    private static void trustedInterfacesXmlGenerator(XmlGenerator gen, Set<String> trustedInterfaces) {
        if (!trustedInterfaces.isEmpty()) {
            gen.open("trusted-interfaces");
            for (String trustedInterface : trustedInterfaces) {
                gen.node("interface", trustedInterface);
            }
            gen.close();
        }
    }

    public static void tcpIpConfigXmlGenerator(XmlGenerator gen, JoinConfig join) {
        TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
        gen.open("tcp-ip", "enabled", tcpIpConfig.isEnabled(),
                "connection-timeout-seconds", tcpIpConfig.getConnectionTimeoutSeconds());
        gen.open("member-list");
        for (String m : tcpIpConfig.getMembers()) {
            gen.node("member", m);
        }
        // </member-list>
        gen.close();
        gen.node("required-member", tcpIpConfig.getRequiredMember());
        // </tcp-ip>
        gen.close();
    }

    private static void interfacesConfigXmlGenerator(XmlGenerator gen, InterfacesConfig interfaces) {
        gen.open("interfaces", "enabled", interfaces.isEnabled());
        for (String i : interfaces.getInterfaces()) {
            gen.node("interface", i);
        }
        gen.close();
    }

    private void sslConfigXmlGenerator(XmlGenerator gen, SSLConfig ssl) {
        if (ssl != null) {
            ssl = new SSLConfig(ssl);
            String factoryClassName = classNameOrImplClass(ssl.getFactoryClassName(), ssl.getFactoryImplementation());
            if (factoryClassName != null) {
                ssl.setFactoryClassName(factoryClassName);
            }
            Properties props = ssl.getProperties();

            if (maskSensitiveFields && props.containsKey("trustStorePassword")) {
                props.setProperty("trustStorePassword", MASK_FOR_SENSITIVE_DATA);
            }

            if (maskSensitiveFields && props.containsKey("keyStorePassword")) {
                props.setProperty("keyStorePassword", MASK_FOR_SENSITIVE_DATA);
            }
        }

        factoryWithPropertiesXmlGenerator(gen, "ssl", ssl);
    }

    protected void factoryWithPropertiesXmlGenerator(XmlGenerator gen, String elementName,
                                                     AbstractBaseFactoryWithPropertiesConfig<?> factoryWithProps) {
        if (factoryWithProps instanceof AbstractFactoryWithPropertiesConfig cfgWithEnabled) {
            gen.open(elementName, "enabled", cfgWithEnabled.isEnabled());
        } else {
            gen.open(elementName);
        }
        if (factoryWithProps != null) {
            gen.node("factory-class-name", factoryWithProps.getFactoryClassName())
                    .appendProperties(factoryWithProps.getProperties());
        }
        gen.close();
    }

    private static void socketInterceptorConfigXmlGenerator(XmlGenerator gen, SocketInterceptorConfig socket) {
        gen.open("socket-interceptor", "enabled", socket != null && socket.isEnabled());
        if (socket != null) {
            gen.node("class-name", classNameOrImplClass(socket.getClassName(), socket.getImplementation()))
                    .appendProperties(socket.getProperties());
        }
        gen.close();
    }

    private void commonSymmetricEncInterceptorConfigXmlBodyGenerator(XmlGenerator gen,
                                                                     AbstractSymmetricEncryptionConfig sec) {
        if (sec == null) {
            return;
        }
        gen.node("algorithm", sec.getAlgorithm())
                .node("salt", getOrMaskValue(sec.getSalt()));
    }

    private void symmetricEncInterceptorConfigXmlGenerator(XmlGenerator gen, SymmetricEncryptionConfig sec) {
        if (sec == null) {
            return;
        }
        gen.open("symmetric-encryption", "enabled", sec.isEnabled());
        commonSymmetricEncInterceptorConfigXmlBodyGenerator(gen, sec);
        gen.node("password", getOrMaskValue(sec.getPassword()))
                .node("iteration-count", sec.getIterationCount());
        gen.close();
    }

    private static void memberAddressProviderConfigXmlGenerator(XmlGenerator gen,
                                                                MemberAddressProviderConfig memberAddressProviderConfig) {
        if (memberAddressProviderConfig == null) {
            return;
        }
        String className = classNameOrImplClass(memberAddressProviderConfig.getClassName(),
                memberAddressProviderConfig.getImplementation());
        if (isNullOrEmpty(className)) {
            return;
        }
        gen.open("member-address-provider", "enabled", memberAddressProviderConfig.isEnabled())
                .node("class-name", className)
                .appendProperties(memberAddressProviderConfig.getProperties())
                .close();
    }

    private static void failureDetectorConfigXmlGenerator(XmlGenerator gen,
                                                          IcmpFailureDetectorConfig icmpFailureDetectorConfig) {
        if (icmpFailureDetectorConfig == null) {
            return;
        }

        gen.open("failure-detector");
        gen.open("icmp", "enabled", icmpFailureDetectorConfig.isEnabled())
                .node("ttl", icmpFailureDetectorConfig.getTtl())
                .node("interval-milliseconds", icmpFailureDetectorConfig.getIntervalMilliseconds())
                .node("max-attempts", icmpFailureDetectorConfig.getMaxAttempts())
                .node("timeout-milliseconds", icmpFailureDetectorConfig.getTimeoutMilliseconds())
                .node("fail-fast-on-startup", icmpFailureDetectorConfig.isFailFastOnStartup())
                .node("parallel-mode", icmpFailureDetectorConfig.isParallelMode())
                .close();
        gen.close();
    }

    private void persistenceXmlGenerator(XmlGenerator gen, Config config) {
        PersistenceConfig prCfg = config.getPersistenceConfig();
        if (prCfg == null) {
            gen.node("persistence", "enabled", "false");
            return;
        }
        gen.open("persistence", "enabled", prCfg.isEnabled())
                .node("base-dir", prCfg.getBaseDir().getAbsolutePath());
        if (prCfg.getBackupDir() != null) {
            gen.node("backup-dir", prCfg.getBackupDir().getAbsolutePath());
        }
        gen.node("parallelism", prCfg.getParallelism())
                .node("validation-timeout-seconds", prCfg.getValidationTimeoutSeconds())
                .node("data-load-timeout-seconds", prCfg.getDataLoadTimeoutSeconds())
                .node("cluster-data-recovery-policy", prCfg.getClusterDataRecoveryPolicy())
                .node("auto-remove-stale-data", prCfg.isAutoRemoveStaleData())
                .node("rebalance-delay-seconds", prCfg.getRebalanceDelaySeconds());

        encryptionAtRestXmlGenerator(gen, prCfg.getEncryptionAtRestConfig());
        gen.close();
    }

    private void dynamicConfigurationXmlGenerator(XmlGenerator gen, Config config) {
        DynamicConfigurationConfig dynamicConfigurationConfig = config.getDynamicConfigurationConfig();

        gen.open("dynamic-configuration")
                .node("persistence-enabled", dynamicConfigurationConfig.isPersistenceEnabled());

        if (dynamicConfigurationConfig.getBackupDir() != null) {
            gen.node("backup-dir", dynamicConfigurationConfig.getBackupDir().getAbsolutePath());
        }

        gen.node("backup-count", dynamicConfigurationConfig.getBackupCount());
        gen.close();
    }

    private void encryptionAtRestXmlGenerator(XmlGenerator gen, EncryptionAtRestConfig encryptionAtRestConfig) {
        if (encryptionAtRestConfig == null) {
            gen.node("encryption-at-rest", "enabled", "false");
            return;
        }
        gen.open("encryption-at-rest", "enabled", encryptionAtRestConfig.isEnabled())
                .node("key-size", encryptionAtRestConfig.getKeySize());
        commonSymmetricEncInterceptorConfigXmlBodyGenerator(gen, encryptionAtRestConfig);
        secureStoreXmlGenerator(gen, encryptionAtRestConfig.getSecureStoreConfig());
        gen.close();
    }

    private void secureStoreXmlGenerator(XmlGenerator gen, SecureStoreConfig secureStoreConfig) {
        if (secureStoreConfig != null) {
            gen.open("secure-store");
            if (secureStoreConfig instanceof JavaKeyStoreSecureStoreConfig config) {
                javaKeyStoreSecureStoreXmlGenerator(gen, config);
            } else if (secureStoreConfig instanceof VaultSecureStoreConfig config) {
                vaultSecureStoreXmlGenerator(gen, config);
            }
            gen.close();
        }
    }

    private void javaKeyStoreSecureStoreXmlGenerator(XmlGenerator gen, JavaKeyStoreSecureStoreConfig secureStoreConfig) {
        gen.open("keystore")
                .node("path", secureStoreConfig.getPath().getAbsolutePath())
                .node("type", secureStoreConfig.getType())
                .node("password", getOrMaskValue(secureStoreConfig.getPassword()))
                .node("polling-interval", secureStoreConfig.getPollingInterval())
                .node("current-key-alias", secureStoreConfig.getCurrentKeyAlias());
        gen.close();
    }

    private void vaultSecureStoreXmlGenerator(XmlGenerator gen, VaultSecureStoreConfig secureStoreConfig) {
        gen.open("vault")
                .node("address", secureStoreConfig.getAddress())
                .node("secret-path", secureStoreConfig.getSecretPath())
                .node("token", getOrMaskValue(secureStoreConfig.getToken()))
                .node("polling-interval", secureStoreConfig.getPollingInterval());
        sslConfigXmlGenerator(gen, secureStoreConfig.getSSLConfig());
        gen.close();
    }

    private static void crdtReplicationXmlGenerator(XmlGenerator gen, Config config) {
        CRDTReplicationConfig replicationConfig = config.getCRDTReplicationConfig();
        gen.open("crdt-replication");
        if (replicationConfig != null) {
            gen.node("replication-period-millis", replicationConfig.getReplicationPeriodMillis())
                    .node("max-concurrent-replication-targets", replicationConfig.getMaxConcurrentReplicationTargets());
        }
        gen.close();
    }

    private static void splitBrainProtectionXmlGenerator(XmlGenerator gen, Config config) {
        for (SplitBrainProtectionConfig splitBrainProtectionConfig : config.getSplitBrainProtectionConfigs().values()) {
            gen.open("split-brain-protection", "name", splitBrainProtectionConfig.getName(),
                    "enabled", splitBrainProtectionConfig.isEnabled())
                    .node("minimum-cluster-size", splitBrainProtectionConfig.getMinimumClusterSize())
                    .node("protect-on", splitBrainProtectionConfig.getProtectOn());
            if (!splitBrainProtectionConfig.getListenerConfigs().isEmpty()) {
                gen.open("listeners");
                for (SplitBrainProtectionListenerConfig listenerConfig : splitBrainProtectionConfig.getListenerConfigs()) {
                    gen.node("listener", classNameOrImplClass(listenerConfig.getClassName(),
                            listenerConfig.getImplementation()));
                }
                gen.close();
            }
            handleSplitBrainProtectionFunction(gen, splitBrainProtectionConfig);
            gen.close();
        }
    }

    private static void cpSubsystemConfig(XmlGenerator gen, Config config) {
        CPSubsystemConfig cpSubsystemConfig = config.getCPSubsystemConfig();
        gen.open("cp-subsystem")
                .node("cp-member-count", cpSubsystemConfig.getCPMemberCount())
                .node("group-size", cpSubsystemConfig.getGroupSize())
                .node("session-time-to-live-seconds", cpSubsystemConfig.getSessionTimeToLiveSeconds())
                .node("session-heartbeat-interval-seconds", cpSubsystemConfig.getSessionHeartbeatIntervalSeconds())
                .node("missing-cp-member-auto-removal-seconds", cpSubsystemConfig.getMissingCPMemberAutoRemovalSeconds())
                .node("fail-on-indeterminate-operation-state", cpSubsystemConfig.isFailOnIndeterminateOperationState())
                .node("persistence-enabled", cpSubsystemConfig.isPersistenceEnabled())
                .node("base-dir", cpSubsystemConfig.getBaseDir().getAbsolutePath())
                .node("data-load-timeout-seconds", cpSubsystemConfig.getDataLoadTimeoutSeconds())
                .node("cp-member-priority", cpSubsystemConfig.getCPMemberPriority())
                .node("map-limit", cpSubsystemConfig.getCPMapLimit());

        RaftAlgorithmConfig raftAlgorithmConfig = cpSubsystemConfig.getRaftAlgorithmConfig();
        gen.open("raft-algorithm")
                .node("leader-election-timeout-in-millis", raftAlgorithmConfig.getLeaderElectionTimeoutInMillis())
                .node("leader-heartbeat-period-in-millis", raftAlgorithmConfig.getLeaderHeartbeatPeriodInMillis())
                .node("max-missed-leader-heartbeat-count", raftAlgorithmConfig.getMaxMissedLeaderHeartbeatCount())
                .node("append-request-max-entry-count", raftAlgorithmConfig.getAppendRequestMaxEntryCount())
                .node("commit-index-advance-count-to-snapshot", raftAlgorithmConfig.getCommitIndexAdvanceCountToSnapshot())
                .node("uncommitted-entry-count-to-reject-new-appends",
                        raftAlgorithmConfig.getUncommittedEntryCountToRejectNewAppends())
                .node("append-request-backoff-timeout-in-millis", raftAlgorithmConfig.getAppendRequestBackoffTimeoutInMillis())
                .close();

        gen.open("semaphores");

        for (SemaphoreConfig semaphoreConfig : cpSubsystemConfig.getSemaphoreConfigs().values()) {
            gen.open("semaphore")
                    .node("name", semaphoreConfig.getName())
                    .node("jdk-compatible", semaphoreConfig.isJDKCompatible())
                    .node("initial-permits", semaphoreConfig.getInitialPermits())
                    .close();
        }

        gen.close().open("locks");

        for (FencedLockConfig lockConfig : cpSubsystemConfig.getLockConfigs().values()) {
            gen.open("fenced-lock")
                    .node("name", lockConfig.getName())
                    .node("lock-acquire-limit", lockConfig.getLockAcquireLimit())
                    .close();
        }

        gen.close().open("maps");

        for (CPMapConfig cpMapConfig : cpSubsystemConfig.getCpMapConfigs().values()) {
            gen.open("map")
               .node("name", cpMapConfig.getName())
               .node("max-size-mb", cpMapConfig.getMaxSizeMb())
               .close();
        }

        gen.close().close();
    }

    private static void instanceTrackingConfig(XmlGenerator gen, Config config) {
        InstanceTrackingConfig trackingConfig = config.getInstanceTrackingConfig();
        gen.open("instance-tracking", "enabled", trackingConfig.isEnabled())
                .node("file-name", trackingConfig.getFileName())
                .node("format-pattern", trackingConfig.getFormatPattern())
                .close();
    }

    private static void metricsConfig(XmlGenerator gen, Config config) {
        MetricsConfig metricsConfig = config.getMetricsConfig();
        gen.open("metrics", "enabled", metricsConfig.isEnabled())
                .open("management-center", "enabled", metricsConfig.getManagementCenterConfig().isEnabled())
                .node("retention-seconds", metricsConfig.getManagementCenterConfig().getRetentionSeconds())
                .close()
                .open("jmx", "enabled", metricsConfig.getJmxConfig().isEnabled())
                .close()
                .node("collection-frequency-seconds", metricsConfig.getCollectionFrequencySeconds())
                .close();
    }

    private static void sqlConfig(XmlGenerator gen, Config config) {
        SqlConfig sqlConfig = config.getSqlConfig();
        JavaSerializationFilterConfig filterConfig = sqlConfig.getJavaReflectionFilterConfig();
        gen.open("sql")
                .node("statement-timeout-millis", sqlConfig.getStatementTimeoutMillis())
                .node("catalog-persistence-enabled", sqlConfig.isCatalogPersistenceEnabled());
        if (filterConfig != null) {
            gen.open("java-reflection-filter", "defaults-disabled", filterConfig.isDefaultsDisabled());
            appendFilterList(gen, "blacklist", filterConfig.getBlacklist());
            appendFilterList(gen, "whitelist", filterConfig.getWhitelist());
            gen.close();
        }
        gen.close();
    }

    private static void jetConfig(XmlGenerator gen, Config config) {
        JetConfig jetConfig = config.getJetConfig();
        EdgeConfig edgeConfig = jetConfig.getDefaultEdgeConfig();
        gen.open("jet", "enabled", jetConfig.isEnabled(), "resource-upload-enabled", jetConfig.isResourceUploadEnabled())
                .node("cooperative-thread-count", jetConfig.getCooperativeThreadCount())
                .node("flow-control-period", jetConfig.getFlowControlPeriodMs())
                .node("backup-count", jetConfig.getBackupCount())
                .node("scale-up-delay-millis", jetConfig.getScaleUpDelayMillis())
                .node("lossless-restart-enabled", jetConfig.isLosslessRestartEnabled())
                .node("max-processor-accumulated-records", jetConfig.getMaxProcessorAccumulatedRecords())
                .open("edge-defaults")
                    .node("queue-size", edgeConfig.getQueueSize())
                    .node("packet-size-limit", edgeConfig.getPacketSizeLimit())
                    .node("receive-window-multiplier", edgeConfig.getReceiveWindowMultiplier())
                .close()
            .close();
    }

    private static void userCodeDeploymentConfig(XmlGenerator gen, Config config) {
        UserCodeDeploymentConfig ucdConfig = config.getUserCodeDeploymentConfig();
        gen.open("user-code-deployment", "enabled", ucdConfig.isEnabled())
                .node("class-cache-mode", ucdConfig.getClassCacheMode())
                .node("provider-mode", ucdConfig.getProviderMode())
                .node("blacklist-prefixes", ucdConfig.getBlacklistedPrefixes())
                .node("whitelist-prefixes", ucdConfig.getWhitelistedPrefixes())
                .node("provider-filter", ucdConfig.getProviderFilter())
                .close();
    }

    private static void handleSplitBrainProtectionFunction(XmlGenerator gen,
                                                           SplitBrainProtectionConfig splitBrainProtectionConfig) {
        if (splitBrainProtectionConfig.
                getFunctionImplementation() instanceof ProbabilisticSplitBrainProtectionFunction qf) {
            long acceptableHeartbeatPause = qf.getAcceptableHeartbeatPauseMillis();
            double threshold = qf.getSuspicionThreshold();
            int maxSampleSize = qf.getMaxSampleSize();
            long minStdDeviation = qf.getMinStdDeviationMillis();
            long firstHeartbeatEstimate = qf.getHeartbeatIntervalMillis();
            gen.open("probabilistic-split-brain-protection", "acceptable-heartbeat-pause-millis", acceptableHeartbeatPause,
                    "suspicion-threshold", threshold,
                    "max-sample-size", maxSampleSize,
                    "min-std-deviation-millis", minStdDeviation,
                    "heartbeat-interval-millis", firstHeartbeatEstimate);
            gen.close();
        } else if (splitBrainProtectionConfig.
                getFunctionImplementation() instanceof RecentlyActiveSplitBrainProtectionFunction qf) {
            gen.open("recently-active-split-brain-protection", "heartbeat-tolerance-millis",
                    qf.getHeartbeatToleranceMillis());
            gen.close();
        } else {
            gen.node("function-class-name",
                    classNameOrImplClass(splitBrainProtectionConfig.getFunctionClassName(),
                            splitBrainProtectionConfig.getFunctionImplementation()));
        }
    }

    private static void nativeMemoryXmlGenerator(XmlGenerator gen, Config config) {
        NativeMemoryConfig nativeMemoryConfig = config.getNativeMemoryConfig();
        if (nativeMemoryConfig == null) {
            gen.node("native-memory", null, "enabled", "false");
            return;
        }
        gen.open("native-memory",
                "enabled", nativeMemoryConfig.isEnabled(),
                "allocator-type", nativeMemoryConfig.getAllocatorType())
                .node("capacity", null,
                        "unit", nativeMemoryConfig.getCapacity().getUnit(),
                        "value", nativeMemoryConfig.getCapacity().getValue())
                .node("min-block-size", nativeMemoryConfig.getMinBlockSize())
                .node("page-size", nativeMemoryConfig.getPageSize())
                .node("metadata-space-percentage", nativeMemoryConfig.getMetadataSpacePercentage());

        PersistentMemoryConfig pmemConfig = nativeMemoryConfig.getPersistentMemoryConfig();
        List<PersistentMemoryDirectoryConfig> directoryConfigs = pmemConfig.getDirectoryConfigs();
        gen.open("persistent-memory",
                "enabled", pmemConfig.isEnabled(),
                "mode", pmemConfig.getMode().name()
        );
        if (!directoryConfigs.isEmpty()) {
            gen.open("directories");
            for (PersistentMemoryDirectoryConfig dirConfig : directoryConfigs) {
                if (dirConfig.isNumaNodeSet()) {
                    gen.node("directory", dirConfig.getDirectory(),
                            "numa-node", dirConfig.getNumaNode());
                } else {
                    gen.node("directory", dirConfig.getDirectory());
                }
            }
            gen.close();
        }
        gen.close().close();
    }

    private static void liteMemberXmlGenerator(XmlGenerator gen, Config config) {
        gen.node("lite-member", null, "enabled", config.isLiteMember());
    }

    private static void restApiXmlGenerator(XmlGenerator gen, NetworkConfig config) {
        RestApiConfig c = config.getRestApiConfig();
        if (c == null) {
            return;
        }
        gen.open("rest-api", "enabled", c.isEnabled());
        for (RestEndpointGroup group : RestEndpointGroup.values()) {
            gen.node("endpoint-group", null, "name", group.name(), "enabled", c.isGroupEnabled(group));
        }
        gen.close();
    }

    private static void memcacheProtocolXmlGenerator(XmlGenerator gen, NetworkConfig config) {
        MemcacheProtocolConfig c = config.getMemcacheProtocolConfig();
        if (c == null) {
            return;
        }
        gen.node("memcache-protocol", null, "enabled", c.isEnabled());
    }

    private static void tpcSocketConfigXmlGenerator(XmlGenerator gen, TpcSocketConfig tpcSocketConfig) {
        gen.open("tpc-socket")
                .node("port-range", tpcSocketConfig.getPortRange())
                .node("receive-buffer-size-kb", tpcSocketConfig.getReceiveBufferSizeKB())
                .node("send-buffer-size-kb", tpcSocketConfig.getSendBufferSizeKB())
                .close();
    }

    private static void appendSerializationFactory(XmlGenerator gen, String elementName, Map<Integer, ?> factoryMap) {
        if (MapUtil.isNullOrEmpty(factoryMap)) {
            return;
        }
        for (Map.Entry<Integer, ?> factory : factoryMap.entrySet()) {
            Object value = factory.getValue();
            String className = value instanceof String s ? s : value.getClass().getName();
            gen.node(elementName, className, "factory-id", factory.getKey().toString());
        }
    }

    private static void appendFilterList(XmlGenerator gen, String listName, ClassFilter classFilterList) {
        if (classFilterList.isEmpty()) {
            return;
        }
        gen.open(listName);
        for (String className : classFilterList.getClasses()) {
            gen.node("class", className);
        }
        for (String packageName : classFilterList.getPackages()) {
            gen.node("package", packageName);
        }
        for (String prefix : classFilterList.getPrefixes()) {
            gen.node("prefix", prefix);
        }
        gen.close();
    }

    private static void integrityCheckerXmlGenerator(final XmlGenerator gen, final Config config) {
        gen.node(
                "integrity-checker",
                null,
                "enabled",
                config.getIntegrityCheckerConfig().isEnabled()
        );
    }

    private static void dataConnectionConfiguration(final XmlGenerator gen, final Config config) {
        for (DataConnectionConfig dataConnectionConfig : config.getDataConnectionConfigs().values()) {
            gen.open(
                            "data-connection",
                            "name",
                            dataConnectionConfig.getName()
                    )
                    .node("type", dataConnectionConfig.getType())
                    .node("shared", dataConnectionConfig.isShared())
                    .appendProperties(dataConnectionConfig.getProperties())
                    .close();
        }
    }

    private static void tpcConfiguration(final XmlGenerator gen, final Config config) {
        TpcConfig tpcConfig = config.getTpcConfig();
        gen.open("tpc", "enabled", tpcConfig.isEnabled())
                .node("eventloop-count", tpcConfig.getEventloopCount())
                .close();
    }

    public static void namespacesConfiguration(XmlGenerator gen, Config config) {
        UserCodeNamespacesConfig userCodeNamespacesConfig = config.getNamespacesConfig();
        if (userCodeNamespacesConfig == null) {
            return;
        }
        gen.open("user-code-namespaces", "enabled", userCodeNamespacesConfig.isEnabled());
        JavaSerializationFilterConfig filterConfig = userCodeNamespacesConfig.getClassFilterConfig();
        if (filterConfig != null) {
            gen.open("class-filter", "defaults-disabled", filterConfig.isDefaultsDisabled());
            appendFilterList(gen, "blacklist", filterConfig.getBlacklist());
            appendFilterList(gen, "whitelist", filterConfig.getWhitelist());
            gen.close();
        }

        namespaceConfigurations(gen, config);
        gen.close();
    }


    private void restServerConfiguration(final XmlGenerator gen, final Config config) {
        RestConfig restConfig = config.getRestConfig();
        gen.open("rest", "enabled", restConfig.isEnabled())
                .node("port", restConfig.getPort())
                .node("security-realm", restConfig.getSecurityRealm())
                .node("token-validity-seconds", restConfig.getTokenValidityDuration().toSeconds())
                .node("request-timeout-seconds", restConfig.getRequestTimeoutDuration().toSeconds())
                .node("max-login-attempts", restConfig.getMaxLoginAttempts())
                .node("lockout-duration-seconds", restConfig.getLockoutDuration().toSeconds());
        restServerSslConfiguration(gen, restConfig.getSsl());
        gen.close();
    }

    private void restServerSslConfiguration(XmlGenerator gen, RestConfig.Ssl ssl) {
        gen.open("ssl", "enabled", ssl.isEnabled())
                .node("client-auth", ssl.getClientAuth().name())
                .node("ciphers", ssl.getCiphers())
                .node("enabled-protocols", ssl.getEnabledProtocols())
                .node("key-alias", ssl.getKeyAlias())
                .node("key-password", getOrMaskValue(ssl.getKeyPassword()))
                .node("key-store", ssl.getKeyStore())
                .node("key-store-password", getOrMaskValue(ssl.getKeyStorePassword()))
                .node("key-store-type", ssl.getKeyStoreType())
                .node("key-store-provider", ssl.getKeyStoreProvider())
                .node("trust-store", ssl.getTrustStore())
                .node("trust-store-password", getOrMaskValue(ssl.getTrustStorePassword()))
                .node("trust-store-type", ssl.getTrustStoreType())
                .node("trust-store-provider", ssl.getTrustStoreProvider())
                .node("protocol", ssl.getProtocol())
                .node("certificate", ssl.getCertificate())
                .node("certificate-key", ssl.getCertificatePrivateKey())
                .node("trust-certificate", ssl.getTrustCertificate())
                .node("trust-certificate-key", ssl.getTrustCertificatePrivateKey())
                .close();
    }

    private void memberAttributesXmlGenerator(XmlGenerator gen, Config config) {
        MemberAttributeConfig memberAttributeConfig = config.getMemberAttributeConfig();
        if (!memberAttributeConfig.getAttributes().isEmpty()) {
            gen.open("member-attributes");
            for (Map.Entry<String, String> attribute : memberAttributeConfig.getAttributes().entrySet()) {
                gen.node("attribute", attribute.getValue(), "name", attribute.getKey());
            }
            gen.close();
        }
    }

    public static void namespaceConfigurations(XmlGenerator gen, Config config) {
        Map<String, UserCodeNamespaceConfig> namespaces = config.getNamespacesConfig().getNamespaceConfigs();
        for (Map.Entry<String, UserCodeNamespaceConfig> entry : namespaces.entrySet()) {
            UserCodeNamespaceConfig userCodeNamespaceConfig = entry.getValue();
            gen.open("namespace", "name", entry.getKey());
            Collection<ResourceDefinition> resourceDefinition =  userCodeNamespaceConfig.getResourceConfigs();
            resourceDefinition.forEach(resource -> {
                String resourceId = resource.id();
                gen.open(translateResourceType(resource.type()), "id", resourceId);
                gen.node("url", resource.url());
                gen.close();
            });
            gen.close();
        }
    }

    private static String translateResourceType(ResourceType type) {
        if (ResourceType.JAR == type) {
            return "jar";
        } else if (ResourceType.JARS_IN_ZIP == type) {
            return "jars-in-zip";
        } else if (ResourceType.CLASS == type) {
            return "class";
        } else {
            throw new IllegalArgumentException("Unknown resource type: " + type);
        }
    }

    /**
         * Utility class to build xml using a {@link StringBuilder}.
         */
    public static final class XmlGenerator {

        private static final int CAPACITY = 64;

        private final StringBuilder xml;
        private final ArrayDeque<String> openNodes = new ArrayDeque<>();

        public XmlGenerator(StringBuilder xml) {
            this.xml = xml;
        }

        public XmlGenerator open(String name, Object... attributes) {
            appendOpenNode(xml, name, attributes);
            openNodes.addLast(name);
            return this;
        }

        public XmlGenerator node(String name, Object contents, Object... attributes) {
            appendNode(xml, name, contents, attributes);
            return this;
        }

        public XmlGenerator nodeIfContents(String name, Object contents, Object... attributes) {
            if (contents != null) {
                appendNode(xml, name, contents, attributes);
            }
            return this;
        }

        public XmlGenerator close() {
            appendCloseNode(xml, openNodes.pollLast());
            return this;
        }

        public XmlGenerator appendLabels(Set<String> labels) {
            if (!labels.isEmpty()) {
                open("client-labels");
                for (String label : labels) {
                    node("label", label);
                }
                close();
            }
            return this;
        }

        public XmlGenerator appendProperties(Properties props) {
            if (!props.isEmpty()) {
                open("properties");
                Set keys = props.keySet();
                for (Object key : keys) {
                    node("property", props.getProperty(key.toString()), "name", key.toString());
                }
                close();
            }
            return this;
        }

        /**
         * Appends the properties as &lt;property name="key"&gt;value&lt;/property&gt; elements inside a &lt;properties&gt;
         * It uses the default tag name as "properties"
         * @param props Properties to be added
         * @return The xml generator
         */
        public XmlGenerator appendProperties(Map<String, ? extends Comparable> props) {
            if (!MapUtil.isNullOrEmpty(props)) {
                open("properties");
                for (Map.Entry<String, ?> entry : props.entrySet()) {
                    node("property", entry.getValue(), "name", entry.getKey());
                }
                close();
            }
            return this;
        }

        private static void appendOpenNode(StringBuilder xml, String name, Object... attributes) {
            xml.append('<').append(name);
            appendAttributes(xml, attributes);
            xml.append('>');
        }

        private static void appendCloseNode(StringBuilder xml, String name) {
            xml.append("</").append(name).append('>');
        }

        private static void appendNode(StringBuilder xml, String name, Object contents, Object... attributes) {
            if (contents != null || attributes.length > 0) {
                xml.append('<').append(name);
                appendAttributes(xml, attributes);
                if (contents != null) {
                    xml.append('>');
                    escapeXml(contents, xml);
                    xml.append("</").append(name).append('>');
                } else {
                    xml.append("/>");
                }
            }
        }

        private static void appendAttributes(StringBuilder xml, Object... attributes) {
            for (int i = 0; i < attributes.length; ) {
                Object attributeName = attributes[i++];
                Object attributeValue = attributes[i++];
                if (attributeValue == null) {
                    continue;
                }
                xml.append(" ").append(attributeName).append("=\"");
                escapeXmlAttr(attributeValue, xml);
                xml.append("\"");
            }
        }

        /**
         * Escapes special characters in XML element contents and appends the result to <code>appendTo</code>.
         */
        private static void escapeXml(Object o, StringBuilder appendTo) {
            if (o == null) {
                appendTo.append("null");
                return;
            }
            String s = o.toString();
            int length = s.length();
            appendTo.ensureCapacity(appendTo.length() + length + CAPACITY);
            for (int i = 0; i < length; i++) {
                char ch = s.charAt(i);
                if (ch == '<') {
                    appendTo.append("&lt;");
                } else if (ch == '&') {
                    appendTo.append("&amp;");
                } else {
                    appendTo.append(ch);
                }
            }
        }

        /**
         * Escapes special characters in XML attribute value and appends the result to <code>appendTo</code>.
         */
        private static void escapeXmlAttr(Object o, StringBuilder appendTo) {
            if (o == null) {
                appendTo.append("null");
                return;
            }
            String s = o.toString();
            int length = s.length();
            appendTo.ensureCapacity(appendTo.length() + length + CAPACITY);
            for (int i = 0; i < length; i++) {
                char ch = s.charAt(i);
                switch (ch) {
                    case '\n':
                        appendTo.append("&#10;");
                        break;
                    case '\r':
                        appendTo.append("&#13;");
                        break;
                    case '"':
                        appendTo.append("&quot;");
                        break;
                    case '\'':
                        appendTo.append("&#39;");
                        break;
                    case '&':
                        appendTo.append("&amp;");
                        break;
                    case '<':
                        appendTo.append("&lt;");
                        break;
                    default:
                        appendTo.append(ch);
                }
            }
        }
    }
}
