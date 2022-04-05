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

package com.hazelcast.client.config;

import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.util.RandomLB;
import com.hazelcast.client.util.RoundRobinLB;
import com.hazelcast.config.AliasedDiscoveryConfig;
import com.hazelcast.config.AutoDetectionConfig;
import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.CompactSerializationConfigAccessor;
import com.hazelcast.config.ConfigXmlGenerator.XmlGenerator;
import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.config.InstanceTrackingConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NearCachePreloaderConfig;
import com.hazelcast.config.PersistentMemoryConfig;
import com.hazelcast.config.PersistentMemoryDirectoryConfig;
import com.hazelcast.config.PredicateConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.config.security.JaasAuthenticationConfig;
import com.hazelcast.config.security.KerberosIdentityConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.config.security.TokenIdentityConfig;
import com.hazelcast.config.security.UsernamePasswordIdentityConfig;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.config.AliasedDiscoveryConfigUtils;
import com.hazelcast.internal.util.MapUtil;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.internal.util.TriTuple;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.query.impl.IndexUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.hazelcast.client.config.impl.ClientAliasedDiscoveryConfigUtils.aliasedDiscoveryConfigsFrom;
import static com.hazelcast.internal.util.StringUtil.formatXml;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;

/**
 * The ClientConfigXmlGenerator is responsible for transforming a
 * {@link ClientConfig} to a Hazelcast Client XML string.
 */
public final class ClientConfigXmlGenerator {

    private ClientConfigXmlGenerator() {
    }

    /**
     * Convenience for {@link #generate(ClientConfig, int)}, which
     * generates the xml without any formatting.
     */
    public static String generate(ClientConfig clientConfig) {
        return generate(clientConfig, -1);
    }

    /**
     * Transforms the given {@link ClientConfig} to xml string
     * formatting the output with given {@code indent}, -1 means no
     * formatting.
     */
    public static String generate(ClientConfig clientConfig, int indent) {
        Preconditions.isNotNull(clientConfig, "ClientConfig");

        StringBuilder xml = new StringBuilder();
        xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>");
        XmlGenerator gen = new XmlGenerator(xml);

        gen.open("hazelcast-client", "xmlns", "http://www.hazelcast.com/schema/client-config",
                "xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance",
                "xsi:schemaLocation", "http://www.hazelcast.com/schema/client-config "
                        + "http://www.hazelcast.com/schema/client-config/hazelcast-client-config-"
                        + Versions.CURRENT_CLUSTER_VERSION.getMajor() + '.'
                        + Versions.CURRENT_CLUSTER_VERSION.getMinor()
                        + ".xsd");

        //InstanceName
        gen.node("instance-name", clientConfig.getInstanceName());
        gen.node("cluster-name", clientConfig.getClusterName());
        //attributes
        gen.appendLabels(clientConfig.getLabels());
        //Properties
        gen.appendProperties(clientConfig.getProperties());
        //Network
        network(gen, clientConfig.getNetworkConfig());
        //Backup Ack To Client
        gen.node("backup-ack-to-client-enabled", clientConfig.isBackupAckToClientEnabled());
        //Security
        security(gen, clientConfig.getSecurityConfig());
        //Listeners
        listener(gen, clientConfig.getListenerConfigs());
        //Serialization
        serialization(gen, clientConfig.getSerializationConfig());
        //Native
        nativeMemory(gen, clientConfig.getNativeMemoryConfig());
        //ProxyFactories
        proxyFactory(gen, clientConfig.getProxyFactoryConfigs());
        //LoadBalancer
        loadBalancer(gen, clientConfig.getLoadBalancer());
        //NearCache
        nearCaches(gen, clientConfig.getNearCacheConfigMap());
        //QueryCaches
        queryCaches(gen, clientConfig.getQueryCacheConfigs());
        //ConnectionStrategy
        connectionStrategy(gen, clientConfig.getConnectionStrategyConfig());
        //UserCodeDeployment
        userCodeDeployment(gen, clientConfig.getUserCodeDeploymentConfig());
        //FlakeIdGenerator
        flakeIdGenerator(gen, clientConfig.getFlakeIdGeneratorConfigMap());
        //Metrics
        metrics(gen, clientConfig.getMetricsConfig());
        instanceTrackingConfig(gen, clientConfig.getInstanceTrackingConfig());

        //close HazelcastClient
        gen.close();

        return formatXml(xml.toString(), indent);
    }

    private static void network(XmlGenerator gen, ClientNetworkConfig network) {
        gen.open("network")
                .node("smart-routing", network.isSmartRouting())
                .node("redo-operation", network.isRedoOperation())
                .node("connection-timeout", network.getConnectionTimeout());

        clusterMembers(gen, network.getAddresses());
        socketOptions(gen, network.getSocketOptions());
        socketInterceptor(gen, network.getSocketInterceptorConfig());
        ssl(gen, network.getSSLConfig());
        aliasedDiscoveryConfigsGenerator(gen, aliasedDiscoveryConfigsFrom(network));
        autoDetection(gen, network.getAutoDetectionConfig());
        discovery(gen, network.getDiscoveryConfig());
        outboundPort(gen, network.getOutboundPortDefinitions());
        icmp(gen, network.getClientIcmpPingConfig());

        gen.close();
    }

    private static void security(XmlGenerator gen, ClientSecurityConfig security) {
        if (security == null) {
            return;
        }
        gen.open("security");
        UsernamePasswordIdentityConfig upConfig = security.getUsernamePasswordIdentityConfig();
        if (upConfig != null) {
            gen.node("username-password", null,
                    "username", upConfig.getUsername(),
                    "password", upConfig.getPassword());
        }
        TokenIdentityConfig tic = security.getTokenIdentityConfig();
        if (tic != null) {
            gen.node("token", tic.getTokenEncoded(), "encoding", tic.getEncoding());
        }
        CredentialsFactoryConfig cfConfig = security.getCredentialsFactoryConfig();
        if (cfConfig != null) {
            gen.open("credentials-factory", "class-name", cfConfig.getClassName())
            .appendProperties(cfConfig.getProperties())
            .close();
        }
        kerberosIdentityGenerator(gen, security.getKerberosIdentityConfig());
        Map<String, RealmConfig> realms = security.getRealmConfigs();
        if (realms != null && !realms.isEmpty()) {
            gen.open("realms");
            for (Map.Entry<String, RealmConfig> realmEntry : realms.entrySet()) {
                securityRealmGenerator(gen, realmEntry.getKey(), realmEntry.getValue());
            }
            gen.close();
        }
        gen.close();
    }

    private static void kerberosIdentityGenerator(XmlGenerator gen, KerberosIdentityConfig c) {
        if (c == null) {
            return;
        }
        gen.open("kerberos")
            .nodeIfContents("realm", c.getRealm())
            .nodeIfContents("principal", c.getPrincipal())
            .nodeIfContents("keytab-file", c.getKeytabFile())
            .nodeIfContents("security-realm", c.getSecurityRealm())
            .nodeIfContents("service-name-prefix", c.getServiceNamePrefix())
            .nodeIfContents("use-canonical-hostname", c.getUseCanonicalHostname())
            .nodeIfContents("spn", c.getSpn())
            .close();
    }

    private static void securityRealmGenerator(XmlGenerator gen, String name, RealmConfig c) {
        gen.open("realm", "name", name);
        if (c.isAuthenticationConfigured()) {
            gen.open("authentication");
            jaasAuthenticationGenerator(gen, c.getJaasAuthenticationConfig());
            gen.close();
        }
        gen.close();
    }

    private static void jaasAuthenticationGenerator(XmlGenerator gen, JaasAuthenticationConfig c) {
        if (c == null) {
            return;
        }
        appendLoginModules(gen, "jaas", c.getLoginModuleConfigs());
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

    private static void listener(XmlGenerator gen, List<ListenerConfig> listeners) {
        if (listeners.isEmpty()) {
            return;
        }
        gen.open("listeners");
        for (ListenerConfig listener : listeners) {
            gen.node("listener", classNameOrImplClass(listener.getClassName(), listener.getImplementation()));
        }
        gen.close();
    }

    private static void serialization(XmlGenerator gen, SerializationConfig serialization) {
        gen.open("serialization")
                .node("portable-version", serialization.getPortableVersion())
                .node("use-native-byte-order", serialization.isUseNativeByteOrder())
                .node("byte-order", serialization.getByteOrder())
                .node("enable-compression", serialization.isEnableCompression())
                .node("enable-shared-object", serialization.isEnableSharedObject())
                .node("allow-unsafe", serialization.isAllowUnsafe())
                .node("allow-override-default-serializers", serialization.isAllowOverrideDefaultSerializers())
                .node("check-class-def-errors", serialization.isCheckClassDefErrors());


        Map<Integer, String> dsfClasses = serialization.getDataSerializableFactoryClasses();
        Map<Integer, DataSerializableFactory> dsfImpls = serialization.getDataSerializableFactories();
        if (!dsfClasses.isEmpty() || !dsfImpls.isEmpty()) {
            gen.open("data-serializable-factories");
            for (Map.Entry<Integer, String> entry : dsfClasses.entrySet()) {
                gen.node("data-serializable-factory", entry.getValue(), "factory-id", entry.getKey());
            }
            for (Map.Entry<Integer, DataSerializableFactory> entry : dsfImpls.entrySet()) {
                gen.node("data-serializable-factory",
                        entry.getValue().getClass().getName(), "factory-id", entry.getKey());
            }
            gen.close();
        }

        Map<Integer, String> portableClasses = serialization.getPortableFactoryClasses();
        Map<Integer, PortableFactory> portableImpls = serialization.getPortableFactories();
        if (!portableClasses.isEmpty() || !portableImpls.isEmpty()) {
            gen.open("portable-factories");
            for (Map.Entry<Integer, String> entry : portableClasses.entrySet()) {
                gen.node("portable-factory", entry.getValue(), "factory-id", entry.getKey());
            }
            for (Map.Entry<Integer, PortableFactory> entry : portableImpls.entrySet()) {
                gen.node("portable-factory",
                        entry.getValue().getClass().getName(), "factory-id", entry.getKey());
            }
            gen.close();
        }

        serializers(gen, serialization);
        compactSerialization(gen, serialization);

        //close serialization
        gen.close();
    }

    private static void compactSerialization(XmlGenerator gen, SerializationConfig serializationConfig) {
        CompactSerializationConfig compactSerializationConfig = serializationConfig.getCompactSerializationConfig();
        if (!compactSerializationConfig.isEnabled()) {
            return;
        }

        gen.open("compact-serialization", "enabled", compactSerializationConfig.isEnabled());

        Map<String, TriTuple<Class, String, CompactSerializer>> registrations
                = CompactSerializationConfigAccessor.getRegistrations(compactSerializationConfig);
        Map<String, TriTuple<String, String, String>> namedRegistries
                = CompactSerializationConfigAccessor.getNamedRegistrations(compactSerializationConfig);
        if (!MapUtil.isNullOrEmpty(registrations) || !MapUtil.isNullOrEmpty(namedRegistries)) {
            gen.open("registered-classes");
            appendRegisteredClasses(gen, registrations);
            appendNamedRegisteredClasses(gen, namedRegistries);
            gen.close();
        }

        gen.close();
    }

    private static void serializers(XmlGenerator gen, SerializationConfig serialization) {
        GlobalSerializerConfig global = serialization.getGlobalSerializerConfig();
        Collection<SerializerConfig> serializers = serialization.getSerializerConfigs();
        if (global != null || !serializers.isEmpty()) {
            gen.open("serializers");
            if (global != null) {
                gen.node("global-serializer", classNameOrImplClass(global.getClassName(), global.getImplementation()),
                        "override-java-serialization", global.isOverrideJavaSerialization());
            }
            for (SerializerConfig serializer : serializers) {
                gen.node("serializer", null,
                        "type-class", classNameOrClass(serializer.getTypeClassName(), serializer.getTypeClass()),
                        "class-name", classNameOrImplClass(serializer.getClassName(), serializer.getImplementation()));
            }
            //close serializers
            gen.close();
        }
    }

    private static void nativeMemory(XmlGenerator gen, NativeMemoryConfig nativeMemory) {
        gen.open("native-memory", "enabled", nativeMemory.isEnabled(),
                "allocator-type", nativeMemory.getAllocatorType())
                .node("size", null, "value", nativeMemory.getSize().getValue(),
                        "unit", nativeMemory.getSize().getUnit())
                .node("min-block-size", nativeMemory.getMinBlockSize())
                .node("page-size", nativeMemory.getPageSize())
                .node("metadata-space-percentage", nativeMemory.getMetadataSpacePercentage());

        PersistentMemoryConfig pmemConfig = nativeMemory.getPersistentMemoryConfig();
        List<PersistentMemoryDirectoryConfig> directoryConfigs = pmemConfig.getDirectoryConfigs();
        gen.open("persistent-memory",
                "enabled", pmemConfig.isEnabled(),
                "mode", pmemConfig.getMode().name());
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

    private static void proxyFactory(XmlGenerator gen, List<ProxyFactoryConfig> proxyFactories) {
        if (proxyFactories.isEmpty()) {
            return;
        }
        gen.open("proxy-factories");
        for (ProxyFactoryConfig proxyFactory : proxyFactories) {
            gen.node("proxy-factory", null,
                    "class-name", classNameOrImplClass(proxyFactory.getClassName(), proxyFactory.getFactoryImpl()),
                    "service", proxyFactory.getService());
        }
        gen.close();
    }

    private static void loadBalancer(XmlGenerator gen, LoadBalancer loadBalancer) {
        if (loadBalancer == null) {
            return;
        }
        String type;
        if (loadBalancer instanceof RandomLB) {
            type = "random";
        } else if (loadBalancer instanceof RoundRobinLB) {
            type = "round-robin";
        } else {
            type = "custom";
        }

        if ("custom".equals(type)) {
            gen.node("load-balancer", loadBalancer.getClass().getName(), "type", type);
        } else {
            gen.node("load-balancer", null, "type", type);
        }
    }

    private static void nearCaches(XmlGenerator gen, Map<String, NearCacheConfig> nearCacheMap) {
        if (nearCacheMap.isEmpty()) {
            return;
        }
        for (Map.Entry<String, NearCacheConfig> entry : nearCacheMap.entrySet()) {
            nearCache(gen, entry.getKey(), entry.getValue());
        }
    }

    private static void queryCaches(XmlGenerator gen, Map<String, Map<String, QueryCacheConfig>> queryCaches) {
        if (queryCaches.isEmpty()) {
            return;
        }
        gen.open("query-caches");
        for (Map.Entry<String, Map<String, QueryCacheConfig>> entry : queryCaches.entrySet()) {
            String mapName = entry.getKey();
            Map<String, QueryCacheConfig> queryCachesPerMap = entry.getValue();
            for (QueryCacheConfig queryCache : queryCachesPerMap.values()) {
                EvictionConfig evictionConfig = queryCache.getEvictionConfig();
                gen.open("query-cache", "mapName", mapName, "name", queryCache.getName())
                        .node("include-value", queryCache.isIncludeValue())
                        .node("in-memory-format", queryCache.getInMemoryFormat())
                        .node("populate", queryCache.isPopulate())
                        .node("coalesce", queryCache.isCoalesce())
                        .node("delay-seconds", queryCache.getDelaySeconds())
                        .node("batch-size", queryCache.getBatchSize())
                        .node("serialize-keys", queryCache.isSerializeKeys())
                        .node("buffer-size", queryCache.getBufferSize())
                        .node("eviction", null, "size", evictionConfig.getSize(),
                                "max-size-policy", evictionConfig.getMaxSizePolicy(),
                                "eviction-policy", evictionConfig.getEvictionPolicy(),
                                "comparator-class-name",
                            classNameOrImplClass(evictionConfig.getComparatorClassName(), evictionConfig.getComparator()));
                queryCachePredicate(gen, queryCache.getPredicateConfig());
                entryListeners(gen, queryCache.getEntryListenerConfigs());
                IndexUtils.generateXml(gen, queryCache.getIndexConfigs());
                //close query-cache
                gen.close();
            }
        }
        //close query-caches
        gen.close();
    }

    private static void userCodeDeployment(XmlGenerator gen, ClientUserCodeDeploymentConfig userCodeDeployment) {
        gen.open("user-code-deployment", "enabled", userCodeDeployment.isEnabled());
        List<String> classNames = userCodeDeployment.getClassNames();
        if (!classNames.isEmpty()) {
            gen.open("classNames");
            for (String className : classNames) {
                gen.node("className", className);
            }
            //close classNames
            gen.close();
        }
        List<String> jarPaths = userCodeDeployment.getJarPaths();
        if (!jarPaths.isEmpty()) {
            gen.open("jarPaths");
            for (String jarPath : jarPaths) {
                gen.node("jarPath", jarPath);
            }
            //close jarPaths
            gen.close();
        }

        //close user-code-deployment
        gen.close();
    }

    private static void flakeIdGenerator(XmlGenerator gen, Map<String, ClientFlakeIdGeneratorConfig> flakeIdGenerators) {
        for (Map.Entry<String, ClientFlakeIdGeneratorConfig> entry : flakeIdGenerators.entrySet()) {
            ClientFlakeIdGeneratorConfig flakeIdGenerator = entry.getValue();
            gen.open("flake-id-generator", "name", entry.getKey())
                    .node("prefetch-count", flakeIdGenerator.getPrefetchCount())
                    .node("prefetch-validity-millis", flakeIdGenerator.getPrefetchValidityMillis())
                    .close();
        }
    }

    private static void entryListeners(XmlGenerator gen, List<EntryListenerConfig> entryListeners) {
        if (entryListeners.isEmpty()) {
            return;
        }
        gen.open("entry-listeners");
        for (EntryListenerConfig listener : entryListeners) {
            gen.node("entry-listener",
                    classNameOrImplClass(listener.getClassName(), listener.getImplementation()),
                    "include-value", listener.isIncludeValue(), "local", listener.isLocal());
        }
        gen.close();
    }

    private static void queryCachePredicate(XmlGenerator gen, PredicateConfig predicate) {
        String sql = predicate.getSql();
        String content = sql != null ? sql : classNameOrImplClass(predicate.getClassName(), predicate.getImplementation());
        String type = sql != null ? "sql" : "class-name";
        gen.node("predicate", content, "type", type);
    }

    private static void clusterMembers(XmlGenerator gen, List<String> addresses) {
        if (addresses == null || addresses.isEmpty()) {
            return;
        }
        gen.open("cluster-members");
        for (String address : addresses) {
            gen.node("address", address);
        }
        gen.close();
    }

    private static void socketOptions(XmlGenerator gen, SocketOptions socketOptions) {
        gen.open("socket-options")
                .node("tcp-no-delay", socketOptions.isTcpNoDelay())
                .node("keep-alive", socketOptions.isKeepAlive())
                .node("reuse-address", socketOptions.isReuseAddress())
                .node("linger-seconds", socketOptions.getLingerSeconds())
                .node("buffer-size", socketOptions.getBufferSize())
                .close();
    }

    private static void socketInterceptor(XmlGenerator gen, SocketInterceptorConfig socketInterceptor) {
        if (socketInterceptor == null) {
            return;
        }
        gen.open("socket-interceptor", "enabled", socketInterceptor.isEnabled())
                .node("class-name", classNameOrImplClass(socketInterceptor.getClassName(),
                        socketInterceptor.getImplementation()))
                .appendProperties(socketInterceptor.getProperties())
                .close();
    }

    private static void ssl(XmlGenerator gen, SSLConfig ssl) {
        if (ssl == null) {
            return;
        }
        gen.open("ssl", "enabled", ssl.isEnabled())
                .node("factory-class-name", classNameOrImplClass(ssl.getFactoryClassName(),
                        ssl.getFactoryImplementation()))
                .appendProperties(ssl.getProperties())
                .close();
    }

    private static void aliasedDiscoveryConfigsGenerator(XmlGenerator gen, List<AliasedDiscoveryConfig<?>> configs) {
        if (configs == null) {
            return;
        }
        for (AliasedDiscoveryConfig<?> c : configs) {
            gen.open(AliasedDiscoveryConfigUtils.tagFor(c), "enabled", c.isEnabled());
            if (c.isUsePublicIp()) {
                gen.node("use-public-ip", "true");
            }
            for (String key : c.getProperties().keySet()) {
                gen.node(key, c.getProperties().get(key));
            }
            gen.close();
        }
    }

    private static void autoDetection(XmlGenerator gen, AutoDetectionConfig config) {
        if (config == null) {
            return;
        }
        gen.open("auto-detection", "enabled", config.isEnabled()).close();
    }

    private static void discovery(XmlGenerator gen, DiscoveryConfig discovery) {
        if (discovery.getNodeFilter() == null && discovery.getNodeFilterClass() == null
                && discovery.getDiscoveryStrategyConfigs().isEmpty()) {
            return;
        }
        gen.open("discovery-strategies")
                .node("node-filter", null, "class",
                        classNameOrImplClass(discovery.getNodeFilterClass(), discovery.getNodeFilter()));
        for (DiscoveryStrategyConfig strategy : discovery.getDiscoveryStrategyConfigs()) {
            gen.open("discovery-strategy", "class", strategy.getClassName(), "enabled", true)
                    .appendProperties(strategy.getProperties())
                    .close();
        }
        gen.close();
    }

    private static void outboundPort(XmlGenerator gen, Collection<String> outboundPortDefinitions) {
        if (outboundPortDefinitions != null && !outboundPortDefinitions.isEmpty()) {
            gen.open("outbound-ports");
            for (String portDefinitions : outboundPortDefinitions) {
                gen.node("ports", portDefinitions);
            }
            gen.close();
        }
    }

    private static void icmp(XmlGenerator gen, ClientIcmpPingConfig icmp) {
        gen.open("icmp-ping", "enabled", icmp.isEnabled())
                .node("timeout-milliseconds", icmp.getTimeoutMilliseconds())
                .node("interval-milliseconds", icmp.getIntervalMilliseconds())
                .node("ttl", icmp.getTtl())
                .node("max-attempts", icmp.getMaxAttempts())
                .node("echo-fail-fast-on-startup", icmp.isEchoFailFastOnStartup())
                .close();
    }

    private static void nearCache(XmlGenerator gen, String name, NearCacheConfig nearCache) {
        EvictionConfig eviction = nearCache.getEvictionConfig();
        NearCachePreloaderConfig preloader = nearCache.getPreloaderConfig();
        gen.open("near-cache", "name", name)
                .node("in-memory-format", nearCache.getInMemoryFormat())
                .node("serialize-keys", nearCache.isSerializeKeys())
                .node("invalidate-on-change", nearCache.isInvalidateOnChange())
                .node("time-to-live-seconds", nearCache.getTimeToLiveSeconds())
                .node("max-idle-seconds", nearCache.getMaxIdleSeconds())
                .node("local-update-policy", nearCache.getLocalUpdatePolicy())
                .node("eviction", null, "size", eviction.getSize(),
                        "max-size-policy", eviction.getMaxSizePolicy(),
                        "eviction-policy", eviction.getEvictionPolicy(),
                        "comparator-class-name", classNameOrImplClass(
                            eviction.getComparatorClassName(), eviction.getComparator()))
                .node("preloader", null, "enabled", preloader.isEnabled(),
                        "directory", preloader.getDirectory(),
                        "store-initial-delay-seconds", preloader.getStoreInitialDelaySeconds(),
                        "store-interval-seconds", preloader.getStoreIntervalSeconds());
        //close near-cache
        gen.close();
    }

    private static void connectionStrategy(XmlGenerator gen, ClientConnectionStrategyConfig connectionStrategy) {
        ConnectionRetryConfig connectionRetry = connectionStrategy.getConnectionRetryConfig();
        gen.open("connection-strategy", "async-start", connectionStrategy.isAsyncStart(),
                "reconnect-mode", connectionStrategy.getReconnectMode());
        gen.open("connection-retry")
                .node("initial-backoff-millis", connectionRetry.getInitialBackoffMillis())
                .node("max-backoff-millis", connectionRetry.getMaxBackoffMillis())
                .node("multiplier", connectionRetry.getMultiplier())
                .node("cluster-connect-timeout-millis", connectionRetry.getClusterConnectTimeoutMillis())
                .node("jitter", connectionRetry.getJitter())
                .close();

        // close connection-strategy
        gen.close();
    }

    private static String classNameOrClass(String className, Class clazz) {
        return !isNullOrEmpty(className) ? className
            : clazz != null ? clazz.getName()
            : null;
    }

    private static String classNameOrImplClass(String className, Object impl) {
        return !isNullOrEmpty(className) ? className
                : impl != null ? impl.getClass().getName()
                : null;
    }

    private static void metrics(XmlGenerator gen, ClientMetricsConfig metricsConfig) {
        gen.open("metrics", "enabled", metricsConfig.isEnabled())
           .open("jmx", "enabled", metricsConfig.getJmxConfig().isEnabled())
           .close()
           .node("collection-frequency-seconds", metricsConfig.getCollectionFrequencySeconds())
           .close();
    }

    private static void instanceTrackingConfig(XmlGenerator gen, InstanceTrackingConfig trackingConfig) {
        gen.open("instance-tracking", "enabled", trackingConfig.isEnabled())
           .node("file-name", trackingConfig.getFileName())
           .node("format-pattern", trackingConfig.getFormatPattern())
           .close();
    }

    private static void appendRegisteredClasses(XmlGenerator gen,
                                                Map<String, TriTuple<Class, String, CompactSerializer>> registries) {
        if (registries.isEmpty()) {
            return;
        }

        for (TriTuple<Class, String, CompactSerializer> registration : registries.values()) {
            Class registeredClass = registration.element1;
            String typeName = registration.element2;
            CompactSerializer serializer = registration.element3;
            if (serializer != null) {
                String serializerClassName = serializer.getClass().getName();
                gen.node("class", registeredClass.getName(), "type-name", typeName, "serializer", serializerClassName);
            } else {
                gen.node("class", registeredClass.getName());
            }
        }
    }

    private static void appendNamedRegisteredClasses(XmlGenerator gen,
                                                     Map<String, TriTuple<String, String, String>> namedRegistries) {
        if (namedRegistries.isEmpty()) {
            return;
        }

        for (TriTuple<String, String, String> registration : namedRegistries.values()) {
            String registeredClassName = registration.element1;
            String typeName = registration.element2;
            String serializerClassName = registration.element3;
            if (serializerClassName != null) {
                gen.node("class", registeredClassName, "type-name", typeName, "serializer", serializerClassName);
            } else {
                gen.node("class", registeredClassName);
            }
        }
    }
}
