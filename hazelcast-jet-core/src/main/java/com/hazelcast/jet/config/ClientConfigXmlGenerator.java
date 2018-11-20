/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.config;

import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.config.ClientAwsConfig;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.client.config.ClientFlakeIdGeneratorConfig;
import com.hazelcast.client.config.ClientIcmpPingConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.ClientSecurityConfig;
import com.hazelcast.client.config.ClientUserCodeDeploymentConfig;
import com.hazelcast.client.config.ProxyFactoryConfig;
import com.hazelcast.client.config.SocketOptions;
import com.hazelcast.client.util.RandomLB;
import com.hazelcast.client.util.RoundRobinLB;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NearCachePreloaderConfig;
import com.hazelcast.config.PredicateConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.security.Credentials;
import com.hazelcast.util.MapUtil;
import com.hazelcast.util.Preconditions;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.util.StringUtil.isNullOrEmpty;

/**
 * The ClientConfigXmlGenerator is responsible for transforming a
 * {@link ClientConfig} to a Hazelcast Client XML string.
 */
public final class ClientConfigXmlGenerator {

    private static final ILogger LOGGER = Logger.getLogger(ClientConfigXmlGenerator.class);
    private static final int CAPACITY = 64;

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
        XmlGenerator gen = new XmlGenerator(xml);

        gen.open("hazelcast-client", "xmlns", "http://www.hazelcast.com/schema/client-config",
                "xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance",
                "xsi:schemaLocation", "http://www.hazelcast.com/schema/client-config " +
                        "http://www.hazelcast.com/schema/client-config/hazelcast-client-config-3.10.xsd");

        //GroupConfig
        group(gen, clientConfig.getGroupConfig());
        //InstanceName
        gen.node("instance-name", clientConfig.getInstanceName());
        //Properties
        gen.appendProperties(clientConfig.getProperties());
        //Network
        network(gen, clientConfig.getNetworkConfig());
        //ExecutorPoolSize
        if (clientConfig.getExecutorPoolSize() > 0) {
            gen.node("executor-pool-size", clientConfig.getExecutorPoolSize());
        }
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
        ClientConnectionStrategyConfig connectionStrategy = clientConfig.getConnectionStrategyConfig();
        gen.node("connection-strategy", null, "async-start", connectionStrategy.isAsyncStart(),
                "reconnect-mode", connectionStrategy.getReconnectMode());
        //UserCodeDeployment
        userCodeDeployment(gen, clientConfig.getUserCodeDeploymentConfig());
        //FlakeIdGenerator
        flakeIdGenerator(gen, clientConfig.getFlakeIdGeneratorConfigMap());

        //close HazelcastClient
        gen.close();

        return format(xml.toString(), indent);
    }

    private static String format(String input, int indent) {
        if (indent < 0) {
            return input;
        }
        if (indent == 0) {
            throw new IllegalArgumentException("Indent should be greater than 0");
        }
        StreamResult xmlOutput = null;
        try {
            Source xmlInput = new StreamSource(new StringReader(input));
            xmlOutput = new StreamResult(new StringWriter());
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            /*
             * Older versions of Xalan still use this method of setting indent values.
             * Attempt to make this work but don't completely fail if it's a problem.
             */
            try {
                transformerFactory.setAttribute("indent-number", indent);
            } catch (IllegalArgumentException e) {
                if (LOGGER.isFinestEnabled()) {
                    LOGGER.finest("Failed to set indent-number attribute; cause: " + e.getMessage());
                }
            }
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            /*
             * Newer versions of Xalan will look for a fully-qualified output property in order to specify amount of
             * indentation to use. Attempt to make this work as well but again don't completely fail if it's a problem.
             */
            try {
                transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", Integer.toString(indent));
            } catch (IllegalArgumentException e) {
                if (LOGGER.isFinestEnabled()) {
                    LOGGER.finest("Failed to set indent-amount property; cause: " + e.getMessage());
                }
            }
            transformer.transform(xmlInput, xmlOutput);
            return xmlOutput.getWriter().toString();
        } catch (Exception e) {
            LOGGER.warning(e);
            return input;
        } finally {
            if (xmlOutput != null) {
                closeResource(xmlOutput.getWriter());
            }
        }
    }

    private static void group(XmlGenerator gen, GroupConfig group) {
        gen.open("group")
           .node("name", group.getName())
           .node("password", group.getPassword())
           .close();
    }

    private static void network(XmlGenerator gen, ClientNetworkConfig network) {
        gen.open("network")
           .node("smart-routing", network.isSmartRouting())
           .node("redo-operation", network.isRedoOperation())
           .node("connection-timeout", network.getConnectionTimeout())
           .node("connection-attempt-period", network.getConnectionAttemptPeriod());

        if (network.getConnectionAttemptLimit() >= 0) {
            gen.node("connection-attempt-limit", network.getConnectionAttemptLimit());
        }

        clusterMembers(gen, network.getAddresses());
        socketOptions(gen, network.getSocketOptions());
        socketInterceptor(gen, network.getSocketInterceptorConfig());
        ssl(gen, network.getSSLConfig());
        aws(gen, network.getAwsConfig());
        discovery(gen, network.getDiscoveryConfig());
        outboundPort(gen, network.getOutboundPortDefinitions());
        icmp(gen, network.getClientIcmpPingConfig());

        gen.close();
    }

    private static void security(XmlGenerator gen, ClientSecurityConfig security) {
        String credentialsClassname = security.getCredentialsClassname();
        Credentials credentials = security.getCredentials();
        if (credentialsClassname == null && credentials == null) {
            return;
        }
        gen.open("security")
           .node("credentials", classNameOrImplClass(credentialsClassname, credentials))
           .close();
    }

    private static void listener(XmlGenerator gen, List<ListenerConfig> listeners) {
        if (listeners.isEmpty()) {
            return;
        }
        gen.open("listeners");
        listeners.forEach(listener -> gen.node("listener",
                classNameOrImplClass(listener.getClassName(), listener.getImplementation())));
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
           .node("check-class-def-errors", serialization.isCheckClassDefErrors());


        Map<Integer, String> dsfClasses = serialization.getDataSerializableFactoryClasses();
        Map<Integer, DataSerializableFactory> dsfImpls = serialization.getDataSerializableFactories();
        if (!dsfClasses.isEmpty() || !dsfImpls.isEmpty()) {
            gen.open("data-serializable-factories");
            dsfClasses.forEach((id, className) ->
                    gen.node("data-serializable-factory", className, "factory-id", id));
            dsfImpls.forEach((id, factory) ->
                    gen.node("data-serializable-factory", factory.getClass().getName(), "factory-id", id));
            gen.close();
        }

        Map<Integer, String> portableClasses = serialization.getPortableFactoryClasses();
        Map<Integer, PortableFactory> portableImpls = serialization.getPortableFactories();
        if (!portableClasses.isEmpty() || !portableImpls.isEmpty()) {
            gen.open("portable-factories");
            portableClasses.forEach((id, className) ->
                    gen.node("portable-factory", className, "factory-id", id));
            portableImpls.forEach((id, factory) ->
                    gen.node("portable-factory", factory.getClass().getName(), "factory-id", id));
            gen.close();
        }

        serializers(gen, serialization);

        //close serialization
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
            serializers.forEach(serializer -> gen.node("serializer", null,
                    "type-class", classNameOrImplClass(serializer.getTypeClassName(), serializer.getTypeClass()),
                    "class-name", classNameOrImplClass(serializer.getClassName(), serializer.getImplementation())));
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
           .node("metadata-space-percentage", nativeMemory.getMetadataSpacePercentage())
           .close();
    }

    private static void proxyFactory(XmlGenerator gen, List<ProxyFactoryConfig> proxyFactories) {
        if (proxyFactories.isEmpty()) {
            return;
        }
        gen.open("proxy-factories");
        proxyFactories.forEach(proxyFactory -> gen.node("proxy-factory", null,
                "class-name", classNameOrImplClass(proxyFactory.getClassName(), proxyFactory.getFactoryImpl()),
                "service", proxyFactory.getService()));
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
            throw new IllegalArgumentException("Unknown load-balancer type: " + loadBalancer);
        }
        gen.node("load-balancer", null, "type", type);
    }

    private static void nearCaches(XmlGenerator gen, Map<String, NearCacheConfig> nearCacheMap) {
        if (nearCacheMap.isEmpty()) {
            return;
        }
        nearCacheMap.forEach((name, nearCache) -> nearCache(gen, name, nearCache));
    }

    private static void queryCaches(XmlGenerator gen, Map<String, Map<String, QueryCacheConfig>> queryCaches) {
        if (queryCaches.isEmpty()) {
            return;
        }
        gen.open("query-caches");
        queryCaches.forEach((mapName, queryCachesPerMap) -> {
            queryCachesPerMap.forEach((name, queryCache) -> {
                gen.open("query-cache", "mapName", mapName, "name", queryCache.getName())
                   .node("include-value", queryCache.isIncludeValue())
                   .node("in-memory-format", queryCache.getInMemoryFormat())
                   .node("populate", queryCache.isPopulate())
                   .node("coalesce", queryCache.isCoalesce())
                   .node("delay-seconds", queryCache.getDelaySeconds())
                   .node("batch-size", queryCache.getBatchSize())
                   .node("buffer-size", queryCache.getBufferSize())
                   .node("eviction", null, "size", queryCache.getEvictionConfig().getSize(),
                           "max-size-policy", queryCache.getEvictionConfig().getMaximumSizePolicy(),
                           "eviction-policy", queryCache.getEvictionConfig().getEvictionPolicy());
                queryCachePredicate(gen, queryCache.getPredicateConfig());
                entryListeners(gen, queryCache.getEntryListenerConfigs());
                indexes(gen, queryCache.getIndexConfigs());
                //close query-cache
                gen.close();
            });
        });
        //close query-caches
        gen.close();
    }

    private static void userCodeDeployment(XmlGenerator gen, ClientUserCodeDeploymentConfig userCodeDeployment) {
        gen.open("user-code-deployment", "enabled", userCodeDeployment.isEnabled());
        List<String> classNames = userCodeDeployment.getClassNames();
        if (!classNames.isEmpty()) {
            gen.open("classNames");
            classNames.forEach(className -> gen.node("className", className));
            //close classNames
            gen.close();
        }
        List<String> jarPaths = userCodeDeployment.getJarPaths();
        if (!jarPaths.isEmpty()) {
            gen.open("jarPaths");
            jarPaths.forEach(jarPath -> gen.node("jarPath", jarPath));
            //close jarPaths
            gen.close();
        }

        //close user-code-deployment
        gen.close();
    }

    private static void flakeIdGenerator(XmlGenerator gen, Map<String, ClientFlakeIdGeneratorConfig> flakeIdGenerators) {
        flakeIdGenerators.forEach((name, flakeIdGenerator) -> {
            gen.open("flake-id-generator", "name", name)
               .node("prefetch-count", flakeIdGenerator.getPrefetchCount())
               .node("prefetch-validity-millis", flakeIdGenerator.getPrefetchValidityMillis())
               .close();
        });
    }

    private static void indexes(XmlGenerator gen, List<MapIndexConfig> indexes) {
        if (indexes.isEmpty()) {
            return;
        }
        gen.open("indexes");
        indexes.forEach(index -> gen.node("index", index.getAttribute(), "ordered", index.isOrdered()));
        gen.close();
    }

    private static void entryListeners(XmlGenerator gen, List<EntryListenerConfig> entryListeners) {
        if (entryListeners.isEmpty()) {
            return;
        }
        gen.open("entry-listeners");
        entryListeners.forEach(listener -> gen.node("entry-listener",
                classNameOrImplClass(listener.getClassName(), listener.getImplementation()),
                "include-value", listener.isIncludeValue(), "local", listener.isLocal()));
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
        addresses.forEach(address -> gen.node("address", address));
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

    private static void aws(XmlGenerator gen, ClientAwsConfig aws) {
        if (aws == null) {
            return;
        }
        gen.open("aws", "enabled", aws.isEnabled(),
                "connection-timeout-seconds", aws.getConnectionTimeoutSeconds())
           .node("inside-aws", aws.isInsideAws())
           .node("access-key", aws.getAccessKey())
           .node("secret-key", aws.getSecretKey())
           .node("iam-role", aws.getIamRole())
           .node("region", aws.getRegion())
           .node("host-header", aws.getHostHeader())
           .node("security-group-name", aws.getSecurityGroupName())
           .node("tag-key", aws.getTagKey())
           .node("tag-value", aws.getTagValue())
           .close();
    }

    private static void discovery(XmlGenerator gen, DiscoveryConfig discovery) {
        if (discovery.getNodeFilter() == null && discovery.getNodeFilterClass() == null
                && discovery.getDiscoveryStrategyConfigs().isEmpty()) {
            return;
        }
        gen.open("discovery-strategies")
           .node("node-filter", null, "class",
                   classNameOrImplClass(discovery.getNodeFilterClass(), discovery.getNodeFilter()));
        discovery.getDiscoveryStrategyConfigs().forEach(strategy -> {
            gen.open("discovery-strategy", "class", strategy.getClassName(), "enabled", true)
               .appendProperties(strategy.getProperties())
               .close();
        });
        gen.close();
    }

    private static void outboundPort(XmlGenerator gen, Collection<String> outboundPortDefinitions) {
        if (outboundPortDefinitions != null && !outboundPortDefinitions.isEmpty()) {
            gen.open("outbound-ports");
            outboundPortDefinitions.forEach(portDefinitions -> gen.node("ports", portDefinitions));
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
           .node("cache-local-entries", nearCache.isCacheLocalEntries())
           .node("time-to-live-seconds", nearCache.getTimeToLiveSeconds())
           .node("max-idle-seconds", nearCache.getMaxIdleSeconds())
           .node("local-update-policy", nearCache.getLocalUpdatePolicy())
           .node("eviction", null, "size", eviction.getSize(),
                   "max-size-policy", eviction.getMaximumSizePolicy(),
                   "eviction-policy", eviction.getEvictionPolicy())
           .node("preloader", null, "enabled", preloader.isEnabled(),
                   "directory", preloader.getDirectory(),
                   "store-initial-delay-seconds", preloader.getStoreInitialDelaySeconds(),
                   "store-interval-seconds", preloader.getStoreIntervalSeconds());
        //close near-cache
        gen.close();
    }

    private static String classNameOrImplClass(String className, Object impl) {
        return !isNullOrEmpty(className) ? className
                : impl != null ? impl.getClass().getName()
                : null;
    }

    private static final class XmlGenerator {

        private final StringBuilder xml;
        private final ArrayDeque<String> openNodes = new ArrayDeque<String>();

        private XmlGenerator(StringBuilder xml) {
            this.xml = xml;
        }

        XmlGenerator open(String name, Object... attributes) {
            appendOpenNode(xml, name, attributes);
            openNodes.addLast(name);
            return this;
        }

        XmlGenerator node(String name, Object contents, Object... attributes) {
            appendNode(xml, name, contents, attributes);
            return this;
        }

        XmlGenerator close() {
            appendCloseNode(xml, openNodes.pollLast());
            return this;
        }

        XmlGenerator appendProperties(Properties props) {
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

        XmlGenerator appendProperties(Map<String, Comparable> props) {
            if (!MapUtil.isNullOrEmpty(props)) {
                open("properties");
                for (Map.Entry<String, Comparable> entry : props.entrySet()) {
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
                xml.append(" ").append(attributes[i++]).append("=\"");
                escapeXmlAttr(attributes[i++], xml);
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
