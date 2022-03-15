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

package com.hazelcast.spring;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientCloudConfig;
import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.client.config.ClientFlakeIdGeneratorConfig;
import com.hazelcast.client.config.ClientIcmpPingConfig;
import com.hazelcast.client.config.ClientMetricsConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.ClientReliableTopicConfig;
import com.hazelcast.client.config.ClientSecurityConfig;
import com.hazelcast.client.config.ClientUserCodeDeploymentConfig;
import com.hazelcast.client.config.ConnectionRetryConfig;
import com.hazelcast.client.config.ProxyFactoryConfig;
import com.hazelcast.client.config.SocketOptions;
import com.hazelcast.client.util.RandomLB;
import com.hazelcast.client.util.RoundRobinLB;
import com.hazelcast.config.AliasedDiscoveryConfig;
import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.InstanceTrackingConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.config.MetricsJmxConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.PredicateConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.security.JaasAuthenticationConfig;
import com.hazelcast.config.security.KerberosIdentityConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.config.security.TokenEncoding;
import com.hazelcast.config.security.TokenIdentityConfig;
import com.hazelcast.config.security.UsernamePasswordIdentityConfig;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.internal.config.AliasedDiscoveryConfigUtils;
import com.hazelcast.spring.config.ConfigFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.support.ManagedMap;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.config.DomConfigHelper.childElements;
import static com.hazelcast.internal.config.DomConfigHelper.cleanNodeName;
import static com.hazelcast.internal.util.StringUtil.upperCaseInternal;
import static com.hazelcast.spring.HazelcastInstanceDefinitionParser.CP_SUBSYSTEM_SUFFIX;
import static org.springframework.beans.factory.support.BeanDefinitionBuilder.rootBeanDefinition;
import static org.springframework.util.Assert.isTrue;

/**
 * BeanDefinitionParser for Hazelcast Client Configuration.
 * <p>
 * <b>Sample Spring XML for Hazelcast Client:</b>
 * <pre>{@code
 *   <hz:client id="client">
 *      <hz:cluster-name>${cluster.name}</hz:cluster-name>
 *      <hz:network
 *          connection-timeout="1000"
 *          redo-operation="true"
 *          smart-routing="true">
 *              <hz:member>10.10.1.2:5701</hz:member>
 *              <hz:member>10.10.1.3:5701</hz:member>
 *      </hz:network>
 *   </hz:client>
 * }</pre>
 */
public class HazelcastClientBeanDefinitionParser extends AbstractHazelcastBeanDefinitionParser {

    @Override
    protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
        SpringXmlBuilder springXmlBuilder = new SpringXmlBuilder(parserContext);
        AbstractBeanDefinition bean = springXmlBuilder.handleClient(element);
        registerCPSubsystemBean(element, parserContext);
        return bean;
    }

    private void registerCPSubsystemBean(Element element, ParserContext parserContext) {
        String instanceBeanRef = element.getAttribute("id");
        BeanDefinitionBuilder cpBeanDefBuilder = BeanDefinitionBuilder.rootBeanDefinition(CPSubsystem.class);
        cpBeanDefBuilder.setFactoryMethodOnBean("getCPSubsystem", instanceBeanRef);
        cpBeanDefBuilder.setLazyInit(true);

        BeanDefinitionHolder holder =
                new BeanDefinitionHolder(cpBeanDefBuilder.getBeanDefinition(), instanceBeanRef + CP_SUBSYSTEM_SUFFIX);
        registerBeanDefinition(holder, parserContext.getRegistry());
    }

    /**
     * Client bean definition builder
     */
    public static class SpringXmlBuilder extends SpringXmlBuilderHelper {

        private static final int INITIAL_CAPACITY = 10;

        private final ParserContext parserContext;
        private final BeanDefinitionBuilder builder;
        private final ManagedMap<String, BeanDefinition> nearCacheConfigMap = new ManagedMap<String, BeanDefinition>();
        private final ManagedMap<String, BeanDefinition> flakeIdGeneratorConfigMap = new ManagedMap<String, BeanDefinition>();
        private final ManagedMap<String, BeanDefinition> reliableTopicConfigMap = new ManagedMap<String, BeanDefinition>();

        SpringXmlBuilder(ParserContext parserContext) {
            this(parserContext, rootBeanDefinition(HazelcastClient.class)
                    .setFactoryMethod("newHazelcastClient")
                    .setDestroyMethodName("shutdown"));
        }

        public SpringXmlBuilder(ParserContext parserContext, BeanDefinitionBuilder builder) {
            this.parserContext = parserContext;
            this.builder = builder;

            this.configBuilder = rootBeanDefinition(ConfigFactory.class, "newClientConfig");
            configBuilder.addPropertyValue("nearCacheConfigMap", nearCacheConfigMap);
            configBuilder.addPropertyValue("flakeIdGeneratorConfigMap", flakeIdGeneratorConfigMap);
            configBuilder.addPropertyValue("reliableTopicConfigMap", reliableTopicConfigMap);
        }

        public AbstractBeanDefinition handleClient(Node rootNode) {
            AbstractBeanDefinition configBean = createConfigBean(rootNode);
            builder.addConstructorArgValue(configBean);
            return builder.getBeanDefinition();
        }

        @SuppressWarnings("checkstyle:cyclomaticcomplexity")
        AbstractBeanDefinition createConfigBean(Node rootNode) {
            handleCommonBeanAttributes(rootNode, builder, parserContext);
            for (Node node : childElements(rootNode)) {
                String nodeName = cleanNodeName(node);
                if ("cluster-name".equals(nodeName)) {
                    configBuilder.addPropertyValue("clusterName", getTextContent(node));
                } else if ("properties".equals(nodeName)) {
                    handleProperties(node, configBuilder);
                } else if ("network".equals(nodeName)) {
                    handleNetwork(node);
                } else if ("listeners".equals(nodeName)) {
                    List listeners = parseListeners(node, ListenerConfig.class);
                    configBuilder.addPropertyValue("listenerConfigs", listeners);
                } else if ("serialization".equals(nodeName)) {
                    handleSerialization(node);
                } else if ("proxy-factories".equals(nodeName)) {
                    List list = parseProxyFactories(node, ProxyFactoryConfig.class);
                    configBuilder.addPropertyValue("proxyFactoryConfigs", list);
                } else if ("load-balancer".equals(nodeName)) {
                    handleLoadBalancer(node);
                } else if ("near-cache".equals(nodeName)) {
                    handleNearCache(node);
                } else if ("spring-aware".equals(nodeName)) {
                    handleSpringAware();
                } else if ("query-caches".equals(nodeName)) {
                    ManagedMap queryCaches = getQueryCaches(node);
                    configBuilder.addPropertyValue("queryCacheConfigs", queryCaches);
                } else if ("connection-strategy".equals(nodeName)) {
                    handleConnectionStrategy(node);
                } else if ("user-code-deployment".equals(nodeName)) {
                    handleUserCodeDeployment(node);
                } else if ("flake-id-generator".equals(nodeName)) {
                    handleFlakeIdGenerator(node);
                } else if ("reliable-topic".equals(nodeName)) {
                    handleReliableTopic(node);
                } else if ("security".equals(nodeName)) {
                    handleSecurity(node);
                } else if ("instance-name".equals(nodeName)) {
                    configBuilder.addPropertyValue("instanceName", getTextContent(node));
                } else if ("labels".equals(nodeName)) {
                    handleLabels(node);
                } else if ("backup-ack-to-client-enabled".equals(nodeName)) {
                    configBuilder.addPropertyValue("backupAckToClientEnabled", getTextContent(node));
                } else if ("metrics".equals(nodeName)) {
                    handleMetrics(node);
                } else if ("instance-tracking".equals(nodeName)) {
                    handleInstanceTracking(node);
                } else if ("native-memory".equals(nodeName)) {
                    handleNativeMemory(node);
                }
            }
            return configBuilder.getBeanDefinition();
        }

        private void handleSecurity(Node node) {
            BeanDefinitionBuilder securityConfigBuilder = createBeanBuilder(ClientSecurityConfig.class);
            AbstractBeanDefinition beanDefinition = securityConfigBuilder.getBeanDefinition();
            fillAttributeValues(node, securityConfigBuilder);
            for (Node child : childElements(node)) {
                String nodeName = cleanNodeName(child);
                if ("credentials-factory".equals(nodeName)) {
                    handleCredentialsFactory(child, securityConfigBuilder);
                } else if ("username-password".equals(nodeName)) {
                    BeanDefinitionBuilder configBuilder = createBeanBuilder(UsernamePasswordIdentityConfig.class)
                            .addConstructorArgValue(getAttribute(child, "username"))
                            .addConstructorArgValue(getAttribute(child, "password"));
                    securityConfigBuilder.addPropertyValue("UsernamePasswordIdentityConfig", configBuilder.getBeanDefinition());
                } else if ("token".equals(nodeName)) {
                    BeanDefinitionBuilder configBuilder = createBeanBuilder(TokenIdentityConfig.class)
                            .addConstructorArgValue(TokenEncoding.getTokenEncoding(getAttribute(child, "encoding")))
                            .addConstructorArgValue(getTextContent(child));
                    securityConfigBuilder.addPropertyValue("TokenIdentityConfig", configBuilder.getBeanDefinition());
                } else if ("kerberos".equals(nodeName)) {
                    createAndFillBeanBuilder(child, KerberosIdentityConfig.class, "KerberosIdentityConfig",
                            securityConfigBuilder);
                } else if ("credentials-ref".equals(nodeName)) {
                    securityConfigBuilder.addPropertyReference("credentials", getTextContent(child));
                } else if ("realms".equals(nodeName)) {
                    handleRealms(child, securityConfigBuilder);
                }
            }
            configBuilder.addPropertyValue("securityConfig", beanDefinition);
        }

        void handleCredentialsFactory(Node node, BeanDefinitionBuilder securityConfigBuilder) {
            BeanDefinitionBuilder credentialsConfigBuilder = createBeanBuilder(CredentialsFactoryConfig.class);
            AbstractBeanDefinition beanDefinition = credentialsConfigBuilder.getBeanDefinition();
            NamedNodeMap attributes = node.getAttributes();
            Node classNameNode = attributes.getNamedItem("class-name");
            String className = classNameNode != null ? getTextContent(classNameNode) : null;
            Node implNode = attributes.getNamedItem("implementation");
            String implementation = implNode != null ? getTextContent(implNode) : null;
            credentialsConfigBuilder.addPropertyValue("className", className);
            if (implementation != null) {
                credentialsConfigBuilder.addPropertyReference("implementation", implementation);
            }
            isTrue(className != null || implementation != null, "One of 'class-name' or 'implementation'"
                    + " attributes is required to create CredentialsFactory!");
            for (Node child : childElements(node)) {
                String nodeName = cleanNodeName(child);
                if ("properties".equals(nodeName)) {
                    handleProperties(child, credentialsConfigBuilder);
                    break;
                }
            }
            securityConfigBuilder.addPropertyValue("credentialsFactoryConfig", beanDefinition);
        }

        private void handleConnectionStrategy(Node node) {
            BeanDefinitionBuilder clientConnectionStrategyConfig = createBeanBuilder(ClientConnectionStrategyConfig.class);
            fillAttributeValues(node, clientConnectionStrategyConfig);
            for (Node child : childElements(node)) {
                String nodeName = cleanNodeName(child);
                if ("connection-retry".equals(nodeName)) {
                    createAndFillBeanBuilder(child, ConnectionRetryConfig.class,
                            "ConnectionRetryConfig", clientConnectionStrategyConfig);
                }
            }
            configBuilder.addPropertyValue("connectionStrategyConfig", clientConnectionStrategyConfig.getBeanDefinition());
        }

        private void handleUserCodeDeployment(Node node) {
            BeanDefinitionBuilder userCodeDeploymentConfig = createBeanBuilder(ClientUserCodeDeploymentConfig.class);
            List<String> jarPaths = new ArrayList<String>(INITIAL_CAPACITY);
            List<String> classNames = new ArrayList<String>(INITIAL_CAPACITY);
            fillAttributeValues(node, userCodeDeploymentConfig);
            for (Node child : childElements(node)) {
                String nodeName = cleanNodeName(child);
                if ("jarpaths".equals(nodeName)) {
                    for (Node child1 : childElements(child)) {
                        String nodeName1 = cleanNodeName(child1);
                        if ("jarpath".equals(nodeName1)) {
                            jarPaths.add(getTextContent(child1));
                        }
                    }
                } else if ("classnames".equals(nodeName)) {
                    for (Node child1 : childElements(child)) {
                        String nodeName1 = cleanNodeName(child1);
                        if ("classname".equals(nodeName1)) {
                            classNames.add(getTextContent(child1));
                        }
                    }
                }
            }
            userCodeDeploymentConfig.addPropertyValue("jarPaths", jarPaths);
            userCodeDeploymentConfig.addPropertyValue("classNames", classNames);

            configBuilder.addPropertyValue("userCodeDeploymentConfig", userCodeDeploymentConfig.getBeanDefinition());
        }

        private void handleNetwork(Node node) {
            BeanDefinitionBuilder clientNetworkConfig = createBeanBuilder(ClientNetworkConfig.class);
            List<String> members = new ArrayList<String>(INITIAL_CAPACITY);
            fillAttributeValues(node, clientNetworkConfig);
            for (Node child : childElements(node)) {
                String nodeName = cleanNodeName(child);
                if ("member".equals(nodeName)) {
                    members.add(getTextContent(child));
                } else if ("socket-options".equals(nodeName)) {
                    createAndFillBeanBuilder(child, SocketOptions.class, "socketOptions", clientNetworkConfig);
                } else if ("socket-interceptor".equals(nodeName)) {
                    handleSocketInterceptorConfig(child, clientNetworkConfig);
                } else if ("ssl".equals(nodeName)) {
                    handleSSLConfig(child, clientNetworkConfig);
                } else if (AliasedDiscoveryConfigUtils.supports(nodeName)) {
                    handleAliasedDiscoveryStrategy(child, clientNetworkConfig, nodeName);
                } else if ("discovery-strategies".equals(nodeName)) {
                    handleDiscoveryStrategies(child, clientNetworkConfig);
                } else if ("auto-detection".equals(nodeName)) {
                    handleAutoDetection(child, clientNetworkConfig);
                } else if ("outbound-ports".equals(nodeName)) {
                    handleOutboundPorts(child, clientNetworkConfig);
                } else if ("icmp-ping".equals(nodeName)) {
                    createAndFillBeanBuilder(child, ClientIcmpPingConfig.class,
                            "clientIcmpPingConfig", clientNetworkConfig);
                } else if ("hazelcast-cloud".equals(nodeName)) {
                    createAndFillBeanBuilder(child, ClientCloudConfig.class,
                            "cloudConfig", clientNetworkConfig);
                }

            }
            clientNetworkConfig.addPropertyValue("addresses", members);

            configBuilder.addPropertyValue("networkConfig", clientNetworkConfig.getBeanDefinition());
        }

        private void handleAliasedDiscoveryStrategy(Node node, BeanDefinitionBuilder builder, String name) {
            AliasedDiscoveryConfig config = AliasedDiscoveryConfigUtils.newConfigFor(name);
            fillAttributesForAliasedDiscoveryStrategy(config, node, builder, name);
        }

        private void handleSSLConfig(Node node, BeanDefinitionBuilder networkConfigBuilder) {
            BeanDefinitionBuilder sslConfigBuilder = createBeanBuilder(SSLConfig.class);
            String implAttribute = "factory-implementation";
            fillAttributeValues(node, sslConfigBuilder, implAttribute);
            Node implNode = node.getAttributes().getNamedItem(implAttribute);
            String implementation = implNode != null ? getTextContent(implNode) : null;
            if (implementation != null) {
                sslConfigBuilder.addPropertyReference(xmlToJavaName(implAttribute), implementation);
            }
            for (Node child : childElements(node)) {
                String name = cleanNodeName(child);
                if ("properties".equals(name)) {
                    handleProperties(child, sslConfigBuilder);
                }
            }
            networkConfigBuilder.addPropertyValue("SSLConfig", sslConfigBuilder.getBeanDefinition());
        }

        private void handleLoadBalancer(Node node) {
            String type = getAttribute(node, "type");
            if ("random".equals(type)) {
                configBuilder.addPropertyValue("loadBalancer", new RandomLB());
            } else if ("round-robin".equals(type)) {
                configBuilder.addPropertyValue("loadBalancer", new RoundRobinLB());
            } else if ("custom".equals(type)) {
                NamedNodeMap attributes = node.getAttributes();
                Node classNameNode = attributes.getNamedItem("class-name");
                String className = classNameNode != null ? getTextContent(classNameNode) : null;
                Node implNode = attributes.getNamedItem("implementation");
                String implementation = implNode != null ? getTextContent(implNode) : null;
                isTrue(className != null ^ implementation != null, "Exactly one of 'class-name' or 'implementation'"
                        + " attributes is required to create LoadBalancer!");
                if (className != null) {
                    BeanDefinitionBuilder loadBalancerBeanDefinition = createBeanBuilder(className);
                    configBuilder.addPropertyValue("loadBalancer", loadBalancerBeanDefinition.getBeanDefinition());
                } else {
                    configBuilder.addPropertyReference("loadBalancer", implementation);
                }
            }
        }

        private void handleNearCache(Node node) {
            BeanDefinitionBuilder nearCacheConfigBuilder = createBeanBuilder(NearCacheConfig.class);
            fillAttributeValues(node, nearCacheConfigBuilder);
            for (Node childNode : childElements(node)) {
                String nodeName = cleanNodeName(childNode);
                if ("eviction".equals(nodeName)) {
                    handleEvictionConfig(childNode, nearCacheConfigBuilder, true);
                } else if ("preloader".equals(nodeName)) {
                    handlePreloaderConfig(childNode, nearCacheConfigBuilder);
                }
            }
            String name = getAttribute(node, "name");
            nearCacheConfigMap.put(name, nearCacheConfigBuilder.getBeanDefinition());
        }

        private void handleFlakeIdGenerator(Node node) {
            BeanDefinitionBuilder configBuilder = createBeanBuilder(ClientFlakeIdGeneratorConfig.class);
            fillAttributeValues(node, configBuilder);
            String name = getAttribute(node, "name");
            flakeIdGeneratorConfigMap.put(name, configBuilder.getBeanDefinition());
        }

        private void handleReliableTopic(Node node) {
            BeanDefinitionBuilder configBuilder = createBeanBuilder(ClientReliableTopicConfig.class);
            String name = getAttribute(node, "name");
            fillValues(node, configBuilder);
            reliableTopicConfigMap.put(name, configBuilder.getBeanDefinition());
        }

        private void handleEvictionConfig(Node node, BeanDefinitionBuilder configBuilder, boolean isNearCache) {
            configBuilder.addPropertyValue("evictionConfig", getEvictionConfig(node, isNearCache, false));
        }

        private void handlePreloaderConfig(Node node, BeanDefinitionBuilder configBuilder) {
            configBuilder.addPropertyValue("preloaderConfig", getPreloaderConfig(node));
        }

        private ManagedMap getQueryCaches(Node childNode) {
            ManagedMap<String, ManagedMap<String, BeanDefinition>> queryCaches
                    = new ManagedMap<String, ManagedMap<String, BeanDefinition>>();
            for (Node queryCacheNode : childElements(childNode)) {
                parseQueryCache(queryCaches, queryCacheNode);
            }
            return queryCaches;
        }

        private void parseQueryCache(ManagedMap<String, ManagedMap<String, BeanDefinition>> queryCaches, Node queryCacheNode) {
            BeanDefinitionBuilder builder = createBeanBuilder(QueryCacheConfig.class);

            NamedNodeMap attributes = queryCacheNode.getAttributes();
            String mapName = getTextContent(attributes.getNamedItem("map-name"));
            String cacheName = getTextContent(attributes.getNamedItem("name"));

            for (Node node : childElements(queryCacheNode)) {
                String nodeName = cleanNodeName(node);
                String textContent = getTextContent(node);

                parseQueryCacheInternal(builder, node, nodeName, textContent);
            }

            builder.addPropertyValue("name", cacheName);

            ManagedMap<String, BeanDefinition> configMap = queryCaches.get(mapName);
            if (configMap == null) {
                configMap = new ManagedMap<String, BeanDefinition>();
                queryCaches.put(mapName, configMap);
            }
            configMap.put(cacheName, builder.getBeanDefinition());
        }

        @SuppressWarnings({"checkstyle:cyclomaticcomplexity"})
        private void parseQueryCacheInternal(BeanDefinitionBuilder builder, Node node, String nodeName, String textContent) {
            if ("predicate".equals(nodeName)) {
                BeanDefinitionBuilder predicateBuilder = getPredicate(node, textContent);
                builder.addPropertyValue("predicateConfig", predicateBuilder.getBeanDefinition());
            } else if ("entry-listeners".equals(nodeName)) {
                ManagedList listeners = getEntryListeners(node);
                builder.addPropertyValue("entryListenerConfigs", listeners);
            } else if ("include-value".equals(nodeName)) {
                builder.addPropertyValue("includeValue", textContent);
            } else if ("batch-size".equals(nodeName)) {
                builder.addPropertyValue("batchSize", textContent);
            } else if ("buffer-size".equals(nodeName)) {
                builder.addPropertyValue("bufferSize", textContent);
            } else if ("delay-seconds".equals(nodeName)) {
                builder.addPropertyValue("delaySeconds", textContent);
            } else if ("in-memory-format".equals(nodeName)) {
                String value = textContent.trim();
                builder.addPropertyValue("inMemoryFormat", InMemoryFormat.valueOf(upperCaseInternal(value)));
            } else if ("coalesce".equals(nodeName)) {
                builder.addPropertyValue("coalesce", textContent);
            } else if ("populate".equals(nodeName)) {
                builder.addPropertyValue("populate", textContent);
            } else if ("serialize-keys".equals(nodeName)) {
                builder.addPropertyValue("serializeKeys", textContent);
            } else if ("indexes".equals(nodeName)) {
                ManagedList indexes = getIndexes(node);
                builder.addPropertyValue("indexConfigs", indexes);
            } else if ("eviction".equals(nodeName)) {
                builder.addPropertyValue("evictionConfig", getEvictionConfig(node, false, false));
            }
        }

        private BeanDefinitionBuilder getPredicate(Node node, String textContent) {
            BeanDefinitionBuilder predicateBuilder = createBeanBuilder(PredicateConfig.class);
            String predicateType = getTextContent(node.getAttributes().getNamedItem("type"));
            if ("sql".equals(predicateType)) {
                predicateBuilder.addPropertyValue("sql", textContent);
            } else if ("class-name".equals(predicateType)) {
                predicateBuilder.addPropertyValue("className", textContent);
            }
            return predicateBuilder;
        }

        private ManagedList<BeanDefinition> getIndexes(Node node) {
            ManagedList<BeanDefinition> indexes = new ManagedList<BeanDefinition>();
            for (Node indexNode : childElements(node)) {
                handleIndex(indexes, indexNode);
            }
            return indexes;
        }

        private ManagedList<BeanDefinition> getEntryListeners(Node node) {
            ManagedList<BeanDefinition> listeners = new ManagedList<BeanDefinition>();
            String implementationAttr = "implementation";
            for (Node listenerNode : childElements(node)) {
                BeanDefinitionBuilder listenerConfBuilder = createBeanBuilder(EntryListenerConfig.class);
                fillAttributeValues(listenerNode, listenerConfBuilder, implementationAttr);
                Node implementationNode = listenerNode.getAttributes().getNamedItem(implementationAttr);
                if (implementationNode != null) {
                    listenerConfBuilder.addPropertyReference(implementationAttr, getTextContent(implementationNode));
                }
                listeners.add(listenerConfBuilder.getBeanDefinition());
            }
            return listeners;
        }

        private void handleOutboundPorts(Node node, BeanDefinitionBuilder networkConfigBuilder) {
            ManagedList<String> outboundPorts = new ManagedList<String>();
            for (Node child : childElements(node)) {
                String name = cleanNodeName(child);
                if ("ports".equals(name)) {
                    String value = getTextContent(child);
                    outboundPorts.add(value);
                }
            }
            networkConfigBuilder.addPropertyValue("outboundPortDefinitions", outboundPorts);
        }


        private void handleLabels(Node node) {
            ManagedList<String> labels = new ManagedList<String>();
            for (Node n : childElements(node)) {
                String name = cleanNodeName(n);
                if (!"label".equals(name)) {
                    continue;
                }
                String label = getTextContent(n);
                labels.add(label);
            }
            configBuilder.addPropertyValue("labels", labels);
        }

        private void handleInstanceTracking(Node node) {
            BeanDefinitionBuilder configBuilder = createBeanBuilder(InstanceTrackingConfig.class);
            fillAttributeValues(node, configBuilder);

            for (Node child : childElements(node)) {
                final String name = cleanNodeName(child);
                if ("file-name".equals(name)) {
                    configBuilder.addPropertyValue("fileName", getTextContent(child));
                } else if ("format-pattern".equals(name)) {
                    configBuilder.addPropertyValue("formatPattern", getTextContent(child));
                }
            }
            this.configBuilder.addPropertyValue("instanceTrackingConfig", configBuilder.getBeanDefinition());
        }

        private void handleMetrics(Node node) {
            BeanDefinitionBuilder metricsConfigBuilder = createBeanBuilder(ClientMetricsConfig.class);
            fillValues(node, metricsConfigBuilder, "jmx");
            Node attrEnabled = node.getAttributes().getNamedItem("enabled");
            metricsConfigBuilder.addPropertyValue("enabled", getTextContent(attrEnabled));

            for (Node child : childElements(node)) {
                String nodeName = cleanNodeName(child);
                if ("jmx".equals(nodeName)) {
                    BeanDefinitionBuilder metricsJmxConfigBuilder = createBeanBuilder(MetricsJmxConfig.class);
                    fillValues(child, metricsJmxConfigBuilder);

                    metricsConfigBuilder.addPropertyValue("jmxConfig", metricsJmxConfigBuilder.getBeanDefinition());
                }
            }

            configBuilder.addPropertyValue("metricsConfig", metricsConfigBuilder.getBeanDefinition());
        }

        private void handleRealms(Node node, BeanDefinitionBuilder securityConfigBuilder) {
            ManagedMap<String, BeanDefinition> realms = new ManagedMap<>();
            for (Node child : childElements(node)) {
                String nodeName = cleanNodeName(child);
                if ("realm".equals(nodeName)) {
                    realms.put(getAttribute(child, "name"), handleRealm(child, securityConfigBuilder));
                }
            }
            securityConfigBuilder.addPropertyValue("realmConfigs", realms);
        }

        private AbstractBeanDefinition handleRealm(Node node, BeanDefinitionBuilder securityConfigBuilder) {
            BeanDefinitionBuilder realmConfigBuilder = createBeanBuilder(RealmConfig.class);
            AbstractBeanDefinition beanDefinition = realmConfigBuilder.getBeanDefinition();
            for (Node child : childElements(node)) {
                String nodeName = cleanNodeName(child);
                if ("authentication".equals(nodeName)) {
                    handleAuthentication(child, realmConfigBuilder);
                }
            }
            return beanDefinition;
        }


        private void handleAuthentication(Node node, BeanDefinitionBuilder realmConfigBuilder) {
            for (Node child : childElements(node)) {
                String nodeName = cleanNodeName(child);
                if ("jaas".equals(nodeName)) {
                    handleLoginModules(child, realmConfigBuilder);
                }
            }
        }

        private void handleLoginModules(Node node, BeanDefinitionBuilder realmConfigBuilder) {
            BeanDefinitionBuilder jaasConfigBuilder = createBeanBuilder(JaasAuthenticationConfig.class);
            AbstractBeanDefinition beanDefinition = jaasConfigBuilder.getBeanDefinition();
            List<BeanDefinition> lms = new ManagedList<>();
            for (Node child : childElements(node)) {
                String nodeName = cleanNodeName(child);
                if ("login-module".equals(nodeName)) {
                    handleLoginModule(child, lms);
                }
            }
            jaasConfigBuilder.addPropertyValue("loginModuleConfigs", lms);
            realmConfigBuilder.addPropertyValue("jaasAuthenticationConfig", beanDefinition);
        }


        private void handleLoginModule(Node node, List<BeanDefinition> list) {
            BeanDefinitionBuilder lmConfigBuilder = createBeanBuilder(LoginModuleConfig.class);
            AbstractBeanDefinition beanDefinition = lmConfigBuilder.getBeanDefinition();
            fillAttributeValues(node, lmConfigBuilder, "class-name", "implementation");
            NamedNodeMap attributes = node.getAttributes();
            Node classNameNode = attributes.getNamedItem("class-name");
            String className = classNameNode != null ? getTextContent(classNameNode) : null;
            Node implNode = attributes.getNamedItem("implementation");
            String implementation = implNode != null ? getTextContent(implNode) : null;
            lmConfigBuilder.addPropertyValue("className", className);
            if (implementation != null) {
                lmConfigBuilder.addPropertyReference("implementation", implementation);
            }
            isTrue(className != null || implementation != null, "One of 'class-name' or 'implementation'"
                    + " attributes is required to create LoginModule!");
            for (Node child : childElements(node)) {
                String nodeName = cleanNodeName(child);
                if ("properties".equals(nodeName)) {
                    handleProperties(child, lmConfigBuilder);
                    break;
                }
            }
            list.add(beanDefinition);
        }
    }

}
