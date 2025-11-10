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

package com.hazelcast.internal.config;

import com.hazelcast.config.AliasedDiscoveryConfig;
import com.hazelcast.config.AttributeConfig;
import com.hazelcast.config.AutoDetectionConfig;
import com.hazelcast.config.CRDTReplicationConfig;
import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.CachePartitionLostListenerConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig.ExpiryPolicyType;
import com.hazelcast.config.CacheSimpleEntryListenerConfig;
import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConsistencyCheckStrategy;
import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.config.DataConnectionConfigValidator;
import com.hazelcast.config.DataPersistenceConfig;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.DiskTierConfig;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.config.DynamicConfigurationConfig;
import com.hazelcast.config.EncryptionAtRestConfig;
import com.hazelcast.config.EndpointConfig;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.FlakeIdGeneratorConfig;
import com.hazelcast.config.HotRestartClusterDataRecoveryPolicy;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.config.IcmpFailureDetectorConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.ItemListenerConfig;
import com.hazelcast.config.JavaKeyStoreSecureStoreConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.LocalDeviceConfig;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapPartitionLostListenerConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.MemberAddressProviderConfig;
import com.hazelcast.config.MemberGroupConfig;
import com.hazelcast.config.MemoryTierConfig;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.MetadataPolicy;
import com.hazelcast.config.MetricsConfig;
import com.hazelcast.config.MetricsJmxConfig;
import com.hazelcast.config.MetricsManagementCenterConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.OnJoinPermissionOperationName;
import com.hazelcast.config.PNCounterConfig;
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.config.PartitionGroupConfig.MemberGroupType;
import com.hazelcast.config.PartitioningAttributeConfig;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.config.PermissionPolicyConfig;
import com.hazelcast.config.PersistenceClusterDataRecoveryPolicy;
import com.hazelcast.config.PersistenceConfig;
import com.hazelcast.config.PredicateConfig;
import com.hazelcast.config.ProbabilisticSplitBrainProtectionConfigBuilder;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.QueueStoreConfig;
import com.hazelcast.config.RecentlyActiveSplitBrainProtectionConfigBuilder;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RestApiConfig;
import com.hazelcast.config.RestEndpointGroup;
import com.hazelcast.config.RestServerEndpointConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.RingbufferStoreConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.config.SecureStoreConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.SecurityInterceptorConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.config.SplitBrainProtectionConfigBuilder;
import com.hazelcast.config.SplitBrainProtectionListenerConfig;
import com.hazelcast.config.SqlConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.config.TieredStoreConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.config.TrustedInterfacesConfigurable;
import com.hazelcast.config.UserCodeDeploymentConfig;
import com.hazelcast.config.UserCodeNamespaceConfig;
import com.hazelcast.config.VaultSecureStoreConfig;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanBatchPublisherConfig;
import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.config.WanCustomPublisherConfig;
import com.hazelcast.config.WanQueueFullBehavior;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.config.WanSyncConfig;
import com.hazelcast.config.cp.CPMapConfig;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.config.cp.FencedLockConfig;
import com.hazelcast.config.cp.RaftAlgorithmConfig;
import com.hazelcast.config.cp.SemaphoreConfig;
import com.hazelcast.config.rest.RestConfig;
import com.hazelcast.config.security.AbstractClusterLoginConfig;
import com.hazelcast.config.security.AccessControlServiceConfig;
import com.hazelcast.config.security.KerberosAuthenticationConfig;
import com.hazelcast.config.security.KerberosIdentityConfig;
import com.hazelcast.config.security.LdapAuthenticationConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.config.security.SimpleAuthenticationConfig;
import com.hazelcast.config.security.TlsAuthenticationConfig;
import com.hazelcast.config.security.TokenEncoding;
import com.hazelcast.config.security.TokenIdentityConfig;
import com.hazelcast.config.tpc.TpcConfig;
import com.hazelcast.config.tpc.TpcSocketConfig;
import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.config.InstanceConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.query.impl.IndexUtils;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionOn;
import com.hazelcast.topic.TopicOverloadPolicy;
import com.hazelcast.wan.WanPublisherState;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import java.io.File;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.hazelcast.config.EndpointConfig.DEFAULT_SOCKET_KEEP_COUNT;
import static com.hazelcast.config.EndpointConfig.DEFAULT_SOCKET_KEEP_IDLE_SECONDS;
import static com.hazelcast.config.EndpointConfig.DEFAULT_SOCKET_KEEP_INTERVAL_SECONDS;
import static com.hazelcast.config.ServerSocketEndpointConfig.DEFAULT_SOCKET_CONNECT_TIMEOUT_SECONDS;
import static com.hazelcast.config.ServerSocketEndpointConfig.DEFAULT_SOCKET_LINGER_SECONDS;
import static com.hazelcast.config.ServerSocketEndpointConfig.DEFAULT_SOCKET_RECEIVE_BUFFER_SIZE_KB;
import static com.hazelcast.config.ServerSocketEndpointConfig.DEFAULT_SOCKET_SEND_BUFFER_SIZE_KB;
import static com.hazelcast.config.security.LdapRoleMappingMode.getRoleMappingMode;
import static com.hazelcast.config.security.LdapSearchScope.getSearchScope;
import static com.hazelcast.internal.config.AliasedDiscoveryConfigUtils.getConfigByTag;
import static com.hazelcast.internal.config.ConfigSections.ADVANCED_NETWORK;
import static com.hazelcast.internal.config.ConfigSections.AUDITLOG;
import static com.hazelcast.internal.config.ConfigSections.CACHE;
import static com.hazelcast.internal.config.ConfigSections.CARDINALITY_ESTIMATOR;
import static com.hazelcast.internal.config.ConfigSections.CLUSTER_NAME;
import static com.hazelcast.internal.config.ConfigSections.CP_SUBSYSTEM;
import static com.hazelcast.internal.config.ConfigSections.CRDT_REPLICATION;
import static com.hazelcast.internal.config.ConfigSections.DATA_CONNECTION;
import static com.hazelcast.internal.config.ConfigSections.DURABLE_EXECUTOR_SERVICE;
import static com.hazelcast.internal.config.ConfigSections.DYNAMIC_CONFIGURATION;
import static com.hazelcast.internal.config.ConfigSections.EXECUTOR_SERVICE;
import static com.hazelcast.internal.config.ConfigSections.FLAKE_ID_GENERATOR;
import static com.hazelcast.internal.config.ConfigSections.HOT_RESTART_PERSISTENCE;
import static com.hazelcast.internal.config.ConfigSections.IMPORT;
import static com.hazelcast.internal.config.ConfigSections.INSTANCE_NAME;
import static com.hazelcast.internal.config.ConfigSections.INSTANCE_TRACKING;
import static com.hazelcast.internal.config.ConfigSections.INTEGRITY_CHECKER;
import static com.hazelcast.internal.config.ConfigSections.JET;
import static com.hazelcast.internal.config.ConfigSections.LICENSE_KEY;
import static com.hazelcast.internal.config.ConfigSections.LIST;
import static com.hazelcast.internal.config.ConfigSections.LISTENERS;
import static com.hazelcast.internal.config.ConfigSections.LITE_MEMBER;
import static com.hazelcast.internal.config.ConfigSections.LOCAL_DEVICE;
import static com.hazelcast.internal.config.ConfigSections.MANAGEMENT_CENTER;
import static com.hazelcast.internal.config.ConfigSections.MAP;
import static com.hazelcast.internal.config.ConfigSections.MEMBER_ATTRIBUTES;
import static com.hazelcast.internal.config.ConfigSections.METRICS;
import static com.hazelcast.internal.config.ConfigSections.MULTIMAP;
import static com.hazelcast.internal.config.ConfigSections.NATIVE_MEMORY;
import static com.hazelcast.internal.config.ConfigSections.NETWORK;
import static com.hazelcast.internal.config.ConfigSections.PARTITION_GROUP;
import static com.hazelcast.internal.config.ConfigSections.PERSISTENCE;
import static com.hazelcast.internal.config.ConfigSections.PN_COUNTER;
import static com.hazelcast.internal.config.ConfigSections.PROPERTIES;
import static com.hazelcast.internal.config.ConfigSections.QUEUE;
import static com.hazelcast.internal.config.ConfigSections.RELIABLE_TOPIC;
import static com.hazelcast.internal.config.ConfigSections.REPLICATED_MAP;
import static com.hazelcast.internal.config.ConfigSections.REST;
import static com.hazelcast.internal.config.ConfigSections.RINGBUFFER;
import static com.hazelcast.internal.config.ConfigSections.SCHEDULED_EXECUTOR_SERVICE;
import static com.hazelcast.internal.config.ConfigSections.SECURITY;
import static com.hazelcast.internal.config.ConfigSections.SERIALIZATION;
import static com.hazelcast.internal.config.ConfigSections.SET;
import static com.hazelcast.internal.config.ConfigSections.SPLIT_BRAIN_PROTECTION;
import static com.hazelcast.internal.config.ConfigSections.SQL;
import static com.hazelcast.internal.config.ConfigSections.TOPIC;
import static com.hazelcast.internal.config.ConfigSections.TPC;
import static com.hazelcast.internal.config.ConfigSections.USER_CODE_DEPLOYMENT;
import static com.hazelcast.internal.config.ConfigSections.USER_CODE_NAMESPACES;
import static com.hazelcast.internal.config.ConfigSections.VECTOR;
import static com.hazelcast.internal.config.ConfigSections.WAN_REPLICATION;
import static com.hazelcast.internal.config.ConfigSections.canOccurMultipleTimes;
import static com.hazelcast.internal.config.ConfigValidator.checkCacheConfig;
import static com.hazelcast.internal.config.ConfigValidator.checkCacheEvictionConfig;
import static com.hazelcast.internal.config.ConfigValidator.checkMapEvictionConfig;
import static com.hazelcast.internal.config.ConfigValidator.checkNearCacheEvictionConfig;
import static com.hazelcast.internal.config.DomConfigHelper.childElements;
import static com.hazelcast.internal.config.DomConfigHelper.childElementsWithName;
import static com.hazelcast.internal.config.DomConfigHelper.cleanNodeName;
import static com.hazelcast.internal.config.DomConfigHelper.firstChildElement;
import static com.hazelcast.internal.config.DomConfigHelper.getBooleanValue;
import static com.hazelcast.internal.config.DomConfigHelper.getDoubleValue;
import static com.hazelcast.internal.config.DomConfigHelper.getIntegerValue;
import static com.hazelcast.internal.config.DomConfigHelper.getLongValue;
import static com.hazelcast.internal.util.StringUtil.equalsIgnoreCase;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;
import static com.hazelcast.internal.util.StringUtil.lowerCaseInternal;
import static com.hazelcast.internal.util.StringUtil.upperCaseInternal;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.time.temporal.ChronoUnit.SECONDS;

@SuppressWarnings({
        "checkstyle:cyclomaticcomplexity",
        "checkstyle:classdataabstractioncoupling",
        "checkstyle:classfanoutcomplexity",
        "checkstyle:methodcount",
        "checkstyle:methodlength"})
public class MemberDomConfigProcessor extends AbstractDomConfigProcessor {

    private static final ILogger LOGGER = Logger.getLogger(MemberDomConfigProcessor.class);

    protected final Config config;

    public MemberDomConfigProcessor(boolean domLevel3, Config config, boolean strict) {
        super(domLevel3, strict);
        this.config = config;
    }

    public MemberDomConfigProcessor(boolean domLevel3, Config config) {
        super(domLevel3);
        this.config = config;
    }

    @Override
    public void buildConfig(Node rootNode) throws Exception {
        for (Node node : childElements(rootNode)) {
            String nodeName = cleanNodeName(node);
            if (occurrenceSet.contains(nodeName)) {
                throw new InvalidConfigurationException(
                        "Duplicate '" + nodeName + "' definition found in the configuration.");
            }
            if (handleNode(node)) {
                continue;
            }
            if (!canOccurMultipleTimes(nodeName)) {
                occurrenceSet.add(nodeName);
            }
        }

        validateNetworkConfig();
    }

    private boolean handleNode(Node node) throws Exception {
        String nodeName = cleanNodeName(node);

        if (matches(INSTANCE_NAME.getName(), nodeName)) {
            config.setInstanceName(getNonEmptyText(node, "Instance name"));
        } else if (matches(NETWORK.getName(), nodeName)) {
            handleNetwork(node);
        } else if (matches(IMPORT.getName(), nodeName)) {
            throw new HazelcastException("Non-expanded <import> element found");
        } else if (matches(CLUSTER_NAME.getName(), nodeName)) {
            config.setClusterName(getNonEmptyText(node, "Clustername"));
        } else if (matches(PROPERTIES.getName(), nodeName)) {
            fillProperties(node, config.getProperties());
        } else if (matches(WAN_REPLICATION.getName(), nodeName)) {
            handleWanReplication(node);
        } else if (matches(EXECUTOR_SERVICE.getName(), nodeName)) {
            handleExecutor(node);
        } else if (matches(DURABLE_EXECUTOR_SERVICE.getName(), nodeName)) {
            handleDurableExecutor(node);
        } else if (matches(SCHEDULED_EXECUTOR_SERVICE.getName(), nodeName)) {
            handleScheduledExecutor(node);
        } else if (matches(QUEUE.getName(), nodeName)) {
            handleQueue(node);
        } else if (matches(MAP.getName(), nodeName)) {
            handleMap(node);
        } else if (matches(MULTIMAP.getName(), nodeName)) {
            handleMultiMap(node);
        } else if (matches(REPLICATED_MAP.getName(), nodeName)) {
            handleReplicatedMap(node);
        } else if (matches(LIST.getName(), nodeName)) {
            handleList(node);
        } else if (matches(SET.getName(), nodeName)) {
            handleSet(node);
        } else if (matches(TOPIC.getName(), nodeName)) {
            handleTopic(node);
        } else if (matches(RELIABLE_TOPIC.getName(), nodeName)) {
            handleReliableTopic(node);
        } else if (matches(CACHE.getName(), nodeName)) {
            handleCache(node);
        } else if (matches(NATIVE_MEMORY.getName(), nodeName)) {
            fillNativeMemoryConfig(node, config.getNativeMemoryConfig());
        } else if (matches(RINGBUFFER.getName(), nodeName)) {
            handleRingbuffer(node);
        } else if (matches(LISTENERS.getName(), nodeName)) {
            handleListeners(node);
        } else if (matches(PARTITION_GROUP.getName(), nodeName)) {
            handlePartitionGroup(node);
        } else if (matches(SERIALIZATION.getName(), nodeName)) {
            handleSerialization(node);
        } else if (matches(SECURITY.getName(), nodeName)) {
            handleSecurity(node);
        } else if (matches(MEMBER_ATTRIBUTES.getName(), nodeName)) {
            handleMemberAttributes(node);
        } else if (matches(LICENSE_KEY.getName(), nodeName)) {
            config.setLicenseKey(getTextContent(node));
        } else if (matches(MANAGEMENT_CENTER.getName(), nodeName)) {
            handleManagementCenterConfig(node);
        } else if (matches(SPLIT_BRAIN_PROTECTION.getName(), nodeName)) {
            handleSplitBrainProtection(node);
        } else if (matches(LITE_MEMBER.getName(), nodeName)) {
            handleLiteMember(node);
        } else if (matches(HOT_RESTART_PERSISTENCE.getName(), nodeName)) {
            handleHotRestartPersistence(node);
        } else if (matches(PERSISTENCE.getName(), nodeName)) {
            handlePersistence(node);
        } else if (matches(USER_CODE_DEPLOYMENT.getName(), nodeName)) {
            handleUserCodeDeployment(node);
        } else if (matches(CARDINALITY_ESTIMATOR.getName(), nodeName)) {
            handleCardinalityEstimator(node);
        } else if (matches(FLAKE_ID_GENERATOR.getName(), nodeName)) {
            handleFlakeIdGenerator(node);
        } else if (matches(CRDT_REPLICATION.getName(), nodeName)) {
            handleCRDTReplication(node);
        } else if (matches(PN_COUNTER.getName(), nodeName)) {
            handlePNCounter(node);
        } else if (matches(ADVANCED_NETWORK.getName(), nodeName)) {
            handleAdvancedNetwork(node);
        } else if (matches(CP_SUBSYSTEM.getName(), nodeName)) {
            handleCPSubsystem(node);
        } else if (matches(AUDITLOG.getName(), nodeName)) {
            fillFactoryWithPropertiesConfig(node, config.getAuditlogConfig());
        } else if (matches(METRICS.getName(), nodeName)) {
            handleMetrics(node);
        } else if (matches(INSTANCE_TRACKING.getName(), nodeName)) {
            handleInstanceTracking(node, config.getInstanceTrackingConfig());
        } else if (matches(SQL.getName(), nodeName)) {
            handleSql(node);
        } else if (matches(JET.getName(), nodeName)) {
            handleJet(node);
        } else if (matches(LOCAL_DEVICE.getName(), nodeName)) {
            handleLocalDevice(node);
        } else if (matches(DYNAMIC_CONFIGURATION.getName(), nodeName)) {
            handleDynamicConfiguration(node);
        } else if (matches(INTEGRITY_CHECKER.getName(), nodeName)) {
            handleIntegrityChecker(node);
        } else if (matches(DATA_CONNECTION.getName(), nodeName)) {
            handleDataConnections(node);
        } else if (matches(TPC.getName(), nodeName)) {
            handleTpc(node);
        } else if (matches(USER_CODE_NAMESPACES.getName(), nodeName)) {
            handleNamespaces(node);
        } else if (matches(REST.getName(), nodeName)) {
            handleRest(node);
        } else if (matches(VECTOR.getName(), nodeName)) {
            handleVector(node);
        } else {
            return true;
        }
        return false;
    }

    private String getNonEmptyText(Node node, String configName) throws InvalidConfigurationException {
        String val = getTextContent(node);
        if (val == null || val.isEmpty()) {
            throw new InvalidConfigurationException("XML configuration is empty: " + configName);
        }
        return val;
    }

    private void handleUserCodeDeployment(Node dcRoot) {
        UserCodeDeploymentConfig dcConfig = config.getUserCodeDeploymentConfig();
        Node attrEnabled = getNamedItemNode(dcRoot, "enabled");
        boolean enabled = getBooleanValue(getTextContent(attrEnabled));
        dcConfig.setEnabled(enabled);

        for (Node n : childElements(dcRoot)) {
            String name = cleanNodeName(n);
            if (matches("class-cache-mode", name)) {
                UserCodeDeploymentConfig.ClassCacheMode classCacheMode
                        = UserCodeDeploymentConfig.ClassCacheMode.valueOf(getTextContent(n));
                dcConfig.setClassCacheMode(classCacheMode);
            } else if (matches("provider-mode", name)) {
                UserCodeDeploymentConfig.ProviderMode providerMode
                        = UserCodeDeploymentConfig.ProviderMode.valueOf(getTextContent(n));
                dcConfig.setProviderMode(providerMode);
            } else if (matches("blacklist-prefixes", name)) {
                dcConfig.setBlacklistedPrefixes(getTextContent(n));
            } else if (matches("whitelist-prefixes", name)) {
                dcConfig.setWhitelistedPrefixes(getTextContent(n));
            } else if (matches("provider-filter", name)) {
                dcConfig.setProviderFilter(getTextContent(n));
            }
        }
        config.setUserCodeDeploymentConfig(dcConfig);
    }

    private void handleHotRestartPersistence(Node hrRoot)
            throws Exception {
        HotRestartPersistenceConfig hrConfig = config.getHotRestartPersistenceConfig()
                .setEnabled(getBooleanValue(getAttribute(hrRoot, "enabled")));

        String parallelismName = "parallelism";
        String validationTimeoutName = "validation-timeout-seconds";
        String dataLoadTimeoutName = "data-load-timeout-seconds";

        for (Node n : childElements(hrRoot)) {
            String name = cleanNodeName(n);
            if (matches("encryption-at-rest", name)) {
                handleEncryptionAtRest(n, hrConfig);
            } else {
                if (matches("base-dir", name)) {
                    hrConfig.setBaseDir(new File(getTextContent(n)).getAbsoluteFile());
                } else if (matches("backup-dir", name)) {
                    hrConfig.setBackupDir(new File(getTextContent(n)).getAbsoluteFile());
                } else if (matches(parallelismName, name)) {
                    hrConfig.setParallelism(getIntegerValue(parallelismName, getTextContent(n)));
                } else if (matches(validationTimeoutName, name)) {
                    hrConfig.setValidationTimeoutSeconds(getIntegerValue(validationTimeoutName, getTextContent(n)));
                } else if (matches(dataLoadTimeoutName, name)) {
                    hrConfig.setDataLoadTimeoutSeconds(getIntegerValue(dataLoadTimeoutName, getTextContent(n)));
                } else if (matches("cluster-data-recovery-policy", name)) {
                    hrConfig.setClusterDataRecoveryPolicy(
                            HotRestartClusterDataRecoveryPolicy.valueOf(upperCaseInternal(getTextContent(n))));
                } else if (matches("auto-remove-stale-data", name)) {
                    hrConfig.setAutoRemoveStaleData(getBooleanValue(getTextContent(n)));
                }
            }
        }
        config.setHotRestartPersistenceConfig(hrConfig);
    }

    private void handlePersistence(Node prRoot)
            throws Exception {
        PersistenceConfig prConfig = config.getPersistenceConfig()
                .setEnabled(getBooleanValue(getAttribute(prRoot, "enabled")));

        String parallelismName = "parallelism";
        String validationTimeoutName = "validation-timeout-seconds";
        String dataLoadTimeoutName = "data-load-timeout-seconds";
        String rebalanceDelaySecondsName = "rebalance-delay-seconds";

        for (Node n : childElements(prRoot)) {
            String name = cleanNodeName(n);
            if (matches("encryption-at-rest", name)) {
                handleEncryptionAtRest(n, prConfig);
            } else {
                if (matches("base-dir", name)) {
                    prConfig.setBaseDir(new File(getTextContent(n)).getAbsoluteFile());
                } else if (matches("backup-dir", name)) {
                    prConfig.setBackupDir(new File(getTextContent(n)).getAbsoluteFile());
                } else if (matches(parallelismName, name)) {
                    prConfig.setParallelism(getIntegerValue(parallelismName, getTextContent(n)));
                } else if (matches(validationTimeoutName, name)) {
                    prConfig.setValidationTimeoutSeconds(getIntegerValue(validationTimeoutName, getTextContent(n)));
                } else if (matches(dataLoadTimeoutName, name)) {
                    prConfig.setDataLoadTimeoutSeconds(getIntegerValue(dataLoadTimeoutName, getTextContent(n)));
                } else if (matches("cluster-data-recovery-policy", name)) {
                    prConfig.setClusterDataRecoveryPolicy(
                            PersistenceClusterDataRecoveryPolicy.valueOf(upperCaseInternal(getTextContent(n))));
                } else if (matches("auto-remove-stale-data", name)) {
                    prConfig.setAutoRemoveStaleData(getBooleanValue(getTextContent(n)));
                } else if (matches("rebalance-delay-seconds", name)) {
                    prConfig.setRebalanceDelaySeconds(getIntegerValue(rebalanceDelaySecondsName, getTextContent(n)));
                }
            }
        }
        config.setPersistenceConfig(prConfig);
    }

    private void handleDynamicConfiguration(Node node) {
        DynamicConfigurationConfig dynamicConfigurationConfig = config.getDynamicConfigurationConfig();
        for (Node n : childElements(node)) {
            String name = cleanNodeName(n);
            if (matches("persistence-enabled", name)) {
                dynamicConfigurationConfig.setPersistenceEnabled(parseBoolean(getTextContent(n)));
            } else if (matches("backup-dir", name)) {
                dynamicConfigurationConfig.setBackupDir(new File(getTextContent(n)).getAbsoluteFile());
            } else if (matches("backup-count", name)) {
                dynamicConfigurationConfig.setBackupCount(parseInt(getTextContent(n)));
            }
        }
    }

    protected void handleLocalDevice(Node parentNode) {
        String name = getAttribute(parentNode, "name");
        LocalDeviceConfig localDeviceConfig =
                (LocalDeviceConfig) ConfigUtils.getByNameOrNew(config.getDeviceConfigs(), name, LocalDeviceConfig.class);

        handleLocalDeviceNode(parentNode, localDeviceConfig);
    }

    protected void handleLocalDeviceNode(Node deviceNode, LocalDeviceConfig localDeviceConfig) {
        String blockSizeName = "block-size";
        String readIOThreadCountName = "read-io-thread-count";
        String writeIOThreadCountName = "write-io-thread-count";

        for (Node n : childElements(deviceNode)) {
            String name = cleanNodeName(n);
            if (matches("base-dir", name)) {
                localDeviceConfig.setBaseDir(new File(getTextContent(n)).getAbsoluteFile());
            } else if (matches("capacity", name)) {
                localDeviceConfig.setCapacity(createCapacity(n));
            } else if (matches(blockSizeName, name)) {
                localDeviceConfig.setBlockSize(getIntegerValue(blockSizeName, getTextContent(n)));
            } else if (matches(readIOThreadCountName, name)) {
                localDeviceConfig.setReadIOThreadCount(getIntegerValue(readIOThreadCountName, getTextContent(n)));
            } else if (matches(writeIOThreadCountName, name)) {
                localDeviceConfig.setWriteIOThreadCount(getIntegerValue(writeIOThreadCountName, getTextContent(n)));
            }
        }
        config.addDeviceConfig(localDeviceConfig);
    }

    private TieredStoreConfig createTieredStoreConfig(Node tsRoot) {
        TieredStoreConfig tieredStoreConfig = new TieredStoreConfig();

        Node attrEnabled = getNamedItemNode(tsRoot, "enabled");
        boolean enabled = getBooleanValue(getTextContent(attrEnabled));
        tieredStoreConfig.setEnabled(enabled);

        for (Node n : childElements(tsRoot)) {
            String name = cleanNodeName(n);

            if (matches("memory-tier", name)) {
                tieredStoreConfig.setMemoryTierConfig(createMemoryTierConfig(n));
            } else if (matches("disk-tier", name)) {
                tieredStoreConfig.setDiskTierConfig(createDiskTierConfig(n));
            }
        }
        return tieredStoreConfig;
    }

    private MemoryTierConfig createMemoryTierConfig(Node node) {
        MemoryTierConfig memoryTierConfig = new MemoryTierConfig();

        for (Node n : childElements(node)) {
            String name = cleanNodeName(n);

            if (matches("capacity", name)) {
                return memoryTierConfig.setCapacity(createCapacity(n));
            }
        }
        return memoryTierConfig;
    }

    private DiskTierConfig createDiskTierConfig(Node node) {
        DiskTierConfig diskTierConfig = new DiskTierConfig();

        Node attrEnabled = getNamedItemNode(node, "enabled");
        boolean enabled = getBooleanValue(getTextContent(attrEnabled));
        diskTierConfig.setEnabled(enabled);

        Node attrDeviceName = getNamedItemNode(node, "device-name");
        if (attrDeviceName != null) {
            diskTierConfig.setDeviceName(getTextContent(attrDeviceName));
        }

        return diskTierConfig;
    }

    private void handleEncryptionAtRest(Node encryptionAtRestRoot, HotRestartPersistenceConfig hrConfig)
            throws Exception {
        EncryptionAtRestConfig encryptionAtRestConfig = new EncryptionAtRestConfig();
        handleViaReflection(encryptionAtRestRoot, hrConfig, encryptionAtRestConfig, "secure-store");
        for (Node secureStore : childElementsWithName(encryptionAtRestRoot, "secure-store", strict)) {
            handleSecureStore(secureStore, encryptionAtRestConfig);
        }
        hrConfig.setEncryptionAtRestConfig(encryptionAtRestConfig);
    }

    private void handleEncryptionAtRest(Node encryptionAtRestRoot, PersistenceConfig prConfig)
            throws Exception {
        EncryptionAtRestConfig encryptionAtRestConfig = new EncryptionAtRestConfig();
        handleViaReflection(encryptionAtRestRoot, prConfig, encryptionAtRestConfig, "secure-store");
        for (Node secureStore : childElementsWithName(encryptionAtRestRoot, "secure-store", strict)) {
            handleSecureStore(secureStore, encryptionAtRestConfig);
        }
        prConfig.setEncryptionAtRestConfig(encryptionAtRestConfig);
    }

    private void handleSecureStore(Node secureStoreRoot, EncryptionAtRestConfig encryptionAtRestConfig) {
        Node n = firstChildElement(secureStoreRoot);
        if (n != null) {
            String name = cleanNodeName(n);
            SecureStoreConfig secureStoreConfig;
            if (matches("keystore", name)) {
                secureStoreConfig = handleJavaKeyStore(n);
            } else if (matches("vault", name)) {
                secureStoreConfig = handleVault(n);
            } else {
                throw new InvalidConfigurationException("Unrecognized Secure Store type: " + name);
            }
            encryptionAtRestConfig.setSecureStoreConfig(secureStoreConfig);
        }
    }

    private SecureStoreConfig handleJavaKeyStore(Node keyStoreRoot) {
        File path = null;
        String password = null;
        String type = null;
        String currentKeyAlias = null;
        int pollingInterval = JavaKeyStoreSecureStoreConfig.DEFAULT_POLLING_INTERVAL;
        for (Node n : childElements(keyStoreRoot)) {
            String name = cleanNodeName(n);
            if (matches("path", name)) {
                path = new File(getTextContent(n)).getAbsoluteFile();
            } else if (matches("type", name)) {
                type = getTextContent(n);
            } else if (matches("password", name)) {
                password = getTextContent(n);
            } else if (matches("current-key-alias", name)) {
                currentKeyAlias = getTextContent(n);
            } else if (matches("polling-interval", name)) {
                pollingInterval = parseInt(getTextContent(n));
            }
        }
        JavaKeyStoreSecureStoreConfig keyStoreSecureStoreConfig = new JavaKeyStoreSecureStoreConfig(path)
                .setPassword(password)
                .setPollingInterval(pollingInterval)
                .setCurrentKeyAlias(currentKeyAlias);

        if (type != null) {
            keyStoreSecureStoreConfig.setType(type);
        }
        return keyStoreSecureStoreConfig;
    }

    private SecureStoreConfig handleVault(Node vaultRoot) {
        String address = null;
        String secretPath = null;
        String token = null;
        SSLConfig sslConfig = null;
        int pollingInterval = VaultSecureStoreConfig.DEFAULT_POLLING_INTERVAL;
        for (Node n : childElements(vaultRoot)) {
            String name = cleanNodeName(n);
            if (matches("address", name)) {
                address = getTextContent(n);
            } else if (matches("secret-path", name)) {
                secretPath = getTextContent(n);
            } else if (matches("token", name)) {
                token = getTextContent(n);
            } else if (matches("ssl", name)) {
                sslConfig = parseSslConfig(n);
            } else if (matches("polling-interval", name)) {
                pollingInterval = parseInt(getTextContent(n));
            }
        }
        return new VaultSecureStoreConfig(address, secretPath, token)
                .setSSLConfig(sslConfig)
                .setPollingInterval(pollingInterval);
    }

    private void handleCRDTReplication(Node root) {
        final CRDTReplicationConfig replicationConfig = new CRDTReplicationConfig();
        final String replicationPeriodMillisName = "replication-period-millis";
        final String maxConcurrentReplicationTargetsName = "max-concurrent-replication-targets";

        for (Node n : childElements(root)) {
            final String name = cleanNodeName(n);
            if (matches(replicationPeriodMillisName, name)) {
                replicationConfig.setReplicationPeriodMillis(
                        getIntegerValue(replicationPeriodMillisName, getTextContent(n)));
            } else if (matches(maxConcurrentReplicationTargetsName, name)) {
                replicationConfig.setMaxConcurrentReplicationTargets(
                        getIntegerValue(maxConcurrentReplicationTargetsName, getTextContent(n)));
            }
        }
        this.config.setCRDTReplicationConfig(replicationConfig);
    }

    private void handleLiteMember(Node node) {
        Node attrEnabled = getNamedItemNode(node, "enabled");
        boolean liteMember = attrEnabled != null && getBooleanValue(getTextContent(attrEnabled));
        config.setLiteMember(liteMember);
    }

    protected void handleSplitBrainProtection(Node node) {
        String name = getAttribute(node, "name");
        SplitBrainProtectionConfig splitBrainProtectionConfig = ConfigUtils.getByNameOrNew(
                config.getSplitBrainProtectionConfigs(),
                name,
                SplitBrainProtectionConfig.class);
        handleSplitBrainProtectionNode(node, splitBrainProtectionConfig, name);
    }

    protected void handleSplitBrainProtectionNode(Node node, SplitBrainProtectionConfig splitBrainProtectionConfig, String name) {
        Node attrEnabled = getNamedItemNode(node, "enabled");
        boolean enabled = attrEnabled != null && getBooleanValue(getTextContent(attrEnabled));
        // probabilistic-split-brain-protection and recently-active-split-brain-protection
        // configs are constructed via SplitBrainProtectionConfigBuilder
        SplitBrainProtectionConfigBuilder splitBrainProtectionConfigBuilder = null;
        splitBrainProtectionConfig.setEnabled(enabled);
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if (matches("minimum-cluster-size", nodeName)) {
                splitBrainProtectionConfig.setMinimumClusterSize(getIntegerValue("minimum-cluster-size", getTextContent(n)));
            } else if (matches("listeners", nodeName)) {
                handleSplitBrainProtectionListeners(splitBrainProtectionConfig, n);
            } else if (matches("protect-on", nodeName)) {
                splitBrainProtectionConfig.setProtectOn(SplitBrainProtectionOn.valueOf(upperCaseInternal(getTextContent(n))));
            } else if (matches("function-class-name", nodeName)) {
                splitBrainProtectionConfig.setFunctionClassName(getTextContent(n));
            } else if (matches("recently-active-split-brain-protection", nodeName)) {
                splitBrainProtectionConfigBuilder =
                        handleRecentlyActiveSplitBrainProtection(name, n, splitBrainProtectionConfig.getMinimumClusterSize());
            } else if (matches("probabilistic-split-brain-protection", nodeName)) {
                splitBrainProtectionConfigBuilder =
                        handleProbabilisticSplitBrainProtection(name, n, splitBrainProtectionConfig.getMinimumClusterSize());
            }
        }
        if (splitBrainProtectionConfigBuilder != null) {
            boolean splitBrainProtectionFunctionDefinedByClassName =
                    !isNullOrEmpty(splitBrainProtectionConfig.getFunctionClassName());
            if (splitBrainProtectionFunctionDefinedByClassName) {
                throw new InvalidConfigurationException("A split brain protection cannot simultaneously"
                        + " define probabilistic-split-brain-protection or "
                        + "recently-active-split-brain-protection and a split brain protection function class name.");
            }
            // ensure parsed attributes are reflected in constructed split brain protection config
            SplitBrainProtectionConfig constructedConfig = splitBrainProtectionConfigBuilder.build();
            constructedConfig.setMinimumClusterSize(splitBrainProtectionConfig.getMinimumClusterSize());
            constructedConfig.setProtectOn(splitBrainProtectionConfig.getProtectOn());
            constructedConfig.setListenerConfigs(splitBrainProtectionConfig.getListenerConfigs());
            splitBrainProtectionConfig = constructedConfig;
        }
        config.addSplitBrainProtectionConfig(splitBrainProtectionConfig);
    }

    protected void handleSplitBrainProtectionListeners(SplitBrainProtectionConfig splitBrainProtectionConfig, Node n) {
        for (Node listenerNode : childElements(n)) {
            if (matches("listener", cleanNodeName(listenerNode))) {
                String listenerClass = getTextContent(listenerNode);
                splitBrainProtectionConfig.addListenerConfig(new SplitBrainProtectionListenerConfig(listenerClass));
            }
        }
    }

    private SplitBrainProtectionConfigBuilder handleRecentlyActiveSplitBrainProtection(String name, Node node,
                                                                                       int splitBrainProtectionSize) {
        SplitBrainProtectionConfigBuilder splitBrainProtectionConfigBuilder;
        int heartbeatToleranceMillis = getIntegerValue("heartbeat-tolerance-millis",
                getAttribute(node, "heartbeat-tolerance-millis"),
                RecentlyActiveSplitBrainProtectionConfigBuilder.DEFAULT_HEARTBEAT_TOLERANCE_MILLIS);
        splitBrainProtectionConfigBuilder = SplitBrainProtectionConfig.newRecentlyActiveSplitBrainProtectionConfigBuilder(name,
                splitBrainProtectionSize,
                heartbeatToleranceMillis);
        return splitBrainProtectionConfigBuilder;
    }

    private SplitBrainProtectionConfigBuilder handleProbabilisticSplitBrainProtection(String name, Node node,
                                                                                      int splitBrainProtectionSize) {
        SplitBrainProtectionConfigBuilder splitBrainProtectionConfigBuilder;
        long acceptableHeartPause = getLongValue("acceptable-heartbeat-pause-millis",
                getAttribute(node, "acceptable-heartbeat-pause-millis"),
                ProbabilisticSplitBrainProtectionConfigBuilder.DEFAULT_HEARTBEAT_PAUSE_MILLIS);
        double threshold = getDoubleValue("suspicion-threshold",
                getAttribute(node, "suspicion-threshold"),
                ProbabilisticSplitBrainProtectionConfigBuilder.DEFAULT_PHI_THRESHOLD);
        int maxSampleSize = getIntegerValue("max-sample-size",
                getAttribute(node, "max-sample-size"),
                ProbabilisticSplitBrainProtectionConfigBuilder.DEFAULT_SAMPLE_SIZE);
        long minStdDeviation = getLongValue("min-std-deviation-millis",
                getAttribute(node, "min-std-deviation-millis"),
                ProbabilisticSplitBrainProtectionConfigBuilder.DEFAULT_MIN_STD_DEVIATION);
        long heartbeatIntervalMillis = getLongValue("heartbeat-interval-millis",
                getAttribute(node, "heartbeat-interval-millis"),
                ProbabilisticSplitBrainProtectionConfigBuilder.DEFAULT_HEARTBEAT_INTERVAL_MILLIS);
        splitBrainProtectionConfigBuilder = SplitBrainProtectionConfig.
                newProbabilisticSplitBrainProtectionConfigBuilder(name, splitBrainProtectionSize)
                .withAcceptableHeartbeatPauseMillis(acceptableHeartPause)
                .withSuspicionThreshold(threshold)
                .withHeartbeatIntervalMillis(heartbeatIntervalMillis)
                .withMinStdDeviationMillis(minStdDeviation)
                .withMaxSampleSize(maxSampleSize);
        return splitBrainProtectionConfigBuilder;
    }

    protected void handleWanReplication(Node node) {
        Node attName = getNamedItemNode(node, "name");
        String name = getTextContent(attName);

        WanReplicationConfig wanReplicationConfig = new WanReplicationConfig();
        wanReplicationConfig.setName(name);

        handleWanReplicationNode(node, wanReplicationConfig);
    }

    void handleWanReplicationNode(Node node, WanReplicationConfig wanReplicationConfig) {
        for (Node nodeTarget : childElements(node)) {
            String nodeName = cleanNodeName(nodeTarget);
            handleWanReplicationChild(wanReplicationConfig, nodeTarget, nodeName);

        }
        config.addWanReplicationConfig(wanReplicationConfig);
    }

    protected void handleWanReplicationChild(WanReplicationConfig wanReplicationConfig,
                                             Node nodeTarget,
                                             String nodeName) {
        if (matches("batch-publisher", nodeName)) {
            WanBatchPublisherConfig config = new WanBatchPublisherConfig();
            handleBatchWanPublisherNode(wanReplicationConfig, nodeTarget, config);
        } else if (matches("custom-publisher", nodeName)) {
            WanCustomPublisherConfig config = new WanCustomPublisherConfig();
            handleCustomWanPublisherNode(wanReplicationConfig, nodeTarget, config);
        } else if (matches("consumer", nodeName)) {
            handleWanConsumerNode(wanReplicationConfig, nodeTarget);
        }
    }

    void handleCustomWanPublisherNode(WanReplicationConfig wanReplicationConfig,
                                      Node nodeTarget,
                                      WanCustomPublisherConfig config) {
        for (Node targetChild : childElements(nodeTarget)) {
            String targetChildName = cleanNodeName(targetChild);
            if (matches("properties", targetChildName)) {
                fillProperties(targetChild, config.getProperties());
            } else if (matches("publisher-id", targetChildName)) {
                config.setPublisherId(getTextContent(targetChild));
            } else if (matches("class-name", targetChildName)) {
                config.setClassName(getTextContent(targetChild));
            }
        }
        wanReplicationConfig.addCustomPublisherConfig(config);
    }

    void handleBatchWanPublisherNode(WanReplicationConfig wanReplicationConfig, Node nodeTarget,
                                     WanBatchPublisherConfig config) {
        for (Node targetChild : childElements(nodeTarget)) {
            String targetChildName = cleanNodeName(targetChild);
            if (matches("cluster-name", targetChildName)) {
                config.setClusterName(getTextContent(targetChild));
            } else if (matches("publisher-id", targetChildName)) {
                config.setPublisherId(getTextContent(targetChild));
            } else if (matches("target-endpoints", targetChildName)) {
                config.setTargetEndpoints(getTextContent(targetChild));
            } else if (matches("snapshot-enabled", targetChildName)) {
                config.setSnapshotEnabled(getBooleanValue(getTextContent(targetChild)));
            } else if (matches("initial-publisher-state", targetChildName)) {
                config.setInitialPublisherState(WanPublisherState.valueOf(upperCaseInternal(getTextContent(targetChild))));
            } else if (matches("queue-capacity", targetChildName)) {
                config.setQueueCapacity(getIntegerValue("queue-capacity", getTextContent(targetChild)));
            } else if (matches("batch-size", targetChildName)) {
                config.setBatchSize(getIntegerValue("batch-size", getTextContent(targetChild)));
            } else if (matches("batch-max-delay-millis", targetChildName)) {
                config.setBatchMaxDelayMillis(getIntegerValue("batch-max-delay-millis", getTextContent(targetChild)));
            } else if (matches("response-timeout-millis", targetChildName)) {
                config.setResponseTimeoutMillis(getIntegerValue("response-timeout-millis", getTextContent(targetChild)));
            } else if (matches("queue-full-behavior", targetChildName)) {
                config.setQueueFullBehavior(WanQueueFullBehavior.valueOf(upperCaseInternal(getTextContent(targetChild))));
            } else if (matches("acknowledge-type", targetChildName)) {
                config.setAcknowledgeType(WanAcknowledgeType.valueOf(upperCaseInternal(getTextContent(targetChild))));
            } else if (matches("discovery-period-seconds", targetChildName)) {
                config.setDiscoveryPeriodSeconds(getIntegerValue("discovery-period-seconds", getTextContent(targetChild)));
            } else if (matches("max-target-endpoints", targetChildName)) {
                config.setMaxTargetEndpoints(getIntegerValue("max-target-endpoints", getTextContent(targetChild)));
            } else if (matches("max-concurrent-invocations", targetChildName)) {
                config.setMaxConcurrentInvocations(getIntegerValue("max-concurrent-invocations", getTextContent(targetChild)));
            } else if (matches("use-endpoint-private-address", targetChildName)) {
                config.setUseEndpointPrivateAddress(getBooleanValue(getTextContent(targetChild)));
            } else if (matches("idle-min-park-ns", targetChildName)) {
                config.setIdleMinParkNs(getIntegerValue("idle-min-park-ns", getTextContent(targetChild)));
            } else if (matches("idle-max-park-ns", targetChildName)) {
                config.setIdleMaxParkNs(getIntegerValue("idle-max-park-ns", getTextContent(targetChild)));
            } else if (matches("properties", targetChildName)) {
                fillProperties(targetChild, config.getProperties());
            } else if (AliasedDiscoveryConfigUtils.supports(targetChildName)) {
                handleAliasedDiscoveryStrategy(config, targetChild, targetChildName);
            } else if (matches("discovery-strategies", targetChildName)) {
                handleDiscoveryStrategies(config.getDiscoveryConfig(), targetChild);
            } else if (matches("sync", targetChildName)) {
                handleWanSync(config.getSyncConfig(), targetChild);
            } else if (matches("endpoint", targetChildName)) {
                config.setEndpoint(getTextContent(targetChild));
            }
        }
        wanReplicationConfig.addBatchReplicationPublisherConfig(config);
    }

    void handleWanConsumerNode(WanReplicationConfig wanReplicationConfig, Node nodeTarget) {
        WanConsumerConfig consumerConfig = new WanConsumerConfig();
        for (Node targetChild : childElements(nodeTarget)) {
            handleWanConsumerConfig(consumerConfig, targetChild);
        }
        wanReplicationConfig.setConsumerConfig(consumerConfig);
    }

    private void handleWanSync(WanSyncConfig wanSyncConfig, Node node) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("consistency-check-strategy", nodeName)) {
                String strategy = getTextContent(child);
                wanSyncConfig.setConsistencyCheckStrategy(
                        ConsistencyCheckStrategy.valueOf(upperCaseInternal(strategy)));
            }
        }
    }

    private void handleWanConsumerConfig(WanConsumerConfig consumerConfig, Node targetChild) {
        String targetChildName = cleanNodeName(targetChild);
        if (matches("class-name", targetChildName)) {
            consumerConfig.setClassName(getTextContent(targetChild));
        } else if (matches("properties", targetChildName)) {
            fillProperties(targetChild, consumerConfig.getProperties());
        } else if (matches("persist-wan-replicated-data", targetChildName)) {
            consumerConfig.setPersistWanReplicatedData(getBooleanValue(getTextContent(targetChild)));
        }
    }

    @SuppressWarnings("java:S3776")
    private void handleNetwork(Node node)
            throws Exception {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("reuse-address", nodeName)) {
                config.getNetworkConfig().setReuseAddress(getBooleanValue(getTextContent(child)));
            } else if (matches("port", nodeName)) {
                handlePort(child, config);
            } else if (matches("outbound-ports", nodeName)) {
                handleOutboundPorts(child);
            } else if (matches("public-address", nodeName)) {
                config.getNetworkConfig().setPublicAddress(getTextContent(child));
            } else if (matches("join", nodeName)) {
                handleJoin(child, false);
            } else if (matches("interfaces", nodeName)) {
                handleInterfaces(child);
            } else if (matches("symmetric-encryption", nodeName)) {
                handleViaReflection(child, config.getNetworkConfig(), new SymmetricEncryptionConfig());
            } else if (matches("ssl", nodeName)) {
                handleSSLConfig(child);
            } else if (matches("socket-interceptor", nodeName)) {
                handleSocketInterceptorConfig(child);
            } else if (matches("member-address-provider", nodeName)) {
                handleMemberAddressProvider(child, false);
            } else if (matches("failure-detector", nodeName)) {
                handleFailureDetector(child, false);
            } else if (matches("rest-api", nodeName)) {
                handleRestApi(child);
            } else if (matches("memcache-protocol", nodeName)) {
                handleMemcacheProtocol(child);
            } else if (matches("tpc-socket", nodeName)) {
                handleTpcSocketConfig(child, config.getNetworkConfig().getTpcSocketConfig());
            }
        }
    }

    private void handleAdvancedNetwork(Node node) throws Exception {
        // XSD 1.0 limitation: with the existing <xs:choice maxOccurs="unbounded"> model,
        // the schema can’t restrict these elements to a single occurrence without a
        // backward-incompatible change (e.g., introducing wrapper elements).
        // To stay compatible, we enforce uniqueness in the parser and fail fast with
        // InvalidConfigurationException when a duplicate singleton is encountered.
        final Set<String> singletonElements = Set.of(
                "join",
                "failure-detector",
                "member-address-provider",
                "member-server-socket-endpoint-config",
                "client-server-socket-endpoint-config",
                "rest-server-socket-endpoint-config",
                "memcache-server-socket-endpoint-config"
        );
        final Set<String> seen = new HashSet<>(singletonElements.size());

        // enabled attribute
        NamedNodeMap attributes = node.getAttributes();
        for (int a = 0; a < attributes.getLength(); a++) {
            Node att = attributes.item(a);
            if (matches("enabled", att.getNodeName())) {
                config.getAdvancedNetworkConfig().setEnabled(getBooleanValue(att.getNodeValue()));
            }
        }

        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (singletonElements.contains(nodeName) && !seen.add(nodeName)) {
                throw new InvalidConfigurationException(
                        "At most one " + nodeName + " is allowed under advanced-network."
                );
            }

            if (matches("join", nodeName)) {
                handleJoin(child, true);
            } else if (matches("wan-endpoint-config", nodeName)) {
                handleWanEndpointConfig(child);
            } else if (matches("member-server-socket-endpoint-config", nodeName)) {
                handleMemberServerSocketEndpointConfig(child);
            } else if (matches("client-server-socket-endpoint-config", nodeName)) {
                handleClientServerSocketEndpointConfig(child);
            } else if (matches("wan-server-socket-endpoint-config", nodeName)) {
                handleWanServerSocketEndpointConfig(child);
            } else if (matches("rest-server-socket-endpoint-config", nodeName)) {
                handleRestServerSocketEndpointConfig(child);
            } else if (matches("memcache-server-socket-endpoint-config", nodeName)) {
                handleMemcacheServerSocketEndpointConfig(child);
            } else if (matches("member-address-provider", nodeName)) {
                handleMemberAddressProvider(child, true);
            } else if (matches("failure-detector", nodeName)) {
                handleFailureDetector(child, true);
            }
        }
    }

    private void handleEndpointConfig(EndpointConfig endpointConfig, Node node) throws Exception {
        String endpointName = getAttribute(node, "name");
        handleEndpointConfig(endpointConfig, node, endpointName);
    }

    protected void handleEndpointConfig(EndpointConfig endpointConfig, Node node, String endpointName)
            throws Exception {
        endpointConfig.setName(endpointName);
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            handleEndpointConfigCommons(child, nodeName, endpointConfig);
        }
        config.getAdvancedNetworkConfig().addWanEndpointConfig(endpointConfig);
    }

    private void handleMemberServerSocketEndpointConfig(Node node) throws Exception {
        ServerSocketEndpointConfig config = (ServerSocketEndpointConfig) this.config.getAdvancedNetworkConfig()
                .getEndpointConfigs().getOrDefault(EndpointQualifier.MEMBER, new ServerSocketEndpointConfig());
        config.setProtocolType(ProtocolType.MEMBER);
        handleServerSocketEndpointConfig(config, node);
    }

    private void handleClientServerSocketEndpointConfig(Node node) throws Exception {
        ServerSocketEndpointConfig config = (ServerSocketEndpointConfig) this.config.getAdvancedNetworkConfig()
                .getEndpointConfigs().getOrDefault(EndpointQualifier.CLIENT, new ServerSocketEndpointConfig());
        config.setProtocolType(ProtocolType.CLIENT);
        handleServerSocketEndpointConfig(config, node);
    }

    protected void handleWanServerSocketEndpointConfig(Node node) throws Exception {
        ServerSocketEndpointConfig config = new ServerSocketEndpointConfig();
        config.setProtocolType(ProtocolType.WAN);
        handleServerSocketEndpointConfig(config, node);
    }

    private void handleRestServerSocketEndpointConfig(Node node) throws Exception {
        RestServerEndpointConfig config = (RestServerEndpointConfig) this.config.getAdvancedNetworkConfig()
                .getEndpointConfigs().getOrDefault(EndpointQualifier.REST, new RestServerEndpointConfig());
        handleServerSocketEndpointConfig(config, node);
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("endpoint-groups", nodeName)) {
                for (Node endpointGroup : childElements(child)) {
                    handleRestEndpointGroup(config, endpointGroup);
                }
            }
        }
    }

    private void handleMemcacheServerSocketEndpointConfig(Node node) throws Exception {
        ServerSocketEndpointConfig config = (ServerSocketEndpointConfig) this.config.getAdvancedNetworkConfig()
                .getEndpointConfigs().getOrDefault(EndpointQualifier.MEMCACHE, new ServerSocketEndpointConfig());
        config.setProtocolType(ProtocolType.MEMCACHE);
        handleServerSocketEndpointConfig(config, node);
    }

    protected void handleWanEndpointConfig(Node node) throws Exception {
        EndpointConfig config = new EndpointConfig();
        config.setProtocolType(ProtocolType.WAN);
        handleEndpointConfig(config, node);
    }

    private void handleServerSocketEndpointConfig(ServerSocketEndpointConfig endpointConfig, Node node) throws Exception {
        String name = getAttribute(node, "name");
        handleServerSocketEndpointConfig(endpointConfig, node, name);
    }

    protected void handleServerSocketEndpointConfig(ServerSocketEndpointConfig endpointConfig, Node node, String name)
            throws Exception {
        endpointConfig.setName(name);
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("port", nodeName)) {
                handlePort(child, endpointConfig);
            } else if (matches("public-address", nodeName)) {
                String address = getTextContent(child);
                endpointConfig.setPublicAddress(address);
            } else if (matches("reuse-address", nodeName)) {
                endpointConfig.setReuseAddress(getBooleanValue(getTextContent(child)));
            } else {
                handleEndpointConfigCommons(child, nodeName, endpointConfig);
            }
        }

        switch (endpointConfig.getProtocolType()) {
            case MEMBER:
                ensureServerSocketEndpointConfig(endpointConfig);
                config.getAdvancedNetworkConfig().setMemberEndpointConfig(endpointConfig);
                break;
            case CLIENT:
                ensureServerSocketEndpointConfig(endpointConfig);
                config.getAdvancedNetworkConfig().setClientEndpointConfig(endpointConfig);
                break;
            case REST:
                ensureServerSocketEndpointConfig(endpointConfig);
                config.getAdvancedNetworkConfig().setRestEndpointConfig((RestServerEndpointConfig) endpointConfig);
                break;
            case WAN:
                config.getAdvancedNetworkConfig().addWanEndpointConfig(endpointConfig);
                break;
            case MEMCACHE:
                config.getAdvancedNetworkConfig().setMemcacheEndpointConfig(endpointConfig);
                break;
            default:
                throw new InvalidConfigurationException("Endpoint config has invalid protocol type "
                        + endpointConfig.getProtocolType());
        }
    }

    private void ensureServerSocketEndpointConfig(EndpointConfig endpointConfig) {
        if (endpointConfig instanceof ServerSocketEndpointConfig) {
            return;
        }
        throw new InvalidConfigurationException("Endpoint configuration of protocol type " + endpointConfig.getProtocolType()
                + " must be defined in a <server-socket-endpoint-config> element");
    }

    private void handleEndpointConfigCommons(Node node, String nodeName, EndpointConfig endpointConfig)
            throws Exception {
        if (matches("outbound-ports", nodeName)) {
            handleOutboundPorts(node, endpointConfig);
        } else if (matches("interfaces", nodeName)) {
            handleInterfaces(node, endpointConfig);
        } else if (matches("ssl", nodeName)) {
            handleSSLConfig(node, endpointConfig);
        } else if (matches("socket-interceptor", nodeName)) {
            handleSocketInterceptorConfig(node, endpointConfig);
        } else if (matches("socket-options", nodeName)) {
            handleSocketOptions(node, endpointConfig);
        } else if (matches("symmetric-encryption", nodeName)) {
            handleViaReflection(node, endpointConfig, new SymmetricEncryptionConfig());
        } else if (matches("tpc-socket", nodeName)) {
            handleTpcSocketConfig(node, endpointConfig.getTpcSocketConfig());
        }
    }

    private void handleTpcSocketConfig(Node node, TpcSocketConfig tpcSocketConfig) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("port-range", nodeName)) {
                tpcSocketConfig.setPortRange(getTextContent(child));
            } else if (matches("receive-buffer-size-kb", nodeName)) {
                tpcSocketConfig.setReceiveBufferSizeKB(
                        getIntegerValue("receive-buffer-size-kb", getTextContent(child)));
            } else if (matches("send-buffer-size-kb", nodeName)) {
                tpcSocketConfig.setSendBufferSizeKB(
                        getIntegerValue("send-buffer-size-kb", getTextContent(child)));
            }
        }
    }

    private void handleSocketOptions(Node node, EndpointConfig endpointConfig) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("buffer-direct", nodeName)) {
                endpointConfig.setSocketBufferDirect(getBooleanValue(getTextContent(child)));
            } else if (matches("tcp-no-delay", nodeName)) {
                endpointConfig.setSocketTcpNoDelay(getBooleanValue(getTextContent(child)));
            } else if (matches("keep-alive", nodeName)) {
                endpointConfig.setSocketKeepAlive(getBooleanValue(getTextContent(child)));
            } else if (matches("connect-timeout-seconds", nodeName)) {
                endpointConfig.setSocketConnectTimeoutSeconds(getIntegerValue("connect-timeout-seconds",
                        getTextContent(child), DEFAULT_SOCKET_CONNECT_TIMEOUT_SECONDS));
            } else if (matches("send-buffer-size-kb", nodeName)) {
                endpointConfig.setSocketSendBufferSizeKb(getIntegerValue("send-buffer-size-kb",
                        getTextContent(child), DEFAULT_SOCKET_SEND_BUFFER_SIZE_KB));
            } else if (matches("receive-buffer-size-kb", nodeName)) {
                endpointConfig.setSocketRcvBufferSizeKb(getIntegerValue("receive-buffer-size-kb",
                        getTextContent(child), DEFAULT_SOCKET_RECEIVE_BUFFER_SIZE_KB));
            } else if (matches("linger-seconds", nodeName)) {
                endpointConfig.setSocketLingerSeconds(getIntegerValue("linger-seconds",
                        getTextContent(child), DEFAULT_SOCKET_LINGER_SECONDS));
            } else if (matches("keep-idle-seconds", nodeName)) {
                endpointConfig.setSocketKeepIdleSeconds(getIntegerValue("keep-idle-seconds",
                        getTextContent(child), DEFAULT_SOCKET_KEEP_IDLE_SECONDS));
            } else if (matches("keep-interval-seconds", nodeName)) {
                endpointConfig.setSocketKeepIntervalSeconds(getIntegerValue("keep-interval-seconds",
                        getTextContent(child), DEFAULT_SOCKET_KEEP_INTERVAL_SECONDS));
            } else if (matches("keep-count", nodeName)) {
                endpointConfig.setSocketKeepCount(getIntegerValue("keep-count",
                        getTextContent(child), DEFAULT_SOCKET_KEEP_COUNT));
            }
        }
    }

    protected void handleExecutor(Node node) throws Exception {
        String name = getTextContent(getNamedItemNode(node, "name"));
        ExecutorConfig executorConfig = ConfigUtils.getByNameOrNew(config.getExecutorConfigs(),
                name,
                ExecutorConfig.class);

        handleViaReflection(node, config, executorConfig);
    }

    protected void handleDurableExecutor(Node node) throws Exception {
        String name = getTextContent(getNamedItemNode(node, "name"));
        DurableExecutorConfig durableExecutorConfig = ConfigUtils.getByNameOrNew(
                config.getDurableExecutorConfigs(),
                name,
                DurableExecutorConfig.class);

        handleViaReflection(node, config, durableExecutorConfig);
    }

    protected void handleScheduledExecutor(Node node) {
        String name = getTextContent(getNamedItemNode(node, "name"));
        ScheduledExecutorConfig scheduledExecutorConfig = ConfigUtils.getByNameOrNew(
                config.getScheduledExecutorConfigs(),
                name,
                ScheduledExecutorConfig.class);

        handleScheduledExecutorNode(node, scheduledExecutorConfig);
    }

    void handleScheduledExecutorNode(Node node, ScheduledExecutorConfig scheduledExecutorConfig) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("merge-policy", nodeName)) {
                MergePolicyConfig mpConfig = createMergePolicyConfig(
                        child, scheduledExecutorConfig.getMergePolicyConfig());
                scheduledExecutorConfig.setMergePolicyConfig(mpConfig);
            } else if (matches("capacity", nodeName)) {
                scheduledExecutorConfig.setCapacity(parseInt(getTextContent(child)));
            } else if (matches("capacity-policy", nodeName)) {
                scheduledExecutorConfig.setCapacityPolicy(ScheduledExecutorConfig.CapacityPolicy.valueOf(getTextContent(child)));
            } else if (matches("durability", nodeName)) {
                scheduledExecutorConfig.setDurability(parseInt(getTextContent(child)));
            } else if (matches("pool-size", nodeName)) {
                scheduledExecutorConfig.setPoolSize(parseInt(getTextContent(child)));
            } else if (matches("split-brain-protection-ref", nodeName)) {
                scheduledExecutorConfig.setSplitBrainProtectionName(getTextContent(child));
            } else if (matches("statistics-enabled", nodeName)) {
                scheduledExecutorConfig.setStatisticsEnabled(getBooleanValue(getTextContent(child)));
            } else if (matches("user-code-namespace", nodeName)) {
                scheduledExecutorConfig.setUserCodeNamespace(getTextContent(child));
            }
        }

        config.addScheduledExecutorConfig(scheduledExecutorConfig);
    }

    protected void handleCardinalityEstimator(Node node) {
        CardinalityEstimatorConfig cardinalityEstimatorConfig =
                ConfigUtils.getByNameOrNew(config.getCardinalityEstimatorConfigs(),
                        getTextContent(getNamedItemNode(node, "name")),
                        CardinalityEstimatorConfig.class);

        handleCardinalityEstimatorNode(node, cardinalityEstimatorConfig);
    }

    void handleCardinalityEstimatorNode(Node node, CardinalityEstimatorConfig cardinalityEstimatorConfig) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("merge-policy", nodeName)) {
                MergePolicyConfig mpConfig = createMergePolicyConfig(
                        child, cardinalityEstimatorConfig.getMergePolicyConfig());
                cardinalityEstimatorConfig.setMergePolicyConfig(mpConfig);
            } else if (matches("backup-count", nodeName)) {
                cardinalityEstimatorConfig.setBackupCount(parseInt(getTextContent(child)));
            } else if (matches("async-backup-count", nodeName)) {
                cardinalityEstimatorConfig.setAsyncBackupCount(parseInt(getTextContent(child)));
            } else if (matches("split-brain-protection-ref", nodeName)) {
                cardinalityEstimatorConfig.setSplitBrainProtectionName(getTextContent(child));
            }
        }

        config.addCardinalityEstimatorConfig(cardinalityEstimatorConfig);
    }

    protected void handlePNCounter(Node node) throws Exception {
        String name = getAttribute(node, "name");
        PNCounterConfig pnCounterConfig = ConfigUtils.getByNameOrNew(
                config.getPNCounterConfigs(),
                name,
                PNCounterConfig.class);
        handleViaReflection(node, config, pnCounterConfig);
    }

    protected void handleFlakeIdGenerator(Node node) {
        String name = getAttribute(node, "name");
        FlakeIdGeneratorConfig generatorConfig = ConfigUtils.getByNameOrNew(
                config.getFlakeIdGeneratorConfigs(),
                name,
                FlakeIdGeneratorConfig.class);
        handleFlakeIdGeneratorNode(node, generatorConfig);
    }

    void handleFlakeIdGeneratorNode(Node node, FlakeIdGeneratorConfig generatorConfig) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("prefetch-count", nodeName)) {
                generatorConfig.setPrefetchCount(Integer.parseInt(getTextContent(child)));
            } else if (matches("prefetch-validity-millis", nodeName)) {
                generatorConfig.setPrefetchValidityMillis(Long.parseLong(getTextContent(child)));
            } else if (matches("epoch-start", nodeName)) {
                generatorConfig.setEpochStart(Long.parseLong(getTextContent(child)));
            } else if (matches("node-id-offset", nodeName)) {
                generatorConfig.setNodeIdOffset(Long.parseLong(getTextContent(child)));
            } else if (matches("bits-sequence", nodeName)) {
                generatorConfig.setBitsSequence(Integer.parseInt(getTextContent(child)));
            } else if (matches("bits-node-id", nodeName)) {
                generatorConfig.setBitsNodeId(Integer.parseInt(getTextContent(child)));
            } else if (matches("allowed-future-millis", nodeName)) {
                generatorConfig.setAllowedFutureMillis(Long.parseLong(getTextContent(child)));
            } else if (matches("statistics-enabled", nodeName)) {
                generatorConfig.setStatisticsEnabled(getBooleanValue(getTextContent(child)));
            }
        }
        config.addFlakeIdGeneratorConfig(generatorConfig);
    }

    private void handleInterfaces(Node node) {
        NamedNodeMap attributes = node.getAttributes();
        InterfacesConfig interfaces = config.getNetworkConfig().getInterfaces();
        for (int a = 0; a < attributes.getLength(); a++) {
            Node att = attributes.item(a);
            if (matches("enabled", att.getNodeName())) {
                interfaces.setEnabled(getBooleanValue(att.getNodeValue()));
            }
        }
        handleInterfacesList(node, interfaces);
    }

    protected void handleInterfacesList(Node node, InterfacesConfig interfaces) {
        for (Node n : childElements(node)) {
            if (matches("interface", lowerCaseInternal(cleanNodeName(n)))) {
                interfaces.addInterface(getTextContent(n));
            }
        }
    }

    private void handleInterfaces(Node node, EndpointConfig endpointConfig) {
        NamedNodeMap attributes = node.getAttributes();
        InterfacesConfig interfaces = endpointConfig.getInterfaces();
        for (int a = 0; a < attributes.getLength(); a++) {
            Node att = attributes.item(a);
            if (matches("enabled", att.getNodeName())) {
                interfaces.setEnabled(getBooleanValue(att.getNodeValue()));
            }
        }
        handleInterfacesList(node, interfaces);
    }

    protected void handleViaReflection(Node node, Object parent, Object child, String... nodeExclusions) throws Exception {
        NamedNodeMap attributes = node.getAttributes();
        if (attributes != null) {
            for (int a = 0; a < attributes.getLength(); a++) {
                Node att = attributes.item(a);
                if (!excludeNode(att, nodeExclusions)) {
                    invokeSetter(child, att, att.getNodeValue());
                }
            }
        }
        for (Node n : childElements(node)) {
            if (n instanceof Element && !excludeNode(n, nodeExclusions)) {
                invokeSetter(child, n, getTextContent(n));
            }
        }
        attachChildConfig(parent, child);
    }

    private boolean excludeNode(Node n, String... nodeExclusions) {
        if (nodeExclusions.length > 0) {
            String name = cleanNodeName(n);
            for (String exclusion : nodeExclusions) {
                if (matches(name, exclusion)) {
                    return true;
                }
            }
        }
        return false;
    }

    private void invokeSetter(Object target, Node node, String argument) {
        Method method = getMethod(target, "set" + toPropertyName(cleanNodeName(node)), true);
        if (method == null) {
            throw new InvalidConfigurationException("Invalid element/attribute name in the configuration: "
                    + cleanNodeName(node));
        }
        Class<?> arg = method.getParameterTypes()[0];
        Object coercedArg =
                arg == String.class ? argument
                        : arg == int.class ? Integer.valueOf(argument)
                        : arg == long.class ? Long.valueOf(argument)
                        : arg == boolean.class ? getBooleanValue(argument)
                        : null;
        if (coercedArg == null) {
            throw new HazelcastException(String.format(
                    "Method %s has unsupported argument type %s", method.getName(), arg.getSimpleName()));
        }
        try {
            method.invoke(target, coercedArg);
        } catch (Exception e) {
            throw new HazelcastException(e);
        }
    }

    private static void attachChildConfig(Object parent, Object child) throws Exception {
        String targetName = child.getClass().getSimpleName();
        Method attacher = getMethod(parent, "set" + targetName, false);
        if (attacher == null) {
            attacher = getMethod(parent, "add" + targetName, false);
        }
        if (attacher == null) {
            throw new HazelcastException(String.format(
                    "%s doesn't accept %s as child", parent.getClass().getSimpleName(), targetName));
        }
        attacher.invoke(parent, child);
    }

    private static Method getMethod(Object target, String methodName, boolean requiresArg) {
        Method[] methods = target.getClass().getMethods();
        for (Method method : methods) {
            if (equalsIgnoreCase(method.getName(), methodName)) {
                if (!requiresArg) {
                    return method;
                }
                if (method.getParameterCount() != 1) {
                    continue;
                }
                Class<?> arg = method.getParameterTypes()[0];
                if (arg == String.class || arg == int.class || arg == long.class || arg == boolean.class) {
                    return method;
                }
            }
        }
        return null;
    }

    private String toPropertyName(String element) {
        // handle reflection incompatible reference properties
        String refPropertyName = handleRefProperty(element);
        if (refPropertyName != null) {
            return refPropertyName;
        }

        StringBuilder sb = new StringBuilder();
        char[] chars = element.toCharArray();
        boolean upper = true;
        for (char c : chars) {
            if (c == '_' || c == '-' || c == '.') {
                upper = true;
            } else if (upper) {
                // Character.toUpperCase is not Locale dependant, so we're safe here.
                sb.append(Character.toUpperCase(c));
                upper = false;
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private String handleRefProperty(String element) {
        if (matches(element, "split-brain-protection-ref")) {
            return "SplitBrainProtectionName";
        }
        return null;
    }

    private void handleJoin(Node node, boolean advancedNetworkConfig) {
        JoinConfig joinConfig = joinConfig(advancedNetworkConfig);
        for (Node child : childElements(node)) {
            String name = cleanNodeName(child);
            if (matches("multicast", name)) {
                handleMulticast(child, advancedNetworkConfig);
            } else if (matches("tcp-ip", name)) {
                handleTcpIp(child, advancedNetworkConfig);
            } else if (AliasedDiscoveryConfigUtils.supports(name)) {
                handleAliasedDiscoveryStrategy(joinConfig, child, name);
            } else if (matches("discovery-strategies", name)) {
                handleDiscoveryStrategies(joinConfig.getDiscoveryConfig(), child);
            } else if (matches("auto-detection", name)) {
                handleAutoDetection(child, advancedNetworkConfig);
            }
        }
        joinConfig.verify();
    }

    protected JoinConfig joinConfig(boolean advancedNetworkConfig) {
        return advancedNetworkConfig
                ? config.getAdvancedNetworkConfig().getJoin()
                : config.getNetworkConfig().getJoin();
    }

    private void handleDiscoveryStrategies(DiscoveryConfig discoveryConfig, Node node) {
        for (Node child : childElements(node)) {
            handleDiscoveryStrategiesChild(discoveryConfig, child);
        }
    }

    protected void handleDiscoveryStrategiesChild(DiscoveryConfig discoveryConfig, Node child) {
        String name = cleanNodeName(child);
        if (matches("discovery-strategy", name)) {
            handleDiscoveryStrategy(child, discoveryConfig);
        } else if (matches("node-filter", name)) {
            handleDiscoveryNodeFilter(child, discoveryConfig);
        }
    }

    void handleDiscoveryNodeFilter(Node node, DiscoveryConfig discoveryConfig) {
        Node att = getNamedItemNode(node, "class");
        if (att != null) {
            discoveryConfig.setNodeFilterClass(getTextContent(att));
        }
    }

    void handleDiscoveryStrategy(Node node, DiscoveryConfig discoveryConfig) {
        boolean enabled = false;
        String clazz = null;

        NamedNodeMap attributes = node.getAttributes();
        for (int a = 0; a < attributes.getLength(); a++) {
            Node att = attributes.item(a);
            if (matches("enabled", lowerCaseInternal(att.getNodeName()))) {
                enabled = getBooleanValue(getTextContent(att));
            } else if (matches("class", att.getNodeName())) {
                clazz = getTextContent(att);
            }
        }

        if (!enabled || clazz == null) {
            return;
        }

        Map<String, Comparable> properties = new HashMap<>();
        for (Node child : childElements(node)) {
            String name = cleanNodeName(child);
            if (matches("properties", name)) {
                fillProperties(child, properties);
            }
        }

        discoveryConfig.addDiscoveryStrategyConfig(new DiscoveryStrategyConfig(clazz, properties));
    }

    private void handleAliasedDiscoveryStrategy(JoinConfig joinConfig, Node node, String tag) {
        AliasedDiscoveryConfig aliasedDiscoveryConfig = getConfigByTag(joinConfig, tag);
        updateConfig(aliasedDiscoveryConfig, node);
    }

    private void handleAliasedDiscoveryStrategy(WanBatchPublisherConfig publisherConfig,
                                                Node node,
                                                String tag) {
        AliasedDiscoveryConfig aliasedDiscoveryConfig = getConfigByTag(publisherConfig, tag);
        updateConfig(aliasedDiscoveryConfig, node);
    }

    private void updateConfig(AliasedDiscoveryConfig config, Node node) {
        NamedNodeMap attributes = node.getAttributes();
        for (int a = 0; a < attributes.getLength(); a++) {
            Node att = attributes.item(a);
            if (matches("enabled", lowerCaseInternal(att.getNodeName()))) {
                config.setEnabled(getBooleanValue(getTextContent(att)));
            } else if (matches(att.getNodeName(), "connection-timeout-seconds")) {
                config.setProperty("connection-timeout-seconds", getTextContent(att));
            }
        }
        for (Node n : childElements(node)) {
            String key = cleanNodeName(n, !matches("eureka", n.getParentNode().getLocalName()));
            String value = getTextContent(n);
            config.setProperty(key, value);
        }
    }

    private void handleMulticast(Node node, boolean advancedNetworkConfig) {
        JoinConfig join = joinConfig(advancedNetworkConfig);
        MulticastConfig multicastConfig = join.getMulticastConfig();
        NamedNodeMap attributes = node.getAttributes();
        for (int a = 0; a < attributes.getLength(); a++) {
            Node att = attributes.item(a);
            if (matches("enabled", lowerCaseInternal(att.getNodeName()))) {
                multicastConfig.setEnabled(getBooleanValue(getTextContent(att)));
            } else if (matches("loopbackmodeenabled", lowerCaseInternal(att.getNodeName()))
                    || matches("loopback-mode-enabled", lowerCaseInternal(att.getNodeName()))) {
                multicastConfig.setLoopbackModeEnabled(getBooleanValue(getTextContent(att)));
            }
        }
        for (Node n : childElements(node)) {
            if (matches("multicast-group", cleanNodeName(n))) {
                multicastConfig.setMulticastGroup(getTextContent(n));
            } else if (matches("multicast-port", cleanNodeName(n))) {
                multicastConfig.setMulticastPort(parseInt(getTextContent(n)));
            } else if (matches("multicast-timeout-seconds", cleanNodeName(n))) {
                multicastConfig.setMulticastTimeoutSeconds(parseInt(getTextContent(n)));
            } else if (matches("multicast-time-to-live-seconds", cleanNodeName(n))) {
                // we need this line for the time being to prevent not reading the multicast-time-to-live-seconds property
                // for more info see: https://github.com/hazelcast/hazelcast/issues/752
                multicastConfig.setMulticastTimeToLive(parseInt(getTextContent(n)));
            } else if (matches("multicast-time-to-live", cleanNodeName(n))) {
                multicastConfig.setMulticastTimeToLive(parseInt(getTextContent(n)));
            } else if (matches("trusted-interfaces", cleanNodeName(n))) {
                handleTrustedInterfaces(multicastConfig, n);
            }
        }
    }

    private void handleAutoDetection(Node node, boolean advancedNetworkConfig) {
        JoinConfig join = joinConfig(advancedNetworkConfig);
        AutoDetectionConfig autoDetectionConfig = join.getAutoDetectionConfig();
        NamedNodeMap attributes = node.getAttributes();
        for (int a = 0; a < attributes.getLength(); a++) {
            Node att = attributes.item(a);
            if (matches("enabled", lowerCaseInternal(att.getNodeName()))) {
                autoDetectionConfig.setEnabled(getBooleanValue(getTextContent(att)));
            }
        }
    }

    protected void handleTrustedInterfaces(TrustedInterfacesConfigurable<?> tiConfig, Node n) {
        for (Node child : childElements(n)) {
            if (matches("interface", lowerCaseInternal(cleanNodeName(child)))) {
                tiConfig.addTrustedInterface(getTextContent(child));
            }
        }
    }

    private void handleTcpIp(Node node, boolean advancedNetworkConfig) {
        NamedNodeMap attributes = node.getAttributes();
        JoinConfig join = joinConfig(advancedNetworkConfig);
        TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
        for (int a = 0; a < attributes.getLength(); a++) {
            Node att = attributes.item(a);
            if (matches(att.getNodeName(), "enabled")) {
                tcpIpConfig.setEnabled(getBooleanValue(getTextContent(att)));
            } else if (matches(att.getNodeName(), "connection-timeout-seconds")) {
                tcpIpConfig.setConnectionTimeoutSeconds(
                        getIntegerValue("connection-timeout-seconds", getTextContent(att)));
            }
        }
        Set<String> memberTags = Set.of("interface", "member", "members");
        for (Node n : childElements(node)) {
            if (matches(cleanNodeName(n), "member-list")) {
                handleMemberList(n, advancedNetworkConfig);
            } else if (matches(cleanNodeName(n), "required-member")) {
                if (tcpIpConfig.getRequiredMember() != null) {
                    throw new InvalidConfigurationException("Duplicate required-member"
                            + " definition found in the configuration. ");
                }
                tcpIpConfig.setRequiredMember(getTextContent(n));
            } else if (memberTags.contains(cleanNodeName(n))) {
                tcpIpConfig.addMember(getTextContent(n));
            }
        }
    }

    protected void handleMemberList(Node node, boolean advancedNetworkConfig) {
        JoinConfig join = joinConfig(advancedNetworkConfig);
        TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if (matches("member", nodeName)) {
                tcpIpConfig.addMember(getTextContent(n));
            }
        }
    }

    protected void handlePort(Node node, Config config) {
        String portStr = getTextContent(node);
        NetworkConfig networkConfig = config.getNetworkConfig();
        if (portStr.length() > 0) {
            networkConfig.setPort(parseInt(portStr));
        }
        NamedNodeMap attributes = node.getAttributes();
        for (int a = 0; a < attributes.getLength(); a++) {
            Node att = attributes.item(a);

            if (matches("auto-increment", att.getNodeName())) {
                networkConfig.setPortAutoIncrement(getBooleanValue(getTextContent(att)));
            } else if (matches("port-count", att.getNodeName())) {
                int portCount = parseInt(getTextContent(att));
                networkConfig.setPortCount(portCount);
            }
        }
    }

    protected void handlePort(Node node, ServerSocketEndpointConfig endpointConfig) {
        String portStr = getTextContent(node);
        if (portStr.length() > 0) {
            endpointConfig.setPort(parseInt(portStr));
        }
        handlePortAttributes(node, endpointConfig);
    }

    protected void handlePortAttributes(Node node, ServerSocketEndpointConfig endpointConfig) {
        NamedNodeMap attributes = node.getAttributes();
        for (int a = 0; a < attributes.getLength(); a++) {
            Node att = attributes.item(a);

            if (matches("auto-increment", att.getNodeName())) {
                endpointConfig.setPortAutoIncrement(getBooleanValue(getTextContent(att)));
            } else if (matches("port-count", att.getNodeName())) {
                int portCount = parseInt(getTextContent(att));
                endpointConfig.setPortCount(portCount);
            }
        }
    }

    protected void handleOutboundPorts(Node child) {
        NetworkConfig networkConfig = config.getNetworkConfig();
        for (Node n : childElements(child)) {
            String nodeName = cleanNodeName(n);
            if (matches("ports", nodeName)) {
                networkConfig.addOutboundPortDefinition(getTextContent(n));
            }
        }
    }

    protected void handleOutboundPorts(Node child, EndpointConfig endpointConfig) {
        for (Node n : childElements(child)) {
            String nodeName = cleanNodeName(n);
            if (matches("ports", nodeName)) {
                endpointConfig.addOutboundPortDefinition(getTextContent(n));
            }
        }
    }

    protected void handleQueue(Node node) {
        String name = getTextContent(getNamedItemNode(node, "name"));
        QueueConfig qConfig = ConfigUtils.getByNameOrNew(
                config.getQueueConfigs(),
                name,
                QueueConfig.class);
        handleQueueNode(node, qConfig);
    }

    void handleQueueNode(Node node, final QueueConfig qConfig) {
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if (matches("max-size", nodeName)) {
                qConfig.setMaxSize(getIntegerValue("max-size", getTextContent(n)));
            } else if (matches("backup-count", nodeName)) {
                qConfig.setBackupCount(getIntegerValue("backup-count", getTextContent(n)));
            } else if (matches("async-backup-count", nodeName)) {
                qConfig.setAsyncBackupCount(getIntegerValue("async-backup-count", getTextContent(n)));
            } else if (matches("item-listeners", nodeName)) {
                handleItemListeners(n, qConfig::addItemListenerConfig);
            } else if (matches("statistics-enabled", nodeName)) {
                qConfig.setStatisticsEnabled(getBooleanValue(getTextContent(n)));
            } else if (matches("queue-store", nodeName)) {
                QueueStoreConfig queueStoreConfig = createQueueStoreConfig(n);
                qConfig.setQueueStoreConfig(queueStoreConfig);
            } else if (matches("split-brain-protection-ref", nodeName)) {
                qConfig.setSplitBrainProtectionName(getTextContent(n));
            } else if (matches("empty-queue-ttl", nodeName)) {
                qConfig.setEmptyQueueTtl(getIntegerValue("empty-queue-ttl", getTextContent(n)));
            } else if (matches("merge-policy", nodeName)) {
                MergePolicyConfig mpConfig = createMergePolicyConfig(n, qConfig.getMergePolicyConfig());
                qConfig.setMergePolicyConfig(mpConfig);
            } else if (matches("priority-comparator-class-name", nodeName)) {
                qConfig.setPriorityComparatorClassName(getTextContent(n));
            } else if (matches("user-code-namespace", nodeName)) {
                qConfig.setUserCodeNamespace(getTextContent(n));
            }
        }
        config.addQueueConfig(qConfig);
    }

    protected void handleItemListeners(Node n, Consumer<ItemListenerConfig> configAddFunction) {
        for (Node listenerNode : childElements(n)) {
            if (matches("item-listener", cleanNodeName(listenerNode))) {
                boolean incValue = getBooleanValue(getTextContent(
                        getNamedItemNode(listenerNode, "include-value")));
                String listenerClass = getTextContent(listenerNode);
                configAddFunction.accept(new ItemListenerConfig(listenerClass, incValue));
            }
        }
    }

    protected void handleList(Node node) {
        String name = getTextContent(getNamedItemNode(node, "name"));
        ListConfig lConfig = ConfigUtils.getByNameOrNew(
                config.getListConfigs(),
                name,
                ListConfig.class);
        handleListNode(node, lConfig);
    }

    void handleListNode(Node node, final ListConfig lConfig) {
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if (matches("max-size", nodeName)) {
                lConfig.setMaxSize(getIntegerValue("max-size", getTextContent(n)));
            } else if (matches("backup-count", nodeName)) {
                lConfig.setBackupCount(getIntegerValue("backup-count", getTextContent(n)));
            } else if (matches("async-backup-count", nodeName)) {
                lConfig.setAsyncBackupCount(getIntegerValue("async-backup-count", getTextContent(n)));
            } else if (matches("item-listeners", nodeName)) {
                handleItemListeners(n, lConfig::addItemListenerConfig);
            } else if (matches("statistics-enabled", nodeName)) {
                lConfig.setStatisticsEnabled(getBooleanValue(getTextContent(n)));
            } else if (matches("split-brain-protection-ref", nodeName)) {
                lConfig.setSplitBrainProtectionName(getTextContent(n));
            } else if (matches("merge-policy", nodeName)) {
                MergePolicyConfig mpConfig = createMergePolicyConfig(n, lConfig.getMergePolicyConfig());
                lConfig.setMergePolicyConfig(mpConfig);
            } else if (matches("user-code-namespace", nodeName)) {
                lConfig.setUserCodeNamespace(getTextContent(n));
            }

        }
        config.addListConfig(lConfig);
    }

    protected void handleSet(Node node) {
        String name = getTextContent(getNamedItemNode(node, "name"));
        SetConfig sConfig = ConfigUtils.getByNameOrNew(config.getSetConfigs(),
                name,
                SetConfig.class);
        handleSetNode(node, sConfig);
    }

    void handleSetNode(Node node, final SetConfig sConfig) {
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if (matches("max-size", nodeName)) {
                sConfig.setMaxSize(getIntegerValue("max-size", getTextContent(n)));
            } else if (matches("backup-count", nodeName)) {
                sConfig.setBackupCount(getIntegerValue("backup-count", getTextContent(n)));
            } else if (matches("async-backup-count", nodeName)) {
                sConfig.setAsyncBackupCount(getIntegerValue("async-backup-count", getTextContent(n)));
            } else if (matches("item-listeners", nodeName)) {
                handleItemListeners(n, sConfig::addItemListenerConfig);
            } else if (matches("statistics-enabled", nodeName)) {
                sConfig.setStatisticsEnabled(getBooleanValue(getTextContent(n)));
            } else if (matches("split-brain-protection-ref", nodeName)) {
                sConfig.setSplitBrainProtectionName(getTextContent(n));
            } else if (matches("merge-policy", nodeName)) {
                MergePolicyConfig mpConfig = createMergePolicyConfig(n, sConfig.getMergePolicyConfig());
                sConfig.setMergePolicyConfig(mpConfig);
            } else if (matches("user-code-namespace", nodeName)) {
                sConfig.setUserCodeNamespace(getTextContent(n));
            }
        }
        config.addSetConfig(sConfig);
    }

    protected void handleMultiMap(Node node) {
        String name = getTextContent(getNamedItemNode(node, "name"));
        MultiMapConfig multiMapConfig = ConfigUtils.getByNameOrNew(
                config.getMultiMapConfigs(),
                name,
                MultiMapConfig.class);
        handleMultiMapNode(node, multiMapConfig);
    }

    void handleMultiMapNode(Node node, final MultiMapConfig multiMapConfig) {
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if (matches("value-collection-type", nodeName)) {
                multiMapConfig.setValueCollectionType(getTextContent(n));
            } else if (matches("backup-count", nodeName)) {
                multiMapConfig.setBackupCount(getIntegerValue("backup-count"
                        , getTextContent(n)));
            } else if (matches("async-backup-count", nodeName)) {
                multiMapConfig.setAsyncBackupCount(getIntegerValue("async-backup-count"
                        , getTextContent(n)));
            } else if (matches("entry-listeners", nodeName)) {
                handleEntryListeners(n, multiMapConfig::addEntryListenerConfig);
            } else if (matches("statistics-enabled", nodeName)) {
                multiMapConfig.setStatisticsEnabled(getBooleanValue(getTextContent(n)));
            } else if (matches("binary", nodeName)) {
                multiMapConfig.setBinary(getBooleanValue(getTextContent(n)));
            } else if (matches("split-brain-protection-ref", nodeName)) {
                multiMapConfig.setSplitBrainProtectionName(getTextContent(n));
            } else if (matches("merge-policy", nodeName)) {
                MergePolicyConfig mpConfig = createMergePolicyConfig(n, multiMapConfig.getMergePolicyConfig());
                multiMapConfig.setMergePolicyConfig(mpConfig);
            } else if (matches("user-code-namespace", nodeName)) {
                multiMapConfig.setUserCodeNamespace(getTextContent(n));
            }
        }
        config.addMultiMapConfig(multiMapConfig);
    }

    protected void handleEntryListeners(Node n, Consumer<EntryListenerConfig> configAddFunction) {
        for (Node listenerNode : childElements(n)) {
            if (matches("entry-listener", cleanNodeName(listenerNode))) {
                boolean incValue = getBooleanValue(getTextContent(
                        getNamedItemNode(listenerNode, "include-value")));
                boolean local = getBooleanValue(getTextContent(
                        getNamedItemNode(listenerNode, "local")));
                String listenerClass = getTextContent(listenerNode);
                configAddFunction.accept(new EntryListenerConfig(listenerClass, local, incValue));
            }
        }
    }

    protected void handleReplicatedMap(Node node) {
        String name = getTextContent(getNamedItemNode(node, "name"));
        final ReplicatedMapConfig replicatedMapConfig = ConfigUtils.getByNameOrNew(
                config.getReplicatedMapConfigs(),
                name,
                ReplicatedMapConfig.class);
        handleReplicatedMapNode(node, replicatedMapConfig);
    }

    void handleReplicatedMapNode(Node node, final ReplicatedMapConfig replicatedMapConfig) {
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if (matches("in-memory-format", nodeName)) {
                replicatedMapConfig.setInMemoryFormat(InMemoryFormat.valueOf(upperCaseInternal(getTextContent(n))));
            } else if (matches("async-fillup", nodeName)) {
                replicatedMapConfig.setAsyncFillup(getBooleanValue(getTextContent(n)));
            } else if (matches("statistics-enabled", nodeName)) {
                replicatedMapConfig.setStatisticsEnabled(getBooleanValue(getTextContent(n)));
            } else if (matches("entry-listeners", nodeName)) {
                handleEntryListeners(n, replicatedMapConfig::addEntryListenerConfig);
            } else if (matches("merge-policy", nodeName)) {
                MergePolicyConfig mpConfig = createMergePolicyConfig(n, replicatedMapConfig.getMergePolicyConfig());
                replicatedMapConfig.setMergePolicyConfig(mpConfig);
            } else if (matches("split-brain-protection-ref", nodeName)) {
                replicatedMapConfig.setSplitBrainProtectionName(getTextContent(n));
            } else if (matches("user-code-namespace", nodeName)) {
                replicatedMapConfig.setUserCodeNamespace(getTextContent(n));
            }
        }
        config.addReplicatedMapConfig(replicatedMapConfig);
    }

    protected void handleMap(Node parentNode) throws Exception {
        String name = getAttribute(parentNode, "name");
        MapConfig mapConfig = ConfigUtils.getByNameOrNew(config.getMapConfigs(), name, MapConfig.class);
        handleMapNode(parentNode, mapConfig);
    }

    void handleMapNode(Node parentNode, final MapConfig mapConfig) throws Exception {
        for (Node node : childElements(parentNode)) {
            String nodeName = cleanNodeName(node);
            if (matches("backup-count", nodeName)) {
                mapConfig.setBackupCount(getIntegerValue("backup-count", getTextContent(node)));
            } else if (matches("metadata-policy", nodeName)) {
                mapConfig.setMetadataPolicy(MetadataPolicy.valueOf(upperCaseInternal(getTextContent(node))));
            } else if (matches("in-memory-format", nodeName)) {
                mapConfig.setInMemoryFormat(InMemoryFormat.valueOf(upperCaseInternal(getTextContent(node))));
            } else if (matches("async-backup-count", nodeName)) {
                mapConfig.setAsyncBackupCount(getIntegerValue("async-backup-count", getTextContent(node)));
            } else if (matches("eviction", nodeName)) {
                mapConfig.setEvictionConfig(getEvictionConfig(node, false, true));
            } else if (matches("time-to-live-seconds", nodeName)) {
                mapConfig.setTimeToLiveSeconds(getIntegerValue("time-to-live-seconds", getTextContent(node)));
            } else if (matches("max-idle-seconds", nodeName)) {
                mapConfig.setMaxIdleSeconds(getIntegerValue("max-idle-seconds", getTextContent(node)));
            } else if (matches("map-store", nodeName)) {
                handleMapStoreConfig(node, mapConfig.getMapStoreConfig());
            } else if (matches("near-cache", nodeName)) {
                mapConfig.setNearCacheConfig(handleNearCacheConfig(node, mapConfig.getNearCacheConfig()));
            } else if (matches("merge-policy", nodeName)) {
                MergePolicyConfig mpConfig = createMergePolicyConfig(node, mapConfig.getMergePolicyConfig());
                mapConfig.setMergePolicyConfig(mpConfig);
            } else if (matches("merkle-tree", nodeName)) {
                handleViaReflection(node, mapConfig, mapConfig.getMerkleTreeConfig());
            } else if (matches("event-journal", nodeName)) {
                handleViaReflection(node, mapConfig, mapConfig.getEventJournalConfig());
            } else if (matches("hot-restart", nodeName)) {
                mapConfig.setHotRestartConfig(createHotRestartConfig(node));
            } else if (matches("data-persistence", nodeName)) {
                mapConfig.setDataPersistenceConfig(createDataPersistenceConfig(node));
            } else if (matches("read-backup-data", nodeName)) {
                mapConfig.setReadBackupData(getBooleanValue(getTextContent(node)));
            } else if (matches("statistics-enabled", nodeName)) {
                mapConfig.setStatisticsEnabled(getBooleanValue(getTextContent(node)));
            } else if (matches("per-entry-stats-enabled", nodeName)) {
                mapConfig.setPerEntryStatsEnabled(getBooleanValue(getTextContent(node)));
            } else if (matches("cache-deserialized-values", nodeName)) {
                CacheDeserializedValues cacheDeserializedValues = CacheDeserializedValues
                        .parseString(getTextContent(node));
                mapConfig.setCacheDeserializedValues(cacheDeserializedValues);
            } else if (matches("wan-replication-ref", nodeName)) {
                mapWanReplicationRefHandle(node, mapConfig);
            } else if (matches("indexes", nodeName)) {
                mapIndexesHandle(node, mapConfig);
            } else if (matches("attributes", nodeName)) {
                attributesHandle(node, mapConfig);
            } else if (matches("entry-listeners", nodeName)) {
                handleEntryListeners(node, mapConfig::addEntryListenerConfig);
            } else if (matches("partition-lost-listeners", nodeName)) {
                mapPartitionLostListenerHandle(node, mapConfig);
            } else if (matches("partition-strategy", nodeName)) {
                mapConfig.setPartitioningStrategyConfig(new PartitioningStrategyConfig(getTextContent(node)));
            } else if (matches("split-brain-protection-ref", nodeName)) {
                mapConfig.setSplitBrainProtectionName(getTextContent(node));
            } else if (matches("query-caches", nodeName)) {
                mapQueryCacheHandler(node, mapConfig);
            } else if (matches("tiered-store", nodeName)) {
                mapConfig.setTieredStoreConfig(createTieredStoreConfig(node));
            } else if (matches("partition-attributes", nodeName)) {
                handlePartitionAttributes(node, mapConfig);
            } else if (matches("user-code-namespace", nodeName)) {
                mapConfig.setUserCodeNamespace(getTextContent(node));
            }
        }
        config.addMapConfig(mapConfig);
    }

    private NearCacheConfig handleNearCacheConfig(Node node, NearCacheConfig existingNearCacheConfig) {
        String name = getAttribute(node, "name");
        name = name == null ? NearCacheConfig.DEFAULT_NAME : name;
        NearCacheConfig nearCacheConfig = existingNearCacheConfig != null
                ? existingNearCacheConfig
                : new NearCacheConfig(name);

        Boolean serializeKeys = null;
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("time-to-live-seconds", nodeName)) {
                nearCacheConfig.setTimeToLiveSeconds(Integer.parseInt(getTextContent(child)));
            } else if (matches("max-idle-seconds", nodeName)) {
                nearCacheConfig.setMaxIdleSeconds(Integer.parseInt(getTextContent(child)));
            } else if (matches("in-memory-format", nodeName)) {
                nearCacheConfig.setInMemoryFormat(InMemoryFormat.valueOf(upperCaseInternal(getTextContent(child))));
            } else if (matches("serialize-keys", nodeName)) {
                serializeKeys = Boolean.parseBoolean(getTextContent(child));
                nearCacheConfig.setSerializeKeys(serializeKeys);
            } else if (matches("invalidate-on-change", nodeName)) {
                nearCacheConfig.setInvalidateOnChange(Boolean.parseBoolean(getTextContent(child)));
            } else if (matches("cache-local-entries", nodeName)) {
                nearCacheConfig.setCacheLocalEntries(Boolean.parseBoolean(getTextContent(child)));
            } else if (matches("local-update-policy", nodeName)) {
                NearCacheConfig.LocalUpdatePolicy policy = NearCacheConfig.LocalUpdatePolicy.valueOf(getTextContent(child));
                nearCacheConfig.setLocalUpdatePolicy(policy);
            } else if (matches("eviction", nodeName)) {
                nearCacheConfig.setEvictionConfig(getEvictionConfig(child, true, false));
            }
        }
        if (serializeKeys != null && !serializeKeys && nearCacheConfig.getInMemoryFormat() == InMemoryFormat.NATIVE) {
            LOGGER.warning("The Near Cache doesn't support keys by-reference with NATIVE in-memory-format."
                    + " This setting will have no effect!");
        }
        return nearCacheConfig;
    }

    private HotRestartConfig createHotRestartConfig(Node node) {
        HotRestartConfig hotRestartConfig = new HotRestartConfig();

        Node attrEnabled = getNamedItemNode(node, "enabled");
        boolean enabled = getBooleanValue(getTextContent(attrEnabled));
        hotRestartConfig.setEnabled(enabled);

        for (Node n : childElements(node)) {
            String name = cleanNodeName(n);
            if (matches("fsync", name)) {
                hotRestartConfig.setFsync(getBooleanValue(getTextContent(n)));
            }
        }
        return hotRestartConfig;
    }

    private DataPersistenceConfig createDataPersistenceConfig(Node node) {
        DataPersistenceConfig dataPersistenceConfig = new DataPersistenceConfig();

        Node attrEnabled = getNamedItemNode(node, "enabled");
        boolean enabled = getBooleanValue(getTextContent(attrEnabled));
        dataPersistenceConfig.setEnabled(enabled);

        for (Node n : childElements(node)) {
            String name = cleanNodeName(n);
            if (matches("fsync", name)) {
                dataPersistenceConfig.setFsync(getBooleanValue(getTextContent(n)));
            }
        }
        return dataPersistenceConfig;
    }

    protected void handleCache(Node node) throws Exception {
        CacheSimpleConfig cacheConfig =
                ConfigUtils.getByNameOrNew(config.getCacheConfigs(),
                        getAttribute(node, "name"),
                        CacheSimpleConfig.class);
        handleCacheNode(node, cacheConfig);
    }

    void handleCacheNode(Node node, CacheSimpleConfig cacheConfig) throws Exception {
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if (matches("key-type", nodeName)) {
                cacheConfig.setKeyType(getAttribute(n, "class-name"));
            } else if (matches("value-type", nodeName)) {
                cacheConfig.setValueType(getAttribute(n, "class-name"));
            } else if (matches("statistics-enabled", nodeName)) {
                cacheConfig.setStatisticsEnabled(getBooleanValue(getTextContent(n)));
            } else if (matches("management-enabled", nodeName)) {
                cacheConfig.setManagementEnabled(getBooleanValue(getTextContent(n)));
            } else if (matches("read-through", nodeName)) {
                cacheConfig.setReadThrough(getBooleanValue(getTextContent(n)));
            } else if (matches("write-through", nodeName)) {
                cacheConfig.setWriteThrough(getBooleanValue(getTextContent(n)));
            } else if (matches("cache-loader-factory", nodeName)) {
                cacheConfig.setCacheLoaderFactory(getAttribute(n, "class-name"));
            } else if (matches("cache-loader", nodeName)) {
                cacheConfig.setCacheLoader(getAttribute(n, "class-name"));
            } else if (matches("cache-writer-factory", nodeName)) {
                cacheConfig.setCacheWriterFactory(getAttribute(n, "class-name"));
            } else if (matches("cache-writer", nodeName)) {
                cacheConfig.setCacheWriter(getAttribute(n, "class-name"));
            } else if (matches("expiry-policy-factory", nodeName)) {
                cacheConfig.setExpiryPolicyFactoryConfig(getExpiryPolicyFactoryConfig(n));
            } else if (matches("cache-entry-listeners", nodeName)) {
                cacheListenerHandle(n, cacheConfig);
            } else if (matches("in-memory-format", nodeName)) {
                cacheConfig.setInMemoryFormat(InMemoryFormat.valueOf(upperCaseInternal(getTextContent(n))));
            } else if (matches("backup-count", nodeName)) {
                cacheConfig.setBackupCount(getIntegerValue("backup-count", getTextContent(n)));
            } else if (matches("async-backup-count", nodeName)) {
                cacheConfig.setAsyncBackupCount(getIntegerValue("async-backup-count", getTextContent(n)));
            } else if (matches("wan-replication-ref", nodeName)) {
                cacheWanReplicationRefHandle(n, cacheConfig);
            } else if (matches("eviction", nodeName)) {
                cacheConfig.setEvictionConfig(getEvictionConfig(n, false, false));
            } else if (matches("split-brain-protection-ref", nodeName)) {
                cacheConfig.setSplitBrainProtectionName(getTextContent(n));
            } else if (matches("partition-lost-listeners", nodeName)) {
                cachePartitionLostListenerHandle(n, cacheConfig);
            } else if (matches("merge-policy", nodeName)) {
                MergePolicyConfig mpConfig = createMergePolicyConfig(n, cacheConfig.getMergePolicyConfig());
                cacheConfig.setMergePolicyConfig(mpConfig);
            } else if (matches("event-journal", nodeName)) {
                EventJournalConfig eventJournalConfig = new EventJournalConfig();
                handleViaReflection(n, cacheConfig, eventJournalConfig);
            } else if (matches("hot-restart", nodeName)) {
                cacheConfig.setHotRestartConfig(createHotRestartConfig(n));
            } else if (matches("data-persistence", nodeName)) {
                cacheConfig.setDataPersistenceConfig(createDataPersistenceConfig(n));
            } else if (matches("disable-per-entry-invalidation-events", nodeName)) {
                cacheConfig.setDisablePerEntryInvalidationEvents(getBooleanValue(getTextContent(n)));
            } else if (matches("merkle-tree", nodeName)) {
                handleViaReflection(n, cacheConfig, cacheConfig.getMerkleTreeConfig());
            } else if (matches("user-code-namespace", nodeName)) {
                cacheConfig.setUserCodeNamespace(getTextContent(n));
            }
        }
        try {
            checkCacheConfig(cacheConfig, null);
        } catch (IllegalArgumentException e) {
            throw new InvalidConfigurationException(e.getMessage());
        }
        config.addCacheConfig(cacheConfig);
    }

    private CacheSimpleConfig.ExpiryPolicyFactoryConfig getExpiryPolicyFactoryConfig(Node node) {
        String className = getAttribute(node, "class-name");
        if (!isNullOrEmpty(className)) {
            return new CacheSimpleConfig.ExpiryPolicyFactoryConfig(className);
        } else {
            CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig timedExpiryPolicyFactoryConfig = null;
            for (Node n : childElements(node)) {
                String nodeName = cleanNodeName(n);
                if (matches("timed-expiry-policy-factory", nodeName)) {
                    timedExpiryPolicyFactoryConfig = getTimedExpiryPolicyFactoryConfig(n);
                }
            }
            if (timedExpiryPolicyFactoryConfig == null) {
                throw new InvalidConfigurationException(
                        "One of the \"class-name\" or \"timed-expire-policy-factory\" configuration "
                                + "is needed for expiry policy factory configuration");
            } else {
                return new CacheSimpleConfig.ExpiryPolicyFactoryConfig(timedExpiryPolicyFactoryConfig);
            }
        }
    }

    private CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig getTimedExpiryPolicyFactoryConfig(
            Node node) {
        String expiryPolicyTypeStr = getAttribute(node, "expiry-policy-type");
        String durationAmountStr = getAttribute(node, "duration-amount");
        String timeUnitStr = getAttribute(node, "time-unit");
        ExpiryPolicyType expiryPolicyType = ExpiryPolicyType.valueOf(upperCaseInternal(expiryPolicyTypeStr));
        if (expiryPolicyType != ExpiryPolicyType.ETERNAL && (isNullOrEmpty(durationAmountStr) || isNullOrEmpty(timeUnitStr))) {
            throw new InvalidConfigurationException(
                    "Both of the \"duration-amount\" or \"time-unit\" attributes "
                            + "are required for expiry policy factory configuration "
                            + "(except \"ETERNAL\" expiry policy type)");
        }
        CacheSimpleConfig.ExpiryPolicyFactoryConfig.DurationConfig durationConfig = null;
        if (expiryPolicyType
                != ExpiryPolicyType.ETERNAL) {
            long durationAmount;
            try {
                durationAmount = parseLong(durationAmountStr);
            } catch (NumberFormatException e) {
                throw new InvalidConfigurationException(
                        "Invalid value for duration amount: " + durationAmountStr, e);
            }
            if (durationAmount <= 0) {
                throw new InvalidConfigurationException(
                        "Duration amount must be positive: " + durationAmount);
            }
            TimeUnit timeUnit;
            try {
                timeUnit = TimeUnit.valueOf(upperCaseInternal(timeUnitStr));
            } catch (IllegalArgumentException e) {
                throw new InvalidConfigurationException(
                        "Invalid value for time unit: " + timeUnitStr, e);
            }
            durationConfig = new CacheSimpleConfig.ExpiryPolicyFactoryConfig.DurationConfig(durationAmount, timeUnit);
        }
        return new CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig(expiryPolicyType, durationConfig);
    }

    private EvictionConfig getEvictionConfig(Node node, boolean isNearCache, boolean isIMap) {
        EvictionConfig evictionConfig = new EvictionConfig();
        if (isIMap) {
            // Set IMap defaults
            evictionConfig
                    .setEvictionPolicy(MapConfig.DEFAULT_EVICTION_POLICY)
                    .setMaxSizePolicy(MapConfig.DEFAULT_MAX_SIZE_POLICY)
                    .setSize(MapConfig.DEFAULT_MAX_SIZE);
        }

        Node size = getNamedItemNode(node, "size");
        Node maxSizePolicy = getNamedItemNode(node, "max-size-policy");
        Node evictionPolicy = getNamedItemNode(node, "eviction-policy");
        Node comparatorClassName = getNamedItemNode(node, "comparator-class-name");

        if (size != null) {
            evictionConfig.setSize(parseInt(getTextContent(size)));
            if (isIMap && evictionConfig.getSize() == 0) {
                evictionConfig.setSize(MapConfig.DEFAULT_MAX_SIZE);
            }
        }
        if (maxSizePolicy != null) {
            evictionConfig.setMaxSizePolicy(MaxSizePolicy.valueOf(upperCaseInternal(getTextContent(maxSizePolicy))));
        }
        if (evictionPolicy != null) {
            evictionConfig.setEvictionPolicy(EvictionPolicy.valueOf(upperCaseInternal(getTextContent(evictionPolicy))));
        }
        if (comparatorClassName != null) {
            String className = getTextContent(comparatorClassName);
            if (!StringUtil.isNullOrEmptyAfterTrim(className)) {
                evictionConfig.setComparatorClassName(className);
            }
        }

        try {
            doEvictionConfigChecks(evictionConfig, isIMap, isNearCache);
        } catch (IllegalArgumentException e) {
            throw new InvalidConfigurationException(e.getMessage());
        }
        return evictionConfig;
    }

    private static void doEvictionConfigChecks(EvictionConfig evictionConfig,
                                               boolean isIMap,
                                               boolean isNearCache) {
        if (isIMap) {
            checkMapEvictionConfig(evictionConfig);
            return;
        }

        if (isNearCache) {
            checkNearCacheEvictionConfig(evictionConfig.getEvictionPolicy(),
                    evictionConfig.getComparatorClassName(), evictionConfig.getComparator());
            return;
        }

        checkCacheEvictionConfig(evictionConfig);
    }

    private void cacheWanReplicationRefHandle(Node n, CacheSimpleConfig cacheConfig) {
        WanReplicationRef wanReplicationRef = new WanReplicationRef();
        String wanName = getAttribute(n, "name");
        wanReplicationRef.setName(wanName);
        for (Node wanChild : childElements(n)) {
            String wanChildName = cleanNodeName(wanChild);
            String wanChildValue = getTextContent(wanChild);
            if (matches("merge-policy-class-name", wanChildName)) {
                wanReplicationRef.setMergePolicyClassName(wanChildValue);
            } else if (matches("filters", wanChildName)) {
                handleWanFilters(wanChild, wanReplicationRef);
            } else if (matches("republishing-enabled", wanChildName)) {
                wanReplicationRef.setRepublishingEnabled(getBooleanValue(wanChildValue));
            }
        }
        cacheConfig.setWanReplicationRef(wanReplicationRef);
    }

    protected void handleWanFilters(Node wanChild, WanReplicationRef wanReplicationRef) {
        for (Node filter : childElements(wanChild)) {
            if (matches("filter-impl", cleanNodeName(filter))) {
                wanReplicationRef.addFilter(getTextContent(filter));
            }
        }
    }

    protected void cachePartitionLostListenerHandle(Node n, CacheSimpleConfig cacheConfig) {
        for (Node listenerNode : childElements(n)) {
            if (matches("partition-lost-listener", cleanNodeName(listenerNode))) {
                String listenerClass = getTextContent(listenerNode);
                cacheConfig.addCachePartitionLostListenerConfig(
                        new CachePartitionLostListenerConfig(listenerClass));
            }
        }
    }

    protected void cacheListenerHandle(Node n, CacheSimpleConfig cacheSimpleConfig) {
        for (Node listenerNode : childElements(n)) {
            if (matches("cache-entry-listener", cleanNodeName(listenerNode))) {
                handleCacheEntryListenerNode(cacheSimpleConfig, listenerNode);
            }
        }
    }

    protected void handleCacheEntryListenerNode(CacheSimpleConfig cacheSimpleConfig, Node listenerNode) {
        CacheSimpleEntryListenerConfig listenerConfig = new CacheSimpleEntryListenerConfig();
        for (Node listenerChildNode : childElements(listenerNode)) {
            if (matches("cache-entry-listener-factory", cleanNodeName(listenerChildNode))) {
                listenerConfig.setCacheEntryListenerFactory(getAttribute(listenerChildNode, "class-name"));
            }
            if (matches("cache-entry-event-filter-factory", cleanNodeName(listenerChildNode))) {
                listenerConfig.setCacheEntryEventFilterFactory(getAttribute(listenerChildNode, "class-name"));
            }
        }
        listenerConfig.setOldValueRequired(getBooleanValue(getTextContent(
                getNamedItemNode(listenerNode, "old-value-required"))));
        listenerConfig.setSynchronous(getBooleanValue(getTextContent(
                getNamedItemNode(listenerNode, "synchronous"))));
        cacheSimpleConfig.addEntryListenerConfig(listenerConfig);
    }

    protected void mapWanReplicationRefHandle(Node n, MapConfig mapConfig) {
        WanReplicationRef wanReplicationRef = new WanReplicationRef();
        String wanName = getAttribute(n, "name");
        wanReplicationRef.setName(wanName);
        handleMapWanReplicationRefNode(n, mapConfig, wanReplicationRef);
    }

    void handleMapWanReplicationRefNode(Node n, MapConfig mapConfig, WanReplicationRef wanReplicationRef) {
        for (Node wanChild : childElements(n)) {
            String wanChildName = cleanNodeName(wanChild);
            String wanChildValue = getTextContent(wanChild);
            if (matches("merge-policy-class-name", wanChildName)) {
                wanReplicationRef.setMergePolicyClassName(wanChildValue);
            } else if (matches("republishing-enabled", wanChildName)) {
                wanReplicationRef.setRepublishingEnabled(getBooleanValue(wanChildValue));
            } else if (matches("filters", wanChildName)) {
                handleWanFilters(wanChild, wanReplicationRef);
            }
        }
        mapConfig.setWanReplicationRef(wanReplicationRef);
    }

    protected void mapIndexesHandle(Node n, MapConfig mapConfig) {
        for (Node indexNode : childElements(n)) {
            if (matches("index", cleanNodeName(indexNode))) {
                IndexConfig indexConfig = IndexUtils.getIndexConfigFromXml(indexNode, domLevel3, strict);

                mapConfig.addIndexConfig(indexConfig);
            }
        }
    }

    protected void queryCacheIndexesHandle(Node n, QueryCacheConfig queryCacheConfig) {
        for (Node indexNode : childElements(n)) {
            if (matches("index", cleanNodeName(indexNode))) {
                IndexConfig indexConfig = IndexUtils.getIndexConfigFromXml(indexNode, domLevel3, strict);

                queryCacheConfig.addIndexConfig(indexConfig);
            }
        }
    }

    protected void attributesHandle(Node n, MapConfig mapConfig) {
        for (Node extractorNode : childElements(n)) {
            if (matches("attribute", cleanNodeName(extractorNode))) {
                String extractor = getTextContent(
                        getNamedItemNode(extractorNode, "extractor-class-name"));
                String name = getTextContent(extractorNode);
                mapConfig.addAttributeConfig(new AttributeConfig(name, extractor));
            }
        }
    }

    protected void mapPartitionLostListenerHandle(Node n, MapConfig mapConfig) {
        for (Node listenerNode : childElements(n)) {
            if (matches("partition-lost-listener", cleanNodeName(listenerNode))) {
                String listenerClass = getTextContent(listenerNode);
                mapConfig.addMapPartitionLostListenerConfig(new MapPartitionLostListenerConfig(listenerClass));
            }
        }
    }

    protected void mapQueryCacheHandler(Node n, MapConfig mapConfig) {
        for (Node queryCacheNode : childElements(n)) {
            if (matches("query-cache", cleanNodeName(queryCacheNode))) {
                String cacheName = getTextContent(
                        getNamedItemNode(queryCacheNode, "name"));
                QueryCacheConfig queryCacheConfig = new QueryCacheConfig(cacheName);
                handleMapQueryCacheNode(mapConfig, queryCacheNode, queryCacheConfig);
            }
        }
    }

    void handleMapQueryCacheNode(MapConfig mapConfig, Node queryCacheNode, final QueryCacheConfig queryCacheConfig) {
        for (Node childNode : childElements(queryCacheNode)) {
            String nodeName = cleanNodeName(childNode);
            if (matches("entry-listeners", nodeName)) {
                handleEntryListeners(childNode, queryCacheConfig::addEntryListenerConfig);
            } else {
                if (matches("include-value", nodeName)) {
                    boolean includeValue = getBooleanValue(getTextContent(childNode));
                    queryCacheConfig.setIncludeValue(includeValue);
                } else if (matches("batch-size", nodeName)) {
                    int batchSize = getIntegerValue("batch-size", getTextContent(childNode));
                    queryCacheConfig.setBatchSize(batchSize);
                } else if (matches("buffer-size", nodeName)) {
                    int bufferSize = getIntegerValue("buffer-size", getTextContent(childNode));
                    queryCacheConfig.setBufferSize(bufferSize);
                } else if (matches("delay-seconds", nodeName)) {
                    int delaySeconds = getIntegerValue("delay-seconds", getTextContent(childNode));
                    queryCacheConfig.setDelaySeconds(delaySeconds);
                } else if (matches("in-memory-format", nodeName)) {
                    queryCacheConfig.setInMemoryFormat(InMemoryFormat.valueOf(upperCaseInternal(getTextContent(childNode))));
                } else if (matches("coalesce", nodeName)) {
                    boolean coalesce = getBooleanValue(getTextContent(childNode));
                    queryCacheConfig.setCoalesce(coalesce);
                } else if (matches("populate", nodeName)) {
                    boolean populate = getBooleanValue(getTextContent(childNode));
                    queryCacheConfig.setPopulate(populate);
                } else if (matches("serialize-keys", nodeName)) {
                    boolean serializeKeys = getBooleanValue(getTextContent(childNode));
                    queryCacheConfig.setSerializeKeys(serializeKeys);
                } else if (matches("indexes", nodeName)) {
                    queryCacheIndexesHandle(childNode, queryCacheConfig);
                } else if (matches("predicate", nodeName)) {
                    queryCachePredicateHandler(childNode, queryCacheConfig);
                } else if (matches("eviction", nodeName)) {
                    queryCacheConfig.setEvictionConfig(getEvictionConfig(childNode, false, false));
                }
            }
        }
        mapConfig.addQueryCacheConfig(queryCacheConfig);
    }

    protected void queryCachePredicateHandler(Node childNode, QueryCacheConfig queryCacheConfig) {
        String predicateType = getTextContent(getNamedItemNode(childNode, "type"));
        String textContent = getTextContent(childNode);
        PredicateConfig predicateConfig = new PredicateConfig();
        if (matches("class-name", predicateType)) {
            predicateConfig.setClassName(textContent);
        } else if (matches("sql", predicateType)) {
            predicateConfig.setSql(textContent);
        }
        queryCacheConfig.setPredicateConfig(predicateConfig);
    }

    private MapStoreConfig handleMapStoreConfig(Node node, MapStoreConfig mapStoreConfig) {
        NamedNodeMap attributes = node.getAttributes();
        boolean enabled = true;
        for (int a = 0; a < attributes.getLength(); a++) {
            Node att = attributes.item(a);
            if (matches("enabled", att.getNodeName())) {
                enabled = getBooleanValue(getTextContent(att));
            } else if (matches("initial-mode", att.getNodeName())) {
                MapStoreConfig.InitialLoadMode mode = MapStoreConfig.InitialLoadMode
                        .valueOf(upperCaseInternal(getTextContent(att)));
                mapStoreConfig.setInitialLoadMode(mode);
            }
        }
        mapStoreConfig.setEnabled(enabled);

        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if (matches("class-name", nodeName)) {
                mapStoreConfig.setClassName(getTextContent(n));
            } else if (matches("factory-class-name", nodeName)) {
                mapStoreConfig.setFactoryClassName(getTextContent(n));
            } else if (matches("write-delay-seconds", nodeName)) {
                mapStoreConfig.setWriteDelaySeconds(getIntegerValue("write-delay-seconds", getTextContent(n)
                ));
            } else if (matches("write-batch-size", nodeName)) {
                mapStoreConfig.setWriteBatchSize(getIntegerValue("write-batch-size", getTextContent(n)
                ));
            } else if (matches("write-coalescing", nodeName)) {
                String writeCoalescing = getTextContent(n);
                if (isNullOrEmpty(writeCoalescing)) {
                    mapStoreConfig.setWriteCoalescing(MapStoreConfig.DEFAULT_WRITE_COALESCING);
                } else {
                    mapStoreConfig.setWriteCoalescing(getBooleanValue(writeCoalescing));
                }
            } else if (matches("offload", nodeName)) {
                String offload = getTextContent(n);
                if (isNullOrEmpty(offload)) {
                    mapStoreConfig.setOffload(MapStoreConfig.DEFAULT_OFFLOAD);
                } else {
                    mapStoreConfig.setOffload(getBooleanValue(offload));
                }
            } else if (matches("properties", nodeName)) {
                fillProperties(n, mapStoreConfig.getProperties());
            }
        }
        return mapStoreConfig;
    }

    private RingbufferStoreConfig createRingbufferStoreConfig(Node node) {
        RingbufferStoreConfig config = new RingbufferStoreConfig();
        NamedNodeMap attributes = node.getAttributes();
        for (int a = 0; a < attributes.getLength(); a++) {
            Node att = attributes.item(a);
            if (matches(att.getNodeName(), "enabled")) {
                config.setEnabled(getBooleanValue(getTextContent(att)));
            }
        }
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if (matches("class-name", nodeName)) {
                config.setClassName(getTextContent(n));
            } else if (matches("factory-class-name", nodeName)) {
                config.setFactoryClassName(getTextContent(n));
            } else if (matches("properties", nodeName)) {
                fillProperties(n, config.getProperties());
            }

        }
        return config;
    }

    protected MergePolicyConfig createMergePolicyConfig(Node node, MergePolicyConfig baseMergePolicyConfig) {
        String policyString = getTextContent(node);
        baseMergePolicyConfig.setPolicy(policyString);
        final String att = getAttribute(node, "batch-size");
        if (att != null) {
            baseMergePolicyConfig.setBatchSize(getIntegerValue("batch-size", att));
        }
        return baseMergePolicyConfig;
    }

    private QueueStoreConfig createQueueStoreConfig(Node node) {
        QueueStoreConfig queueStoreConfig = new QueueStoreConfig();
        NamedNodeMap attributes = node.getAttributes();
        for (int a = 0; a < attributes.getLength(); a++) {
            Node att = attributes.item(a);
            if (matches(att.getNodeName(), "enabled")) {
                queueStoreConfig.setEnabled(getBooleanValue(getTextContent(att)));
            }
        }
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if (matches("class-name", nodeName)) {
                queueStoreConfig.setClassName(getTextContent(n));
            } else if (matches("factory-class-name", nodeName)) {
                queueStoreConfig.setFactoryClassName(getTextContent(n));
            } else if (matches("properties", nodeName)) {
                fillProperties(n, queueStoreConfig.getProperties());
            }
        }
        return queueStoreConfig;
    }

    private void handleSSLConfig(Node node) {
        SSLConfig sslConfig = parseSslConfig(node);
        config.getNetworkConfig().setSSLConfig(sslConfig);
    }

    private void handleSSLConfig(Node node, EndpointConfig endpointConfig) {
        SSLConfig sslConfig = parseSslConfig(node);
        endpointConfig.setSSLConfig(sslConfig);
    }

    private void handleMemberAddressProvider(Node node, boolean advancedNetworkConfig) {
        MemberAddressProviderConfig memberAddressProviderConfig = memberAddressProviderConfig(advancedNetworkConfig);

        Node enabledNode = getNamedItemNode(node, "enabled");
        boolean enabled = enabledNode != null && getBooleanValue(getTextContent(enabledNode));
        memberAddressProviderConfig.setEnabled(enabled);

        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if (matches(nodeName, "class-name")) {
                String className = getTextContent(n);
                memberAddressProviderConfig.setClassName(className);
            } else if (matches(nodeName, "properties")) {
                fillProperties(n, memberAddressProviderConfig.getProperties());
            }
        }
    }

    private MemberAddressProviderConfig memberAddressProviderConfig(boolean advancedNetworkConfig) {
        return advancedNetworkConfig
                ? config.getAdvancedNetworkConfig().getMemberAddressProviderConfig()
                : config.getNetworkConfig().getMemberAddressProviderConfig();
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    private void handleFailureDetector(Node node, boolean advancedNetworkConfig) {
        if (!node.hasChildNodes()) {
            return;
        }

        for (Node child : childElements(node)) {
            // icmp only
            if (!matches(cleanNodeName(child), "icmp")) {
                throw new IllegalStateException("Unsupported child under failure-detector");
            }

            Node enabledNode = getNamedItemNode(child, "enabled");
            boolean enabled = enabledNode != null && getBooleanValue(getTextContent(enabledNode));
            IcmpFailureDetectorConfig icmpFailureDetectorConfig = new IcmpFailureDetectorConfig();

            icmpFailureDetectorConfig.setEnabled(enabled);
            for (Node n : childElements(child)) {
                String nodeName = cleanNodeName(n);

                if (matches(nodeName, "ttl")) {
                    int ttl = parseInt(getTextContent(n));
                    icmpFailureDetectorConfig.setTtl(ttl);
                } else if (matches(nodeName, "timeout-milliseconds")) {
                    int timeout = parseInt(getTextContent(n));
                    icmpFailureDetectorConfig.setTimeoutMilliseconds(timeout);
                } else if (matches(nodeName, "parallel-mode")) {
                    boolean mode = parseBoolean(getTextContent(n));
                    icmpFailureDetectorConfig.setParallelMode(mode);
                } else if (matches(nodeName, "fail-fast-on-startup")) {
                    boolean failOnStartup = parseBoolean(getTextContent(n));
                    icmpFailureDetectorConfig.setFailFastOnStartup(failOnStartup);
                } else if (matches(nodeName, "max-attempts")) {
                    int attempts = parseInt(getTextContent(n));
                    icmpFailureDetectorConfig.setMaxAttempts(attempts);
                } else if (matches(nodeName, "interval-milliseconds")) {
                    int interval = parseInt(getTextContent(n));
                    icmpFailureDetectorConfig.setIntervalMilliseconds(interval);
                }
            }
            if (advancedNetworkConfig) {
                config.getAdvancedNetworkConfig().setIcmpFailureDetectorConfig(icmpFailureDetectorConfig);
            } else {
                config.getNetworkConfig().setIcmpFailureDetectorConfig(icmpFailureDetectorConfig);
            }
        }
    }

    private void handleSocketInterceptorConfig(Node node) {
        SocketInterceptorConfig socketInterceptorConfig = parseSocketInterceptorConfig(node);
        config.getNetworkConfig().setSocketInterceptorConfig(socketInterceptorConfig);
    }

    private void handleSocketInterceptorConfig(Node node, EndpointConfig endpointConfig) {
        SocketInterceptorConfig socketInterceptorConfig = parseSocketInterceptorConfig(node);
        endpointConfig.setSocketInterceptorConfig(socketInterceptorConfig);
    }

    protected void handleTopic(Node node) {
        Node attName = getNamedItemNode(node, "name");
        String name = getTextContent(attName);
        TopicConfig tConfig = new TopicConfig();
        tConfig.setName(name);
        handleTopicNode(node, tConfig);
    }

    void handleTopicNode(Node node, final TopicConfig tConfig) {
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if (matches(nodeName, "global-ordering-enabled")) {
                tConfig.setGlobalOrderingEnabled(getBooleanValue(getTextContent(n)));
            } else if (matches("message-listeners", nodeName)) {
                handleMessageListeners(n, listenerConfig -> {
                    tConfig.addMessageListenerConfig(listenerConfig);
                    return null;
                });
            } else if (matches("statistics-enabled", nodeName)) {
                tConfig.setStatisticsEnabled(getBooleanValue(getTextContent(n)));
            } else if (matches("multi-threading-enabled", nodeName)) {
                tConfig.setMultiThreadingEnabled(getBooleanValue(getTextContent(n)));
            } else if (matches("user-code-namespace", nodeName)) {
                tConfig.setUserCodeNamespace(getTextContent(n));
            }
        }
        config.addTopicConfig(tConfig);
    }

    protected void handleNamespaces(Node node) {
        Node enabledNode = getNamedItemNode(node, "enabled");
        boolean enabled = enabledNode != null && getBooleanValue(getTextContent(enabledNode));
        config.getNamespacesConfig().setEnabled(enabled);

        if (enabled) {
            handleNamespacesNode(node);
        }
    }

    void handleNamespacesNode(Node node) {
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if (matches(nodeName, "namespace")) {
                Node attName = getNamedItemNode(n, "name");
                String name = getTextContent(attName);
                UserCodeNamespaceConfig nsConfig = new UserCodeNamespaceConfig(name);
                handleResources(n, nsConfig);
                config.getNamespacesConfig().addNamespaceConfig(nsConfig);
            } else if (matches(nodeName, "class-filter")) {
                fillJavaSerializationFilter(n, config.getNamespacesConfig());
            }
        }
    }

    void handleResources(Node node, final UserCodeNamespaceConfig nsConfig) {
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if (matches(nodeName, "jar")) {
                handleJarNode(n, nsConfig);
            }
            if (matches(nodeName, "jars-in-zip")) {
                handleJarsInZipNode(n, nsConfig);
            }
            if (matches(nodeName, "class")) {
                handleClassNode(n, nsConfig);
            }
        }
    }

    void handleClassNode(Node node, final UserCodeNamespaceConfig nsConfig) {
        URL url = getNamespaceResourceUrl(node);
        String id = getAttribute(node, "id");
        if (url != null) {
            nsConfig.addClass(url, id);
        }
    }

    void handleJarNode(Node node, final UserCodeNamespaceConfig nsConfig) {
        URL url = getNamespaceResourceUrl(node);
        String id = getAttribute(node, "id");
        if (url != null) {
            nsConfig.addJar(url, id);
        }
    }

    void handleJarsInZipNode(Node node, final UserCodeNamespaceConfig nsConfig) {
        URL url = getNamespaceResourceUrl(node);
        String id = getAttribute(node, "id");
        if (url != null) {
            nsConfig.addJarsInZip(url, id);
        }
    }

    private URL getNamespaceResourceUrl(Node node) {
        URL url = null;
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if (matches(nodeName, "url")) {
                try {
                    url = new URI(getTextContent(n)).toURL();
                    break;
                } catch (MalformedURLException | URISyntaxException e) {
                    throw new InvalidConfigurationException("Malformed resource URL", e);
                }
            }
        }
        return url;
    }

    protected void handleReliableTopic(Node node) {
        Node attName = getNamedItemNode(node, "name");
        String name = getTextContent(attName);
        ReliableTopicConfig topicConfig = new ReliableTopicConfig(name);
        handleReliableTopicNode(node, topicConfig);
    }

    void handleReliableTopicNode(Node node, final ReliableTopicConfig topicConfig) {
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if (matches("read-batch-size", nodeName)) {
                String batchSize = getTextContent(n);
                topicConfig.setReadBatchSize(getIntegerValue("read-batch-size", batchSize));
            } else if (matches("statistics-enabled", nodeName)) {
                topicConfig.setStatisticsEnabled(getBooleanValue(getTextContent(n)));
            } else if (matches("topic-overload-policy", nodeName)) {
                TopicOverloadPolicy topicOverloadPolicy = TopicOverloadPolicy.valueOf(upperCaseInternal(getTextContent(n)));
                topicConfig.setTopicOverloadPolicy(topicOverloadPolicy);
            } else if (matches("message-listeners", nodeName)) {
                handleMessageListeners(n, listenerConfig -> {
                    topicConfig.addMessageListenerConfig(listenerConfig);
                    return null;
                });
            } else if (matches("user-code-namespace", nodeName)) {
                topicConfig.setUserCodeNamespace(getTextContent(n));
            }
        }
        config.addReliableTopicConfig(topicConfig);
    }

    void handleMessageListeners(Node n, Function<ListenerConfig, Void> configAddFunction) {
        for (Node listenerNode : childElements(n)) {
            if (matches("message-listener", cleanNodeName(listenerNode))) {
                configAddFunction.apply(new ListenerConfig(getTextContent(listenerNode)));
            }
        }
    }

    protected void handleRingbuffer(Node node) {
        String name = getTextContent(getNamedItemNode(node, "name"));
        handleRingBufferNode(node, ConfigUtils.getByNameOrNew(
                config.getRingbufferConfigs(),
                name,
                RingbufferConfig.class));
    }

    void handleRingBufferNode(Node node, RingbufferConfig rbConfig) {
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if (matches("capacity", nodeName)) {
                int capacity = getIntegerValue("capacity", getTextContent(n));
                rbConfig.setCapacity(capacity);
            } else if (matches("backup-count", nodeName)) {
                int backupCount = getIntegerValue("backup-count", getTextContent(n));
                rbConfig.setBackupCount(backupCount);
            } else if (matches("async-backup-count", nodeName)) {
                int asyncBackupCount = getIntegerValue("async-backup-count", getTextContent(n));
                rbConfig.setAsyncBackupCount(asyncBackupCount);
            } else if (matches("time-to-live-seconds", nodeName)) {
                int timeToLiveSeconds = getIntegerValue("time-to-live-seconds", getTextContent(n));
                rbConfig.setTimeToLiveSeconds(timeToLiveSeconds);
            } else if (matches("in-memory-format", nodeName)) {
                InMemoryFormat inMemoryFormat = InMemoryFormat.valueOf(upperCaseInternal(getTextContent(n)));
                rbConfig.setInMemoryFormat(inMemoryFormat);
            } else if (matches("ringbuffer-store", nodeName)) {
                RingbufferStoreConfig ringbufferStoreConfig = createRingbufferStoreConfig(n);
                rbConfig.setRingbufferStoreConfig(ringbufferStoreConfig);
            } else if (matches("split-brain-protection-ref", nodeName)) {
                rbConfig.setSplitBrainProtectionName(getTextContent(n));
            } else if (matches("merge-policy", nodeName)) {
                MergePolicyConfig mpConfig = createMergePolicyConfig(n, rbConfig.getMergePolicyConfig());
                rbConfig.setMergePolicyConfig(mpConfig);
            } else if (matches("user-code-namespace", nodeName)) {
                rbConfig.setUserCodeNamespace(getTextContent(n));
            }
        }
        config.addRingBufferConfig(rbConfig);
    }

    protected void handleListeners(Node node) {
        for (Node child : childElements(node)) {
            if (matches("listener", cleanNodeName(child))) {
                String listenerClass = getTextContent(child);
                config.addListenerConfig(new ListenerConfig(listenerClass));
            }
        }
    }

    private void handlePartitionGroup(Node node) {
        Node enabledNode = getNamedItemNode(node, "enabled");
        boolean enabled = enabledNode != null && getBooleanValue(getTextContent(enabledNode));
        config.getPartitionGroupConfig().setEnabled(enabled);
        Node groupTypeNode = getNamedItemNode(node, "group-type");
        MemberGroupType groupType = groupTypeNode != null
                ? PartitionGroupConfig.MemberGroupType.valueOf(upperCaseInternal(getTextContent(groupTypeNode)))
                : PartitionGroupConfig.MemberGroupType.PER_MEMBER;
        config.getPartitionGroupConfig().setGroupType(groupType);
        for (Node child : childElements(node)) {
            if (matches("member-group", cleanNodeName(child))) {
                handleMemberGroup(child, config);
            }
        }
    }

    protected void handleMemberGroup(Node node, Config config) {
        MemberGroupConfig memberGroupConfig = new MemberGroupConfig();
        for (Node child : childElements(node)) {
            if (matches("interface", cleanNodeName(child))) {
                memberGroupConfig.addInterface(getTextContent(child));
            }
        }
        config.getPartitionGroupConfig().addMemberGroupConfig(memberGroupConfig);
    }

    private void handleSerialization(Node node) {
        SerializationConfig serializationConfig = parseSerialization(node);
        config.setSerializationConfig(serializationConfig);
    }

    private void handleManagementCenterConfig(Node node) {
        ManagementCenterConfig managementCenterConfig = config.getManagementCenterConfig();

        Node scriptingEnabledNode = getNamedItemNode(node, "scripting-enabled");
        if (scriptingEnabledNode != null) {
            managementCenterConfig.setScriptingEnabled(getBooleanValue(getTextContent(scriptingEnabledNode)));
        }

        Node consoleEnabledNode = getNamedItemNode(node, "console-enabled");
        if (consoleEnabledNode != null) {
            managementCenterConfig.setConsoleEnabled(getBooleanValue(getTextContent(consoleEnabledNode)));
        }

        Node dataAccessEnabledNode = getNamedItemNode(node, "data-access-enabled");
        if (dataAccessEnabledNode != null) {
            managementCenterConfig.setDataAccessEnabled(getBooleanValue(getTextContent(dataAccessEnabledNode)));
        }

        for (Node n : childElements(node)) {
            if (matches("trusted-interfaces", cleanNodeName(n))) {
                handleTrustedInterfaces(managementCenterConfig, n);
            }
        }
    }

    private void handleSecurity(Node node) {
        Node enabledNode = getNamedItemNode(node, "enabled");
        boolean enabled = enabledNode != null && getBooleanValue(getTextContent(enabledNode));
        config.getSecurityConfig().setEnabled(enabled);
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("realms", nodeName)) {
                handleRealms(child);
            } else if (matches("member-authentication", nodeName)) {
                config.getSecurityConfig().setMemberRealm(getAttribute(child, "realm"));
            } else if (matches("client-authentication", nodeName)) {
                config.getSecurityConfig().setClientRealm(getAttribute(child, "realm"));
            } else if (matches("client-permission-policy", nodeName)) {
                handlePermissionPolicy(child);
            } else if (matches("client-permissions", nodeName)) {
                handleSecurityPermissions(child);
            } else if (matches("security-interceptors", nodeName)) {
                handleSecurityInterceptors(child);
            } else if (matches("client-block-unmapped-actions", nodeName)) {
                config.getSecurityConfig().setClientBlockUnmappedActions(getBooleanValue(getTextContent(child)));
            }
        }
    }

    protected void handleRealms(Node node) {
        for (Node child : childElements(node)) {
            if (matches("realm", cleanNodeName(child))) {
                handleRealm(child);
            }
        }
    }

    private void handleSecurityInterceptors(Node node) {
        SecurityConfig cfg = config.getSecurityConfig();
        for (Node child : childElements(node)) {
            handleSecurityInterceptorsChild(cfg, child);
        }
    }

    protected void handleSecurityInterceptorsChild(SecurityConfig cfg, Node child) {
        String nodeName = cleanNodeName(child);
        if (matches("interceptor", nodeName)) {
            Node classNameNode = getNamedItemNode(child, "class-name");
            String className = getTextContent(classNameNode);
            cfg.addSecurityInterceptorConfig(new SecurityInterceptorConfig(className));
        }
    }

    protected void handleMemberAttributes(Node node) {
        for (Node n : childElements(node)) {
            String name = cleanNodeName(n);
            if (!matches("attribute", name)) {
                continue;
            }
            String value = getTextContent(n);
            String attributeName = getTextContent(getNamedItemNode(n, "name"));
            handleMemberAttributesNode(attributeName, value);
        }
    }

    void handleMemberAttributesNode(String attributeName, String value) {
        config.getMemberAttributeConfig().setAttribute(attributeName, value);
    }

    private void handlePermissionPolicy(Node node) {
        Node classNameNode = getNamedItemNode(node, "class-name");
        String className = getTextContent(classNameNode);
        SecurityConfig cfg = config.getSecurityConfig();
        PermissionPolicyConfig policyConfig = new PermissionPolicyConfig(className);
        cfg.setClientPolicyConfig(policyConfig);
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("properties", nodeName)) {
                fillProperties(child, policyConfig.getProperties());
                break;
            }
        }
    }

    protected void handleSecurityPermissions(Node node) {
        String onJoinOp = getAttribute(node, "on-join-operation");
        if (onJoinOp != null) {
            OnJoinPermissionOperationName onJoinPermissionOperation = OnJoinPermissionOperationName
                    .valueOf(upperCaseInternal(onJoinOp));
            config.getSecurityConfig().setOnJoinPermissionOperation(onJoinPermissionOperation);
        }
        config.getSecurityConfig().setPermissionPriorityGrant(getBooleanValue(getAttribute(node, "priority-grant")));
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            PermissionType type = PermissionConfig.PermissionType.getType(nodeName);
            if (type == null) {
                throw new InvalidConfigurationException("Security permission type is not valid " + nodeName);
            }
            handleSecurityPermission(child, type);
        }
    }

    void handleSecurityPermission(Node node, PermissionConfig.PermissionType type) {
        SecurityConfig cfg = config.getSecurityConfig();
        Node nameNode = getNamedItemNode(node, "name");
        String name = nameNode != null ? getTextContent(nameNode) : null;
        Node principalNode = getNamedItemNode(node, "principal");
        String principal = principalNode != null ? getTextContent(principalNode) : null;
        PermissionConfig permConfig = new PermissionConfig(type, name, principal);
        permConfig.setDeny(getBooleanValue(getAttribute(node, "deny")));
        cfg.addClientPermissionConfig(permConfig);
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("endpoints", nodeName)) {
                handleSecurityPermissionEndpoints(child, permConfig);
            } else if (matches("actions", nodeName)) {
                handleSecurityPermissionActions(child, permConfig);
            }
        }
    }

    void handleSecurityPermissionEndpoints(Node node, PermissionConfig permConfig) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("endpoint", nodeName)) {
                permConfig.addEndpoint(getTextContent(child));
            }
        }
    }

    void handleSecurityPermissionActions(Node node, PermissionConfig permConfig) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("action", nodeName)) {
                permConfig.addAction(getTextContent(child));
            }
        }
    }

    private void handleMemcacheProtocol(Node node) {
        config.getNetworkConfig().getMemcacheProtocolConfig()
                .setEnabled(getBooleanValue(getAttribute(node, "enabled")));
    }

    private void handleRestApi(Node node) {
        RestApiConfig restApiConfig = config.getNetworkConfig().getRestApiConfig();
        boolean enabled = getBooleanValue(getAttribute(node, "enabled"));
        restApiConfig.setEnabled(enabled);
        handleRestApiEndpointGroups(node);
    }

    protected void handleRestApiEndpointGroups(Node node) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("endpoint-group", nodeName)) {
                String name = getAttribute(child, "name");
                handleEndpointGroup(child, name);
            }
        }
    }

    private void handleRestEndpointGroup(RestServerEndpointConfig config, Node node) {
        boolean enabled = getBooleanValue(getAttribute(node, "enabled"));
        String name = extractName(node);
        RestEndpointGroup endpointGroup;
        try {
            endpointGroup = RestEndpointGroup.valueOf(name);
        } catch (IllegalArgumentException e) {
            throw new InvalidConfigurationException("Wrong name attribute value was provided in endpoint-group element: " + name
                    + "\nAllowed values: " + Arrays.toString(RestEndpointGroup.values()));
        }

        if (enabled) {
            config.enableGroups(endpointGroup);
        } else {
            config.disableGroups(endpointGroup);
        }
    }

    protected String extractName(Node node) {
        return getAttribute(node, "name");
    }

    void handleEndpointGroup(Node node, String name) {
        boolean enabled = getBooleanValue(getAttribute(node, "enabled"));
        RestEndpointGroup endpointGroup = lookupEndpointGroup(name);

        RestApiConfig restApiConfig = config.getNetworkConfig().getRestApiConfig();
        if (enabled) {
            restApiConfig.enableGroups(endpointGroup);
        } else {
            restApiConfig.disableGroups(endpointGroup);
        }
    }

    private RestEndpointGroup lookupEndpointGroup(String name) {
        return Arrays.stream(RestEndpointGroup.values())
                .filter(value -> value.toString().replace("_", "")
                        .equals(name.toUpperCase().replace("_", "")))
                .findAny()
                .orElseThrow(() -> new InvalidConfigurationException(
                        "Wrong name attribute value was provided in endpoint-group element: " + name
                                + "\nAllowed values: " + Arrays.toString(RestEndpointGroup.values())));
    }

    private void handleCPSubsystem(Node node) {
        CPSubsystemConfig cpSubsystemConfig = config.getCPSubsystemConfig();
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("raft-algorithm", nodeName)) {
                handleRaftAlgorithm(cpSubsystemConfig.getRaftAlgorithmConfig(), child);
            } else if (matches("semaphores", nodeName)) {
                handleSemaphores(cpSubsystemConfig, child);
            } else if (matches("locks", nodeName)) {
                handleFencedLocks(cpSubsystemConfig, child);
            } else if (matches("maps", nodeName)) {
                handleCPMaps(cpSubsystemConfig, child);
            } else {
                if (matches("cp-member-count", nodeName)) {
                    cpSubsystemConfig.setCPMemberCount(Integer.parseInt(getTextContent(child)));
                } else if (matches("group-size", nodeName)) {
                    cpSubsystemConfig.setGroupSize(Integer.parseInt(getTextContent(child)));
                } else if (matches("session-time-to-live-seconds", nodeName)) {
                    cpSubsystemConfig.setSessionTimeToLiveSeconds(Integer.parseInt(getTextContent(child)));
                } else if (matches("session-heartbeat-interval-seconds", nodeName)) {
                    cpSubsystemConfig.setSessionHeartbeatIntervalSeconds(Integer.parseInt(getTextContent(child)));
                } else if (matches("missing-cp-member-auto-removal-seconds", nodeName)) {
                    cpSubsystemConfig.setMissingCPMemberAutoRemovalSeconds(Integer.parseInt(getTextContent(child)));
                } else if (matches("fail-on-indeterminate-operation-state", nodeName)) {
                    cpSubsystemConfig.setFailOnIndeterminateOperationState(Boolean.parseBoolean(getTextContent(child)));
                } else if (matches("persistence-enabled", nodeName)) {
                    cpSubsystemConfig.setPersistenceEnabled(Boolean.parseBoolean(getTextContent(child)));
                } else if (matches("base-dir", nodeName)) {
                    cpSubsystemConfig.setBaseDir(new File(getTextContent(child)).getAbsoluteFile());
                } else if (matches("data-load-timeout-seconds", nodeName)) {
                    cpSubsystemConfig.setDataLoadTimeoutSeconds(Integer.parseInt(getTextContent(child)));
                } else if (matches("cp-member-priority", nodeName)) {
                    cpSubsystemConfig.setCPMemberPriority(Integer.parseInt(getTextContent(child)));
                } else if (matches("map-limit", nodeName)) {
                    cpSubsystemConfig.setCPMapLimit(Integer.parseInt(getTextContent(child)));
                }
            }
        }
    }

    void handleCPMaps(CPSubsystemConfig cpSubsystemConfig, Node node) {
        for (Node child : childElements(node)) {
            CPMapConfig cpMapConfig = new CPMapConfig();
            for (Node subChild : childElements(child)) {
                String nodeName = cleanNodeName(subChild);
                if (matches("name", nodeName)) {
                    cpMapConfig.setName(getTextContent(subChild));
                } else if (matches("max-size-mb", nodeName)) {
                    cpMapConfig.setMaxSizeMb(Integer.parseInt(getTextContent(subChild)));
                }
            }
            cpSubsystemConfig.addCPMapConfig(cpMapConfig);
        }
    }

    private void handleRaftAlgorithm(RaftAlgorithmConfig raftAlgorithmConfig, Node node) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("leader-election-timeout-in-millis", nodeName)) {
                raftAlgorithmConfig.setLeaderElectionTimeoutInMillis(Long.parseLong(getTextContent(child)));
            } else if (matches("leader-heartbeat-period-in-millis", nodeName)) {
                raftAlgorithmConfig.setLeaderHeartbeatPeriodInMillis(Long.parseLong(getTextContent(child)));
            } else if (matches("max-missed-leader-heartbeat-count", nodeName)) {
                raftAlgorithmConfig.setMaxMissedLeaderHeartbeatCount(Integer.parseInt(getTextContent(child)));
            } else if (matches("append-request-max-entry-count", nodeName)) {
                raftAlgorithmConfig.setAppendRequestMaxEntryCount(Integer.parseInt(getTextContent(child)));
            } else if (matches("commit-index-advance-count-to-snapshot", nodeName)) {
                raftAlgorithmConfig.setCommitIndexAdvanceCountToSnapshot(Integer.parseInt(getTextContent(child)));
            } else if (matches("uncommitted-entry-count-to-reject-new-appends", nodeName)) {
                raftAlgorithmConfig.setUncommittedEntryCountToRejectNewAppends(Integer.parseInt(getTextContent(child)));
            } else if (matches("append-request-backoff-timeout-in-millis", nodeName)) {
                raftAlgorithmConfig.setAppendRequestBackoffTimeoutInMillis(Long.parseLong(getTextContent(child)));
            }
        }
    }

    void handleSemaphores(CPSubsystemConfig cpSubsystemConfig, Node node) {
        for (Node child : childElements(node)) {
            SemaphoreConfig semaphoreConfig = new SemaphoreConfig();
            for (Node subChild : childElements(child)) {
                String nodeName = cleanNodeName(subChild);
                if (matches("name", nodeName)) {
                    semaphoreConfig.setName(getTextContent(subChild));
                } else if (matches("jdk-compatible", nodeName)) {
                    semaphoreConfig.setJDKCompatible(Boolean.parseBoolean(getTextContent(subChild)));
                } else if (matches("initial-permits", nodeName)) {
                    semaphoreConfig.setInitialPermits(Integer.parseInt(getTextContent(subChild)));
                }
            }
            cpSubsystemConfig.addSemaphoreConfig(semaphoreConfig);
        }
    }

    void handleFencedLocks(CPSubsystemConfig cpSubsystemConfig, Node node) {
        for (Node child : childElements(node)) {
            FencedLockConfig lockConfig = new FencedLockConfig();
            for (Node subChild : childElements(child)) {
                String nodeName = cleanNodeName(subChild);
                if (matches("name", nodeName)) {
                    lockConfig.setName(getTextContent(subChild));
                } else if (matches("lock-acquire-limit", nodeName)) {
                    lockConfig.setLockAcquireLimit(Integer.parseInt(getTextContent(subChild)));
                }
            }
            cpSubsystemConfig.addLockConfig(lockConfig);
        }
    }

    private void handleMetrics(Node node) {
        MetricsConfig metricsConfig = config.getMetricsConfig();

        NamedNodeMap attributes = node.getAttributes();
        for (int a = 0; a < attributes.getLength(); a++) {
            Node att = attributes.item(a);
            if (matches("enabled", att.getNodeName())) {
                boolean enabled = getBooleanValue(getAttribute(node, "enabled"));
                metricsConfig.setEnabled(enabled);
            }
        }

        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("management-center", nodeName)) {
                handleMetricsManagementCenter(child);
            } else if (matches("jmx", nodeName)) {
                handleMetricsJmx(child);
            } else if (matches("collection-frequency-seconds", nodeName)) {
                metricsConfig.setCollectionFrequencySeconds(Integer.parseInt(getTextContent(child)));
            }
        }
    }

    private void handleMetricsManagementCenter(Node node) {
        MetricsManagementCenterConfig managementCenterConfig = config.getMetricsConfig().getManagementCenterConfig();

        NamedNodeMap attributes = node.getAttributes();
        for (int a = 0; a < attributes.getLength(); a++) {
            Node att = attributes.item(a);
            if (matches("enabled", att.getNodeName())) {
                boolean enabled = getBooleanValue(getAttribute(node, "enabled"));
                managementCenterConfig.setEnabled(enabled);
            }

            for (Node child : childElements(node)) {
                String nodeName = cleanNodeName(child);
                if (matches("retention-seconds", nodeName)) {
                    managementCenterConfig.setRetentionSeconds(Integer.parseInt(getTextContent(child)));
                }
            }
        }
    }

    private void handleMetricsJmx(Node node) {
        MetricsJmxConfig jmxConfig = config.getMetricsConfig().getJmxConfig();

        NamedNodeMap attributes = node.getAttributes();
        for (int a = 0; a < attributes.getLength(); a++) {
            Node att = attributes.item(a);
            if (matches("enabled", att.getNodeName())) {
                boolean enabled = getBooleanValue(getAttribute(node, "enabled"));
                jmxConfig.setEnabled(enabled);
            }
        }
    }

    private void handleJet(Node node) {
        JetConfig jetConfig = config.getJetConfig();

        NamedNodeMap attributes = node.getAttributes();
        for (int a = 0; a < attributes.getLength(); a++) {
            Node attribute = attributes.item(a);
            if (matches("enabled", attribute.getNodeName())) {
                boolean enabled = getBooleanValue(getAttribute(node, "enabled"));
                jetConfig.setEnabled(enabled);
            } else if (matches("resource-upload-enabled", attribute.getNodeName())) {
                boolean resourceUploadEnabled = getBooleanValue(getAttribute(node, "resource-upload-enabled"));
                jetConfig.setResourceUploadEnabled(resourceUploadEnabled);
            }
        }
        // if JetConfig contains InstanceConfig(deprecated) fields
        // <instance> tag will be ignored.
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("cooperative-thread-count", nodeName)) {
                jetConfig.setCooperativeThreadCount(
                        getIntegerValue("cooperative-thread-count", getTextContent(child)));
            } else if (matches("flow-control-period", nodeName)) {
                jetConfig.setFlowControlPeriodMs(
                        getIntegerValue("flow-control-period", getTextContent(child)));
            } else if (matches("backup-count", nodeName)) {
                jetConfig.setBackupCount(
                        getIntegerValue("backup-count", getTextContent(child)));
            } else if (matches("scale-up-delay-millis", nodeName)) {
                jetConfig.setScaleUpDelayMillis(
                        getLongValue("scale-up-delay-millis", getTextContent(child)));
            } else if (matches("lossless-restart-enabled", nodeName)) {
                jetConfig.setLosslessRestartEnabled(getBooleanValue(getTextContent(child)));
            } else if (matches("max-processor-accumulated-records", nodeName)) {
                jetConfig.setMaxProcessorAccumulatedRecords(
                        getLongValue("max-processor-accumulated-records", getTextContent(child)));
            } else if (matches("edge-defaults", nodeName)) {
                handleEdgeDefaults(jetConfig, child);
            } else if (matches("instance", nodeName)) {
                if (jetConfigContainsInstanceConfigFields(node)) {
                    LOGGER.warning("<instance> tag will be ignored "
                            + "since <jet> tag already contains the instance fields.");
                } else {
                    LOGGER.warning("<instance> tag is deprecated, use <jet> tag directly for configuration.");
                    handleInstance(jetConfig, child);
                }
            }
        }
    }

    @SuppressWarnings("checkstyle:BooleanExpressionComplexity")
    private boolean jetConfigContainsInstanceConfigFields(Node node) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if ("cooperative-thread-count".equals(nodeName)
                    || "flow-control-period".equals(nodeName)
                    || "backup-count".equals(nodeName)
                    || "scale-up-delay-millis".equals(nodeName)
                    || "lossless-restart-enabled".equals(nodeName)
                    || "max-processor-accumulated-records".equals(nodeName)) {
                return true;
            }
        }
        return false;
    }

    private void handleInstance(JetConfig jetConfig, Node node) {
        InstanceConfig instanceConfig = jetConfig.getInstanceConfig();
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("cooperative-thread-count", nodeName)) {
                instanceConfig.setCooperativeThreadCount(
                        getIntegerValue("cooperative-thread-count", getTextContent(child)));
            } else if (matches("flow-control-period", nodeName)) {
                instanceConfig.setFlowControlPeriodMs(
                        getIntegerValue("flow-control-period", getTextContent(child)));
            } else if (matches("backup-count", nodeName)) {
                instanceConfig.setBackupCount(
                        getIntegerValue("backup-count", getTextContent(child)));
            } else if (matches("scale-up-delay-millis", nodeName)) {
                instanceConfig.setScaleUpDelayMillis(
                        getLongValue("scale-up-delay-millis", getTextContent(child)));
            } else if (matches("lossless-restart-enabled", nodeName)) {
                instanceConfig.setLosslessRestartEnabled(getBooleanValue(getTextContent(child)));
            } else if (matches("max-processor-accumulated-records", nodeName)) {
                instanceConfig.setMaxProcessorAccumulatedRecords(
                        getLongValue("max-processor-accumulated-records", getTextContent(child)));
            }
        }
    }

    private void handleEdgeDefaults(JetConfig jetConfig, Node node) {
        EdgeConfig edgeConfig = jetConfig.getDefaultEdgeConfig();
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("queue-size", nodeName)) {
                edgeConfig.setQueueSize(getIntegerValue("queue-size", getTextContent(child)));
            } else if (matches("packet-size-limit", nodeName)) {
                edgeConfig.setPacketSizeLimit(getIntegerValue("packet-size-limit", getTextContent(child)));
            } else if (matches("receive-window-multiplier", nodeName)) {
                edgeConfig.setReceiveWindowMultiplier(
                        getIntegerValue("receive-window-multiplier", getTextContent(child)));
            }
        }
    }

    private void handleSql(Node node) {
        SqlConfig sqlConfig = config.getSqlConfig();

        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("statement-timeout-millis", nodeName)) {
                sqlConfig.setStatementTimeoutMillis(Long.parseLong(getTextContent(child)));
            }
            if (matches("catalog-persistence-enabled", nodeName)) {
                sqlConfig.setCatalogPersistenceEnabled(Boolean.parseBoolean(getTextContent(child)));
            }
            if (matches("java-reflection-filter", nodeName)) {
                sqlConfig.setJavaReflectionFilterConfig(getJavaFilter(child));
            }
        }
    }

    protected void handleRealm(Node node) {
        String realmName = getAttribute(node, "name");
        RealmConfig realmConfig = new RealmConfig();
        config.getSecurityConfig().addRealmConfig(realmName, realmConfig);
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("authentication", nodeName)) {
                handleAuthentication(realmConfig, child);
            } else if (matches("identity", nodeName)) {
                handleIdentity(realmConfig, child);
            } else if (matches("access-control-service", nodeName)) {
                handleAccessControlService(realmConfig, child);
            }
        }
    }

    private void handleAuthentication(RealmConfig realmConfig, Node node) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("jaas", nodeName)) {
                handleJaasAuthentication(realmConfig, child);
            } else if (matches("tls", nodeName)) {
                handleTlsAuthentication(realmConfig, child);
            } else if (matches("ldap", nodeName)) {
                realmConfig.setLdapAuthenticationConfig(createLdapAuthentication(child));
            } else if (matches("kerberos", nodeName)) {
                handleKerberosAuthentication(realmConfig, child);
            } else if (matches("simple", nodeName)) {
                handleSimpleAuthentication(realmConfig, child);
            }
        }
    }

    private void handleIdentity(RealmConfig realmConfig, Node node) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("username-password", nodeName)) {
                realmConfig.setUsernamePasswordIdentityConfig(getAttribute(child, "username"), getAttribute(child, "password"));
            } else if (matches("credentials-factory", nodeName)) {
                handleCredentialsFactory(realmConfig, child);
            } else if (matches("token", nodeName)) {
                handleToken(realmConfig, child);
            } else if (matches("kerberos", nodeName)) {
                handleKerberosIdentity(realmConfig, child);
            }
        }
    }

    private void handleAccessControlService(RealmConfig realmConfig, Node node) {
        AccessControlServiceConfig acs = new AccessControlServiceConfig();
        realmConfig.setAccessControlServiceConfig(acs);
        fillBaseFactoryWithPropertiesConfig(node, acs);
    }

    protected void handleToken(RealmConfig realmConfig, Node node) {
        TokenEncoding encoding = TokenEncoding.getTokenEncoding(getAttribute(node, "encoding"));
        TokenIdentityConfig tic = new TokenIdentityConfig(encoding, getTextContent(node));
        realmConfig.setTokenIdentityConfig(tic);
    }

    protected void handleKerberosIdentity(RealmConfig realmConfig, Node node) {
        KerberosIdentityConfig kerbIdentity = new KerberosIdentityConfig();
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("realm", nodeName)) {
                kerbIdentity.setRealm(getTextContent(child));
            } else if (matches("security-realm", nodeName)) {
                kerbIdentity.setSecurityRealm(getTextContent(child));
            } else if (matches("principal", nodeName)) {
                kerbIdentity.setPrincipal(getTextContent(child));
            } else if (matches("keytab-file", nodeName)) {
                kerbIdentity.setKeytabFile(getTextContent(child));
            } else if (matches("service-name-prefix", nodeName)) {
                kerbIdentity.setServiceNamePrefix(getTextContent(child));
            } else if (matches("spn", nodeName)) {
                kerbIdentity.setSpn(getTextContent(child));
            } else if (matches("use-canonical-hostname", nodeName)) {
                kerbIdentity.setUseCanonicalHostname(getBooleanValue(getTextContent(child)));
            }
        }
        realmConfig.setKerberosIdentityConfig(kerbIdentity);
    }

    protected void handleTlsAuthentication(RealmConfig realmConfig, Node node) {
        String roleAttribute = getAttribute(node, "roleAttribute");
        TlsAuthenticationConfig tlsCfg = new TlsAuthenticationConfig();
        fillClusterLoginConfig(tlsCfg, node);

        if (roleAttribute != null) {
            tlsCfg.setRoleAttribute(roleAttribute);
        }
        realmConfig.setTlsAuthenticationConfig(tlsCfg);
    }

    protected LdapAuthenticationConfig createLdapAuthentication(Node node) {
        LdapAuthenticationConfig ldapCfg = new LdapAuthenticationConfig();
        fillClusterLoginConfig(ldapCfg, node);
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("url", nodeName)) {
                ldapCfg.setUrl(getTextContent(child));
            } else if (matches("socket-factory-class-name", nodeName)) {
                ldapCfg.setSocketFactoryClassName(getTextContent(child));
            } else if (matches("parse-dn", nodeName)) {
                ldapCfg.setParseDn(getBooleanValue(getTextContent(child)));
            } else if (matches("role-context", nodeName)) {
                ldapCfg.setRoleContext(getTextContent(child));
            } else if (matches("role-filter", nodeName)) {
                ldapCfg.setRoleFilter(getTextContent(child));
            } else if (matches("role-mapping-attribute", nodeName)) {
                ldapCfg.setRoleMappingAttribute(getTextContent(child));
            } else if (matches("role-mapping-mode", nodeName)) {
                ldapCfg.setRoleMappingMode(getRoleMappingMode(getTextContent(child)));
            } else if (matches("role-name-attribute", nodeName)) {
                ldapCfg.setRoleNameAttribute(getTextContent(child));
            } else if (matches("role-recursion-max-depth", nodeName)) {
                ldapCfg.setRoleRecursionMaxDepth(getIntegerValue("role-recursion-max-depth", getTextContent(child)));
            } else if (matches("role-search-scope", nodeName)) {
                ldapCfg.setRoleSearchScope(getSearchScope(getTextContent(child)));
            } else if (matches("user-name-attribute", nodeName)) {
                ldapCfg.setUserNameAttribute(getTextContent(child));
            } else if (matches("system-user-dn", nodeName)) {
                ldapCfg.setSystemUserDn(getTextContent(child));
            } else if (matches("system-user-password", nodeName)) {
                ldapCfg.setSystemUserPassword(getTextContent(child));
            } else if (matches("system-authentication", nodeName)) {
                ldapCfg.setSystemAuthentication(getTextContent(child));
            } else if (matches("security-realm", nodeName)) {
                ldapCfg.setSecurityRealm(getTextContent(child));
            } else if (matches("password-attribute", nodeName)) {
                ldapCfg.setPasswordAttribute(getTextContent(child));
            } else if (matches("user-context", nodeName)) {
                ldapCfg.setUserContext(getTextContent(child));
            } else if (matches("user-filter", nodeName)) {
                ldapCfg.setUserFilter(getTextContent(child));
            } else if (matches("user-search-scope", nodeName)) {
                ldapCfg.setUserSearchScope(getSearchScope(getTextContent(child)));
            } else if (matches("skip-authentication", nodeName)) {
                ldapCfg.setSkipAuthentication(getBooleanValue(getTextContent(child)));
            }
        }
        return ldapCfg;
    }

    protected void handleKerberosAuthentication(RealmConfig realmConfig, Node node) {
        KerberosAuthenticationConfig krbCfg = new KerberosAuthenticationConfig();
        fillClusterLoginConfig(krbCfg, node);
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("relax-flags-check", nodeName)) {
                krbCfg.setRelaxFlagsCheck(getBooleanValue(getTextContent(child)));
            } else if (matches("use-name-without-realm", nodeName)) {
                krbCfg.setUseNameWithoutRealm(getBooleanValue(getTextContent(child)));
            } else if (matches("security-realm", nodeName)) {
                krbCfg.setSecurityRealm(getTextContent(child));
            } else if (matches("principal", nodeName)) {
                krbCfg.setPrincipal(getTextContent(child));
            } else if (matches("keytab-file", nodeName)) {
                krbCfg.setKeytabFile(getTextContent(child));
            } else if (matches("ldap", nodeName)) {
                krbCfg.setLdapAuthenticationConfig(createLdapAuthentication(child));
            }
        }
        realmConfig.setKerberosAuthenticationConfig(krbCfg);
    }

    protected void handleSimpleAuthentication(RealmConfig realmConfig, Node node) {
        SimpleAuthenticationConfig simpleCfg = new SimpleAuthenticationConfig();
        fillClusterLoginConfig(simpleCfg, node);
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("user", nodeName)) {
                addSimpleUser(simpleCfg, child);
            } else if (matches("role-separator", nodeName)) {
                simpleCfg.setRoleSeparator(getTextContent(child));
            }
        }
        realmConfig.setSimpleAuthenticationConfig(simpleCfg);
    }

    private void addSimpleUser(SimpleAuthenticationConfig simpleCfg, Node node) {
        String username = getAttribute(node, "username");
        List<String> roles = new ArrayList<>();
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("role", nodeName)) {
                roles.add(getTextContent(child));
            }
        }
        SimpleAuthenticationConfig.UserDto userDto = new SimpleAuthenticationConfig.UserDto(getAttribute(node, "password"),
                roles.toArray(new String[roles.size()]));
        simpleCfg.addUser(username, userDto);
    }

    private void handleCredentialsFactory(RealmConfig realmConfig, Node node) {
        String className = getAttribute(node, "class-name");
        CredentialsFactoryConfig credentialsFactoryConfig = new CredentialsFactoryConfig(className);
        realmConfig.setCredentialsFactoryConfig(credentialsFactoryConfig);
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("properties", nodeName)) {
                fillProperties(child, credentialsFactoryConfig.getProperties());
            }
        }
    }

    private void handleIntegrityChecker(final Node node) {
        Node attrEnabled = getNamedItemNode(node, "enabled");
        boolean enabled = attrEnabled != null && getBooleanValue(getTextContent(attrEnabled));
        config.getIntegrityCheckerConfig().setEnabled(enabled);
    }

    private void handleTpc(Node node) {
        Node attrEnabled = getNamedItemNode(node, "enabled");
        boolean enabled = attrEnabled != null && getBooleanValue(getTextContent(attrEnabled));
        TpcConfig tpcConfig = config.getTpcConfig();
        tpcConfig.setEnabled(enabled);

        for (Node child : childElements(node)) {
            String childName = cleanNodeName(child);
            if (matches("eventloop-count", childName)) {
                tpcConfig.setEventloopCount(getIntegerValue("eventloop-count", getTextContent(child)));
            }
        }
    }

    protected void handleDataConnections(Node node) {
        String name = getAttribute(node, "name");
        DataConnectionConfig dataConnectionConfig = ConfigUtils.getByNameOrNew(config.getDataConnectionConfigs(),
                name, DataConnectionConfig.class);
        handleDataConnection(node, dataConnectionConfig);
        DataConnectionConfigValidator.validate(dataConnectionConfig);
    }

    protected void handleDataConnection(Node node, DataConnectionConfig dataConnectionConfig) {
        for (Node child : childElements(node)) {
            String childName = cleanNodeName(child);
            if (matches("type", childName)) {
                dataConnectionConfig.setType(getTextContent(child));
            } else if (matches("properties", childName)) {
                fillProperties(child, dataConnectionConfig.getProperties());
            } else if (matches("shared", childName)) {
                dataConnectionConfig.setShared(getBooleanValue(getTextContent(child)));
            }
        }
    }

    private void handleRest(Node node) {
        Node enabledNode = getNamedItemNode(node, "enabled");
        boolean enabled = enabledNode != null && getBooleanValue(getTextContent(enabledNode));
        RestConfig restConfig = config.getRestConfig();
        restConfig.setEnabled(enabled);

        final String portName = "port";
        final String securityRealmName = "security-realm";
        final String tokenValiditySecondsName = "token-validity-seconds";
        final String requestTimeoutSecondsName = "request-timeout-seconds";
        final String maxLoginAttempts = "max-login-attempts";
        final String lockoutDuration = "lockout-duration-seconds";

        for (Node child : childElements(node)) {
            String childName = cleanNodeName(child);
            if (matches(portName, childName)) {
                restConfig.setPort(getIntegerValue(portName, getTextContent(child)));
            } else if (matches(securityRealmName, childName)) {
                restConfig.setSecurityRealm(getTextContent(child));
            } else if (matches(tokenValiditySecondsName, childName)) {
                int durationSeconds = getIntegerValue(tokenValiditySecondsName, getTextContent(child));
                restConfig.setTokenValidityDuration(Duration.of(durationSeconds, SECONDS));
            } else if (matches("ssl", childName)) {
                handleRestSsl(restConfig.getSsl(), child);
            } else if (matches(requestTimeoutSecondsName, childName)) {
                restConfig.setRequestTimeoutDuration(Duration
                        .ofSeconds(getIntegerValue(requestTimeoutSecondsName, getTextContent(child))));
            } else if (matches(maxLoginAttempts, childName)) {
                restConfig.setMaxLoginAttempts(getIntegerValue(maxLoginAttempts, getTextContent(child)));
            } else if (matches(lockoutDuration, childName)) {
                int durationSeconds = getIntegerValue(lockoutDuration, getTextContent(child));
                restConfig.setLockoutDuration(Duration.of(durationSeconds, SECONDS));
            }
        }
    }

    private void handleRestSsl(RestConfig.Ssl ssl, Node node) {
        Node enabledNode = getNamedItemNode(node, "enabled");
        boolean enabled = enabledNode != null && getBooleanValue(getTextContent(enabledNode));
        ssl.setEnabled(enabled);

        for (Node child : childElements(node)) {
            String childName = cleanNodeName(child);
            if (matches("client-auth", childName)) {
                ssl.setClientAuth(RestConfig.Ssl.ClientAuth.valueOf(getTextContent(child)));
            } else if (matches("ciphers", childName)) {
                ssl.setCiphers(getTextContent(child));
            } else if (matches("enabled-protocols", childName)) {
                ssl.setEnabledProtocols(getTextContent(child));
            } else if (matches("key-alias", childName)) {
                ssl.setKeyAlias(getTextContent(child));
            } else if (matches("key-password", childName)) {
                ssl.setKeyPassword(getTextContent(child));
            } else if (matches("key-store", childName)) {
                ssl.setKeyStore(getTextContent(child));
            } else if (matches("key-store-password", childName)) {
                ssl.setKeyStorePassword(getTextContent(child));
            } else if (matches("key-store-type", childName)) {
                ssl.setKeyStoreType(getTextContent(child));
            } else if (matches("key-store-provider", childName)) {
                ssl.setKeyStoreProvider(getTextContent(child));
            } else if (matches("trust-store", childName)) {
                ssl.setTrustStore(getTextContent(child));
            } else if (matches("trust-store-password", childName)) {
                ssl.setTrustStorePassword(getTextContent(child));
            } else if (matches("trust-store-type", childName)) {
                ssl.setTrustStoreType(getTextContent(child));
            } else if (matches("trust-store-provider", childName)) {
                ssl.setTrustStoreProvider(getTextContent(child));
            } else if (matches("protocol", childName)) {
                ssl.setProtocol(getTextContent(child));
            } else if (matches("certificate", childName)) {
                ssl.setCertificate(getTextContent(child));
            } else if (matches("certificate-key", childName)) {
                ssl.setCertificatePrivateKey(getTextContent(child));
            } else if (matches("trust-certificate", childName)) {
                ssl.setTrustCertificate(getTextContent(child));
            } else if (matches("trust-certificate-key", childName)) {
                ssl.setTrustCertificatePrivateKey(getTextContent(child));
            }
        }
    }

    protected void handlePartitionAttributes(Node node, MapConfig mapConfig) {
        for (final Node childElement : childElements(node)) {
            final PartitioningAttributeConfig attributeConfig = new PartitioningAttributeConfig();
            handlePartitioningAttributeConfig(childElement, attributeConfig);

            mapConfig.getPartitioningAttributeConfigs().add(attributeConfig);
        }
    }

    protected void handlePartitioningAttributeConfig(Node node, PartitioningAttributeConfig config) {
        config.setAttributeName(getTextContent(node));
    }

    protected void handleVector(Node node) {
        String name = getAttribute(node, "name");
        VectorCollectionConfig collectionConfig = ConfigUtils.getByNameOrNew(
                config.getVectorCollectionConfigs(),
                name,
                VectorCollectionConfig.class
        );
        handleVectorNode(node, collectionConfig);
    }

    protected void handleVectorNode(Node parentNode, VectorCollectionConfig collectionConfig) {
        for (Node node : childElements(parentNode)) {
            String nodeName = cleanNodeName(node);
            if (matches("indexes", nodeName)) {
                handleVectorIndexesNode(node, collectionConfig);
            } else if (matches("backup-count", nodeName)) {
                collectionConfig.setBackupCount(getIntegerValue("backup-count", getTextContent(node)));
            } else if (matches("async-backup-count", nodeName)) {
                collectionConfig.setAsyncBackupCount(getIntegerValue("async-backup-count", getTextContent(node)));
            } else if (matches("merge-policy", nodeName)) {
                MergePolicyConfig mpConfig = createMergePolicyConfig(node, collectionConfig.getMergePolicyConfig());
                collectionConfig.setMergePolicyConfig(mpConfig);
            } else if (matches("split-brain-protection-ref", nodeName)) {
                collectionConfig.setSplitBrainProtectionName(getTextContent(node));
            } else if (matches("user-code-namespace", nodeName)) {
                collectionConfig.setUserCodeNamespace(getTextContent(node));
            }
        }
        config.addVectorCollectionConfig(collectionConfig);
    }

    protected void handleVectorIndexesNode(Node indexesNode, VectorCollectionConfig collectionConfig) {
        for (Node n : childElements(indexesNode)) {
            String nodeName = cleanNodeName(n);
            if (matches("index", nodeName)) {
                handleVectorIndex(n, collectionConfig);
            }
        }
    }

    protected void handleVectorIndex(Node node, VectorCollectionConfig collectionConfig) {
        VectorIndexConfig indexConfig = new VectorIndexConfig();
        var name = getAttribute(node, "name");
        if (name != null) {
            indexConfig.setName(name);
        }
        handleVectorIndexNode(node, indexConfig);
        collectionConfig.addVectorIndexConfig(indexConfig);
    }

    protected void handleVectorIndexNode(Node node, VectorIndexConfig indexConfig) {
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if (matches("name", nodeName)) {
                indexConfig.setName(getTextContent(n));
            } else if (matches("dimension", nodeName)) {
                indexConfig.setDimension(getIntegerValue("dimension", getTextContent(n)));
            } else if (matches("metric", nodeName)) {
                indexConfig.setMetric(Metric.valueOf(getTextContent(n)));
            } else if (matches("max-degree", nodeName)) {
                indexConfig.setMaxDegree(getIntegerValue("max-degree", getTextContent(n)));
            } else if (matches("ef-construction", nodeName)) {
                indexConfig.setEfConstruction(getIntegerValue("ef-construction", getTextContent(n)));
            } else if (matches("use-deduplication", nodeName)) {
                indexConfig.setUseDeduplication(getBooleanValue(getTextContent(n)));
            }

        }
    }

    protected void fillClusterLoginConfig(AbstractClusterLoginConfig<?> config, Node node) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("skip-identity", nodeName)) {
                config.setSkipIdentity(getBooleanValue(getTextContent(child)));
            } else if (matches("skip-endpoint", nodeName)) {
                config.setSkipEndpoint(getBooleanValue(getTextContent(child)));
            } else if (matches("skip-role", nodeName)) {
                config.setSkipRole(getBooleanValue(getTextContent(child)));
            }
        }
    }

    private void validateNetworkConfig() {
        if (occurrenceSet.contains("network")
                && occurrenceSet.stream().anyMatch(c -> matches("advanced-network", c))
                && config.getAdvancedNetworkConfig().isEnabled()) {
            throw new InvalidConfigurationException("Ambiguous configuration: cannot include both <network> and "
                    + "an enabled <advanced-network> element. Configure network using one of <network> or "
                    + "<advanced-network enabled=\"true\">.");
        }
    }
}
