/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.AuditlogConfig;
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
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.DurableExecutorConfig;
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
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapPartitionLostListenerConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.MemberAddressProviderConfig;
import com.hazelcast.config.MemberGroupConfig;
import com.hazelcast.config.MemcacheProtocolConfig;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.MerkleTreeConfig;
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
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.config.PermissionPolicyConfig;
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
import com.hazelcast.config.TopicConfig;
import com.hazelcast.config.TrustedInterfacesConfigurable;
import com.hazelcast.config.UserCodeDeploymentConfig;
import com.hazelcast.config.VaultSecureStoreConfig;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanBatchPublisherConfig;
import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.config.WanCustomPublisherConfig;
import com.hazelcast.config.WanQueueFullBehavior;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.config.WanSyncConfig;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.config.cp.FencedLockConfig;
import com.hazelcast.config.cp.RaftAlgorithmConfig;
import com.hazelcast.config.cp.SemaphoreConfig;
import com.hazelcast.config.security.AbstractClusterLoginConfig;
import com.hazelcast.config.security.KerberosAuthenticationConfig;
import com.hazelcast.config.security.KerberosIdentityConfig;
import com.hazelcast.config.security.LdapAuthenticationConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.config.security.TlsAuthenticationConfig;
import com.hazelcast.config.security.TokenEncoding;
import com.hazelcast.config.security.TokenIdentityConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.util.StringUtil;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

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
import static com.hazelcast.internal.config.ConfigSections.DURABLE_EXECUTOR_SERVICE;
import static com.hazelcast.internal.config.ConfigSections.EXECUTOR_SERVICE;
import static com.hazelcast.internal.config.ConfigSections.FLAKE_ID_GENERATOR;
import static com.hazelcast.internal.config.ConfigSections.HOT_RESTART_PERSISTENCE;
import static com.hazelcast.internal.config.ConfigSections.IMPORT;
import static com.hazelcast.internal.config.ConfigSections.INSTANCE_NAME;
import static com.hazelcast.internal.config.ConfigSections.INSTANCE_TRACKING;
import static com.hazelcast.internal.config.ConfigSections.LICENSE_KEY;
import static com.hazelcast.internal.config.ConfigSections.LIST;
import static com.hazelcast.internal.config.ConfigSections.LISTENERS;
import static com.hazelcast.internal.config.ConfigSections.LITE_MEMBER;
import static com.hazelcast.internal.config.ConfigSections.MANAGEMENT_CENTER;
import static com.hazelcast.internal.config.ConfigSections.MAP;
import static com.hazelcast.internal.config.ConfigSections.MEMBER_ATTRIBUTES;
import static com.hazelcast.internal.config.ConfigSections.METRICS;
import static com.hazelcast.internal.config.ConfigSections.MULTIMAP;
import static com.hazelcast.internal.config.ConfigSections.NATIVE_MEMORY;
import static com.hazelcast.internal.config.ConfigSections.NETWORK;
import static com.hazelcast.internal.config.ConfigSections.PARTITION_GROUP;
import static com.hazelcast.internal.config.ConfigSections.PN_COUNTER;
import static com.hazelcast.internal.config.ConfigSections.PROPERTIES;
import static com.hazelcast.internal.config.ConfigSections.QUEUE;
import static com.hazelcast.internal.config.ConfigSections.RELIABLE_TOPIC;
import static com.hazelcast.internal.config.ConfigSections.REPLICATED_MAP;
import static com.hazelcast.internal.config.ConfigSections.RINGBUFFER;
import static com.hazelcast.internal.config.ConfigSections.SCHEDULED_EXECUTOR_SERVICE;
import static com.hazelcast.internal.config.ConfigSections.SECURITY;
import static com.hazelcast.internal.config.ConfigSections.SERIALIZATION;
import static com.hazelcast.internal.config.ConfigSections.SET;
import static com.hazelcast.internal.config.ConfigSections.SPLIT_BRAIN_PROTECTION;
import static com.hazelcast.internal.config.ConfigSections.SQL;
import static com.hazelcast.internal.config.ConfigSections.TOPIC;
import static com.hazelcast.internal.config.ConfigSections.USER_CODE_DEPLOYMENT;
import static com.hazelcast.internal.config.ConfigSections.WAN_REPLICATION;
import static com.hazelcast.internal.config.ConfigSections.canOccurMultipleTimes;
import static com.hazelcast.internal.config.ConfigUtils.matches;
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
import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;
import static com.hazelcast.internal.util.StringUtil.lowerCaseInternal;
import static com.hazelcast.internal.util.StringUtil.upperCaseInternal;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

@SuppressWarnings({
        "checkstyle:cyclomaticcomplexity",
        "checkstyle:classdataabstractioncoupling",
        "checkstyle:classfanoutcomplexity",
        "checkstyle:methodcount",
        "checkstyle:methodlength"})
public class MemberDomConfigProcessor extends AbstractDomConfigProcessor {

    private static final ILogger LOGGER = Logger.getLogger(MemberDomConfigProcessor.class);

    protected final Config config;

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
            if (handleNode(node, nodeName)) {
                continue;
            }
            if (!canOccurMultipleTimes(nodeName)) {
                occurrenceSet.add(nodeName);
            }
        }

        validateNetworkConfig();
    }

    private boolean handleNode(Node node, String nodeName) throws Exception {
        if (INSTANCE_NAME.isEqual(nodeName)) {
            config.setInstanceName(getNonEmptyText(node, "Instance name"));
        } else if (NETWORK.isEqual(nodeName)) {
            handleNetwork(node);
        } else if (IMPORT.isEqual(nodeName)) {
            throw new HazelcastException("Non-expanded <import> element found");
        } else if (CLUSTER_NAME.isEqual(nodeName)) {
            config.setClusterName(getNonEmptyText(node, "Clustername"));
        } else if (PROPERTIES.isEqual(nodeName)) {
            fillProperties(node, config.getProperties());
        } else if (WAN_REPLICATION.isEqual(nodeName)) {
            handleWanReplication(node);
        } else if (EXECUTOR_SERVICE.isEqual(nodeName)) {
            handleExecutor(node);
        } else if (DURABLE_EXECUTOR_SERVICE.isEqual(nodeName)) {
            handleDurableExecutor(node);
        } else if (SCHEDULED_EXECUTOR_SERVICE.isEqual(nodeName)) {
            handleScheduledExecutor(node);
        } else if (QUEUE.isEqual(nodeName)) {
            handleQueue(node);
        } else if (MAP.isEqual(nodeName)) {
            handleMap(node);
        } else if (MULTIMAP.isEqual(nodeName)) {
            handleMultiMap(node);
        } else if (REPLICATED_MAP.isEqual(nodeName)) {
            handleReplicatedMap(node);
        } else if (LIST.isEqual(nodeName)) {
            handleList(node);
        } else if (SET.isEqual(nodeName)) {
            handleSet(node);
        } else if (TOPIC.isEqual(nodeName)) {
            handleTopic(node);
        } else if (RELIABLE_TOPIC.isEqual(nodeName)) {
            handleReliableTopic(node);
        } else if (CACHE.isEqual(nodeName)) {
            handleCache(node);
        } else if (NATIVE_MEMORY.isEqual(nodeName)) {
            fillNativeMemoryConfig(node, config.getNativeMemoryConfig());
        } else if (RINGBUFFER.isEqual(nodeName)) {
            handleRingbuffer(node);
        } else if (LISTENERS.isEqual(nodeName)) {
            handleListeners(node);
        } else if (PARTITION_GROUP.isEqual(nodeName)) {
            handlePartitionGroup(node);
        } else if (SERIALIZATION.isEqual(nodeName)) {
            handleSerialization(node);
        } else if (SECURITY.isEqual(nodeName)) {
            handleSecurity(node);
        } else if (MEMBER_ATTRIBUTES.isEqual(nodeName)) {
            handleMemberAttributes(node);
        } else if (LICENSE_KEY.isEqual(nodeName)) {
            config.setLicenseKey(getTextContent(node));
        } else if (MANAGEMENT_CENTER.isEqual(nodeName)) {
            handleManagementCenterConfig(node);
        } else if (SPLIT_BRAIN_PROTECTION.isEqual(nodeName)) {
            handleSplitBrainProtection(node);
        } else if (LITE_MEMBER.isEqual(nodeName)) {
            handleLiteMember(node);
        } else if (HOT_RESTART_PERSISTENCE.isEqual(nodeName)) {
            handleHotRestartPersistence(node);
        } else if (USER_CODE_DEPLOYMENT.isEqual(nodeName)) {
            handleUserCodeDeployment(node);
        } else if (CARDINALITY_ESTIMATOR.isEqual(nodeName)) {
            handleCardinalityEstimator(node);
        } else if (FLAKE_ID_GENERATOR.isEqual(nodeName)) {
            handleFlakeIdGenerator(node);
        } else if (CRDT_REPLICATION.isEqual(nodeName)) {
            handleCRDTReplication(node);
        } else if (PN_COUNTER.isEqual(nodeName)) {
            handlePNCounter(node);
        } else if (ADVANCED_NETWORK.isEqual(nodeName)) {
            handleAdvancedNetwork(node);
        } else if (CP_SUBSYSTEM.isEqual(nodeName)) {
            handleCPSubsystem(node);
        } else if (AUDITLOG.isEqual(nodeName)) {
            config.setAuditlogConfig(fillFactoryWithPropertiesConfig(node, new AuditlogConfig()));
        } else if (METRICS.isEqual(nodeName)) {
            handleMetrics(node);
        } else if (INSTANCE_TRACKING.isEqual(nodeName)) {
            handleInstanceTracking(node, config.getInstanceTrackingConfig());
        } else if (SQL.isEqual(nodeName)) {
            handleSql(node);
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
        UserCodeDeploymentConfig dcConfig = new UserCodeDeploymentConfig();
        Node attrEnabled = DomConfigHelper.getNamedItemNode(dcRoot, "enabled");
        boolean enabled = getBooleanValue(getTextContent(attrEnabled));
        dcConfig.setEnabled(enabled);

        for (Node n : childElements(dcRoot)) {
            String name = cleanNodeName(n);
            if (matches("class-cache-mode", name)) {
                String value = getTextContent(n);
                UserCodeDeploymentConfig.ClassCacheMode classCacheMode = UserCodeDeploymentConfig.ClassCacheMode.valueOf(value);
                dcConfig.setClassCacheMode(classCacheMode);
            } else if (matches("provider-mode", name)) {
                String value = getTextContent(n);
                UserCodeDeploymentConfig.ProviderMode providerMode = UserCodeDeploymentConfig.ProviderMode.valueOf(value);
                dcConfig.setProviderMode(providerMode);
            } else if (matches("blacklist-prefixes", name)) {
                String value = getTextContent(n);
                dcConfig.setBlacklistedPrefixes(value);
            } else if (matches("whitelist-prefixes", name)) {
                String value = getTextContent(n);
                dcConfig.setWhitelistedPrefixes(value);
            } else if (matches("provider-filter", name)) {
                String value = getTextContent(n);
                dcConfig.setProviderFilter(value);
            }
        }
        config.setUserCodeDeploymentConfig(dcConfig);
    }

    private void handleHotRestartPersistence(Node hrRoot)
            throws Exception {
        HotRestartPersistenceConfig hrConfig = new HotRestartPersistenceConfig()
                .setEnabled(getBooleanValue(getAttribute(hrRoot, "enabled")));

        String parallelismName = "parallelism";
        String validationTimeoutName = "validation-timeout-seconds";
        String dataLoadTimeoutName = "data-load-timeout-seconds";

        for (Node n : childElements(hrRoot)) {
            String name = cleanNodeName(n);
            if (matches("encryption-at-rest", name)) {
                handleEncryptionAtRest(n, hrConfig);
            } else {
                String value = getTextContent(n);
                if (matches("base-dir", name)) {
                    hrConfig.setBaseDir(new File(value).getAbsoluteFile());
                } else if (matches("backup-dir", name)) {
                    hrConfig.setBackupDir(new File(value).getAbsoluteFile());
                } else if (matches(parallelismName, name)) {
                    hrConfig.setParallelism(getIntegerValue(parallelismName, value));
                } else if (matches(validationTimeoutName, name)) {
                    hrConfig.setValidationTimeoutSeconds(getIntegerValue(validationTimeoutName, value));
                } else if (matches(dataLoadTimeoutName, name)) {
                    hrConfig.setDataLoadTimeoutSeconds(getIntegerValue(dataLoadTimeoutName, value));
                } else if (matches("cluster-data-recovery-policy", name)) {
                    hrConfig.setClusterDataRecoveryPolicy(HotRestartClusterDataRecoveryPolicy.valueOf(upperCaseInternal(value)));
                } else if (matches("auto-remove-stale-data", name)) {
                    hrConfig.setAutoRemoveStaleData(getBooleanValue(value));
                }
            }
        }
        config.setHotRestartPersistenceConfig(hrConfig);
    }

    private void handleEncryptionAtRest(Node encryptionAtRestRoot, HotRestartPersistenceConfig hrConfig)
            throws Exception {
        EncryptionAtRestConfig encryptionAtRestConfig = new EncryptionAtRestConfig();
        handleViaReflection(encryptionAtRestRoot, hrConfig, encryptionAtRestConfig, "secure-store");
        for (Node secureStore : childElementsWithName(encryptionAtRestRoot, "secure-store")) {
            handleSecureStore(secureStore, encryptionAtRestConfig);
        }
        hrConfig.setEncryptionAtRestConfig(encryptionAtRestConfig);
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
            String value = getTextContent(n);
            if (matches("path", name)) {
                path = new File(value).getAbsoluteFile();
            } else if (matches("type", name)) {
                type = value;
            } else if (matches("password", name)) {
                password = value;
            } else if (matches("current-key-alias", name)) {
                currentKeyAlias = value;
            } else if (matches("polling-interval", name)) {
                pollingInterval = parseInt(value);
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
            String value = getTextContent(n);
            if (matches("address", name)) {
                address = value;
            } else if (matches("secret-path", name)) {
                secretPath = value;
            } else if (matches("token", name)) {
                token = value;
            } else if (matches("ssl", name)) {
                sslConfig = parseSslConfig(n);
            } else if (matches("polling-interval", name)) {
                pollingInterval = parseInt(value);
            }
        }
        VaultSecureStoreConfig vaultSecureStoreConfig = new VaultSecureStoreConfig(address, secretPath, token)
                .setSSLConfig(sslConfig)
                .setPollingInterval(pollingInterval);
        return vaultSecureStoreConfig;
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
        Node attrEnabled = DomConfigHelper.getNamedItemNode(node, "enabled");
        boolean liteMember = attrEnabled != null && getBooleanValue(getTextContent(attrEnabled));
        config.setLiteMember(liteMember);
    }

    protected void handleSplitBrainProtection(Node node) {
        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig();
        String name = getAttribute(node, "name");
        splitBrainProtectionConfig.setName(name);
        handleSplitBrainProtectionNode(node, splitBrainProtectionConfig, name);
    }

    protected void handleSplitBrainProtectionNode(Node node, SplitBrainProtectionConfig splitBrainProtectionConfig, String name) {
        Node attrEnabled = DomConfigHelper.getNamedItemNode(node, "enabled");
        boolean enabled = attrEnabled != null && getBooleanValue(getTextContent(attrEnabled));
        // probabilistic-split-brain-protection and recently-active-split-brain-protection
        // configs are constructed via SplitBrainProtectionConfigBuilder
        SplitBrainProtectionConfigBuilder splitBrainProtectionConfigBuilder = null;
        splitBrainProtectionConfig.setEnabled(enabled);
        for (Node n : childElements(node)) {
            String value = getTextContent(n).trim();
            String nodeName = cleanNodeName(n);
            if (matches("minimum-cluster-size", nodeName)) {
                splitBrainProtectionConfig.setMinimumClusterSize(getIntegerValue("minimum-cluster-size", value));
            } else if (matches("listeners", nodeName)) {
                handleSplitBrainProtectionListeners(splitBrainProtectionConfig, n);
            } else if (matches("protect-on", nodeName)) {
                splitBrainProtectionConfig.setProtectOn(SplitBrainProtectionOn.valueOf(upperCaseInternal(value)));
            } else if (matches("function-class-name", nodeName)) {
                splitBrainProtectionConfig.setFunctionClassName(value);
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
                        + " define probabilistic-split-brain-protectionm or "
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
        Node attName = DomConfigHelper.getNamedItemNode(node, "name");
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

    private void handleNetwork(Node node)
            throws Exception {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("reuse-address", nodeName)) {
                String value = getTextContent(child).trim();
                config.getNetworkConfig().setReuseAddress(getBooleanValue(value));
            } else if (matches("port", nodeName)) {
                handlePort(child, config);
            } else if (matches("outbound-ports", nodeName)) {
                handleOutboundPorts(child);
            } else if (matches("public-address", nodeName)) {
                String address = getTextContent(child);
                config.getNetworkConfig().setPublicAddress(address);
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
            }
        }
    }

    private void handleAdvancedNetwork(Node node)
            throws Exception {
        NamedNodeMap attributes = node.getAttributes();
        for (int a = 0; a < attributes.getLength(); a++) {
            Node att = attributes.item(a);
            if (matches("enabled", att.getNodeName())) {
                String value = att.getNodeValue();
                config.getAdvancedNetworkConfig().setEnabled(getBooleanValue(value));
            }
        }
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
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
        ServerSocketEndpointConfig config = new ServerSocketEndpointConfig();
        config.setProtocolType(ProtocolType.MEMBER);
        handleServerSocketEndpointConfig(config, node);
    }

    private void handleClientServerSocketEndpointConfig(Node node) throws Exception {
        ServerSocketEndpointConfig config = new ServerSocketEndpointConfig();
        config.setProtocolType(ProtocolType.CLIENT);
        handleServerSocketEndpointConfig(config, node);
    }

    protected void handleWanServerSocketEndpointConfig(Node node) throws Exception {
        ServerSocketEndpointConfig config = new ServerSocketEndpointConfig();
        config.setProtocolType(ProtocolType.WAN);
        handleServerSocketEndpointConfig(config, node);
    }

    private void handleRestServerSocketEndpointConfig(Node node) throws Exception {
        RestServerEndpointConfig config = new RestServerEndpointConfig();
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
        ServerSocketEndpointConfig config = new ServerSocketEndpointConfig();
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
                String value = getTextContent(child).trim();
                endpointConfig.setReuseAddress(getBooleanValue(value));
            } else {
                handleEndpointConfigCommons(child, nodeName, endpointConfig);
            }
        }
        addEndpointConfig(endpointConfig);
    }

    private void addEndpointConfig(EndpointConfig endpointConfig) {
        switch (endpointConfig.getProtocolType()) {
            case MEMBER:
                ensureServerSocketEndpointConfig(endpointConfig);
                config.getAdvancedNetworkConfig().setMemberEndpointConfig((ServerSocketEndpointConfig) endpointConfig);
                break;
            case CLIENT:
                ensureServerSocketEndpointConfig(endpointConfig);
                config.getAdvancedNetworkConfig().setClientEndpointConfig((ServerSocketEndpointConfig) endpointConfig);
                break;
            case REST:
                ensureServerSocketEndpointConfig(endpointConfig);
                config.getAdvancedNetworkConfig().setRestEndpointConfig((RestServerEndpointConfig) endpointConfig);
                break;
            case WAN:
                config.getAdvancedNetworkConfig().addWanEndpointConfig(endpointConfig);
                break;
            case MEMCACHE:
                config.getAdvancedNetworkConfig().setMemcacheEndpointConfig((ServerSocketEndpointConfig) endpointConfig);
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
            }
        }
    }

    protected void handleExecutor(Node node) throws Exception {
        ExecutorConfig executorConfig = new ExecutorConfig();
        handleViaReflection(node, config, executorConfig);
    }

    protected void handleDurableExecutor(Node node) throws Exception {
        DurableExecutorConfig durableExecutorConfig = new DurableExecutorConfig();
        handleViaReflection(node, config, durableExecutorConfig);
    }

    protected void handleScheduledExecutor(Node node) {
        ScheduledExecutorConfig scheduledExecutorConfig = new ScheduledExecutorConfig();
        scheduledExecutorConfig.setName(getTextContent(DomConfigHelper.getNamedItemNode(node, "name")));

        handleScheduledExecutorNode(node, scheduledExecutorConfig);
    }

    void handleScheduledExecutorNode(Node node, ScheduledExecutorConfig scheduledExecutorConfig) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("merge-policy", nodeName)) {
                scheduledExecutorConfig.setMergePolicyConfig(createMergePolicyConfig(child));
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
            }
        }

        config.addScheduledExecutorConfig(scheduledExecutorConfig);
    }

    protected void handleCardinalityEstimator(Node node) {
        CardinalityEstimatorConfig cardinalityEstimatorConfig = new CardinalityEstimatorConfig();
        cardinalityEstimatorConfig.setName(getTextContent(DomConfigHelper.getNamedItemNode(node, "name")));

        handleCardinalityEstimatorNode(node, cardinalityEstimatorConfig);
    }

    void handleCardinalityEstimatorNode(Node node, CardinalityEstimatorConfig cardinalityEstimatorConfig) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("merge-policy", nodeName)) {
                MergePolicyConfig mergePolicyConfig = createMergePolicyConfig(child);
                cardinalityEstimatorConfig.setMergePolicyConfig(mergePolicyConfig);
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
        PNCounterConfig pnCounterConfig = new PNCounterConfig();
        handleViaReflection(node, config, pnCounterConfig);
    }

    protected void handleFlakeIdGenerator(Node node) {
        String name = getAttribute(node, "name");
        FlakeIdGeneratorConfig generatorConfig = new FlakeIdGeneratorConfig(name);
        handleFlakeIdGeneratorNode(node, generatorConfig);
    }

    void handleFlakeIdGeneratorNode(Node node, FlakeIdGeneratorConfig generatorConfig) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            String value = getTextContent(child).trim();
            if (matches("prefetch-count", nodeName)) {
                generatorConfig.setPrefetchCount(Integer.parseInt(value));
            } else if (matches("prefetch-validity-millis", nodeName)) {
                generatorConfig.setPrefetchValidityMillis(Long.parseLong(value));
            } else if (matches("epoch-start", nodeName)) {
                generatorConfig.setEpochStart(Long.parseLong(value));
            } else if (matches("node-id-offset", nodeName)) {
                generatorConfig.setNodeIdOffset(Long.parseLong(value));
            } else if (matches("bits-sequence", nodeName)) {
                generatorConfig.setBitsSequence(Integer.parseInt(value));
            } else if (matches("bits-node-id", nodeName)) {
                generatorConfig.setBitsNodeId(Integer.parseInt(value));
            } else if (matches("allowed-future-millis", nodeName)) {
                generatorConfig.setAllowedFutureMillis(Long.parseLong(value));
            } else if (matches("statistics-enabled", nodeName)) {
                generatorConfig.setStatisticsEnabled(getBooleanValue(value));
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
                String value = att.getNodeValue();
                interfaces.setEnabled(getBooleanValue(value));
            }
        }
        handleInterfacesList(node, interfaces);
    }

    protected void handleInterfacesList(Node node, InterfacesConfig interfaces) {
        for (Node n : childElements(node)) {
            if (matches("interface", lowerCaseInternal(cleanNodeName(n)))) {
                String value = getTextContent(n).trim();
                interfaces.addInterface(value);
            }
        }
    }

    private void handleInterfaces(Node node, EndpointConfig endpointConfig) {
        NamedNodeMap attributes = node.getAttributes();
        InterfacesConfig interfaces = endpointConfig.getInterfaces();
        for (int a = 0; a < attributes.getLength(); a++) {
            Node att = attributes.item(a);
            if (matches("enabled", att.getNodeName())) {
                String value = att.getNodeValue();
                interfaces.setEnabled(getBooleanValue(value));
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
                invokeSetter(child, n, getTextContent(n).trim());
            }
        }
        attachChildConfig(parent, child);
    }

    private static boolean excludeNode(Node n, String... nodeExclusions) {
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

    private static void invokeSetter(Object target, Node node, String argument) {
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
            if (method.getName().equalsIgnoreCase(methodName)) {
                if (!requiresArg) {
                    return method;
                }
                Class<?>[] args = method.getParameterTypes();
                if (args.length != 1) {
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

    private static String toPropertyName(String element) {
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
                sb.append(Character.toUpperCase(c));
                upper = false;
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private static String handleRefProperty(String element) {
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
        Node att = DomConfigHelper.getNamedItemNode(node, "class");
        if (att != null) {
            discoveryConfig.setNodeFilterClass(getTextContent(att).trim());
        }
    }

    void handleDiscoveryStrategy(Node node, DiscoveryConfig discoveryConfig) {
        boolean enabled = false;
        String clazz = null;

        NamedNodeMap attributes = node.getAttributes();
        for (int a = 0; a < attributes.getLength(); a++) {
            Node att = attributes.item(a);
            String value = getTextContent(att).trim();
            if (matches("enabled", lowerCaseInternal(att.getNodeName()))) {
                enabled = getBooleanValue(value);
            } else if (matches("class", att.getNodeName())) {
                clazz = value;
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
            String value = getTextContent(att).trim();
            if (matches("enabled", att.getNodeName().toLowerCase())) {
                config.setEnabled(getBooleanValue(value));
            } else if (matches(att.getNodeName(), "connection-timeout-seconds")) {
                config.setProperty("connection-timeout-seconds", value);
            }
        }
        for (Node n : childElements(node)) {
            String key = cleanNodeName(n, !matches("eureka", n.getParentNode().getLocalName()));
            String value = getTextContent(n).trim();
            config.setProperty(key, value);
        }
    }

    private void handleMulticast(Node node, boolean advancedNetworkConfig) {
        JoinConfig join = joinConfig(advancedNetworkConfig);
        MulticastConfig multicastConfig = join.getMulticastConfig();
        NamedNodeMap attributes = node.getAttributes();
        for (int a = 0; a < attributes.getLength(); a++) {
            Node att = attributes.item(a);
            String value = getTextContent(att).trim();
            if (matches("enabled", lowerCaseInternal(att.getNodeName()))) {
                multicastConfig.setEnabled(getBooleanValue(value));
            } else if (matches("loopbackmodeenabled", lowerCaseInternal(att.getNodeName()))
                    || matches("loopback-mode-enabled", lowerCaseInternal(att.getNodeName()))) {
                multicastConfig.setLoopbackModeEnabled(getBooleanValue(value));
            }
        }
        for (Node n : childElements(node)) {
            String value = getTextContent(n).trim();
            if (matches("multicast-group", cleanNodeName(n))) {
                multicastConfig.setMulticastGroup(value);
            } else if (matches("multicast-port", cleanNodeName(n))) {
                multicastConfig.setMulticastPort(parseInt(value));
            } else if (matches("multicast-timeout-seconds", cleanNodeName(n))) {
                multicastConfig.setMulticastTimeoutSeconds(parseInt(value));
            } else if (matches("multicast-time-to-live-seconds", cleanNodeName(n))) {
                // we need this line for the time being to prevent not reading the multicast-time-to-live-seconds property
                // for more info see: https://github.com/hazelcast/hazelcast/issues/752
                multicastConfig.setMulticastTimeToLive(parseInt(value));
            } else if (matches("multicast-time-to-live", cleanNodeName(n))) {
                multicastConfig.setMulticastTimeToLive(parseInt(value));
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
            String value = getTextContent(att).trim();
            if (matches("enabled", lowerCaseInternal(att.getNodeName()))) {
                autoDetectionConfig.setEnabled(getBooleanValue(value));
            }
        }
    }

    protected void handleTrustedInterfaces(TrustedInterfacesConfigurable<?> tiConfig, Node n) {
        for (Node child : childElements(n)) {
            if (matches("interface", lowerCaseInternal(cleanNodeName(child)))) {
                tiConfig.addTrustedInterface(getTextContent(child).trim());
            }
        }
    }

    private void handleTcpIp(Node node, boolean advancedNetworkConfig) {
        NamedNodeMap attributes = node.getAttributes();
        JoinConfig join = joinConfig(advancedNetworkConfig);
        TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
        for (int a = 0; a < attributes.getLength(); a++) {
            Node att = attributes.item(a);
            String value = getTextContent(att).trim();
            if (matches(att.getNodeName(), "enabled")) {
                tcpIpConfig.setEnabled(getBooleanValue(value));
            } else if (matches(att.getNodeName(), "connection-timeout-seconds")) {
                tcpIpConfig.setConnectionTimeoutSeconds(getIntegerValue("connection-timeout-seconds", value));
            }
        }
        Set<String> memberTags = new HashSet<>(Arrays.asList("interface", "member", "members"));
        for (Node n : childElements(node)) {
            String value = getTextContent(n).trim();
            if (matches(cleanNodeName(n), "member-list")) {
                handleMemberList(n, advancedNetworkConfig);
            } else if (matches(cleanNodeName(n), "required-member")) {
                if (tcpIpConfig.getRequiredMember() != null) {
                    throw new InvalidConfigurationException("Duplicate required-member"
                            + " definition found in the configuration. ");
                }
                tcpIpConfig.setRequiredMember(value);
            } else if (memberTags.contains(cleanNodeName(n))) {
                tcpIpConfig.addMember(value);
            }
        }
    }

    protected void handleMemberList(Node node, boolean advancedNetworkConfig) {
        JoinConfig join = joinConfig(advancedNetworkConfig);
        TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if (matches("member", nodeName)) {
                String value = getTextContent(n).trim();
                tcpIpConfig.addMember(value);
            }
        }
    }

    protected void handlePort(Node node, Config config) {
        String portStr = getTextContent(node).trim();
        NetworkConfig networkConfig = config.getNetworkConfig();
        if (portStr.length() > 0) {
            networkConfig.setPort(parseInt(portStr));
        }
        NamedNodeMap attributes = node.getAttributes();
        for (int a = 0; a < attributes.getLength(); a++) {
            Node att = attributes.item(a);
            String value = getTextContent(att).trim();

            if (matches("auto-increment", att.getNodeName())) {
                networkConfig.setPortAutoIncrement(getBooleanValue(value));
            } else if (matches("port-count", att.getNodeName())) {
                int portCount = parseInt(value);
                networkConfig.setPortCount(portCount);
            }
        }
    }

    protected void handlePort(Node node, ServerSocketEndpointConfig endpointConfig) {
        String portStr = getTextContent(node).trim();
        if (portStr.length() > 0) {
            endpointConfig.setPort(parseInt(portStr));
        }
        handlePortAttributes(node, endpointConfig);
    }

    protected void handlePortAttributes(Node node, ServerSocketEndpointConfig endpointConfig) {
        NamedNodeMap attributes = node.getAttributes();
        for (int a = 0; a < attributes.getLength(); a++) {
            Node att = attributes.item(a);
            String value = getTextContent(att).trim();

            if (matches("auto-increment", att.getNodeName())) {
                endpointConfig.setPortAutoIncrement(getBooleanValue(value));
            } else if (matches("port-count", att.getNodeName())) {
                int portCount = parseInt(value);
                endpointConfig.setPortCount(portCount);
            }
        }
    }

    protected void handleOutboundPorts(Node child) {
        NetworkConfig networkConfig = config.getNetworkConfig();
        for (Node n : childElements(child)) {
            String nodeName = cleanNodeName(n);
            if (matches("ports", nodeName)) {
                String value = getTextContent(n);
                networkConfig.addOutboundPortDefinition(value);
            }
        }
    }

    protected void handleOutboundPorts(Node child, EndpointConfig endpointConfig) {
        for (Node n : childElements(child)) {
            String nodeName = cleanNodeName(n);
            if (matches("ports", nodeName)) {
                String value = getTextContent(n);
                endpointConfig.addOutboundPortDefinition(value);
            }
        }
    }

    protected void handleQueue(Node node) {
        Node attName = DomConfigHelper.getNamedItemNode(node, "name");
        String name = getTextContent(attName);
        QueueConfig qConfig = new QueueConfig();
        qConfig.setName(name);
        handleQueueNode(node, qConfig);
    }

    void handleQueueNode(Node node, final QueueConfig qConfig) {
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            String value = getTextContent(n).trim();
            if (matches("max-size", nodeName)) {
                qConfig.setMaxSize(getIntegerValue("max-size", value));
            } else if (matches("backup-count", nodeName)) {
                qConfig.setBackupCount(getIntegerValue("backup-count", value));
            } else if (matches("async-backup-count", nodeName)) {
                qConfig.setAsyncBackupCount(getIntegerValue("async-backup-count", value));
            } else if (matches("item-listeners", nodeName)) {
                handleItemListeners(n, itemListenerConfig -> {
                    qConfig.addItemListenerConfig(itemListenerConfig);
                    return null;
                });
            } else if (matches("statistics-enabled", nodeName)) {
                qConfig.setStatisticsEnabled(getBooleanValue(value));
            } else if (matches("queue-store", nodeName)) {
                QueueStoreConfig queueStoreConfig = createQueueStoreConfig(n);
                qConfig.setQueueStoreConfig(queueStoreConfig);
            } else if (matches("split-brain-protection-ref", nodeName)) {
                qConfig.setSplitBrainProtectionName(value);
            } else if (matches("empty-queue-ttl", nodeName)) {
                qConfig.setEmptyQueueTtl(getIntegerValue("empty-queue-ttl", value));
            } else if (matches("merge-policy", nodeName)) {
                MergePolicyConfig mergePolicyConfig = createMergePolicyConfig(n);
                qConfig.setMergePolicyConfig(mergePolicyConfig);
            }
        }
        config.addQueueConfig(qConfig);
    }

    protected void handleItemListeners(Node n, Function<ItemListenerConfig, Void> configAddFunction) {
        for (Node listenerNode : childElements(n)) {
            if (matches("item-listener", cleanNodeName(listenerNode))) {
                boolean incValue = getBooleanValue(getTextContent(
                  DomConfigHelper.getNamedItemNode(listenerNode, "include-value")));
                String listenerClass = getTextContent(listenerNode);
                configAddFunction.apply(new ItemListenerConfig(listenerClass, incValue));
            }
        }
    }

    protected void handleList(Node node) {
        Node attName = DomConfigHelper.getNamedItemNode(node, "name");
        String name = getTextContent(attName);
        ListConfig lConfig = new ListConfig();
        lConfig.setName(name);
        handleListNode(node, lConfig);
    }

    void handleListNode(Node node, final ListConfig lConfig) {
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            String value = getTextContent(n).trim();
            if (matches("max-size", nodeName)) {
                lConfig.setMaxSize(getIntegerValue("max-size", value));
            } else if (matches("backup-count", nodeName)) {
                lConfig.setBackupCount(getIntegerValue("backup-count", value));
            } else if (matches("async-backup-count", nodeName)) {
                lConfig.setAsyncBackupCount(getIntegerValue("async-backup-count", value));
            } else if (matches("item-listeners", nodeName)) {
                handleItemListeners(n, itemListenerConfig -> {
                    lConfig.addItemListenerConfig(itemListenerConfig);
                    return null;
                });
            } else if (matches("statistics-enabled", nodeName)) {
                lConfig.setStatisticsEnabled(getBooleanValue(value));
            } else if (matches("split-brain-protection-ref", nodeName)) {
                lConfig.setSplitBrainProtectionName(value);
            } else if (matches("merge-policy", nodeName)) {
                MergePolicyConfig mergePolicyConfig = createMergePolicyConfig(n);
                lConfig.setMergePolicyConfig(mergePolicyConfig);
            }

        }
        config.addListConfig(lConfig);
    }

    protected void handleSet(Node node) {
        Node attName = DomConfigHelper.getNamedItemNode(node, "name");
        String name = getTextContent(attName);
        SetConfig sConfig = new SetConfig();
        sConfig.setName(name);
        handleSetNode(node, sConfig);
    }

    void handleSetNode(Node node, final SetConfig sConfig) {
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            String value = getTextContent(n).trim();
            if (matches("max-size", nodeName)) {
                sConfig.setMaxSize(getIntegerValue("max-size", value));
            } else if (matches("backup-count", nodeName)) {
                sConfig.setBackupCount(getIntegerValue("backup-count", value));
            } else if (matches("async-backup-count", nodeName)) {
                sConfig.setAsyncBackupCount(getIntegerValue("async-backup-count", value));
            } else if (matches("item-listeners", nodeName)) {
                handleItemListeners(n, itemListenerConfig -> {
                    sConfig.addItemListenerConfig(itemListenerConfig);
                    return null;
                });
            } else if (matches("statistics-enabled", nodeName)) {
                sConfig.setStatisticsEnabled(getBooleanValue(value));
            } else if (matches("split-brain-protection-ref", nodeName)) {
                sConfig.setSplitBrainProtectionName(value);
            } else if (matches("merge-policy", nodeName)) {
                MergePolicyConfig mergePolicyConfig = createMergePolicyConfig(n);
                sConfig.setMergePolicyConfig(mergePolicyConfig);
            }
        }
        config.addSetConfig(sConfig);
    }

    protected void handleMultiMap(Node node) {
        Node attName = DomConfigHelper.getNamedItemNode(node, "name");
        String name = getTextContent(attName);
        MultiMapConfig multiMapConfig = new MultiMapConfig();
        multiMapConfig.setName(name);
        handleMultiMapNode(node, multiMapConfig);
    }

    void handleMultiMapNode(Node node, final MultiMapConfig multiMapConfig) {
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            String value = getTextContent(n).trim();
            if (matches("value-collection-type", nodeName)) {
                multiMapConfig.setValueCollectionType(value);
            } else if (matches("backup-count", nodeName)) {
                multiMapConfig.setBackupCount(getIntegerValue("backup-count"
                        , value));
            } else if (matches("async-backup-count", nodeName)) {
                multiMapConfig.setAsyncBackupCount(getIntegerValue("async-backup-count"
                        , value));
            } else if (matches("entry-listeners", nodeName)) {
                handleEntryListeners(n, entryListenerConfig -> {
                    multiMapConfig.addEntryListenerConfig(entryListenerConfig);
                    return null;
                });
            } else if (matches("statistics-enabled", nodeName)) {
                multiMapConfig.setStatisticsEnabled(getBooleanValue(value));
            } else if (matches("binary", nodeName)) {
                multiMapConfig.setBinary(getBooleanValue(value));
            } else if (matches("split-brain-protection-ref", nodeName)) {
                multiMapConfig.setSplitBrainProtectionName(value);
            } else if (matches("merge-policy", nodeName)) {
                MergePolicyConfig mergePolicyConfig = createMergePolicyConfig(n);
                multiMapConfig.setMergePolicyConfig(mergePolicyConfig);
            }
        }
        config.addMultiMapConfig(multiMapConfig);
    }

    protected void handleEntryListeners(Node n, Function<EntryListenerConfig, Void> configAddFunction) {
        for (Node listenerNode : childElements(n)) {
            if (matches("entry-listener", cleanNodeName(listenerNode))) {
                boolean incValue = getBooleanValue(getTextContent(
                  DomConfigHelper.getNamedItemNode(listenerNode, "include-value")));
                boolean local = getBooleanValue(getTextContent(
                  DomConfigHelper.getNamedItemNode(listenerNode, "local")));
                String listenerClass = getTextContent(listenerNode);
                configAddFunction.apply(new EntryListenerConfig(listenerClass, local, incValue));
            }
        }
    }

    protected void handleReplicatedMap(Node node) {
        Node attName = DomConfigHelper.getNamedItemNode(node, "name");
        String name = getTextContent(attName);
        final ReplicatedMapConfig replicatedMapConfig = new ReplicatedMapConfig();
        replicatedMapConfig.setName(name);
        handleReplicatedMapNode(node, replicatedMapConfig);
    }

    void handleReplicatedMapNode(Node node, final ReplicatedMapConfig replicatedMapConfig) {
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            String value = getTextContent(n).trim();
            if (matches("in-memory-format", nodeName)) {
                replicatedMapConfig.setInMemoryFormat(InMemoryFormat.valueOf(upperCaseInternal(value)));
            } else if (matches("async-fillup", nodeName)) {
                replicatedMapConfig.setAsyncFillup(getBooleanValue(value));
            } else if (matches("statistics-enabled", nodeName)) {
                replicatedMapConfig.setStatisticsEnabled(getBooleanValue(value));
            } else if (matches("entry-listeners", nodeName)) {
                handleEntryListeners(n, entryListenerConfig -> {
                    replicatedMapConfig.addEntryListenerConfig(entryListenerConfig);
                    return null;
                });
            } else if (matches("merge-policy", nodeName)) {
                MergePolicyConfig mergePolicyConfig = createMergePolicyConfig(n);
                replicatedMapConfig.setMergePolicyConfig(mergePolicyConfig);
            } else if (matches("split-brain-protection-ref", nodeName)) {
                replicatedMapConfig.setSplitBrainProtectionName(value);
            }
        }
        config.addReplicatedMapConfig(replicatedMapConfig);
    }

    protected void handleMap(Node parentNode) throws Exception {
        String name = getAttribute(parentNode, "name");
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName(name);
        handleMapNode(parentNode, mapConfig);
    }

    void handleMapNode(Node parentNode, final MapConfig mapConfig) throws Exception {
        for (Node node : childElements(parentNode)) {
            String nodeName = cleanNodeName(node);
            String value = getTextContent(node).trim();
            if (matches("backup-count", nodeName)) {
                mapConfig.setBackupCount(getIntegerValue("backup-count", value));
            } else if (matches("metadata-policy", nodeName)) {
                mapConfig.setMetadataPolicy(MetadataPolicy.valueOf(upperCaseInternal(value)));
            } else if (matches("in-memory-format", nodeName)) {
                mapConfig.setInMemoryFormat(InMemoryFormat.valueOf(upperCaseInternal(value)));
            } else if (matches("async-backup-count", nodeName)) {
                mapConfig.setAsyncBackupCount(getIntegerValue("async-backup-count", value));
            } else if (matches("eviction", nodeName)) {
                mapConfig.setEvictionConfig(getEvictionConfig(node, false, true));
            } else if (matches("time-to-live-seconds", nodeName)) {
                mapConfig.setTimeToLiveSeconds(getIntegerValue("time-to-live-seconds", value));
            } else if (matches("max-idle-seconds", nodeName)) {
                mapConfig.setMaxIdleSeconds(getIntegerValue("max-idle-seconds", value));
            } else if (matches("map-store", nodeName)) {
                MapStoreConfig mapStoreConfig = createMapStoreConfig(node);
                mapConfig.setMapStoreConfig(mapStoreConfig);
            } else if (matches("near-cache", nodeName)) {
                mapConfig.setNearCacheConfig(handleNearCacheConfig(node));
            } else if (matches("merge-policy", nodeName)) {
                MergePolicyConfig mergePolicyConfig = createMergePolicyConfig(node);
                mapConfig.setMergePolicyConfig(mergePolicyConfig);
            } else if (matches("merkle-tree", nodeName)) {
                MerkleTreeConfig merkleTreeConfig = new MerkleTreeConfig();
                handleViaReflection(node, mapConfig, merkleTreeConfig);
            } else if (matches("event-journal", nodeName)) {
                EventJournalConfig eventJournalConfig = new EventJournalConfig();
                handleViaReflection(node, mapConfig, eventJournalConfig);
            } else if (matches("hot-restart", nodeName)) {
                mapConfig.setHotRestartConfig(createHotRestartConfig(node));
            } else if (matches("read-backup-data", nodeName)) {
                mapConfig.setReadBackupData(getBooleanValue(value));
            } else if (matches("statistics-enabled", nodeName)) {
                mapConfig.setStatisticsEnabled(getBooleanValue(value));
            } else if (matches("cache-deserialized-values", nodeName)) {
                CacheDeserializedValues cacheDeserializedValues = CacheDeserializedValues.parseString(value);
                mapConfig.setCacheDeserializedValues(cacheDeserializedValues);
            } else if (matches("wan-replication-ref", nodeName)) {
                mapWanReplicationRefHandle(node, mapConfig);
            } else if (matches("indexes", nodeName)) {
                mapIndexesHandle(node, mapConfig);
            } else if (matches("attributes", nodeName)) {
                attributesHandle(node, mapConfig);
            } else if (matches("entry-listeners", nodeName)) {
                handleEntryListeners(node, entryListenerConfig -> {
                    mapConfig.addEntryListenerConfig(entryListenerConfig);
                    return null;
                });
            } else if (matches("partition-lost-listeners", nodeName)) {
                mapPartitionLostListenerHandle(node, mapConfig);
            } else if (matches("partition-strategy", nodeName)) {
                mapConfig.setPartitioningStrategyConfig(new PartitioningStrategyConfig(value));
            } else if (matches("split-brain-protection-ref", nodeName)) {
                mapConfig.setSplitBrainProtectionName(value);
            } else if (matches("query-caches", nodeName)) {
                mapQueryCacheHandler(node, mapConfig);
            }
        }
        config.addMapConfig(mapConfig);
    }

    private NearCacheConfig handleNearCacheConfig(Node node) {
        String name = getAttribute(node, "name");
        NearCacheConfig nearCacheConfig = new NearCacheConfig(name);
        Boolean serializeKeys = null;
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            String value = getTextContent(child).trim();
            if (matches("time-to-live-seconds", nodeName)) {
                nearCacheConfig.setTimeToLiveSeconds(Integer.parseInt(value));
            } else if (matches("max-idle-seconds", nodeName)) {
                nearCacheConfig.setMaxIdleSeconds(Integer.parseInt(value));
            } else if (matches("in-memory-format", nodeName)) {
                nearCacheConfig.setInMemoryFormat(InMemoryFormat.valueOf(upperCaseInternal(value)));
            } else if (matches("serialize-keys", nodeName)) {
                serializeKeys = Boolean.parseBoolean(value);
                nearCacheConfig.setSerializeKeys(serializeKeys);
            } else if (matches("invalidate-on-change", nodeName)) {
                nearCacheConfig.setInvalidateOnChange(Boolean.parseBoolean(value));
            } else if (matches("cache-local-entries", nodeName)) {
                nearCacheConfig.setCacheLocalEntries(Boolean.parseBoolean(value));
            } else if (matches("local-update-policy", nodeName)) {
                NearCacheConfig.LocalUpdatePolicy policy = NearCacheConfig.LocalUpdatePolicy.valueOf(value);
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

        Node attrEnabled = DomConfigHelper.getNamedItemNode(node, "enabled");
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

    protected void handleCache(Node node) throws Exception {
        String name = getAttribute(node, "name");
        CacheSimpleConfig cacheConfig = new CacheSimpleConfig();
        cacheConfig.setName(name);
        handleCacheNode(node, cacheConfig);
    }

    void handleCacheNode(Node node, CacheSimpleConfig cacheConfig) throws Exception {
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            String value = getTextContent(n).trim();
            if (matches("key-type", nodeName)) {
                cacheConfig.setKeyType(getAttribute(n, "class-name"));
            } else if (matches("value-type", nodeName)) {
                cacheConfig.setValueType(getAttribute(n, "class-name"));
            } else if (matches("statistics-enabled", nodeName)) {
                cacheConfig.setStatisticsEnabled(getBooleanValue(value));
            } else if (matches("management-enabled", nodeName)) {
                cacheConfig.setManagementEnabled(getBooleanValue(value));
            } else if (matches("read-through", nodeName)) {
                cacheConfig.setReadThrough(getBooleanValue(value));
            } else if (matches("write-through", nodeName)) {
                cacheConfig.setWriteThrough(getBooleanValue(value));
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
                cacheConfig.setInMemoryFormat(InMemoryFormat.valueOf(upperCaseInternal(value)));
            } else if (matches("backup-count", nodeName)) {
                cacheConfig.setBackupCount(getIntegerValue("backup-count", value));
            } else if (matches("async-backup-count", nodeName)) {
                cacheConfig.setAsyncBackupCount(getIntegerValue("async-backup-count", value));
            } else if (matches("wan-replication-ref", nodeName)) {
                cacheWanReplicationRefHandle(n, cacheConfig);
            } else if (matches("eviction", nodeName)) {
                cacheConfig.setEvictionConfig(getEvictionConfig(n, false, false));
            } else if (matches("split-brain-protection-ref", nodeName)) {
                cacheConfig.setSplitBrainProtectionName(value);
            } else if (matches("partition-lost-listeners", nodeName)) {
                cachePartitionLostListenerHandle(n, cacheConfig);
            } else if (matches("merge-policy", nodeName)) {
                cacheConfig.setMergePolicyConfig(createMergePolicyConfig(n));
            } else if (matches("event-journal", nodeName)) {
                EventJournalConfig eventJournalConfig = new EventJournalConfig();
                handleViaReflection(n, cacheConfig, eventJournalConfig);
            } else if (matches("hot-restart", nodeName)) {
                cacheConfig.setHotRestartConfig(createHotRestartConfig(n));
            } else if (matches("disable-per-entry-invalidation-events", nodeName)) {
                cacheConfig.setDisablePerEntryInvalidationEvents(getBooleanValue(value));
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

        Node size = DomConfigHelper.getNamedItemNode(node, "size");
        Node maxSizePolicy = DomConfigHelper.getNamedItemNode(node, "max-size-policy");
        Node evictionPolicy = DomConfigHelper.getNamedItemNode(node, "eviction-policy");
        Node comparatorClassName = DomConfigHelper.getNamedItemNode(node, "comparator-class-name");

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
          DomConfigHelper.getNamedItemNode(listenerNode, "old-value-required"))));
        listenerConfig.setSynchronous(getBooleanValue(getTextContent(
          DomConfigHelper.getNamedItemNode(listenerNode, "synchronous"))));
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
                IndexConfig indexConfig = IndexUtils.getIndexConfigFromXml(indexNode, domLevel3);

                mapConfig.addIndexConfig(indexConfig);
            }
        }
    }

    protected void queryCacheIndexesHandle(Node n, QueryCacheConfig queryCacheConfig) {
        for (Node indexNode : childElements(n)) {
            if (matches("index", cleanNodeName(indexNode))) {
                IndexConfig indexConfig = IndexUtils.getIndexConfigFromXml(indexNode, domLevel3);

                queryCacheConfig.addIndexConfig(indexConfig);
            }
        }
    }

    protected void attributesHandle(Node n, MapConfig mapConfig) {
        for (Node extractorNode : childElements(n)) {
            if (matches("attribute", cleanNodeName(extractorNode))) {
                String extractor = getTextContent(
                  DomConfigHelper.getNamedItemNode(extractorNode, "extractor-class-name"));
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
                  DomConfigHelper.getNamedItemNode(queryCacheNode, "name"));
                QueryCacheConfig queryCacheConfig = new QueryCacheConfig(cacheName);
                handleMapQueryCacheNode(mapConfig, queryCacheNode, queryCacheConfig);
            }
        }
    }

    void handleMapQueryCacheNode(MapConfig mapConfig, Node queryCacheNode, final QueryCacheConfig queryCacheConfig) {
        for (Node childNode : childElements(queryCacheNode)) {
            String nodeName = cleanNodeName(childNode);
            if (matches("entry-listeners", nodeName)) {
                handleEntryListeners(childNode, entryListenerConfig -> {
                    queryCacheConfig.addEntryListenerConfig(entryListenerConfig);
                    return null;
                });
            } else {
                String textContent = getTextContent(childNode);
                if (matches("include-value", nodeName)) {
                    boolean includeValue = getBooleanValue(textContent);
                    queryCacheConfig.setIncludeValue(includeValue);
                } else if (matches("batch-size", nodeName)) {
                    int batchSize = getIntegerValue("batch-size", textContent.trim());
                    queryCacheConfig.setBatchSize(batchSize);
                } else if (matches("buffer-size", nodeName)) {
                    int bufferSize = getIntegerValue("buffer-size", textContent.trim());
                    queryCacheConfig.setBufferSize(bufferSize);
                } else if (matches("delay-seconds", nodeName)) {
                    int delaySeconds = getIntegerValue("delay-seconds", textContent.trim());
                    queryCacheConfig.setDelaySeconds(delaySeconds);
                } else if (matches("in-memory-format", nodeName)) {
                    String value = textContent.trim();
                    queryCacheConfig.setInMemoryFormat(InMemoryFormat.valueOf(upperCaseInternal(value)));
                } else if (matches("coalesce", nodeName)) {
                    boolean coalesce = getBooleanValue(textContent);
                    queryCacheConfig.setCoalesce(coalesce);
                } else if (matches("populate", nodeName)) {
                    boolean populate = getBooleanValue(textContent);
                    queryCacheConfig.setPopulate(populate);
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
        String predicateType = getTextContent(DomConfigHelper.getNamedItemNode(childNode, "type"));
        String textContent = getTextContent(childNode);
        PredicateConfig predicateConfig = new PredicateConfig();
        if (matches("class-name", predicateType)) {
            predicateConfig.setClassName(textContent);
        } else if (matches("sql", predicateType)) {
            predicateConfig.setSql(textContent);
        }
        queryCacheConfig.setPredicateConfig(predicateConfig);
    }

    private MapStoreConfig createMapStoreConfig(Node node) {
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        NamedNodeMap attributes = node.getAttributes();
        for (int a = 0; a < attributes.getLength(); a++) {
            Node att = attributes.item(a);
            String value = getTextContent(att).trim();
            if (matches("enabled", att.getNodeName())) {
                mapStoreConfig.setEnabled(getBooleanValue(value));
            } else if (matches("initial-mode", att.getNodeName())) {
                MapStoreConfig.InitialLoadMode mode = MapStoreConfig.InitialLoadMode
                        .valueOf(upperCaseInternal(getTextContent(att)));
                mapStoreConfig.setInitialLoadMode(mode);
            }
        }
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if (matches("class-name", nodeName)) {
                mapStoreConfig.setClassName(getTextContent(n).trim());
            } else if (matches("factory-class-name", nodeName)) {
                mapStoreConfig.setFactoryClassName(getTextContent(n).trim());
            } else if (matches("write-delay-seconds", nodeName)) {
                mapStoreConfig.setWriteDelaySeconds(getIntegerValue("write-delay-seconds", getTextContent(n).trim()
                ));
            } else if (matches("write-batch-size", nodeName)) {
                mapStoreConfig.setWriteBatchSize(getIntegerValue("write-batch-size", getTextContent(n).trim()
                ));
            } else if (matches("write-coalescing", nodeName)) {
                String writeCoalescing = getTextContent(n).trim();
                if (isNullOrEmpty(writeCoalescing)) {
                    mapStoreConfig.setWriteCoalescing(MapStoreConfig.DEFAULT_WRITE_COALESCING);
                } else {
                    mapStoreConfig.setWriteCoalescing(getBooleanValue(writeCoalescing));
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
            String value = getTextContent(att).trim();
            if (matches(att.getNodeName(), "enabled")) {
                config.setEnabled(getBooleanValue(value));
            }
        }
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if (matches("class-name", nodeName)) {
                config.setClassName(getTextContent(n).trim());
            } else if (matches("factory-class-name", nodeName)) {
                config.setFactoryClassName(getTextContent(n).trim());
            } else if (matches("properties", nodeName)) {
                fillProperties(n, config.getProperties());
            }

        }
        return config;
    }

    protected MergePolicyConfig createMergePolicyConfig(Node node) {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig();
        String policyString = getTextContent(node).trim();
        mergePolicyConfig.setPolicy(policyString);
        final String att = getAttribute(node, "batch-size");
        if (att != null) {
            mergePolicyConfig.setBatchSize(getIntegerValue("batch-size", att));
        }
        return mergePolicyConfig;
    }

    private QueueStoreConfig createQueueStoreConfig(Node node) {
        QueueStoreConfig queueStoreConfig = new QueueStoreConfig();
        NamedNodeMap attributes = node.getAttributes();
        for (int a = 0; a < attributes.getLength(); a++) {
            Node att = attributes.item(a);
            String value = getTextContent(att).trim();
            if (matches(att.getNodeName(), "enabled")) {
                queueStoreConfig.setEnabled(getBooleanValue(value));
            }
        }
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if (matches("class-name", nodeName)) {
                queueStoreConfig.setClassName(getTextContent(n).trim());
            } else if (matches("factory-class-name", nodeName)) {
                queueStoreConfig.setFactoryClassName(getTextContent(n).trim());
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

        Node enabledNode = DomConfigHelper.getNamedItemNode(node, "enabled");
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

            Node enabledNode = DomConfigHelper.getNamedItemNode(child, "enabled");
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
        Node attName = DomConfigHelper.getNamedItemNode(node, "name");
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
            }
        }
        config.addTopicConfig(tConfig);
    }

    protected void handleReliableTopic(Node node) {
        Node attName = DomConfigHelper.getNamedItemNode(node, "name");
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
        Node attName = DomConfigHelper.getNamedItemNode(node, "name");
        String name = getTextContent(attName);
        RingbufferConfig rbConfig = new RingbufferConfig(name);
        handleRingBufferNode(node, rbConfig);
    }

    void handleRingBufferNode(Node node, RingbufferConfig rbConfig) {
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            String value = getTextContent(n).trim();
            if (matches("capacity", nodeName)) {
                int capacity = getIntegerValue("capacity", value);
                rbConfig.setCapacity(capacity);
            } else if (matches("backup-count", nodeName)) {
                int backupCount = getIntegerValue("backup-count", value);
                rbConfig.setBackupCount(backupCount);
            } else if (matches("async-backup-count", nodeName)) {
                int asyncBackupCount = getIntegerValue("async-backup-count", value);
                rbConfig.setAsyncBackupCount(asyncBackupCount);
            } else if (matches("time-to-live-seconds", nodeName)) {
                int timeToLiveSeconds = getIntegerValue("time-to-live-seconds", value);
                rbConfig.setTimeToLiveSeconds(timeToLiveSeconds);
            } else if (matches("in-memory-format", nodeName)) {
                InMemoryFormat inMemoryFormat = InMemoryFormat.valueOf(upperCaseInternal(value));
                rbConfig.setInMemoryFormat(inMemoryFormat);
            } else if (matches("ringbuffer-store", nodeName)) {
                RingbufferStoreConfig ringbufferStoreConfig = createRingbufferStoreConfig(n);
                rbConfig.setRingbufferStoreConfig(ringbufferStoreConfig);
            } else if (matches("split-brain-protection-ref", nodeName)) {
                rbConfig.setSplitBrainProtectionName(value);
            } else if (matches("merge-policy", nodeName)) {
                MergePolicyConfig mergePolicyConfig = createMergePolicyConfig(n);
                rbConfig.setMergePolicyConfig(mergePolicyConfig);
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
        Node enabledNode = DomConfigHelper.getNamedItemNode(node, "enabled");
        boolean enabled = enabledNode != null && getBooleanValue(getTextContent(enabledNode));
        config.getPartitionGroupConfig().setEnabled(enabled);
        Node groupTypeNode = DomConfigHelper.getNamedItemNode(node, "group-type");
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
                String value = getTextContent(child);
                memberGroupConfig.addInterface(value);
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

        Node scriptingEnabledNode = DomConfigHelper.getNamedItemNode(node, "scripting-enabled");
        if (scriptingEnabledNode != null) {
            managementCenterConfig.setScriptingEnabled(getBooleanValue(getTextContent(scriptingEnabledNode)));
        }

        for (Node n : childElements(node)) {
            String value = getTextContent(n).trim();
            if (matches("trusted-interfaces", cleanNodeName(n))) {
                handleTrustedInterfaces(managementCenterConfig, n);
            }
        }

    }

    private void handleSecurity(Node node) {
        Node enabledNode = DomConfigHelper.getNamedItemNode(node, "enabled");
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
            Node classNameNode = DomConfigHelper.getNamedItemNode(child, "class-name");
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
            String attributeName = getTextContent(DomConfigHelper.getNamedItemNode(n, "name"));
            handleMemberAttributesNode(n, attributeName, value);
        }
    }

    void handleMemberAttributesNode(Node n, String attributeName, String value) {
        config.getMemberAttributeConfig().setAttribute(attributeName, value);
    }

    private void handlePermissionPolicy(Node node) {
        Node classNameNode = DomConfigHelper.getNamedItemNode(node, "class-name");
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
        Node nameNode = DomConfigHelper.getNamedItemNode(node, "name");
        String name = nameNode != null ? getTextContent(nameNode) : "*";
        Node principalNode = DomConfigHelper.getNamedItemNode(node, "principal");
        String principal = principalNode != null ? getTextContent(principalNode) : "*";
        PermissionConfig permConfig = new PermissionConfig(type, name, principal);
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
                permConfig.addEndpoint(getTextContent(child).trim());
            }
        }
    }

    void handleSecurityPermissionActions(Node node, PermissionConfig permConfig) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("action", nodeName)) {
                permConfig.addAction(getTextContent(child).trim());
            }
        }
    }

    private void handleMemcacheProtocol(Node node) {
        MemcacheProtocolConfig memcacheProtocolConfig = new MemcacheProtocolConfig();
        config.getNetworkConfig().setMemcacheProtocolConfig(memcacheProtocolConfig);
        boolean enabled = getBooleanValue(getAttribute(node, "enabled"));
        memcacheProtocolConfig.setEnabled(enabled);
    }

    private void handleRestApi(Node node) {
        RestApiConfig restApiConfig = new RestApiConfig();
        config.getNetworkConfig().setRestApiConfig(restApiConfig);
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
        RestEndpointGroup endpointGroup;
        try {
            endpointGroup = RestEndpointGroup.valueOf(name);
        } catch (IllegalArgumentException e) {
            throw new InvalidConfigurationException("Wrong name attribute value was provided in endpoint-group element: " + name
                    + "\nAllowed values: " + Arrays.toString(RestEndpointGroup.values()));
        }
        RestApiConfig restApiConfig = config.getNetworkConfig().getRestApiConfig();
        if (enabled) {
            restApiConfig.enableGroups(endpointGroup);
        } else {
            restApiConfig.disableGroups(endpointGroup);
        }
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
            } else {
                String value = getTextContent(child).trim();
                if (matches("cp-member-count", nodeName)) {
                    cpSubsystemConfig.setCPMemberCount(Integer.parseInt(value));
                } else if (matches("group-size", nodeName)) {
                    cpSubsystemConfig.setGroupSize(Integer.parseInt(value));
                } else if (matches("session-time-to-live-seconds", nodeName)) {
                    cpSubsystemConfig.setSessionTimeToLiveSeconds(Integer.parseInt(value));
                } else if (matches("session-heartbeat-interval-seconds", nodeName)) {
                    cpSubsystemConfig.setSessionHeartbeatIntervalSeconds(Integer.parseInt(value));
                } else if (matches("missing-cp-member-auto-removal-seconds", nodeName)) {
                    cpSubsystemConfig.setMissingCPMemberAutoRemovalSeconds(Integer.parseInt(value));
                } else if (matches("fail-on-indeterminate-operation-state", nodeName)) {
                    cpSubsystemConfig.setFailOnIndeterminateOperationState(Boolean.parseBoolean(value));
                } else if (matches("persistence-enabled", nodeName)) {
                    cpSubsystemConfig.setPersistenceEnabled(Boolean.parseBoolean(value));
                } else if (matches("base-dir", nodeName)) {
                    cpSubsystemConfig.setBaseDir(new File(value).getAbsoluteFile());
                } else if (matches("data-load-timeout-seconds", nodeName)) {
                    cpSubsystemConfig.setDataLoadTimeoutSeconds(Integer.parseInt(value));
                }
            }
        }
    }

    private void handleRaftAlgorithm(RaftAlgorithmConfig raftAlgorithmConfig, Node node) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            String value = getTextContent(child).trim();
            if (matches("leader-election-timeout-in-millis", nodeName)) {
                raftAlgorithmConfig.setLeaderElectionTimeoutInMillis(Long.parseLong(value));
            } else if (matches("leader-heartbeat-period-in-millis", nodeName)) {
                raftAlgorithmConfig.setLeaderHeartbeatPeriodInMillis(Long.parseLong(value));
            } else if (matches("max-missed-leader-heartbeat-count", nodeName)) {
                raftAlgorithmConfig.setMaxMissedLeaderHeartbeatCount(Integer.parseInt(value));
            } else if (matches("append-request-max-entry-count", nodeName)) {
                raftAlgorithmConfig.setAppendRequestMaxEntryCount(Integer.parseInt(value));
            } else if (matches("commit-index-advance-count-to-snapshot", nodeName)) {
                raftAlgorithmConfig.setCommitIndexAdvanceCountToSnapshot(Integer.parseInt(value));
            } else if (matches("uncommitted-entry-count-to-reject-new-appends", nodeName)) {
                raftAlgorithmConfig.setUncommittedEntryCountToRejectNewAppends(Integer.parseInt(value));
            } else if (matches("append-request-backoff-timeout-in-millis", nodeName)) {
                raftAlgorithmConfig.setAppendRequestBackoffTimeoutInMillis(Long.parseLong(value));
            }
        }
    }

    void handleSemaphores(CPSubsystemConfig cpSubsystemConfig, Node node) {
        for (Node child : childElements(node)) {
            SemaphoreConfig semaphoreConfig = new SemaphoreConfig();
            for (Node subChild : childElements(child)) {
                String nodeName = cleanNodeName(subChild);
                String value = getTextContent(subChild).trim();
                if (matches("name", nodeName)) {
                    semaphoreConfig.setName(value);
                } else if (matches("jdk-compatible", nodeName)) {
                    semaphoreConfig.setJDKCompatible(Boolean.parseBoolean(value));
                } else if (matches("initial-permits", nodeName)) {
                    semaphoreConfig.setInitialPermits(Integer.parseInt(value));
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
                String value = getTextContent(subChild).trim();
                if (matches("name", nodeName)) {
                    lockConfig.setName(value);
                } else if (matches("lock-acquire-limit", nodeName)) {
                    lockConfig.setLockAcquireLimit(Integer.parseInt(value));
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
            String value = getTextContent(child).trim();
            if (matches("management-center", nodeName)) {
                handleMetricsManagementCenter(child);
            } else if (matches("jmx", nodeName)) {
                handleMetricsJmx(child);
            } else if (matches("collection-frequency-seconds", nodeName)) {
                metricsConfig.setCollectionFrequencySeconds(Integer.parseInt(value));
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
                String value = getTextContent(child).trim();
                if (matches("retention-seconds", nodeName)) {
                    managementCenterConfig.setRetentionSeconds(Integer.parseInt(value));
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

    private void handleSql(Node node) {
        SqlConfig sqlConfig = config.getSqlConfig();

        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            String value = getTextContent(child).trim();
            if (matches("executor-pool-size", nodeName)) {
                sqlConfig.setExecutorPoolSize(Integer.parseInt(value));
            } else if (matches("operation-pool-size", nodeName)) {
                sqlConfig.setOperationPoolSize(Integer.parseInt(value));
            } else if (matches("statement-timeout-millis", nodeName)) {
                sqlConfig.setStatementTimeoutMillis(Long.parseLong(value));
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
            } else if (matches("ldap", nodeName)) {
                krbCfg.setLdapAuthenticationConfig(createLdapAuthentication(child));
            }
        }
        realmConfig.setKerberosAuthenticationConfig(krbCfg);
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

    private void fillClusterLoginConfig(AbstractClusterLoginConfig<?> config, Node node) {
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
