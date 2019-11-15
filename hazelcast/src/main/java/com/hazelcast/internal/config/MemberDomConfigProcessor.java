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

package com.hazelcast.internal.config;

import com.hazelcast.config.AliasedDiscoveryConfig;
import com.hazelcast.config.AttributeConfig;
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
import com.hazelcast.config.CustomWanPublisherConfig;
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
import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.config.MCMutualAuthConfig;
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
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.config.ServicesConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.config.SplitBrainProtectionConfigBuilder;
import com.hazelcast.config.SplitBrainProtectionListenerConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.config.UserCodeDeploymentConfig;
import com.hazelcast.config.VaultSecureStoreConfig;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanBatchReplicationPublisherConfig;
import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.config.WanQueueFullBehavior;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.config.WanSyncConfig;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.config.cp.FencedLockConfig;
import com.hazelcast.config.cp.RaftAlgorithmConfig;
import com.hazelcast.config.cp.SemaphoreConfig;
import com.hazelcast.config.security.JaasAuthenticationConfig;
import com.hazelcast.config.security.LdapAuthenticationConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.config.security.TlsAuthenticationConfig;
import com.hazelcast.config.security.TokenEncoding;
import com.hazelcast.config.security.TokenIdentityConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.services.ServiceConfigurationParser;
import com.hazelcast.internal.util.ExceptionUtil;
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
import java.util.List;
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
import static com.hazelcast.internal.config.ConfigSections.SERVICES;
import static com.hazelcast.internal.config.ConfigSections.SET;
import static com.hazelcast.internal.config.ConfigSections.SPLIT_BRAIN_PROTECTION;
import static com.hazelcast.internal.config.ConfigSections.TOPIC;
import static com.hazelcast.internal.config.ConfigSections.USER_CODE_DEPLOYMENT;
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

        if (occurrenceSet.contains("network") && occurrenceSet.contains("advanced-network")
                && config.getAdvancedNetworkConfig().isEnabled()) {
            throw new InvalidConfigurationException("Ambiguous configuration: cannot include both <network> and "
                    + "an enabled <advanced-network> element. Configure network using one of <network> or "
                    + "<advanced-network enabled=\"true\">.");
        }
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
        } else if (SERVICES.isEqual(nodeName)) {
            handleServices(node);
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
        } else if (METRICS.isEqual(nodeName)) {
            handleMetrics(node);
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
        Node attrEnabled = dcRoot.getAttributes().getNamedItem("enabled");
        boolean enabled = getBooleanValue(getTextContent(attrEnabled));
        dcConfig.setEnabled(enabled);

        String classCacheModeName = "class-cache-mode";
        String providerModeName = "provider-mode";
        String blacklistPrefixesName = "blacklist-prefixes";
        String whitelistPrefixesName = "whitelist-prefixes";
        String providerFilterName = "provider-filter";

        for (Node n : childElements(dcRoot)) {
            String name = cleanNodeName(n);
            if (classCacheModeName.equals(name)) {
                String value = getTextContent(n);
                UserCodeDeploymentConfig.ClassCacheMode classCacheMode = UserCodeDeploymentConfig.ClassCacheMode.valueOf(value);
                dcConfig.setClassCacheMode(classCacheMode);
            } else if (providerModeName.equals(name)) {
                String value = getTextContent(n);
                UserCodeDeploymentConfig.ProviderMode providerMode = UserCodeDeploymentConfig.ProviderMode.valueOf(value);
                dcConfig.setProviderMode(providerMode);
            } else if (blacklistPrefixesName.equals(name)) {
                String value = getTextContent(n);
                dcConfig.setBlacklistedPrefixes(value);
            } else if (whitelistPrefixesName.equals(name)) {
                String value = getTextContent(n);
                dcConfig.setWhitelistedPrefixes(value);
            } else if (providerFilterName.equals(name)) {
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
        String clusterDataRecoveryPolicyName = "cluster-data-recovery-policy";
        String autoRemoveStaleDataName = "auto-remove-stale-data";

        for (Node n : childElements(hrRoot)) {
            String name = cleanNodeName(n);
            if ("encryption-at-rest".equals(name)) {
                handleEncryptionAtRest(n, hrConfig);
            } else {
                String value = getTextContent(n);
                if ("base-dir".equals(name)) {
                    hrConfig.setBaseDir(new File(value).getAbsoluteFile());
                } else if ("backup-dir".equals(name)) {
                    hrConfig.setBackupDir(new File(value).getAbsoluteFile());
                } else if (parallelismName.equals(name)) {
                    hrConfig.setParallelism(getIntegerValue(parallelismName, value));
                } else if (validationTimeoutName.equals(name)) {
                    hrConfig.setValidationTimeoutSeconds(getIntegerValue(validationTimeoutName, value));
                } else if (dataLoadTimeoutName.equals(name)) {
                    hrConfig.setDataLoadTimeoutSeconds(getIntegerValue(dataLoadTimeoutName, value));
                } else if (clusterDataRecoveryPolicyName.equals(name)) {
                    hrConfig.setClusterDataRecoveryPolicy(HotRestartClusterDataRecoveryPolicy.valueOf(upperCaseInternal(value)));
                } else if (autoRemoveStaleDataName.equals(name)) {
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
            if ("keystore".equals(name)) {
                secureStoreConfig = handleJavaKeyStore(n);
            } else if ("vault".equals(name)) {
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
            if ("path".equals(name)) {
                path = new File(value).getAbsoluteFile();
            } else if ("type".equals(name)) {
                type = value;
            } else if ("password".equals(name)) {
                password = value;
            } else if ("current-key-alias".equals(name)) {
                currentKeyAlias = value;
            } else if ("polling-interval".equals(name)) {
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
            if ("address".equals(name)) {
                address = value;
            } else if ("secret-path".equals(name)) {
                secretPath = value;
            } else if ("token".equals(name)) {
                token = value;
            } else if ("ssl".equals(name)) {
                sslConfig = parseSslConfig(n);
            } else if ("polling-interval".equals(name)) {
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
            if (replicationPeriodMillisName.equals(name)) {
                replicationConfig.setReplicationPeriodMillis(
                        getIntegerValue(replicationPeriodMillisName, getTextContent(n)));
            } else if (maxConcurrentReplicationTargetsName.equals(name)) {
                replicationConfig.setMaxConcurrentReplicationTargets(
                        getIntegerValue(maxConcurrentReplicationTargetsName, getTextContent(n)));
            }
        }
        this.config.setCRDTReplicationConfig(replicationConfig);
    }

    private void handleLiteMember(Node node) {
        Node attrEnabled = node.getAttributes().getNamedItem("enabled");
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
        Node attrEnabled = node.getAttributes().getNamedItem("enabled");
        boolean enabled = attrEnabled != null && getBooleanValue(getTextContent(attrEnabled));
        // probabilistic-split-brain-protection and recently-active-split-brain-protection
        // configs are constructed via SplitBrainProtectionConfigBuilder
        SplitBrainProtectionConfigBuilder splitBrainProtectionConfigBuilder = null;
        splitBrainProtectionConfig.setEnabled(enabled);
        for (Node n : childElements(node)) {
            String value = getTextContent(n).trim();
            String nodeName = cleanNodeName(n);
            if ("minimum-cluster-size".equals(nodeName)) {
                splitBrainProtectionConfig.setMinimumClusterSize(getIntegerValue("minimum-cluster-size", value));
            } else if ("listeners".equals(nodeName)) {
                handleSplitBrainProtectionListeners(splitBrainProtectionConfig, n);
            } else if ("protect-on".equals(nodeName)) {
                splitBrainProtectionConfig.setProtectOn(SplitBrainProtectionOn.valueOf(upperCaseInternal(value)));
            } else if ("function-class-name".equals(nodeName)) {
                splitBrainProtectionConfig.setFunctionClassName(value);
            } else if ("recently-active-split-brain-protection".equals(nodeName)) {
                splitBrainProtectionConfigBuilder =
                        handleRecentlyActiveSplitBrainProtection(name, n, splitBrainProtectionConfig.getMinimumClusterSize());
            } else if ("probabilistic-split-brain-protection".equals(nodeName)) {
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
            if ("listener".equals(cleanNodeName(listenerNode))) {
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

    private void handleServices(Node node) {
        Node attDefaults = node.getAttributes().getNamedItem("enable-defaults");
        boolean enableDefaults = attDefaults == null || getBooleanValue(getTextContent(attDefaults));
        ServicesConfig servicesConfig = config.getServicesConfig();
        servicesConfig.setEnableDefaults(enableDefaults);

        handleServiceNodes(node, servicesConfig);
    }

    protected void handleServiceNodes(Node node, ServicesConfig servicesConfig) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if ("service".equals(nodeName)) {
                ServiceConfig serviceConfig = new ServiceConfig();
                String enabledValue = getAttribute(child, "enabled");
                boolean enabled = getBooleanValue(enabledValue);
                serviceConfig.setEnabled(enabled);

                for (Node n : childElements(child)) {
                    handleServiceNode(n, serviceConfig);
                }
                servicesConfig.addServiceConfig(serviceConfig);
            }
        }
    }

    protected void handleServiceNode(Node n, ServiceConfig serviceConfig) {
        String value = cleanNodeName(n);
        if ("name".equals(value)) {
            String name = getTextContent(n);
            serviceConfig.setName(name);
        } else if ("class-name".equals(value)) {
            String className = getTextContent(n);
            serviceConfig.setClassName(className);
        } else if ("properties".equals(value)) {
            fillProperties(n, serviceConfig.getProperties());
        } else if ("configuration".equals(value)) {
            Node parserNode = n.getAttributes().getNamedItem("parser");
            String parserClass = getTextContent(parserNode);
            if (parserNode == null || parserClass == null) {
                throw new InvalidConfigurationException("Parser is required!");
            }
            try {
                ServiceConfigurationParser parser = ClassLoaderUtil.newInstance(config.getClassLoader(), parserClass);
                Object obj = parser.parse((Element) n);
                serviceConfig.setConfigObject(obj);
            } catch (Exception e) {
                ExceptionUtil.sneakyThrow(e);
            }
        }
    }

    protected void handleWanReplication(Node node) {
        Node attName = node.getAttributes().getNamedItem("name");
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
        if ("batch-publisher".equals(nodeName)) {
            WanBatchReplicationPublisherConfig config = new WanBatchReplicationPublisherConfig();
            handleBatchWanPublisherNode(wanReplicationConfig, nodeTarget, config);
        } else if ("custom-publisher".equals(nodeName)) {
            CustomWanPublisherConfig config = new CustomWanPublisherConfig();
            handleCustomWanPublisherNode(wanReplicationConfig, nodeTarget, config);
        } else if ("consumer".equals(nodeName)) {
            handleWanConsumerNode(wanReplicationConfig, nodeTarget);
        }
    }

    void handleCustomWanPublisherNode(WanReplicationConfig wanReplicationConfig,
                                      Node nodeTarget,
                                      CustomWanPublisherConfig config) {
        for (Node targetChild : childElements(nodeTarget)) {
            String targetChildName = cleanNodeName(targetChild);
            if ("properties".equals(targetChildName)) {
                fillProperties(targetChild, config.getProperties());
            } else if ("publisher-id".equals(targetChildName)) {
                config.setPublisherId(getTextContent(targetChild));
            } else if ("class-name".equals(targetChildName)) {
                config.setClassName(getTextContent(targetChild));
            }
        }
        wanReplicationConfig.addCustomPublisherConfig(config);
    }

    void handleBatchWanPublisherNode(WanReplicationConfig wanReplicationConfig, Node nodeTarget,
                                     WanBatchReplicationPublisherConfig config) {
        for (Node targetChild : childElements(nodeTarget)) {
            String targetChildName = cleanNodeName(targetChild);
            if ("cluster-name".equals(targetChildName)) {
                config.setClusterName(getTextContent(targetChild));
            } else if ("publisher-id".equals(targetChildName)) {
                config.setPublisherId(getTextContent(targetChild));
            } else if ("target-endpoints".equals(targetChildName)) {
                config.setTargetEndpoints(getTextContent(targetChild));
            } else if ("snapshot-enabled".equals(targetChildName)) {
                config.setSnapshotEnabled(getBooleanValue(getTextContent(targetChild)));
            } else if ("initial-publisher-state".equals(targetChildName)) {
                config.setInitialPublisherState(WanPublisherState.valueOf(upperCaseInternal(getTextContent(targetChild))));
            } else if ("queue-capacity".equals(targetChildName)) {
                config.setQueueCapacity(getIntegerValue("queue-capacity", getTextContent(targetChild)));
            } else if ("batch-size".equals(targetChildName)) {
                config.setBatchSize(getIntegerValue("batch-size", getTextContent(targetChild)));
            } else if ("batch-max-delay-millis".equals(targetChildName)) {
                config.setBatchMaxDelayMillis(getIntegerValue("batch-max-delay-millis", getTextContent(targetChild)));
            } else if ("response-timeout-millis".equals(targetChildName)) {
                config.setResponseTimeoutMillis(getIntegerValue("response-timeout-millis", getTextContent(targetChild)));
            } else if ("queue-full-behavior".equals(targetChildName)) {
                config.setQueueFullBehavior(WanQueueFullBehavior.valueOf(upperCaseInternal(getTextContent(targetChild))));
            } else if ("acknowledge-type".equals(targetChildName)) {
                config.setAcknowledgeType(WanAcknowledgeType.valueOf(upperCaseInternal(getTextContent(targetChild))));
            } else if ("discovery-period-seconds".equals(targetChildName)) {
                config.setDiscoveryPeriodSeconds(getIntegerValue("discovery-period-seconds", getTextContent(targetChild)));
            } else if ("max-target-endpoints".equals(targetChildName)) {
                config.setMaxTargetEndpoints(getIntegerValue("max-target-endpoints", getTextContent(targetChild)));
            } else if ("max-concurrent-invocations".equals(targetChildName)) {
                config.setMaxConcurrentInvocations(getIntegerValue("max-concurrent-invocations", getTextContent(targetChild)));
            } else if ("use-endpoint-private-address".equals(targetChildName)) {
                config.setUseEndpointPrivateAddress(getBooleanValue(getTextContent(targetChild)));
            } else if ("idle-min-park-ns".equals(targetChildName)) {
                config.setIdleMinParkNs(getIntegerValue("idle-min-park-ns", getTextContent(targetChild)));
            } else if ("idle-max-park-ns".equals(targetChildName)) {
                config.setIdleMaxParkNs(getIntegerValue("idle-max-park-ns", getTextContent(targetChild)));
            } else if ("properties".equals(targetChildName)) {
                fillProperties(targetChild, config.getProperties());
            } else if (AliasedDiscoveryConfigUtils.supports(targetChildName)) {
                handleAliasedDiscoveryStrategy(config, targetChild, targetChildName);
            } else if ("discovery-strategies".equals(targetChildName)) {
                handleDiscoveryStrategies(config.getDiscoveryConfig(), targetChild);
            } else if ("wan-sync".equals(targetChildName)) {
                handleWanSync(config.getWanSyncConfig(), targetChild);
            } else if ("endpoint".equals(targetChildName)) {
                config.setEndpoint(getTextContent(targetChild));
            }
        }
        wanReplicationConfig.addWanBatchReplicationPublisherConfig(config);
    }

    void handleWanConsumerNode(WanReplicationConfig wanReplicationConfig, Node nodeTarget) {
        WanConsumerConfig consumerConfig = new WanConsumerConfig();
        for (Node targetChild : childElements(nodeTarget)) {
            handleWanConsumerConfig(consumerConfig, targetChild);
        }
        wanReplicationConfig.setWanConsumerConfig(consumerConfig);
    }

    private void handleWanSync(WanSyncConfig wanSyncConfig, Node node) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if ("consistency-check-strategy".equals(nodeName)) {
                String strategy = getTextContent(child);
                wanSyncConfig.setConsistencyCheckStrategy(
                        ConsistencyCheckStrategy.valueOf(upperCaseInternal(strategy)));
            }
        }
    }

    private void handleWanConsumerConfig(WanConsumerConfig consumerConfig, Node targetChild) {
        String targetChildName = cleanNodeName(targetChild);
        if ("class-name".equals(targetChildName)) {
            consumerConfig.setClassName(getTextContent(targetChild));
        } else if ("properties".equals(targetChildName)) {
            fillProperties(targetChild, consumerConfig.getProperties());
        } else if ("persist-wan-replicated-data".equals(targetChildName)) {
            consumerConfig.setPersistWanReplicatedData(getBooleanValue(getTextContent(targetChild)));
        }
    }

    private void handleNetwork(Node node)
            throws Exception {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if ("reuse-address".equals(nodeName)) {
                String value = getTextContent(child).trim();
                config.getNetworkConfig().setReuseAddress(getBooleanValue(value));
            } else if ("port".equals(nodeName)) {
                handlePort(child, config);
            } else if ("outbound-ports".equals(nodeName)) {
                handleOutboundPorts(child);
            } else if ("public-address".equals(nodeName)) {
                String address = getTextContent(child);
                config.getNetworkConfig().setPublicAddress(address);
            } else if ("join".equals(nodeName)) {
                handleJoin(child, false);
            } else if ("interfaces".equals(nodeName)) {
                handleInterfaces(child);
            } else if ("symmetric-encryption".equals(nodeName)) {
                handleViaReflection(child, config.getNetworkConfig(), new SymmetricEncryptionConfig());
            } else if ("ssl".equals(nodeName)) {
                handleSSLConfig(child);
            } else if ("socket-interceptor".equals(nodeName)) {
                handleSocketInterceptorConfig(child);
            } else if ("member-address-provider".equals(nodeName)) {
                handleMemberAddressProvider(child, false);
            } else if ("failure-detector".equals(nodeName)) {
                handleFailureDetector(child, false);
            } else if ("rest-api".equals(nodeName)) {
                handleRestApi(child);
            } else if ("memcache-protocol".equals(nodeName)) {
                handleMemcacheProtocol(child);
            }
        }
    }

    private void handleAdvancedNetwork(Node node)
            throws Exception {
        NamedNodeMap attributes = node.getAttributes();
        for (int a = 0; a < attributes.getLength(); a++) {
            Node att = attributes.item(a);
            if ("enabled".equals(att.getNodeName())) {
                String value = att.getNodeValue();
                config.getAdvancedNetworkConfig().setEnabled(getBooleanValue(value));
            }
        }
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if ("join".equals(nodeName)) {
                handleJoin(child, true);
            } else if ("wan-endpoint-config".equals(nodeName)) {
                handleWanEndpointConfig(child);
            } else if ("member-server-socket-endpoint-config".equals(nodeName)) {
                handleMemberServerSocketEndpointConfig(child);
            } else if ("client-server-socket-endpoint-config".equals(nodeName)) {
                handleClientServerSocketEndpointConfig(child);
            } else if ("wan-server-socket-endpoint-config".equals(nodeName)) {
                handleWanServerSocketEndpointConfig(child);
            } else if ("rest-server-socket-endpoint-config".equals(nodeName)) {
                handleRestServerSocketEndpointConfig(child);
            } else if ("memcache-server-socket-endpoint-config".equals(nodeName)) {
                handleMemcacheServerSocketEndpointConfig(child);
            } else if ("member-address-provider".equals(nodeName)) {
                handleMemberAddressProvider(child, true);
            } else if ("failure-detector".equals(nodeName)) {
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
            if ("endpoint-groups".equals(nodeName)) {
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
            if ("port".equals(nodeName)) {
                handlePort(child, endpointConfig);
            } else if ("public-address".equals(nodeName)) {
                String address = getTextContent(child);
                endpointConfig.setPublicAddress(address);
            } else if ("reuse-address".equals(nodeName)) {
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
        if ("outbound-ports".equals(nodeName)) {
            handleOutboundPorts(node, endpointConfig);
        } else if ("interfaces".equals(nodeName)) {
            handleInterfaces(node, endpointConfig);
        } else if ("ssl".equals(nodeName)) {
            handleSSLConfig(node, endpointConfig);
        } else if ("socket-interceptor".equals(nodeName)) {
            handleSocketInterceptorConfig(node, endpointConfig);
        } else if ("socket-options".equals(nodeName)) {
            handleSocketOptions(node, endpointConfig);
        } else if ("symmetric-encryption".equals(nodeName)) {
            handleViaReflection(node, endpointConfig, new SymmetricEncryptionConfig());
        }
    }

    private void handleSocketOptions(Node node, EndpointConfig endpointConfig) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if ("buffer-direct".equals(nodeName)) {
                endpointConfig.setSocketBufferDirect(getBooleanValue(getTextContent(child)));
            } else if ("tcp-no-delay".equals(nodeName)) {
                endpointConfig.setSocketTcpNoDelay(getBooleanValue(getTextContent(child)));
            } else if ("keep-alive".equals(nodeName)) {
                endpointConfig.setSocketKeepAlive(getBooleanValue(getTextContent(child)));
            } else if ("connect-timeout-seconds".equals(nodeName)) {
                endpointConfig.setSocketConnectTimeoutSeconds(getIntegerValue("connect-timeout-seconds",
                        getTextContent(child), DEFAULT_SOCKET_CONNECT_TIMEOUT_SECONDS));
            } else if ("send-buffer-size-kb".equals(nodeName)) {
                endpointConfig.setSocketSendBufferSizeKb(getIntegerValue("send-buffer-size-kb",
                        getTextContent(child), DEFAULT_SOCKET_SEND_BUFFER_SIZE_KB));
            } else if ("receive-buffer-size-kb".equals(nodeName)) {
                endpointConfig.setSocketRcvBufferSizeKb(getIntegerValue("receive-buffer-size-kb",
                        getTextContent(child), DEFAULT_SOCKET_RECEIVE_BUFFER_SIZE_KB));
            } else if ("linger-seconds".equals(nodeName)) {
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
        scheduledExecutorConfig.setName(getTextContent(node.getAttributes().getNamedItem("name")));

        handleScheduledExecutorNode(node, scheduledExecutorConfig);
    }

    void handleScheduledExecutorNode(Node node, ScheduledExecutorConfig scheduledExecutorConfig) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if ("merge-policy".equals(nodeName)) {
                scheduledExecutorConfig.setMergePolicyConfig(createMergePolicyConfig(child));
            } else if ("capacity".equals(nodeName)) {
                scheduledExecutorConfig.setCapacity(parseInt(getTextContent(child)));
            } else if ("durability".equals(nodeName)) {
                scheduledExecutorConfig.setDurability(parseInt(getTextContent(child)));
            } else if ("pool-size".equals(nodeName)) {
                scheduledExecutorConfig.setPoolSize(parseInt(getTextContent(child)));
            } else if ("split-brain-protection-ref".equals(nodeName)) {
                scheduledExecutorConfig.setSplitBrainProtectionName(getTextContent(child));
            }
        }

        config.addScheduledExecutorConfig(scheduledExecutorConfig);
    }

    protected void handleCardinalityEstimator(Node node) {
        CardinalityEstimatorConfig cardinalityEstimatorConfig = new CardinalityEstimatorConfig();
        cardinalityEstimatorConfig.setName(getTextContent(node.getAttributes().getNamedItem("name")));

        handleCardinalityEstimatorNode(node, cardinalityEstimatorConfig);
    }

    void handleCardinalityEstimatorNode(Node node, CardinalityEstimatorConfig cardinalityEstimatorConfig) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if ("merge-policy".equals(nodeName)) {
                MergePolicyConfig mergePolicyConfig = createMergePolicyConfig(child);
                cardinalityEstimatorConfig.setMergePolicyConfig(mergePolicyConfig);
            } else if ("backup-count".equals(nodeName)) {
                cardinalityEstimatorConfig.setBackupCount(parseInt(getTextContent(child)));
            } else if ("async-backup-count".equals(nodeName)) {
                cardinalityEstimatorConfig.setAsyncBackupCount(parseInt(getTextContent(child)));
            } else if ("split-brain-protection-ref".equals(nodeName)) {
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
            if ("prefetch-count".equals(nodeName)) {
                generatorConfig.setPrefetchCount(Integer.parseInt(value));
            } else if ("prefetch-validity-millis".equalsIgnoreCase(nodeName)) {
                generatorConfig.setPrefetchValidityMillis(Long.parseLong(value));
            } else if ("id-offset".equalsIgnoreCase(nodeName)) {
                generatorConfig.setIdOffset(Long.parseLong(value));
            } else if ("node-id-offset".equalsIgnoreCase(nodeName)) {
                generatorConfig.setNodeIdOffset(Long.parseLong(value));
            } else if ("statistics-enabled".equals(nodeName)) {
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
            if ("enabled".equals(att.getNodeName())) {
                String value = att.getNodeValue();
                interfaces.setEnabled(getBooleanValue(value));
            }
        }
        handleInterfacesList(node, interfaces);
    }

    protected void handleInterfacesList(Node node, InterfacesConfig interfaces) {
        for (Node n : childElements(node)) {
            if ("interface".equals(lowerCaseInternal(cleanNodeName(n)))) {
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
            if ("enabled".equals(att.getNodeName())) {
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
                if (name.equals(exclusion)) {
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
        if (element.equals("split-brain-protection-ref")) {
            return "SplitBrainProtectionName";
        }
        return null;
    }

    private void handleJoin(Node node, boolean advancedNetworkConfig) {
        JoinConfig joinConfig = joinConfig(advancedNetworkConfig);
        for (Node child : childElements(node)) {
            String name = cleanNodeName(child);
            if ("multicast".equals(name)) {
                handleMulticast(child, advancedNetworkConfig);
            } else if ("tcp-ip".equals(name)) {
                handleTcpIp(child, advancedNetworkConfig);
            } else if (AliasedDiscoveryConfigUtils.supports(name)) {
                handleAliasedDiscoveryStrategy(joinConfig, child, name);
            } else if ("discovery-strategies".equals(name)) {
                handleDiscoveryStrategies(joinConfig.getDiscoveryConfig(), child);
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
        if ("discovery-strategy".equals(name)) {
            handleDiscoveryStrategy(child, discoveryConfig);
        } else if ("node-filter".equals(name)) {
            handleDiscoveryNodeFilter(child, discoveryConfig);
        }
    }

    void handleDiscoveryNodeFilter(Node node, DiscoveryConfig discoveryConfig) {
        NamedNodeMap attributes = node.getAttributes();
        Node att = attributes.getNamedItem("class");
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
            if ("enabled".equals(lowerCaseInternal(att.getNodeName()))) {
                enabled = getBooleanValue(value);
            } else if ("class".equals(att.getNodeName())) {
                clazz = value;
            }
        }

        if (!enabled || clazz == null) {
            return;
        }

        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        for (Node child : childElements(node)) {
            String name = cleanNodeName(child);
            if ("properties".equals(name)) {
                fillProperties(child, properties);
            }
        }

        discoveryConfig.addDiscoveryStrategyConfig(new DiscoveryStrategyConfig(clazz, properties));
    }

    private void handleAliasedDiscoveryStrategy(JoinConfig joinConfig, Node node, String tag) {
        AliasedDiscoveryConfig aliasedDiscoveryConfig = getConfigByTag(joinConfig, tag);
        updateConfig(aliasedDiscoveryConfig, node);
    }

    private void handleAliasedDiscoveryStrategy(WanBatchReplicationPublisherConfig publisherConfig,
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
            if ("enabled".equals(lowerCaseInternal(att.getNodeName()))) {
                config.setEnabled(getBooleanValue(value));
            } else if (att.getNodeName().equals("connection-timeout-seconds")) {
                config.setProperty("connection-timeout-seconds", value);
            }
        }
        for (Node n : childElements(node)) {
            String key = cleanNodeName(n);
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
            if ("enabled".equals(lowerCaseInternal(att.getNodeName()))) {
                multicastConfig.setEnabled(getBooleanValue(value));
            } else if ("loopbackmodeenabled".equals(lowerCaseInternal(att.getNodeName()))
                    || "loopback-mode-enabled".equals(lowerCaseInternal(att.getNodeName()))) {
                multicastConfig.setLoopbackModeEnabled(getBooleanValue(value));
            }
        }
        for (Node n : childElements(node)) {
            String value = getTextContent(n).trim();
            if ("multicast-group".equals(cleanNodeName(n))) {
                multicastConfig.setMulticastGroup(value);
            } else if ("multicast-port".equals(cleanNodeName(n))) {
                multicastConfig.setMulticastPort(parseInt(value));
            } else if ("multicast-timeout-seconds".equals(cleanNodeName(n))) {
                multicastConfig.setMulticastTimeoutSeconds(parseInt(value));
            } else if ("multicast-time-to-live-seconds".equals(cleanNodeName(n))) {
                // we need this line for the time being to prevent not reading the multicast-time-to-live-seconds property
                // for more info see: https://github.com/hazelcast/hazelcast/issues/752
                multicastConfig.setMulticastTimeToLive(parseInt(value));
            } else if ("multicast-time-to-live".equals(cleanNodeName(n))) {
                multicastConfig.setMulticastTimeToLive(parseInt(value));
            } else if ("trusted-interfaces".equals(cleanNodeName(n))) {
                handleTrustedInterfaces(multicastConfig, n);
            }
        }
    }

    protected void handleTrustedInterfaces(MulticastConfig multicastConfig, Node n) {
        for (Node child : childElements(n)) {
            if ("interface".equals(lowerCaseInternal(cleanNodeName(child)))) {
                multicastConfig.addTrustedInterface(getTextContent(child).trim());
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
            if (att.getNodeName().equals("enabled")) {
                tcpIpConfig.setEnabled(getBooleanValue(value));
            } else if (att.getNodeName().equals("connection-timeout-seconds")) {
                tcpIpConfig.setConnectionTimeoutSeconds(getIntegerValue("connection-timeout-seconds", value));
            }
        }
        Set<String> memberTags = new HashSet<String>(Arrays.asList("interface", "member", "members"));
        for (Node n : childElements(node)) {
            String value = getTextContent(n).trim();
            if (cleanNodeName(n).equals("member-list")) {
                handleMemberList(n, advancedNetworkConfig);
            } else if (cleanNodeName(n).equals("required-member")) {
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
            if ("member".equals(nodeName)) {
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

            if ("auto-increment".equals(att.getNodeName())) {
                networkConfig.setPortAutoIncrement(getBooleanValue(value));
            } else if ("port-count".equals(att.getNodeName())) {
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

            if ("auto-increment".equals(att.getNodeName())) {
                endpointConfig.setPortAutoIncrement(getBooleanValue(value));
            } else if ("port-count".equals(att.getNodeName())) {
                int portCount = parseInt(value);
                endpointConfig.setPortCount(portCount);
            }
        }
    }

    protected void handleOutboundPorts(Node child) {
        NetworkConfig networkConfig = config.getNetworkConfig();
        for (Node n : childElements(child)) {
            String nodeName = cleanNodeName(n);
            if ("ports".equals(nodeName)) {
                String value = getTextContent(n);
                networkConfig.addOutboundPortDefinition(value);
            }
        }
    }

    protected void handleOutboundPorts(Node child, EndpointConfig endpointConfig) {
        for (Node n : childElements(child)) {
            String nodeName = cleanNodeName(n);
            if ("ports".equals(nodeName)) {
                String value = getTextContent(n);
                endpointConfig.addOutboundPortDefinition(value);
            }
        }
    }

    protected void handleQueue(Node node) {
        Node attName = node.getAttributes().getNamedItem("name");
        String name = getTextContent(attName);
        QueueConfig qConfig = new QueueConfig();
        qConfig.setName(name);
        handleQueueNode(node, qConfig);
    }

    void handleQueueNode(Node node, final QueueConfig qConfig) {
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            String value = getTextContent(n).trim();
            if ("max-size".equals(nodeName)) {
                qConfig.setMaxSize(getIntegerValue("max-size", value));
            } else if ("backup-count".equals(nodeName)) {
                qConfig.setBackupCount(getIntegerValue("backup-count", value));
            } else if ("async-backup-count".equals(nodeName)) {
                qConfig.setAsyncBackupCount(getIntegerValue("async-backup-count", value));
            } else if ("item-listeners".equals(nodeName)) {
                handleItemListeners(n, new Function<ItemListenerConfig, Void>() {
                    @Override
                    public Void apply(ItemListenerConfig itemListenerConfig) {
                        qConfig.addItemListenerConfig(itemListenerConfig);
                        return null;
                    }
                });
            } else if ("statistics-enabled".equals(nodeName)) {
                qConfig.setStatisticsEnabled(getBooleanValue(value));
            } else if ("queue-store".equals(nodeName)) {
                QueueStoreConfig queueStoreConfig = createQueueStoreConfig(n);
                qConfig.setQueueStoreConfig(queueStoreConfig);
            } else if ("split-brain-protection-ref".equals(nodeName)) {
                qConfig.setSplitBrainProtectionName(value);
            } else if ("empty-queue-ttl".equals(nodeName)) {
                qConfig.setEmptyQueueTtl(getIntegerValue("empty-queue-ttl", value));
            } else if ("merge-policy".equals(nodeName)) {
                MergePolicyConfig mergePolicyConfig = createMergePolicyConfig(n);
                qConfig.setMergePolicyConfig(mergePolicyConfig);
            }
        }
        config.addQueueConfig(qConfig);
    }

    protected void handleItemListeners(Node n, Function<ItemListenerConfig, Void> configAddFunction) {
        for (Node listenerNode : childElements(n)) {
            if ("item-listener".equals(cleanNodeName(listenerNode))) {
                NamedNodeMap attrs = listenerNode.getAttributes();
                boolean incValue = getBooleanValue(getTextContent(attrs.getNamedItem("include-value")));
                String listenerClass = getTextContent(listenerNode);
                configAddFunction.apply(new ItemListenerConfig(listenerClass, incValue));
            }
        }
    }

    protected void handleList(Node node) {
        Node attName = node.getAttributes().getNamedItem("name");
        String name = getTextContent(attName);
        ListConfig lConfig = new ListConfig();
        lConfig.setName(name);
        handleListNode(node, lConfig);
    }

    void handleListNode(Node node, final ListConfig lConfig) {
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            String value = getTextContent(n).trim();
            if ("max-size".equals(nodeName)) {
                lConfig.setMaxSize(getIntegerValue("max-size", value));
            } else if ("backup-count".equals(nodeName)) {
                lConfig.setBackupCount(getIntegerValue("backup-count", value));
            } else if ("async-backup-count".equals(nodeName)) {
                lConfig.setAsyncBackupCount(getIntegerValue("async-backup-count", value));
            } else if ("item-listeners".equals(nodeName)) {
                handleItemListeners(n, new Function<ItemListenerConfig, Void>() {
                    @Override
                    public Void apply(ItemListenerConfig itemListenerConfig) {
                        lConfig.addItemListenerConfig(itemListenerConfig);
                        return null;
                    }
                });
            } else if ("statistics-enabled".equals(nodeName)) {
                lConfig.setStatisticsEnabled(getBooleanValue(value));
            } else if ("split-brain-protection-ref".equals(nodeName)) {
                lConfig.setSplitBrainProtectionName(value);
            } else if ("merge-policy".equals(nodeName)) {
                MergePolicyConfig mergePolicyConfig = createMergePolicyConfig(n);
                lConfig.setMergePolicyConfig(mergePolicyConfig);
            }

        }
        config.addListConfig(lConfig);
    }

    protected void handleSet(Node node) {
        Node attName = node.getAttributes().getNamedItem("name");
        String name = getTextContent(attName);
        SetConfig sConfig = new SetConfig();
        sConfig.setName(name);
        handleSetNode(node, sConfig);
    }

    void handleSetNode(Node node, final SetConfig sConfig) {
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            String value = getTextContent(n).trim();
            if ("max-size".equals(nodeName)) {
                sConfig.setMaxSize(getIntegerValue("max-size", value));
            } else if ("backup-count".equals(nodeName)) {
                sConfig.setBackupCount(getIntegerValue("backup-count", value));
            } else if ("async-backup-count".equals(nodeName)) {
                sConfig.setAsyncBackupCount(getIntegerValue("async-backup-count", value));
            } else if ("item-listeners".equals(nodeName)) {
                handleItemListeners(n, new Function<ItemListenerConfig, Void>() {
                    @Override
                    public Void apply(ItemListenerConfig itemListenerConfig) {
                        sConfig.addItemListenerConfig(itemListenerConfig);
                        return null;
                    }
                });
            } else if ("statistics-enabled".equals(nodeName)) {
                sConfig.setStatisticsEnabled(getBooleanValue(value));
            } else if ("split-brain-protection-ref".equals(nodeName)) {
                sConfig.setSplitBrainProtectionName(value);
            } else if ("merge-policy".equals(nodeName)) {
                MergePolicyConfig mergePolicyConfig = createMergePolicyConfig(n);
                sConfig.setMergePolicyConfig(mergePolicyConfig);
            }
        }
        config.addSetConfig(sConfig);
    }

    protected void handleMultiMap(Node node) {
        Node attName = node.getAttributes().getNamedItem("name");
        String name = getTextContent(attName);
        MultiMapConfig multiMapConfig = new MultiMapConfig();
        multiMapConfig.setName(name);
        handleMultiMapNode(node, multiMapConfig);
    }

    void handleMultiMapNode(Node node, final MultiMapConfig multiMapConfig) {
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            String value = getTextContent(n).trim();
            if ("value-collection-type".equals(nodeName)) {
                multiMapConfig.setValueCollectionType(value);
            } else if ("backup-count".equals(nodeName)) {
                multiMapConfig.setBackupCount(getIntegerValue("backup-count"
                        , value));
            } else if ("async-backup-count".equals(nodeName)) {
                multiMapConfig.setAsyncBackupCount(getIntegerValue("async-backup-count"
                        , value));
            } else if ("entry-listeners".equals(nodeName)) {
                handleEntryListeners(n, new Function<EntryListenerConfig, Void>() {
                    @Override
                    public Void apply(EntryListenerConfig entryListenerConfig) {
                        multiMapConfig.addEntryListenerConfig(entryListenerConfig);
                        return null;
                    }
                });
            } else if ("statistics-enabled".equals(nodeName)) {
                multiMapConfig.setStatisticsEnabled(getBooleanValue(value));
            } else if ("binary".equals(nodeName)) {
                multiMapConfig.setBinary(getBooleanValue(value));
            } else if ("split-brain-protection-ref".equals(nodeName)) {
                multiMapConfig.setSplitBrainProtectionName(value);
            } else if ("merge-policy".equals(nodeName)) {
                MergePolicyConfig mergePolicyConfig = createMergePolicyConfig(n);
                multiMapConfig.setMergePolicyConfig(mergePolicyConfig);
            }
        }
        config.addMultiMapConfig(multiMapConfig);
    }

    protected void handleEntryListeners(Node n, Function<EntryListenerConfig, Void> configAddFunction) {
        for (Node listenerNode : childElements(n)) {
            if ("entry-listener".equals(cleanNodeName(listenerNode))) {
                NamedNodeMap attrs = listenerNode.getAttributes();
                boolean incValue = getBooleanValue(getTextContent(attrs.getNamedItem("include-value")));
                boolean local = getBooleanValue(getTextContent(attrs.getNamedItem("local")));
                String listenerClass = getTextContent(listenerNode);
                configAddFunction.apply(new EntryListenerConfig(listenerClass, local, incValue));
            }
        }
    }

    @SuppressWarnings("deprecation")
    protected void handleReplicatedMap(Node node) {
        Node attName = node.getAttributes().getNamedItem("name");
        String name = getTextContent(attName);
        final ReplicatedMapConfig replicatedMapConfig = new ReplicatedMapConfig();
        replicatedMapConfig.setName(name);
        handleReplicatedMapNode(node, replicatedMapConfig);
    }

    void handleReplicatedMapNode(Node node, final ReplicatedMapConfig replicatedMapConfig) {
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            String value = getTextContent(n).trim();
            if ("in-memory-format".equals(nodeName)) {
                replicatedMapConfig.setInMemoryFormat(InMemoryFormat.valueOf(upperCaseInternal(value)));
            } else if ("async-fillup".equals(nodeName)) {
                replicatedMapConfig.setAsyncFillup(getBooleanValue(value));
            } else if ("statistics-enabled".equals(nodeName)) {
                replicatedMapConfig.setStatisticsEnabled(getBooleanValue(value));
            } else if ("entry-listeners".equals(nodeName)) {
                handleEntryListeners(n, entryListenerConfig -> {
                    replicatedMapConfig.addEntryListenerConfig(entryListenerConfig);
                    return null;
                });
            } else if ("merge-policy".equals(nodeName)) {
                MergePolicyConfig mergePolicyConfig = createMergePolicyConfig(n);
                replicatedMapConfig.setMergePolicyConfig(mergePolicyConfig);
            } else if ("split-brain-protection-ref".equals(nodeName)) {
                replicatedMapConfig.setSplitBrainProtectionName(value);
            }
        }
        config.addReplicatedMapConfig(replicatedMapConfig);
    }

    @SuppressWarnings("deprecation")
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
            if ("backup-count".equals(nodeName)) {
                mapConfig.setBackupCount(getIntegerValue("backup-count", value));
            } else if ("metadata-policy".equals(nodeName)) {
                mapConfig.setMetadataPolicy(MetadataPolicy.valueOf(upperCaseInternal(value)));
            } else if ("in-memory-format".equals(nodeName)) {
                mapConfig.setInMemoryFormat(InMemoryFormat.valueOf(upperCaseInternal(value)));
            } else if ("async-backup-count".equals(nodeName)) {
                mapConfig.setAsyncBackupCount(getIntegerValue("async-backup-count", value));
            } else if ("eviction".equals(nodeName)) {
                mapConfig.setEvictionConfig(getEvictionConfig(node, false, true));
            } else if ("time-to-live-seconds".equals(nodeName)) {
                mapConfig.setTimeToLiveSeconds(getIntegerValue("time-to-live-seconds", value));
            } else if ("max-idle-seconds".equals(nodeName)) {
                mapConfig.setMaxIdleSeconds(getIntegerValue("max-idle-seconds", value));
            } else if ("map-store".equals(nodeName)) {
                MapStoreConfig mapStoreConfig = createMapStoreConfig(node);
                mapConfig.setMapStoreConfig(mapStoreConfig);
            } else if ("near-cache".equals(nodeName)) {
                mapConfig.setNearCacheConfig(handleNearCacheConfig(node));
            } else if ("merge-policy".equals(nodeName)) {
                MergePolicyConfig mergePolicyConfig = createMergePolicyConfig(node);
                mapConfig.setMergePolicyConfig(mergePolicyConfig);
            } else if ("merkle-tree".equals(nodeName)) {
                MerkleTreeConfig merkleTreeConfig = new MerkleTreeConfig();
                handleViaReflection(node, mapConfig, merkleTreeConfig);
            } else if ("event-journal".equals(nodeName)) {
                EventJournalConfig eventJournalConfig = new EventJournalConfig();
                handleViaReflection(node, mapConfig, eventJournalConfig);
            } else if ("hot-restart".equals(nodeName)) {
                mapConfig.setHotRestartConfig(createHotRestartConfig(node));
            } else if ("read-backup-data".equals(nodeName)) {
                mapConfig.setReadBackupData(getBooleanValue(value));
            } else if ("statistics-enabled".equals(nodeName)) {
                mapConfig.setStatisticsEnabled(getBooleanValue(value));
            } else if ("cache-deserialized-values".equals(nodeName)) {
                CacheDeserializedValues cacheDeserializedValues = CacheDeserializedValues.parseString(value);
                mapConfig.setCacheDeserializedValues(cacheDeserializedValues);
            } else if ("wan-replication-ref".equals(nodeName)) {
                mapWanReplicationRefHandle(node, mapConfig);
            } else if ("indexes".equals(nodeName)) {
                mapIndexesHandle(node, mapConfig);
            } else if ("attributes".equals(nodeName)) {
                attributesHandle(node, mapConfig);
            } else if ("entry-listeners".equals(nodeName)) {
                handleEntryListeners(node, entryListenerConfig -> {
                    mapConfig.addEntryListenerConfig(entryListenerConfig);
                    return null;
                });
            } else if ("partition-lost-listeners".equals(nodeName)) {
                mapPartitionLostListenerHandle(node, mapConfig);
            } else if ("partition-strategy".equals(nodeName)) {
                mapConfig.setPartitioningStrategyConfig(new PartitioningStrategyConfig(value));
            } else if ("split-brain-protection-ref".equals(nodeName)) {
                mapConfig.setSplitBrainProtectionName(value);
            } else if ("query-caches".equals(nodeName)) {
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
            if ("time-to-live-seconds".equals(nodeName)) {
                nearCacheConfig.setTimeToLiveSeconds(Integer.parseInt(value));
            } else if ("max-idle-seconds".equals(nodeName)) {
                nearCacheConfig.setMaxIdleSeconds(Integer.parseInt(value));
            } else if ("in-memory-format".equals(nodeName)) {
                nearCacheConfig.setInMemoryFormat(InMemoryFormat.valueOf(upperCaseInternal(value)));
            } else if ("serialize-keys".equals(nodeName)) {
                serializeKeys = Boolean.parseBoolean(value);
                nearCacheConfig.setSerializeKeys(serializeKeys);
            } else if ("invalidate-on-change".equals(nodeName)) {
                nearCacheConfig.setInvalidateOnChange(Boolean.parseBoolean(value));
            } else if ("cache-local-entries".equals(nodeName)) {
                nearCacheConfig.setCacheLocalEntries(Boolean.parseBoolean(value));
            } else if ("local-update-policy".equals(nodeName)) {
                NearCacheConfig.LocalUpdatePolicy policy = NearCacheConfig.LocalUpdatePolicy.valueOf(value);
                nearCacheConfig.setLocalUpdatePolicy(policy);
            } else if ("eviction".equals(nodeName)) {
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

        Node attrEnabled = node.getAttributes().getNamedItem("enabled");
        boolean enabled = getBooleanValue(getTextContent(attrEnabled));
        hotRestartConfig.setEnabled(enabled);

        for (Node n : childElements(node)) {
            String name = cleanNodeName(n);
            if ("fsync".equals(name)) {
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
            if ("key-type".equals(nodeName)) {
                cacheConfig.setKeyType(getAttribute(n, "class-name"));
            } else if ("value-type".equals(nodeName)) {
                cacheConfig.setValueType(getAttribute(n, "class-name"));
            } else if ("statistics-enabled".equals(nodeName)) {
                cacheConfig.setStatisticsEnabled(getBooleanValue(value));
            } else if ("management-enabled".equals(nodeName)) {
                cacheConfig.setManagementEnabled(getBooleanValue(value));
            } else if ("read-through".equals(nodeName)) {
                cacheConfig.setReadThrough(getBooleanValue(value));
            } else if ("write-through".equals(nodeName)) {
                cacheConfig.setWriteThrough(getBooleanValue(value));
            } else if ("cache-loader-factory".equals(nodeName)) {
                cacheConfig.setCacheLoaderFactory(getAttribute(n, "class-name"));
            } else if ("cache-loader".equals(nodeName)) {
                cacheConfig.setCacheLoader(getAttribute(n, "class-name"));
            } else if ("cache-writer-factory".equals(nodeName)) {
                cacheConfig.setCacheWriterFactory(getAttribute(n, "class-name"));
            } else if ("cache-writer".equals(nodeName)) {
                cacheConfig.setCacheWriter(getAttribute(n, "class-name"));
            } else if ("expiry-policy-factory".equals(nodeName)) {
                cacheConfig.setExpiryPolicyFactoryConfig(getExpiryPolicyFactoryConfig(n));
            } else if ("cache-entry-listeners".equals(nodeName)) {
                cacheListenerHandle(n, cacheConfig);
            } else if ("in-memory-format".equals(nodeName)) {
                cacheConfig.setInMemoryFormat(InMemoryFormat.valueOf(upperCaseInternal(value)));
            } else if ("backup-count".equals(nodeName)) {
                cacheConfig.setBackupCount(getIntegerValue("backup-count", value));
            } else if ("async-backup-count".equals(nodeName)) {
                cacheConfig.setAsyncBackupCount(getIntegerValue("async-backup-count", value));
            } else if ("wan-replication-ref".equals(nodeName)) {
                cacheWanReplicationRefHandle(n, cacheConfig);
            } else if ("eviction".equals(nodeName)) {
                cacheConfig.setEvictionConfig(getEvictionConfig(n, false, false));
            } else if ("split-brain-protection-ref".equals(nodeName)) {
                cacheConfig.setSplitBrainProtectionName(value);
            } else if ("partition-lost-listeners".equals(nodeName)) {
                cachePartitionLostListenerHandle(n, cacheConfig);
            } else if ("merge-policy".equals(nodeName)) {
                cacheConfig.setMergePolicyConfig(createMergePolicyConfig(n));
            } else if ("event-journal".equals(nodeName)) {
                EventJournalConfig eventJournalConfig = new EventJournalConfig();
                handleViaReflection(n, cacheConfig, eventJournalConfig);
            } else if ("hot-restart".equals(nodeName)) {
                cacheConfig.setHotRestartConfig(createHotRestartConfig(n));
            } else if ("disable-per-entry-invalidation-events".equals(nodeName)) {
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
                if ("timed-expiry-policy-factory".equals(nodeName)) {
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

        Node size = node.getAttributes().getNamedItem("size");
        Node maxSizePolicy = node.getAttributes().getNamedItem("max-size-policy");
        Node evictionPolicy = node.getAttributes().getNamedItem("eviction-policy");
        Node comparatorClassName = node.getAttributes().getNamedItem("comparator-class-name");

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
            evictionConfig.setComparatorClassName(getTextContent(comparatorClassName));
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
            if ("merge-policy".equals(wanChildName)) {
                wanReplicationRef.setMergePolicy(wanChildValue);
            } else if ("filters".equals(wanChildName)) {
                handleWanFilters(wanChild, wanReplicationRef);
            } else if ("republishing-enabled".equals(wanChildName)) {
                wanReplicationRef.setRepublishingEnabled(getBooleanValue(wanChildValue));
            }
        }
        cacheConfig.setWanReplicationRef(wanReplicationRef);
    }

    protected void handleWanFilters(Node wanChild, WanReplicationRef wanReplicationRef) {
        for (Node filter : childElements(wanChild)) {
            if ("filter-impl".equals(cleanNodeName(filter))) {
                wanReplicationRef.addFilter(getTextContent(filter));
            }
        }
    }

    protected void cachePartitionLostListenerHandle(Node n, CacheSimpleConfig cacheConfig) {
        for (Node listenerNode : childElements(n)) {
            if ("partition-lost-listener".equals(cleanNodeName(listenerNode))) {
                String listenerClass = getTextContent(listenerNode);
                cacheConfig.addCachePartitionLostListenerConfig(
                        new CachePartitionLostListenerConfig(listenerClass));
            }
        }
    }

    protected void cacheListenerHandle(Node n, CacheSimpleConfig cacheSimpleConfig) {
        for (Node listenerNode : childElements(n)) {
            if ("cache-entry-listener".equals(cleanNodeName(listenerNode))) {
                handleCacheEntryListenerNode(cacheSimpleConfig, listenerNode);
            }
        }
    }

    protected void handleCacheEntryListenerNode(CacheSimpleConfig cacheSimpleConfig, Node listenerNode) {
        CacheSimpleEntryListenerConfig listenerConfig = new CacheSimpleEntryListenerConfig();
        for (Node listenerChildNode : childElements(listenerNode)) {
            if ("cache-entry-listener-factory".equals(cleanNodeName(listenerChildNode))) {
                listenerConfig.setCacheEntryListenerFactory(getAttribute(listenerChildNode, "class-name"));
            }
            if ("cache-entry-event-filter-factory".equals(cleanNodeName(listenerChildNode))) {
                listenerConfig.setCacheEntryEventFilterFactory(getAttribute(listenerChildNode, "class-name"));
            }
        }
        NamedNodeMap attrs = listenerNode.getAttributes();
        listenerConfig.setOldValueRequired(getBooleanValue(getTextContent(attrs.getNamedItem("old-value-required"))));
        listenerConfig.setSynchronous(getBooleanValue(getTextContent(attrs.getNamedItem("synchronous"))));
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
            if ("merge-policy".equals(wanChildName)) {
                wanReplicationRef.setMergePolicy(wanChildValue);
            } else if ("republishing-enabled".equals(wanChildName)) {
                wanReplicationRef.setRepublishingEnabled(getBooleanValue(wanChildValue));
            } else if ("filters".equals(wanChildName)) {
                handleWanFilters(wanChild, wanReplicationRef);
            }
        }
        mapConfig.setWanReplicationRef(wanReplicationRef);
    }

    protected void mapIndexesHandle(Node n, MapConfig mapConfig) {
        for (Node indexNode : childElements(n)) {
            if ("index".equals(cleanNodeName(indexNode))) {
                IndexConfig indexConfig = IndexUtils.getIndexConfigFromXml(indexNode, domLevel3);

                mapConfig.addIndexConfig(indexConfig);
            }
        }
    }

    protected void queryCacheIndexesHandle(Node n, QueryCacheConfig queryCacheConfig) {
        for (Node indexNode : childElements(n)) {
            if ("index".equals(cleanNodeName(indexNode))) {
                IndexConfig indexConfig = IndexUtils.getIndexConfigFromXml(indexNode, domLevel3);

                queryCacheConfig.addIndexConfig(indexConfig);
            }
        }
    }

    protected void attributesHandle(Node n, MapConfig mapConfig) {
        for (Node extractorNode : childElements(n)) {
            if ("attribute".equals(cleanNodeName(extractorNode))) {
                NamedNodeMap attrs = extractorNode.getAttributes();
                String extractor = getTextContent(attrs.getNamedItem("extractor-class-name"));
                String name = getTextContent(extractorNode);
                mapConfig.addAttributeConfig(new AttributeConfig(name, extractor));
            }
        }
    }

    protected void mapPartitionLostListenerHandle(Node n, MapConfig mapConfig) {
        for (Node listenerNode : childElements(n)) {
            if ("partition-lost-listener".equals(cleanNodeName(listenerNode))) {
                String listenerClass = getTextContent(listenerNode);
                mapConfig.addMapPartitionLostListenerConfig(new MapPartitionLostListenerConfig(listenerClass));
            }
        }
    }

    protected void mapQueryCacheHandler(Node n, MapConfig mapConfig) {
        for (Node queryCacheNode : childElements(n)) {
            if ("query-cache".equals(cleanNodeName(queryCacheNode))) {
                NamedNodeMap attrs = queryCacheNode.getAttributes();
                String cacheName = getTextContent(attrs.getNamedItem("name"));
                QueryCacheConfig queryCacheConfig = new QueryCacheConfig(cacheName);
                handleMapQueryCacheNode(mapConfig, queryCacheNode, queryCacheConfig);
            }
        }
    }

    void handleMapQueryCacheNode(MapConfig mapConfig, Node queryCacheNode, final QueryCacheConfig queryCacheConfig) {
        for (Node childNode : childElements(queryCacheNode)) {
            String nodeName = cleanNodeName(childNode);
            if ("entry-listeners".equals(nodeName)) {
                handleEntryListeners(childNode, new Function<EntryListenerConfig, Void>() {
                    @Override
                    public Void apply(EntryListenerConfig entryListenerConfig) {
                        queryCacheConfig.addEntryListenerConfig(entryListenerConfig);
                        return null;
                    }
                });
            } else {
                String textContent = getTextContent(childNode);
                if ("include-value".equals(nodeName)) {
                    boolean includeValue = getBooleanValue(textContent);
                    queryCacheConfig.setIncludeValue(includeValue);
                } else if ("batch-size".equals(nodeName)) {
                    int batchSize = getIntegerValue("batch-size", textContent.trim());
                    queryCacheConfig.setBatchSize(batchSize);
                } else if ("buffer-size".equals(nodeName)) {
                    int bufferSize = getIntegerValue("buffer-size", textContent.trim());
                    queryCacheConfig.setBufferSize(bufferSize);
                } else if ("delay-seconds".equals(nodeName)) {
                    int delaySeconds = getIntegerValue("delay-seconds", textContent.trim());
                    queryCacheConfig.setDelaySeconds(delaySeconds);
                } else if ("in-memory-format".equals(nodeName)) {
                    String value = textContent.trim();
                    queryCacheConfig.setInMemoryFormat(InMemoryFormat.valueOf(upperCaseInternal(value)));
                } else if ("coalesce".equals(nodeName)) {
                    boolean coalesce = getBooleanValue(textContent);
                    queryCacheConfig.setCoalesce(coalesce);
                } else if ("populate".equals(nodeName)) {
                    boolean populate = getBooleanValue(textContent);
                    queryCacheConfig.setPopulate(populate);
                } else if ("indexes".equals(nodeName)) {
                    queryCacheIndexesHandle(childNode, queryCacheConfig);
                } else if ("predicate".equals(nodeName)) {
                    queryCachePredicateHandler(childNode, queryCacheConfig);
                } else if ("eviction".equals(nodeName)) {
                    queryCacheConfig.setEvictionConfig(getEvictionConfig(childNode, false, false));
                }
            }
        }
        mapConfig.addQueryCacheConfig(queryCacheConfig);
    }

    protected void queryCachePredicateHandler(Node childNode, QueryCacheConfig queryCacheConfig) {
        NamedNodeMap predicateAttributes = childNode.getAttributes();
        String predicateType = getTextContent(predicateAttributes.getNamedItem("type"));
        String textContent = getTextContent(childNode);
        PredicateConfig predicateConfig = new PredicateConfig();
        if ("class-name".equals(predicateType)) {
            predicateConfig.setClassName(textContent);
        } else if ("sql".equals(predicateType)) {
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
            if ("enabled".equals(att.getNodeName())) {
                mapStoreConfig.setEnabled(getBooleanValue(value));
            } else if ("initial-mode".equals(att.getNodeName())) {
                MapStoreConfig.InitialLoadMode mode = MapStoreConfig.InitialLoadMode
                        .valueOf(upperCaseInternal(getTextContent(att)));
                mapStoreConfig.setInitialLoadMode(mode);
            }
        }
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if ("class-name".equals(nodeName)) {
                mapStoreConfig.setClassName(getTextContent(n).trim());
            } else if ("factory-class-name".equals(nodeName)) {
                mapStoreConfig.setFactoryClassName(getTextContent(n).trim());
            } else if ("write-delay-seconds".equals(nodeName)) {
                mapStoreConfig.setWriteDelaySeconds(getIntegerValue("write-delay-seconds", getTextContent(n).trim()
                ));
            } else if ("write-batch-size".equals(nodeName)) {
                mapStoreConfig.setWriteBatchSize(getIntegerValue("write-batch-size", getTextContent(n).trim()
                ));
            } else if ("write-coalescing".equals(nodeName)) {
                String writeCoalescing = getTextContent(n).trim();
                if (isNullOrEmpty(writeCoalescing)) {
                    mapStoreConfig.setWriteCoalescing(MapStoreConfig.DEFAULT_WRITE_COALESCING);
                } else {
                    mapStoreConfig.setWriteCoalescing(getBooleanValue(writeCoalescing));
                }
            } else if ("properties".equals(nodeName)) {
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
            if (att.getNodeName().equals("enabled")) {
                config.setEnabled(getBooleanValue(value));
            }
        }
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if ("class-name".equals(nodeName)) {
                config.setClassName(getTextContent(n).trim());
            } else if ("factory-class-name".equals(nodeName)) {
                config.setFactoryClassName(getTextContent(n).trim());
            } else if ("properties".equals(nodeName)) {
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
            if (att.getNodeName().equals("enabled")) {
                queueStoreConfig.setEnabled(getBooleanValue(value));
            }
        }
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if ("class-name".equals(nodeName)) {
                queueStoreConfig.setClassName(getTextContent(n).trim());
            } else if ("factory-class-name".equals(nodeName)) {
                queueStoreConfig.setFactoryClassName(getTextContent(n).trim());
            } else if ("properties".equals(nodeName)) {
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

    private void handleMcMutualAuthConfig(Node node) {
        MCMutualAuthConfig mcMutualAuthConfig = new MCMutualAuthConfig();
        NamedNodeMap attributes = node.getAttributes();
        Node enabledNode = attributes.getNamedItem("enabled");
        boolean enabled = enabledNode != null && getBooleanValue(getTextContent(enabledNode).trim());
        mcMutualAuthConfig.setEnabled(enabled);

        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if ("factory-class-name".equals(nodeName)) {
                mcMutualAuthConfig.setFactoryClassName(getTextContent(n).trim());
            } else if ("properties".equals(nodeName)) {
                fillProperties(n, mcMutualAuthConfig.getProperties());
            }
        }

        config.getManagementCenterConfig().setMutualAuthConfig(mcMutualAuthConfig);
    }

    private void handleMemberAddressProvider(Node node, boolean advancedNetworkConfig) {
        MemberAddressProviderConfig memberAddressProviderConfig = memberAddressProviderConfig(advancedNetworkConfig);

        Node enabledNode = node.getAttributes().getNamedItem("enabled");
        boolean enabled = enabledNode != null && getBooleanValue(getTextContent(enabledNode));
        memberAddressProviderConfig.setEnabled(enabled);

        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if (nodeName.equals("class-name")) {
                String className = getTextContent(n);
                memberAddressProviderConfig.setClassName(className);
            } else if (nodeName.equals("properties")) {
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
            if (!cleanNodeName(child).equals("icmp")) {
                throw new IllegalStateException("Unsupported child under failure-detector");
            }

            Node enabledNode = child.getAttributes().getNamedItem("enabled");
            boolean enabled = enabledNode != null && getBooleanValue(getTextContent(enabledNode));
            IcmpFailureDetectorConfig icmpFailureDetectorConfig = new IcmpFailureDetectorConfig();

            icmpFailureDetectorConfig.setEnabled(enabled);
            for (Node n : childElements(child)) {
                String nodeName = cleanNodeName(n);

                if (nodeName.equals("ttl")) {
                    int ttl = parseInt(getTextContent(n));
                    icmpFailureDetectorConfig.setTtl(ttl);
                } else if (nodeName.equals("timeout-milliseconds")) {
                    int timeout = parseInt(getTextContent(n));
                    icmpFailureDetectorConfig.setTimeoutMilliseconds(timeout);
                } else if (nodeName.equals("parallel-mode")) {
                    boolean mode = parseBoolean(getTextContent(n));
                    icmpFailureDetectorConfig.setParallelMode(mode);
                } else if (nodeName.equals("fail-fast-on-startup")) {
                    boolean failOnStartup = parseBoolean(getTextContent(n));
                    icmpFailureDetectorConfig.setFailFastOnStartup(failOnStartup);
                } else if (nodeName.equals("max-attempts")) {
                    int attempts = parseInt(getTextContent(n));
                    icmpFailureDetectorConfig.setMaxAttempts(attempts);
                } else if (nodeName.equals("interval-milliseconds")) {
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
        Node attName = node.getAttributes().getNamedItem("name");
        String name = getTextContent(attName);
        TopicConfig tConfig = new TopicConfig();
        tConfig.setName(name);
        handleTopicNode(node, tConfig);
    }

    void handleTopicNode(Node node, final TopicConfig tConfig) {
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if (nodeName.equals("global-ordering-enabled")) {
                tConfig.setGlobalOrderingEnabled(getBooleanValue(getTextContent(n)));
            } else if ("message-listeners".equals(nodeName)) {
                handleMessageListeners(n, new Function<ListenerConfig, Void>() {
                    @Override
                    public Void apply(ListenerConfig listenerConfig) {
                        tConfig.addMessageListenerConfig(listenerConfig);
                        return null;
                    }
                });
            } else if ("statistics-enabled".equals(nodeName)) {
                tConfig.setStatisticsEnabled(getBooleanValue(getTextContent(n)));
            } else if ("multi-threading-enabled".equals(nodeName)) {
                tConfig.setMultiThreadingEnabled(getBooleanValue(getTextContent(n)));
            }
        }
        config.addTopicConfig(tConfig);
    }

    protected void handleReliableTopic(Node node) {
        Node attName = node.getAttributes().getNamedItem("name");
        String name = getTextContent(attName);
        ReliableTopicConfig topicConfig = new ReliableTopicConfig(name);
        handleReliableTopicNode(node, topicConfig);
    }

    void handleReliableTopicNode(Node node, final ReliableTopicConfig topicConfig) {
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if ("read-batch-size".equals(nodeName)) {
                String batchSize = getTextContent(n);
                topicConfig.setReadBatchSize(getIntegerValue("read-batch-size", batchSize));
            } else if ("statistics-enabled".equals(nodeName)) {
                topicConfig.setStatisticsEnabled(getBooleanValue(getTextContent(n)));
            } else if ("topic-overload-policy".equals(nodeName)) {
                TopicOverloadPolicy topicOverloadPolicy = TopicOverloadPolicy.valueOf(upperCaseInternal(getTextContent(n)));
                topicConfig.setTopicOverloadPolicy(topicOverloadPolicy);
            } else if ("message-listeners".equals(nodeName)) {
                handleMessageListeners(n, new Function<ListenerConfig, Void>() {
                    @Override
                    public Void apply(ListenerConfig listenerConfig) {
                        topicConfig.addMessageListenerConfig(listenerConfig);
                        return null;
                    }
                });
            }
        }
        config.addReliableTopicConfig(topicConfig);
    }

    void handleMessageListeners(Node n, Function<ListenerConfig, Void> configAddFunction) {
        for (Node listenerNode : childElements(n)) {
            if ("message-listener".equals(cleanNodeName(listenerNode))) {
                configAddFunction.apply(new ListenerConfig(getTextContent(listenerNode)));
            }
        }
    }

    protected void handleRingbuffer(Node node) {
        Node attName = node.getAttributes().getNamedItem("name");
        String name = getTextContent(attName);
        RingbufferConfig rbConfig = new RingbufferConfig(name);
        handleRingBufferNode(node, rbConfig);
    }

    void handleRingBufferNode(Node node, RingbufferConfig rbConfig) {
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            String value = getTextContent(n).trim();
            if ("capacity".equals(nodeName)) {
                int capacity = getIntegerValue("capacity", value);
                rbConfig.setCapacity(capacity);
            } else if ("backup-count".equals(nodeName)) {
                int backupCount = getIntegerValue("backup-count", value);
                rbConfig.setBackupCount(backupCount);
            } else if ("async-backup-count".equals(nodeName)) {
                int asyncBackupCount = getIntegerValue("async-backup-count", value);
                rbConfig.setAsyncBackupCount(asyncBackupCount);
            } else if ("time-to-live-seconds".equals(nodeName)) {
                int timeToLiveSeconds = getIntegerValue("time-to-live-seconds", value);
                rbConfig.setTimeToLiveSeconds(timeToLiveSeconds);
            } else if ("in-memory-format".equals(nodeName)) {
                InMemoryFormat inMemoryFormat = InMemoryFormat.valueOf(upperCaseInternal(value));
                rbConfig.setInMemoryFormat(inMemoryFormat);
            } else if ("ringbuffer-store".equals(nodeName)) {
                RingbufferStoreConfig ringbufferStoreConfig = createRingbufferStoreConfig(n);
                rbConfig.setRingbufferStoreConfig(ringbufferStoreConfig);
            } else if ("split-brain-protection-ref".equals(nodeName)) {
                rbConfig.setSplitBrainProtectionName(value);
            } else if ("merge-policy".equals(nodeName)) {
                MergePolicyConfig mergePolicyConfig = createMergePolicyConfig(n);
                rbConfig.setMergePolicyConfig(mergePolicyConfig);
            }
        }
        config.addRingBufferConfig(rbConfig);
    }

    protected void handleListeners(Node node) {
        for (Node child : childElements(node)) {
            if ("listener".equals(cleanNodeName(child))) {
                String listenerClass = getTextContent(child);
                config.addListenerConfig(new ListenerConfig(listenerClass));
            }
        }
    }

    private void handlePartitionGroup(Node node) {
        NamedNodeMap attributes = node.getAttributes();
        Node enabledNode = attributes.getNamedItem("enabled");
        boolean enabled = enabledNode != null && getBooleanValue(getTextContent(enabledNode));
        config.getPartitionGroupConfig().setEnabled(enabled);
        Node groupTypeNode = attributes.getNamedItem("group-type");
        MemberGroupType groupType = groupTypeNode != null
                ? PartitionGroupConfig.MemberGroupType.valueOf(upperCaseInternal(getTextContent(groupTypeNode)))
                : PartitionGroupConfig.MemberGroupType.PER_MEMBER;
        config.getPartitionGroupConfig().setGroupType(groupType);
        for (Node child : childElements(node)) {
            if ("member-group".equals(cleanNodeName(child))) {
                handleMemberGroup(child, config);
            }
        }
    }

    protected void handleMemberGroup(Node node, Config config) {
        MemberGroupConfig memberGroupConfig = new MemberGroupConfig();
        for (Node child : childElements(node)) {
            if ("interface".equals(cleanNodeName(child))) {
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
        NamedNodeMap attrs = node.getAttributes();

        Node enabledNode = attrs.getNamedItem("enabled");
        boolean enabled = enabledNode != null && getBooleanValue(getTextContent(enabledNode));

        Node intervalNode = attrs.getNamedItem("update-interval");
        int interval = intervalNode != null ? getIntegerValue("update-interval",
                getTextContent(intervalNode)) : ManagementCenterConfig.UPDATE_INTERVAL;

        ManagementCenterConfig managementCenterConfig = config.getManagementCenterConfig();
        managementCenterConfig.setEnabled(enabled);
        managementCenterConfig.setUpdateInterval(interval);

        Node scriptingEnabledNode = attrs.getNamedItem("scripting-enabled");
        if (scriptingEnabledNode != null) {
            managementCenterConfig.setScriptingEnabled(getBooleanValue(getTextContent(scriptingEnabledNode)));
        }

        handleManagementCenterChildElements(node, managementCenterConfig);
    }

    private void handleManagementCenterChildElements(Node node, ManagementCenterConfig managementCenterConfig) {
        // < 3.9 - Backwards compatibility
        boolean isComplexType = false;
        List<String> complexTypeElements = Arrays.asList("url", "mutual-auth");
        for (Node c : childElements(node)) {
            if (complexTypeElements.contains(c.getNodeName())) {
                isComplexType = true;
                break;
            }
        }

        if (!isComplexType) {
            String url = getTextContent(node);
            managementCenterConfig.setUrl("".equals(url) ? null : url);
        } else {
            for (Node child : childElements(node)) {
                if ("url".equals(cleanNodeName(child))) {
                    String url = getTextContent(child);
                    managementCenterConfig.setUrl(url);
                } else if ("mutual-auth".equals(cleanNodeName(child))) {
                    handleMcMutualAuthConfig(child);
                }
            }
        }
    }

    private void handleSecurity(Node node) {
        NamedNodeMap attributes = node.getAttributes();
        Node enabledNode = attributes.getNamedItem("enabled");
        boolean enabled = enabledNode != null && getBooleanValue(getTextContent(enabledNode));
        config.getSecurityConfig().setEnabled(enabled);
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if ("realms".equals(nodeName)) {
                handleRealms(child);
            } else if ("member-authentication".equals(nodeName)) {
                config.getSecurityConfig().setMemberRealm(getAttribute(child, "realm"));
            } else if ("client-authentication".equals(nodeName)) {
                config.getSecurityConfig().setClientRealm(getAttribute(child, "realm"));
            } else if ("client-permission-policy".equals(nodeName)) {
                handlePermissionPolicy(child);
            } else if ("client-permissions".equals(nodeName)) {
                handleSecurityPermissions(child);
            } else if ("security-interceptors".equals(nodeName)) {
                handleSecurityInterceptors(child);
            } else if ("client-block-unmapped-actions".equals(nodeName)) {
                config.getSecurityConfig().setClientBlockUnmappedActions(getBooleanValue(getTextContent(child)));
            }
        }
    }

    protected void handleRealms(Node node) {
        for (Node child : childElements(node)) {
            if ("realm".equals(cleanNodeName(child))) {
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
        if ("interceptor".equals(nodeName)) {
            NamedNodeMap attrs = child.getAttributes();
            Node classNameNode = attrs.getNamedItem("class-name");
            String className = getTextContent(classNameNode);
            cfg.addSecurityInterceptorConfig(new SecurityInterceptorConfig(className));
        }
    }

    protected void handleMemberAttributes(Node node) {
        for (Node n : childElements(node)) {
            String name = cleanNodeName(n);
            if (!"attribute".equals(name)) {
                continue;
            }
            String value = getTextContent(n);
            String attributeName = getTextContent(n.getAttributes().getNamedItem("name"));
            handleMemberAttributesNode(n, attributeName, value);
        }
    }

    void handleMemberAttributesNode(Node n, String attributeName, String value) {
        config.getMemberAttributeConfig().setAttribute(attributeName, value);
    }

    LoginModuleConfig handleLoginModule(Node node) {
        NamedNodeMap attrs = node.getAttributes();
        Node classNameNode = attrs.getNamedItem("class-name");
        String className = getTextContent(classNameNode);
        Node usageNode = attrs.getNamedItem("usage");
        LoginModuleConfig.LoginModuleUsage usage =
                usageNode != null ? LoginModuleConfig.LoginModuleUsage.get(getTextContent(usageNode))
                        : LoginModuleConfig.LoginModuleUsage.REQUIRED;
        LoginModuleConfig moduleConfig = new LoginModuleConfig(className, usage);
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if ("properties".equals(nodeName)) {
                fillProperties(child, moduleConfig.getProperties());
                break;
            }
        }
        return moduleConfig;
    }

    private void handlePermissionPolicy(Node node) {
        NamedNodeMap attrs = node.getAttributes();
        Node classNameNode = attrs.getNamedItem("class-name");
        String className = getTextContent(classNameNode);
        SecurityConfig cfg = config.getSecurityConfig();
        PermissionPolicyConfig policyConfig = new PermissionPolicyConfig(className);
        cfg.setClientPolicyConfig(policyConfig);
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if ("properties".equals(nodeName)) {
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
            PermissionType type;
            if ("map-permission".equals(nodeName)) {
                type = PermissionConfig.PermissionType.MAP;
            } else if ("queue-permission".equals(nodeName)) {
                type = PermissionConfig.PermissionType.QUEUE;
            } else if ("multimap-permission".equals(nodeName)) {
                type = PermissionConfig.PermissionType.MULTIMAP;
            } else if ("topic-permission".equals(nodeName)) {
                type = PermissionConfig.PermissionType.TOPIC;
            } else if ("list-permission".equals(nodeName)) {
                type = PermissionConfig.PermissionType.LIST;
            } else if ("set-permission".equals(nodeName)) {
                type = PermissionConfig.PermissionType.SET;
            } else if ("lock-permission".equals(nodeName)) {
                type = PermissionConfig.PermissionType.LOCK;
            } else if ("atomic-long-permission".equals(nodeName)) {
                type = PermissionConfig.PermissionType.ATOMIC_LONG;
            } else if ("atomic-reference-permission".equals(nodeName)) {
                type = PermissionConfig.PermissionType.ATOMIC_REFERENCE;
            } else if ("countdown-latch-permission".equals(nodeName)) {
                type = PermissionConfig.PermissionType.COUNTDOWN_LATCH;
            } else if ("semaphore-permission".equals(nodeName)) {
                type = PermissionConfig.PermissionType.SEMAPHORE;
            } else if ("flake-id-generator-permission".equals(nodeName)) {
                type = PermissionConfig.PermissionType.FLAKE_ID_GENERATOR;
            } else if ("executor-service-permission".equals(nodeName)) {
                type = PermissionConfig.PermissionType.EXECUTOR_SERVICE;
            } else if ("transaction-permission".equals(nodeName)) {
                type = PermissionConfig.PermissionType.TRANSACTION;
            } else if ("all-permissions".equals(nodeName)) {
                type = PermissionConfig.PermissionType.ALL;
            } else if ("durable-executor-service-permission".equals(nodeName)) {
                type = PermissionConfig.PermissionType.DURABLE_EXECUTOR_SERVICE;
            } else if ("cardinality-estimator-permission".equals(nodeName)) {
                type = PermissionConfig.PermissionType.CARDINALITY_ESTIMATOR;
            } else if ("scheduled-executor-permission".equals(nodeName)) {
                type = PermissionConfig.PermissionType.SCHEDULED_EXECUTOR;
            } else if ("pn-counter-permission".equals(nodeName)) {
                type = PermissionConfig.PermissionType.PN_COUNTER;
            } else if ("cache-permission".equals(nodeName)) {
                type = PermissionConfig.PermissionType.CACHE;
            } else if ("user-code-deployment-permission".equals(nodeName)) {
                type = PermissionConfig.PermissionType.USER_CODE_DEPLOYMENT;
            } else if (PermissionConfig.PermissionType.CONFIG.getNodeName().equals(nodeName)) {
                type = PermissionConfig.PermissionType.CONFIG;
            } else {
                continue;
            }
            handleSecurityPermission(child, type);
        }
    }

    void handleSecurityPermission(Node node, PermissionConfig.PermissionType type) {
        SecurityConfig cfg = config.getSecurityConfig();
        NamedNodeMap attrs = node.getAttributes();
        Node nameNode = attrs.getNamedItem("name");
        String name = nameNode != null ? getTextContent(nameNode) : "*";
        Node principalNode = attrs.getNamedItem("principal");
        String principal = principalNode != null ? getTextContent(principalNode) : "*";
        PermissionConfig permConfig = new PermissionConfig(type, name, principal);
        cfg.addClientPermissionConfig(permConfig);
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if ("endpoints".equals(nodeName)) {
                handleSecurityPermissionEndpoints(child, permConfig);
            } else if ("actions".equals(nodeName)) {
                handleSecurityPermissionActions(child, permConfig);
            }
        }
    }

    void handleSecurityPermissionEndpoints(Node node, PermissionConfig permConfig) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if ("endpoint".equals(nodeName)) {
                permConfig.addEndpoint(getTextContent(child).trim());
            }
        }
    }

    void handleSecurityPermissionActions(Node node, PermissionConfig permConfig) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if ("action".equals(nodeName)) {
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
            if ("endpoint-group".equals(nodeName)) {
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
            if ("raft-algorithm".equals(nodeName)) {
                handleRaftAlgorithm(cpSubsystemConfig.getRaftAlgorithmConfig(), child);
            } else if ("semaphores".equals(nodeName)) {
                handleSemaphores(cpSubsystemConfig, child);
            } else if ("locks".equals(nodeName)) {
                handleFencedLocks(cpSubsystemConfig, child);
            } else {
                String value = getTextContent(child).trim();
                if ("cp-member-count".equals(nodeName)) {
                    cpSubsystemConfig.setCPMemberCount(Integer.parseInt(value));
                } else if ("group-size".equals(nodeName)) {
                    cpSubsystemConfig.setGroupSize(Integer.parseInt(value));
                } else if ("session-time-to-live-seconds".equals(nodeName)) {
                    cpSubsystemConfig.setSessionTimeToLiveSeconds(Integer.parseInt(value));
                } else if ("session-heartbeat-interval-seconds".equals(nodeName)) {
                    cpSubsystemConfig.setSessionHeartbeatIntervalSeconds(Integer.parseInt(value));
                } else if ("missing-cp-member-auto-removal-seconds".equals(nodeName)) {
                    cpSubsystemConfig.setMissingCPMemberAutoRemovalSeconds(Integer.parseInt(value));
                } else if ("fail-on-indeterminate-operation-state".equals(nodeName)) {
                    cpSubsystemConfig.setFailOnIndeterminateOperationState(Boolean.parseBoolean(value));
                } else if ("persistence-enabled".equals(nodeName)) {
                    cpSubsystemConfig.setPersistenceEnabled(Boolean.parseBoolean(value));
                } else if ("base-dir".equals(nodeName)) {
                    cpSubsystemConfig.setBaseDir(new File(value).getAbsoluteFile());
                } else if ("data-load-timeout-seconds".equals(nodeName)) {
                    cpSubsystemConfig.setDataLoadTimeoutSeconds(Integer.parseInt(value));
                }
            }
        }
    }

    private void handleRaftAlgorithm(RaftAlgorithmConfig raftAlgorithmConfig, Node node) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            String value = getTextContent(child).trim();
            if ("leader-election-timeout-in-millis".equals(nodeName)) {
                raftAlgorithmConfig.setLeaderElectionTimeoutInMillis(Long.parseLong(value));
            } else if ("leader-heartbeat-period-in-millis".equals(nodeName)) {
                raftAlgorithmConfig.setLeaderHeartbeatPeriodInMillis(Long.parseLong(value));
            } else if ("max-missed-leader-heartbeat-count".equals(nodeName)) {
                raftAlgorithmConfig.setMaxMissedLeaderHeartbeatCount(Integer.parseInt(value));
            } else if ("append-request-max-entry-count".equals(nodeName)) {
                raftAlgorithmConfig.setAppendRequestMaxEntryCount(Integer.parseInt(value));
            } else if ("commit-index-advance-count-to-snapshot".equals(nodeName)) {
                raftAlgorithmConfig.setCommitIndexAdvanceCountToSnapshot(Integer.parseInt(value));
            } else if ("uncommitted-entry-count-to-reject-new-appends".equals(nodeName)) {
                raftAlgorithmConfig.setUncommittedEntryCountToRejectNewAppends(Integer.parseInt(value));
            } else if ("append-request-backoff-timeout-in-millis".equals(nodeName)) {
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
                if ("name".equals(nodeName)) {
                    semaphoreConfig.setName(value);
                } else if ("jdk-compatible".equals(nodeName)) {
                    semaphoreConfig.setJDKCompatible(Boolean.parseBoolean(value));
                } else if ("initial-permits".equals(nodeName)) {
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
                if ("name".equals(nodeName)) {
                    lockConfig.setName(value);
                } else if ("lock-acquire-limit".equals(nodeName)) {
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
            if ("enabled".equals(att.getNodeName())) {
                boolean enabled = getBooleanValue(getAttribute(node, "enabled"));
                metricsConfig.setEnabled(enabled);
            } else if ("mc-enabled".equals(att.getNodeName())) {
                boolean enabled = getBooleanValue(getAttribute(node, "mc-enabled"));
                metricsConfig.setMcEnabled(enabled);
            } else if ("jmx-enabled".equals(att.getNodeName())) {
                boolean enabled = getBooleanValue(getAttribute(node, "jmx-enabled"));
                metricsConfig.setJmxEnabled(enabled);
            }
        }

        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            String value = getTextContent(child).trim();
            if ("collection-interval-seconds".equals(nodeName)) {
                metricsConfig.setCollectionIntervalSeconds(Integer.parseInt(value));
            } else if ("retention-seconds".equals(nodeName)) {
                metricsConfig.setRetentionSeconds(Integer.parseInt(value));
            } else if ("metrics-for-data-structures".equals(nodeName)) {
                metricsConfig.setMetricsForDataStructuresEnabled(Boolean.parseBoolean(value));
            } else if ("minimum-level".equals(nodeName)) {
                metricsConfig.setMinimumLevel(ProbeLevel.valueOf(value));
            }
        }
    }

    protected void handleRealm(Node node) {
        String realmName = getAttribute(node, "name");
        RealmConfig realmConfig = new RealmConfig();
        config.getSecurityConfig().addRealmConfig(realmName, realmConfig);
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if ("authentication".equals(nodeName)) {
                handleAuthentication(realmConfig, child);
            } else if ("identity".contentEquals(nodeName)) {
                handleIdentity(realmConfig, child);
            }
        }
    }

    private void handleAuthentication(RealmConfig realmConfig, Node node) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if ("jaas".equals(nodeName)) {
                handleJaasAuthentication(realmConfig, child);
            } else if ("tls".contentEquals(nodeName)) {
                handleTlsAuthentication(realmConfig, child);
            } else if ("ldap".contentEquals(nodeName)) {
                handleLdapAuthentication(realmConfig, child);
            }
        }
    }

    private void handleIdentity(RealmConfig realmConfig, Node node) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if ("username-password".equals(nodeName)) {
                realmConfig.setUsernamePasswordIdentityConfig(getAttribute(child, "username"), getAttribute(child, "password"));
            } else if ("credentials-factory".contentEquals(nodeName)) {
                handleCredentialsFactory(realmConfig, child);
            } else if ("token".contentEquals(nodeName)) {
                handleToken(realmConfig, child);
            }
        }
    }

    protected void handleToken(RealmConfig realmConfig, Node node) {
        TokenEncoding encoding = TokenEncoding.getTokenEncoding(getAttribute(node, "encoding"));
        TokenIdentityConfig tic = new TokenIdentityConfig(encoding, getTextContent(node));
        realmConfig.setTokenIdentityConfig(tic);
    }

    protected void handleTlsAuthentication(RealmConfig realmConfig, Node node) {
        String roleAttribute = getAttribute(node, "roleAttribute");
        TlsAuthenticationConfig tlsCfg = new TlsAuthenticationConfig();

        if (roleAttribute != null) {
            tlsCfg.setRoleAttribute(roleAttribute);
        }
        realmConfig.setTlsAuthenticationConfig(tlsCfg);
    }

    protected void handleLdapAuthentication(RealmConfig realmConfig, Node node) {
        LdapAuthenticationConfig ldapCfg = new LdapAuthenticationConfig();
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if ("url".equals(nodeName)) {
                ldapCfg.setUrl(getTextContent(child));
            } else if ("socket-factory-class-name".contentEquals(nodeName)) {
                ldapCfg.setSocketFactoryClassName(getTextContent(child));
            } else if ("parse-dn".contentEquals(nodeName)) {
                ldapCfg.setParseDn(getBooleanValue(getTextContent(child)));
            } else if ("role-context".contentEquals(nodeName)) {
                ldapCfg.setRoleContext(getTextContent(child));
            } else if ("role-filter".contentEquals(nodeName)) {
                ldapCfg.setRoleFilter(getTextContent(child));
            } else if ("role-mapping-attribute".contentEquals(nodeName)) {
                ldapCfg.setRoleMappingAttribute(getTextContent(child));
            } else if ("role-mapping-mode".contentEquals(nodeName)) {
                ldapCfg.setRoleMappingMode(getRoleMappingMode(getTextContent(child)));
            } else if ("role-name-attribute".contentEquals(nodeName)) {
                ldapCfg.setRoleNameAttribute(getTextContent(child));
            } else if ("role-recursion-max-depth".contentEquals(nodeName)) {
                ldapCfg.setRoleRecursionMaxDepth(getIntegerValue("role-recursion-max-depth", getTextContent(child)));
            } else if ("role-search-scope".contentEquals(nodeName)) {
                ldapCfg.setRoleSearchScope(getSearchScope(getTextContent(child)));
            } else if ("user-name-attribute".contentEquals(nodeName)) {
                ldapCfg.setUserNameAttribute(getTextContent(child));
            } else if ("system-user-dn".contentEquals(nodeName)) {
                ldapCfg.setSystemUserDn(getTextContent(child));
            } else if ("system-user-password".contentEquals(nodeName)) {
                ldapCfg.setSystemUserPassword(getTextContent(child));
            } else if ("password-attribute".contentEquals(nodeName)) {
                ldapCfg.setPasswordAttribute(getTextContent(child));
            } else if ("user-context".contentEquals(nodeName)) {
                ldapCfg.setUserContext(getTextContent(child));
            } else if ("user-filter".contentEquals(nodeName)) {
                ldapCfg.setUserFilter(getTextContent(child));
            } else if ("user-search-scope".contentEquals(nodeName)) {
                ldapCfg.setUserSearchScope(getSearchScope(getTextContent(child)));
            }
        }
        realmConfig.setLdapAuthenticationConfig(ldapCfg);
    }

    protected void handleJaasAuthentication(RealmConfig realmConfig, Node node) {
        JaasAuthenticationConfig jaasAuthenticationConfig = new JaasAuthenticationConfig();
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if ("login-module".equals(nodeName)) {
                jaasAuthenticationConfig.addLoginModuleConfig(handleLoginModule(child));
            }
        }
        realmConfig.setJaasAuthenticationConfig(jaasAuthenticationConfig);
    }

    private void handleCredentialsFactory(RealmConfig realmConfig, Node node) {
        String className = getAttribute(node, "class-name");
        CredentialsFactoryConfig credentialsFactoryConfig = new CredentialsFactoryConfig(className);
        realmConfig.setCredentialsFactoryConfig(credentialsFactoryConfig);
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if ("properties".equals(nodeName)) {
                fillProperties(child, credentialsFactoryConfig.getProperties());
            }
        }
    }
}
