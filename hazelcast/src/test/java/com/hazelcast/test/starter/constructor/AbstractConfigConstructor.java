/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.starter.constructor;

import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.spi.merge.HigherHitsMergePolicy;
import com.hazelcast.spi.merge.LatestAccessMergePolicy;
import com.hazelcast.spi.merge.LatestUpdateMergePolicy;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.test.starter.HazelcastProxyFactory.ProxyPolicy.RETURN_SAME;
import static com.hazelcast.test.starter.HazelcastProxyFactory.generateProxyForInterface;
import static com.hazelcast.test.starter.HazelcastProxyFactory.proxyObjectForStarter;
import static com.hazelcast.test.starter.HazelcastProxyFactory.shouldProxy;
import static com.hazelcast.test.starter.HazelcastStarterUtils.debug;
import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;
import static com.hazelcast.test.starter.ReflectionUtils.getSetter;
import static com.hazelcast.test.starter.ReflectionUtils.hasField;
import static com.hazelcast.test.starter.ReflectionUtils.invokeMethod;
import static com.hazelcast.test.starter.ReflectionUtils.invokeSetter;
import static com.hazelcast.test.starter.ReflectionUtils.setFieldValueReflectively;
import static java.lang.reflect.Proxy.isProxyClass;

/**
 * Abstract class for constructors of config classes.
 */
abstract class AbstractConfigConstructor extends AbstractStarterObjectConstructor {

    AbstractConfigConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    static Object cloneConfig(Object thisConfigObject, ClassLoader classloader) throws Exception {
        if (thisConfigObject == null) {
            return null;
        }

        Class<?> thisConfigClass = thisConfigObject.getClass();
        if (shouldProxy(thisConfigClass, new Class[0]) == RETURN_SAME) {
            return thisConfigObject;
        }

        Class<?> otherConfigClass = classloader.loadClass(thisConfigClass.getName());
        if (isSplitBrainProtectionFunctionImplementation(thisConfigClass)) {
            return cloneSplitBrainProtectionFunctionImplementation(thisConfigObject, otherConfigClass);
        }

        if (isEvictionConfig(thisConfigClass)) {
            return cloneEvictionConfig(thisConfigObject, otherConfigClass);
        }

        if (isWanReplicationRef(thisConfigClass)) {
            return cloneWanReplicationRef(thisConfigObject, otherConfigClass);
        }

        if (isWanReplicationConfig(thisConfigClass)) {
            return cloneWanReplicationConfig(thisConfigObject, otherConfigClass);
        }

        // RU_COMPAT_4_1
        // since empty constructor is added this could be reverted in later versions
        Object otherConfigObject;
        if (thisConfigClass.getName().equals("com.hazelcast.config.JavaKeyStoreSecureStoreConfig")) {
            otherConfigObject = cloneJavaKeyStoreConfig(thisConfigObject, otherConfigClass);
        } else {
            otherConfigObject = ClassLoaderUtil.newInstance(otherConfigClass.getClassLoader(), otherConfigClass.getName());
        }

        for (Method method : thisConfigClass.getMethods()) {
            if (!isGetter(method)) {
                continue;
            }
            Class<?> returnType = method.getReturnType();
            Class<?> otherReturnType;
            try {
                otherReturnType = getOtherReturnType(classloader, returnType);
            } catch (ClassNotFoundException e) {
                // new configuration option, return type was not found in target classloader
                debug("Configuration option %s is not available in target classloader: %s",
                        method.getName(), e.getMessage());
                continue;
            }

            Method setter = getSetter(otherConfigClass, otherReturnType, createSetterName(method));
            if (setter != null) {
                String returnTypeName = returnType.getName();
                if (isMapStoreConfig(thisConfigClass) && method.getName().equals("getImplementation")) {
                    proxyMapStoreImplementations(thisConfigObject, otherConfigObject);
                } else if (Properties.class.isAssignableFrom(returnType)) {
                    Properties original = (Properties) method.invoke(thisConfigObject);
                    invokeMethod(setter, otherConfigObject, copy(original));
                } else if (Map.class.isAssignableFrom(returnType) || ConcurrentMap.class.isAssignableFrom(returnType)) {
                    @SuppressWarnings("unchecked")
                    Map<Object, Object> map = (Map<Object, Object>) method.invoke(thisConfigObject);
                    Map<Object, Object> otherMap = ConcurrentMap.class.isAssignableFrom(returnType)
                            ? new ConcurrentHashMap<>() : new HashMap<>();
                    copyMap(map, otherMap, classloader);
                    invokeMethod(setter, otherConfigObject, otherMap);
                } else if (returnType.equals(List.class)) {
                    List<?> list = (List<?>) method.invoke(thisConfigObject);
                    List<Object> otherList = new ArrayList<>();
                    for (Object item : list) {
                        Object otherItem = cloneConfig(item, classloader);
                        otherList.add(otherItem);
                    }
                    invokeMethod(setter, otherConfigObject, otherList);
                } else if (returnType.isEnum()) {
                    Enum<?> thisSubConfigObject = (Enum<?>) method.invoke(thisConfigObject);
                    Object otherEnumValue = cloneEnum(classloader, thisSubConfigObject.getClass().getName(), thisSubConfigObject);
                    invokeMethod(setter, otherConfigObject, otherEnumValue);
                } else if (returnTypeName.startsWith("java") || returnType.isPrimitive()) {
                    Object thisSubConfigObject = method.invoke(thisConfigObject);
                    invokeMethod(setter, otherConfigObject, thisSubConfigObject);
                } else if (returnTypeName.equals("com.hazelcast.ringbuffer.RingbufferStore")
                        || returnTypeName.equals("com.hazelcast.ringbuffer.RingbufferStoreFactory")
                        || returnTypeName.equals("com.hazelcast.collection.QueueStore")
                        || returnTypeName.equals("com.hazelcast.collection.QueueStoreFactory")) {
                    cloneStoreInstance(classloader, method, setter, thisConfigObject, otherConfigObject, returnTypeName);
                } else if (returnTypeName.startsWith("com.hazelcast.memory.MemorySize")) {
                    // ignore
                } else if (returnTypeName.startsWith("com.hazelcast")) {
                    Object thisSubConfigObject = method.invoke(thisConfigObject);
                    Object otherSubConfig = cloneConfig(thisSubConfigObject, classloader);
                    invokeMethod(setter, otherConfigObject, otherSubConfig);
                }
            }
        }

        if (isConfig(thisConfigClass)) {
            cloneGroupConfig(thisConfigObject, otherConfigObject);
            cloneMerkleTreeConfig(thisConfigObject, otherConfigObject);
        }

        return otherConfigObject;
    }

    private static void cloneMerkleTreeConfig(Object thisConfigObject, Object otherConfigObject) throws Exception {
        boolean is4_x = !hasField(thisConfigObject.getClass(), "mapMerkleTreeConfigs");
        if (is4_x) {
            // copying from 4.0 to 3.12
            Map<String, Object> mapConfigs = getFieldValueReflectively(thisConfigObject, "mapConfigs");
            Map<String, Object> merkleTreeConfigs = getFieldValueReflectively(otherConfigObject, "mapMerkleTreeConfigs");

            for (Entry<String, Object> mapConfigEntry : mapConfigs.entrySet()) {
                String mapName = mapConfigEntry.getKey();
                Object mapConfig = mapConfigEntry.getValue();
                Object merkleTreeConfig = getFieldValueReflectively(mapConfig, "merkleTreeConfig");
                boolean isEnabled = getFieldValueReflectively(merkleTreeConfig, "enabled");
                int depth = getFieldValueReflectively(merkleTreeConfig, "depth");

                Object otherMerkleTree = ClassLoaderUtil.newInstance(otherConfigObject.getClass().getClassLoader(),
                        "com.hazelcast.config.MerkleTreeConfig");
                setFieldValueReflectively(otherMerkleTree, "enabled", isEnabled);
                setFieldValueReflectively(otherMerkleTree, "depth", depth);
                setFieldValueReflectively(otherMerkleTree, "mapName", mapName);
                merkleTreeConfigs.put(mapName, otherMerkleTree);
            }
        } else {
            // copying from 3.12 to 4.0
            Map<String, Object> merkleTreeConfigs = getFieldValueReflectively(thisConfigObject, "mapMerkleTreeConfigs");

            for (Entry<String, Object> merkleTreeEntry : merkleTreeConfigs.entrySet()) {
                String mapName = merkleTreeEntry.getKey();
                Object merkleTreeConfig = merkleTreeEntry.getValue();
                boolean isEnabled = getFieldValueReflectively(merkleTreeConfig, "enabled");
                int depth = getFieldValueReflectively(merkleTreeConfig, "depth");

                Method getMapConfigMethod = otherConfigObject.getClass().getMethod("getMapConfig", String.class);
                Object mapConfig = invokeMethod(getMapConfigMethod, otherConfigObject, mapName);
                Object otherMerkleTree = getFieldValueReflectively(mapConfig, "merkleTreeConfig");
                setFieldValueReflectively(otherMerkleTree, "enabled", isEnabled);
                setFieldValueReflectively(otherMerkleTree, "depth", depth);
            }
        }
    }

    private static void copyMap(Map<Object, Object> source, Map<Object, Object> destination, ClassLoader classLoader) throws Exception {
        for (Entry<Object, Object> entry : source.entrySet()) {
            // keys are either Strings or, since 3.12, EndpointQualifiers
            Object key = entry.getKey();
            Object mappedKey = proxyObjectForStarter(classLoader, key);
            Object value = entry.getValue();
            Object otherMapItem = cloneConfig(value, classLoader);
            destination.put(mappedKey, otherMapItem);
        }
    }

    private static boolean isGetter(Method method) {
        if (!method.getName().startsWith("get") && !method.getName().startsWith("is")) {
            return false;
        }
        if (method.getParameterTypes().length != 0) {
            return false;
        }
        return !void.class.equals(method.getReturnType());
    }

    private static Class<?> getOtherReturnType(ClassLoader classloader, Class<?> returnType) throws Exception {
        String returnTypeName = returnType.getName();
        if (returnTypeName.startsWith("com.hazelcast")) {
            return classloader.loadClass(returnTypeName);
        }
        return returnType;
    }

    /**
     * Creates a proxy class for a store implementation from the current
     * classloader for the proxied classloader.
     */
    private static void cloneStoreInstance(ClassLoader targetClassLoader,
                                           Method storeGetter,
                                           Method targetStoreSetter,
                                           Object thisConfigObject,
                                           Object otherConfigObject,
                                           String targetStoreClass) throws Exception {
        Object thisStoreObject = storeGetter.invoke(thisConfigObject);
        if (thisStoreObject == null) {
            return;
        }
        Class<?> thisStoreClass = thisStoreObject.getClass();
        if (isProxyClass(thisStoreClass) || targetClassLoader.equals(thisStoreClass.getClassLoader())) {
            invokeMethod(targetStoreSetter, otherConfigObject, thisStoreObject);
        } else {
            Class<?> otherStoreClass = targetClassLoader.loadClass(targetStoreClass);
            Object otherStoreObject = generateProxyForInterface(thisStoreObject, targetClassLoader, otherStoreClass);
            invokeMethod(targetStoreSetter, otherConfigObject, otherStoreObject);
        }
    }

    private static String createSetterName(Method getter) {
        if (getter.getName().startsWith("get")) {
            return "s" + getter.getName().substring(1);
        }
        if (getter.getName().startsWith("is")) {
            return "set" + getter.getName().substring(2);
        }
        throw new IllegalArgumentException("Unknown getter method name: " + getter.getName());
    }

    private static Properties copy(Properties original) {
        if (original == null) {
            return null;
        }
        Properties copy = new Properties();
        for (String name : original.stringPropertyNames()) {
            copy.setProperty(name, original.getProperty(name));
        }
        return copy;
    }

    /**
     * Clones the built-in SplitBrainProtectionFunction implementations.
     */
    private static Object cloneSplitBrainProtectionFunctionImplementation(Object splitBrainProtectionFunction,
                                                                          Class<?> targetClass) throws Exception {
        if (targetClass.getName().equals("com.hazelcast.splitbrainprotection.impl.ProbabilisticSplitBrainProtectionFunction")) {
            int size = getFieldValueReflectively(splitBrainProtectionFunction, "splitBrainProtectionSize");
            double suspicionThreshold = getFieldValueReflectively(splitBrainProtectionFunction, "suspicionThreshold");
            int maxSampleSize = getFieldValueReflectively(splitBrainProtectionFunction, "maxSampleSize");
            long minStdDeviationMillis = getFieldValueReflectively(splitBrainProtectionFunction, "minStdDeviationMillis");
            long acceptableHeartbeatPauseMillis = getFieldValueReflectively(splitBrainProtectionFunction,
                    "acceptableHeartbeatPauseMillis");
            long heartbeatIntervalMillis = getFieldValueReflectively(splitBrainProtectionFunction, "heartbeatIntervalMillis");

            Constructor<?> constructor = targetClass.getConstructor(Integer.TYPE, Long.TYPE, Long.TYPE, Integer.TYPE, Long.TYPE,
                    Double.TYPE);

            return constructor.newInstance(size, heartbeatIntervalMillis, acceptableHeartbeatPauseMillis,
                    maxSampleSize, minStdDeviationMillis, suspicionThreshold);
        } else if (targetClass.getName().equals("com.hazelcast.splitbrainprotection.impl.RecentlyActiveSplitBrainProtectionFunction")) {
            int size = getFieldValueReflectively(splitBrainProtectionFunction, "splitBrainProtectionSize");
            int heartbeatToleranceMillis = getFieldValueReflectively(splitBrainProtectionFunction, "heartbeatToleranceMillis");

            Constructor<?> constructor = targetClass.getConstructor(Integer.TYPE, Integer.TYPE);
            return constructor.newInstance(size, heartbeatToleranceMillis);
        } else {
            debug("Did not handle configured SplitBrainProtectionFunction implementation %s", targetClass.getName());
            return null;
        }
    }

    /**
     * Clones EvictionConfig.
     */
    private static Object cloneEvictionConfig(Object thisConfigObject, Class<?> otherConfigClass) throws Exception {
        // doesn't support comparator instances
        int size = getFieldValueReflectively(thisConfigObject, "size");
        Object maxSizePolicy = getFieldValueReflectively(thisConfigObject, "maxSizePolicy");
        Object evictionPolicy = getFieldValueReflectively(thisConfigObject, "evictionPolicy");
        String comparatorClassName = getFieldValueReflectively(thisConfigObject, "comparatorClassName");

        Constructor<?> constructor = otherConfigClass.getConstructor();
        Object thatConfigObject = constructor.newInstance();

        invokeSetter(thatConfigObject, "setSize", int.class, size);
        boolean is4_x = !hasField(thisConfigObject.getClass(), "readOnly");
        String otherMSPClassName;
        String methodName;
        if (is4_x) {
            // transforming from 4.0 to 3.12
            otherMSPClassName = "com.hazelcast.config.EvictionConfig$MaxSizePolicy";
            methodName = "setMaximumSizePolicy";
        } else {
            // transforming from 3.12 to 4.0
            otherMSPClassName = "com.hazelcast.config.MaxSizePolicy";
            methodName = "setMaxSizePolicy";
        }

        Enum<?> otherMSP = cloneEnum(otherConfigClass.getClassLoader(), otherMSPClassName, maxSizePolicy);
        invokeSetter(thatConfigObject, methodName, otherMSP.getClass(), otherMSP);

        Enum<?> otherEP = cloneEnum(otherConfigClass.getClassLoader(), evictionPolicy.getClass().getName(), evictionPolicy);
        invokeSetter(thatConfigObject, "setEvictionPolicy", otherEP.getClass(), otherEP);

        if (comparatorClassName != null) {
            invokeSetter(thatConfigObject, "setComparatorClassName", String.class, comparatorClassName);
        }

        return thatConfigObject;
    }

    /**
     * Clones the WanReplicationRef configuration.
     */
    private static Object cloneWanReplicationRef(Object wanReplicationRef, Class<?> targetClass) throws Exception {
        String name = getFieldValueReflectively(wanReplicationRef, "name");
        boolean republishingEnabled = getFieldValueReflectively(wanReplicationRef, "republishingEnabled");
        boolean is4_x = hasField(wanReplicationRef.getClass(), "mergePolicyClassName");
        String mergePolicyClassName;
        if (is4_x) {
            // transforming from 4.0 to 3.12
            mergePolicyClassName = getFieldValueReflectively(wanReplicationRef, "mergePolicyClassName");
        } else {
            // transforming from 3.12 to 4.0
            mergePolicyClassName = getFieldValueReflectively(wanReplicationRef, "mergePolicy");
            switch (mergePolicyClassName) {
                case "com.hazelcast.map.merge.HigherHitsMapMergePolicy":
                case "com.hazelcast.cache.merge.HigherHitsCacheMergePolicy":
                    mergePolicyClassName = HigherHitsMergePolicy.class.getName();
                    break;
                case "com.hazelcast.map.merge.LatestUpdateMapMergePolicy":
                    mergePolicyClassName = LatestUpdateMergePolicy.class.getName();
                    break;
                case "com.hazelcast.cache.merge.LatestAccessCacheMergePolicy":
                    mergePolicyClassName = LatestAccessMergePolicy.class.getName();
                    break;
                case "com.hazelcast.map.merge.PassThroughMergePolicy":
                case "com.hazelcast.cache.merge.PassThroughCacheMergePolicy":
                    mergePolicyClassName = PassThroughMergePolicy.class.getName();
                    break;
                case "com.hazelcast.map.merge.PutIfAbsentMapMergePolicy":
                case "com.hazelcast.cache.merge.PutIfAbsentCacheMergePolicy":
                    mergePolicyClassName = PutIfAbsentMergePolicy.class.getName();
                    break;
            }
        }

        List<String> filters = getFieldValueReflectively(wanReplicationRef, "filters");
        Constructor<?> constructor = targetClass.getConstructor(String.class, String.class, List.class, Boolean.TYPE);
        return constructor.newInstance(name, mergePolicyClassName, filters, republishingEnabled);
    }

    /**
     * Clones the WanReplicationConfig configuration to the target class and
     * classloader.
     */
    private static Object cloneWanReplicationConfig(Object wanReplicationConfig, Class<?> targetClass) throws Exception {
        String name = getFieldValueReflectively(wanReplicationConfig, "name");
        Object otherConfig = ClassLoaderUtil.newInstance(targetClass.getClassLoader(), targetClass.getName());
        invokeSetter(otherConfig, "setName", String.class, name);
        boolean is4_x = hasField(wanReplicationConfig.getClass(), "batchPublisherConfigs");

        if (is4_x) {
            // copying from 4.0 to 3.12
            // not supported: custom publisher configuration, publisher implementation
            // responseTimeoutMillis, discoveryPeriodSeconds, maxTargetEndpoints
            // useEndpointPrivateAddress, idleMinParkNs, idleMaxParkNs
            // awsConfig, gcpConfig, azureConfig, kubernetesConfig, eurekaConfig, discoveryConfig
            Object consumerConfig = getFieldValueReflectively(wanReplicationConfig, "consumerConfig");

            if (consumerConfig != null) {
                Object convertedConsumer = cloneConfig(consumerConfig, targetClass.getClassLoader());
                invokeSetter(otherConfig, "setWanConsumerConfig", convertedConsumer.getClass(), convertedConsumer);
            }

            List<Object> batchPublisherConfigs = getFieldValueReflectively(wanReplicationConfig, "batchPublisherConfigs");
            ArrayList<Object> convertedPublishers = new ArrayList<>(batchPublisherConfigs.size());
            for (Object publisherConfig : batchPublisherConfigs) {
                String clusterName = getFieldValueReflectively(publisherConfig, "clusterName");
                boolean snapshotEnabled = getFieldValueReflectively(publisherConfig, "snapshotEnabled");
                Object initialPublisherState = getFieldValueReflectively(publisherConfig, "initialPublisherState");
                int queueCapacity = getFieldValueReflectively(publisherConfig, "queueCapacity");
                int batchSize = getFieldValueReflectively(publisherConfig, "batchSize");
                int batchMaxDelayMillis = getFieldValueReflectively(publisherConfig, "batchMaxDelayMillis");
                Object queueFullBehavior = getFieldValueReflectively(publisherConfig, "queueFullBehavior");
                Object acknowledgeType = getFieldValueReflectively(publisherConfig, "acknowledgeType");
                int maxConcurrentInvocations = getFieldValueReflectively(publisherConfig, "maxConcurrentInvocations");
                String targetEndpoints = getFieldValueReflectively(publisherConfig, "targetEndpoints");
                Object syncConfig = getFieldValueReflectively(publisherConfig, "syncConfig");
                Object consistencyCheckStrategy = getFieldValueReflectively(syncConfig, "consistencyCheckStrategy");
                String endpoint = getFieldValueReflectively(publisherConfig, "endpoint");
                String publisherId = getFieldValueReflectively(publisherConfig, "publisherId");

                Object convertedConfig = ClassLoaderUtil.newInstance(targetClass.getClassLoader(), "com.hazelcast.config.WanPublisherConfig");
                invokeSetter(convertedConfig, "setGroupName", String.class, clusterName);
                invokeSetter(convertedConfig, "setClassName", String.class, "com.hazelcast.enterprise.wan.replication.WanBatchReplication");
                invokeSetter(convertedConfig, "setEndpoint", String.class, endpoint);
                invokeSetter(convertedConfig, "setQueueCapacity", int.class, queueCapacity);
                invokeSetter(convertedConfig, "setPublisherId", String.class, publisherId);

                Enum<?> otherIPS = cloneEnum(targetClass.getClassLoader(), "com.hazelcast.config.WanPublisherState", initialPublisherState);
                invokeSetter(convertedConfig, "setInitialPublisherState", otherIPS.getClass(), otherIPS);

                Enum<?> otherQFB = cloneEnum(targetClass.getClassLoader(), "com.hazelcast.config.WANQueueFullBehavior", queueFullBehavior);
                invokeSetter(convertedConfig, "setQueueFullBehavior", otherQFB.getClass(), otherQFB);

                Object convertedSyncConfig = getFieldValueReflectively(convertedConfig, "wanSyncConfig");
                Enum<?> otherCCS = cloneEnum(targetClass.getClassLoader(), "com.hazelcast.config.ConsistencyCheckStrategy", consistencyCheckStrategy);
                invokeSetter(convertedSyncConfig, "setConsistencyCheckStrategy", otherCCS.getClass(), otherCCS);

                HashMap<Object, Object> props = new HashMap<>();
                props.put("group.password", "dev-pass");
                props.put("endpoints", targetEndpoints);
                props.put("snapshot.enabled", snapshotEnabled);
                props.put("batch.size", batchSize);
                props.put("batch.max.delay.millis", batchMaxDelayMillis);
                props.put("ack.type", acknowledgeType.toString());
                props.put("max.concurrent.invocations", maxConcurrentInvocations);

                invokeSetter(convertedConfig, "setProperties", Map.class, props);
                convertedPublishers.add(convertedConfig);
            }
            invokeSetter(otherConfig, "setWanPublisherConfigs", List.class, convertedPublishers);
        } else {
            // copying from 3.12 to 4.0
            // not supported: custom publisher configuration, publisher implementation
            // responseTimeoutMillis, discoveryPeriodSeconds, maxTargetEndpoints
            // useEndpointPrivateAddress, idleMinParkNs, idleMaxParkNs
            // awsConfig, gcpConfig, azureConfig, kubernetesConfig, eurekaConfig, discoveryConfig
            Object consumerConfig = getFieldValueReflectively(wanReplicationConfig, "wanConsumerConfig");
            List<Object> wanPublisherConfigs = getFieldValueReflectively(wanReplicationConfig, "wanPublisherConfigs");
            ArrayList<Object> convertedPublishers = new ArrayList<>(wanPublisherConfigs.size());

            if (consumerConfig != null) {
                Object convertedConsumer = cloneConfig(consumerConfig, targetClass.getClassLoader());
                invokeSetter(otherConfig, "setConsumerConfig", convertedConsumer.getClass(), convertedConsumer);
            }

            for (Object publisherConfig : wanPublisherConfigs) {
                String groupName = getFieldValueReflectively(publisherConfig, "groupName");
                String publisherId = getFieldValueReflectively(publisherConfig, "publisherId");
                int queueCapacity = getFieldValueReflectively(publisherConfig, "queueCapacity");
                Object queueFullBehavior = getFieldValueReflectively(publisherConfig, "queueFullBehavior");
                Object initialPublisherState = getFieldValueReflectively(publisherConfig, "initialPublisherState");
                Map<String, Comparable<?>> properties = getFieldValueReflectively(publisherConfig, "properties");
                String className = getFieldValueReflectively(publisherConfig, "className");
                Object syncConfig = getFieldValueReflectively(publisherConfig, "wanSyncConfig");
                Object consistencyCheckStrategy = getFieldValueReflectively(syncConfig, "consistencyCheckStrategy");
                String endpoint = getFieldValueReflectively(publisherConfig, "endpoint");

                if (!className.equals("com.hazelcast.enterprise.wan.replication.WanBatchReplication")) {
                    // not copying custom replication
                    continue;
                }

                Object convertedConfig = ClassLoaderUtil.newInstance(targetClass.getClassLoader(),
                        "com.hazelcast.config.WanBatchPublisherConfig");
                invokeSetter(convertedConfig, "setClusterName", String.class, groupName);
                invokeSetter(convertedConfig, "setTargetEndpoints", String.class, properties.get("endpoints"));
                invokeSetter(convertedConfig, "setSnapshotEnabled", boolean.class, properties.get("snapshot.enabled"));
                invokeSetter(convertedConfig, "setEndpoint", String.class, endpoint);
                invokeSetter(convertedConfig, "setPublisherId", String.class, publisherId);
                invokeSetter(convertedConfig, "setQueueCapacity", int.class, queueCapacity);
                invokeSetter(convertedConfig, "setBatchSize", int.class, properties.get("batch.size"));
                invokeSetter(convertedConfig, "setBatchMaxDelayMillis", int.class, properties.get("batch.max.delay.millis"));
                invokeSetter(convertedConfig, "setMaxConcurrentInvocations", int.class, properties.get("max.concurrent.invocations"));

                Object ackType = properties.get("ack.type");
                Enum<?> otherAT = cloneEnum(targetClass.getClassLoader(), "com.hazelcast.config.WanAcknowledgeType", ackType);
                invokeSetter(convertedConfig, "setAcknowledgeType", otherAT.getClass(), otherAT);

                Enum<?> otherIPS = cloneEnum(targetClass.getClassLoader(), "com.hazelcast.wan.WanPublisherState", initialPublisherState);
                invokeSetter(convertedConfig, "setInitialPublisherState", otherIPS.getClass(), otherIPS);

                Enum<?> otherQFB = cloneEnum(targetClass.getClassLoader(), "com.hazelcast.config.WanQueueFullBehavior", queueFullBehavior);
                invokeSetter(convertedConfig, "setQueueFullBehavior", otherQFB.getClass(), otherQFB);

                Object convertedSyncConfig = getFieldValueReflectively(convertedConfig, "syncConfig");
                Enum<?> otherCCS = cloneEnum(targetClass.getClassLoader(), "com.hazelcast.config.ConsistencyCheckStrategy", consistencyCheckStrategy);
                invokeSetter(convertedSyncConfig, "setConsistencyCheckStrategy", otherCCS.getClass(), otherCCS);

                convertedPublishers.add(convertedConfig);
            }
            invokeSetter(otherConfig, "setBatchPublisherConfigs", List.class, convertedPublishers);
        }

        return otherConfig;
    }

    private static boolean isSplitBrainProtectionFunctionImplementation(Class<?> klass) throws Exception {
        ClassLoader classLoader = klass.getClassLoader();
        Class<?> quorumFunctionInterface;
        try {
            quorumFunctionInterface = classLoader.loadClass("com.hazelcast.quorum.QuorumFunction");
        } catch (ClassNotFoundException e) {
            // target classloader is 4.x
            quorumFunctionInterface
                    = classLoader.loadClass("com.hazelcast.splitbrainprotection.SplitBrainProtectionFunction");
        }
        return quorumFunctionInterface.isAssignableFrom(klass);
    }

    /**
     * Copies group name/cluster name configuration between config objects.
     *
     * @param thisConfigObject  config object from which the group name is copied
     * @param otherConfigObject config object to which the group name is copied
     * @throws IllegalArgumentException if the specified object is not an
     *                                  instance of the class or interface declaring the underlying
     *                                  field (or a subclass or implementor thereof).
     */
    private static void cloneGroupConfig(Object thisConfigObject, Object otherConfigObject) throws IllegalAccessException {
        boolean is4_x = hasField(thisConfigObject.getClass(), "clusterName");
        if (is4_x) {
            // copying from 4.0 to 3.12
            String clusterName = getFieldValueReflectively(thisConfigObject, "clusterName");
            Object groupConfig = getFieldValueReflectively(otherConfigObject, "groupConfig");
            invokeSetter(groupConfig, "setName", String.class, clusterName);
        } else {
            // copying from 3.12 to 4.0
            Object groupConfig = getFieldValueReflectively(thisConfigObject, "groupConfig");
            String clusterName = getFieldValueReflectively(groupConfig, "name");
            invokeSetter(otherConfigObject, "setClusterName", String.class, clusterName);
        }
    }

    private static void proxyMapStoreImplementations(Object thisConfigObject, Object otherConfigObject) throws Exception {
        Class<?> otherClass = otherConfigObject.getClass();
        ClassLoader otherClassLoader = otherClass.getClassLoader();
        Method getter = thisConfigObject.getClass().getMethod("getImplementation");
        Class<?> returnType = getter.getReturnType();
        Class<?> otherParameterType = getOtherReturnType(otherClassLoader, returnType);
        boolean is4_x = !hasField(thisConfigObject.getClass(), "readOnly");
        cloneStoreInstance(otherClassLoader,
                getter,
                otherConfigObject.getClass().getMethod("setImplementation", otherParameterType),
                thisConfigObject, otherConfigObject,
                is4_x ? "com.hazelcast.core.MapStore" : "com.hazelcast.map.MapStore");
    }

    private static boolean isEvictionConfig(Class<?> klass) throws Exception {
        return isAssignableFrom(klass, "com.hazelcast.config.EvictionConfig");
    }

    private static boolean isWanReplicationRef(Class<?> klass) throws Exception {
        return isAssignableFrom(klass, "com.hazelcast.config.WanReplicationRef");
    }

    private static boolean isWanReplicationConfig(Class<?> klass) throws Exception {
        return isAssignableFrom(klass, "com.hazelcast.config.WanReplicationConfig");
    }

    private static boolean isConfig(Class<?> klass) throws Exception {
        return isAssignableFrom(klass, "com.hazelcast.config.Config");
    }

    private static boolean isMapStoreConfig(Class<?> klass) throws Exception {
        return isAssignableFrom(klass, "com.hazelcast.config.MapStoreConfig");
    }

    private static boolean isAssignableFrom(Class<?> klass, String className)
            throws ClassNotFoundException {
        ClassLoader classLoader = klass.getClassLoader();
        Class<?> configClass = classLoader.loadClass(className);
        return configClass.isAssignableFrom(klass);
    }

    private static Enum<?> cloneEnum(ClassLoader targetClassLoader,
                                     String targetClassName,
                                     Object enumObject) throws ClassNotFoundException {
        Class otherQueueFullBehaviourClass = targetClassLoader.loadClass(targetClassName);
        return Enum.valueOf(otherQueueFullBehaviourClass, enumObject.toString());
    }

    // RU_COMPAT_4_1
    // since empty constructor is added this could be reverted in later versions
    private static Object cloneJavaKeyStoreConfig(Object javaKeyStoreConfig, Class<?> targetClass) throws Exception {
        File path = getFieldValueReflectively(javaKeyStoreConfig, "path");
        Constructor<?> constructor = targetClass.getConstructor(File.class);
        return constructor.newInstance(path);
    }
}
