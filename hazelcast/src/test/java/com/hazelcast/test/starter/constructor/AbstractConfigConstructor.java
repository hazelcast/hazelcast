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

package com.hazelcast.test.starter.constructor;

import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.spi.merge.HigherHitsMergePolicy;
import com.hazelcast.spi.merge.LatestAccessMergePolicy;
import com.hazelcast.spi.merge.LatestUpdateMergePolicy;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.test.starter.HazelcastProxyFactory.ProxyPolicy.RETURN_SAME;
import static com.hazelcast.test.starter.HazelcastProxyFactory.generateProxyForInterface;
import static com.hazelcast.test.starter.HazelcastProxyFactory.proxyObjectForStarter;
import static com.hazelcast.test.starter.HazelcastProxyFactory.shouldProxy;
import static com.hazelcast.test.starter.HazelcastStarterUtils.debug;
import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;
import static com.hazelcast.test.starter.ReflectionUtils.hasField;
import static java.lang.reflect.Proxy.isProxyClass;

/**
 * Abstract class for constructors of config classes.
 */
abstract class AbstractConfigConstructor extends AbstractStarterObjectConstructor {

    AbstractConfigConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @SuppressWarnings("unchecked")
    static Object cloneConfig(Object thisConfigObject, ClassLoader classloader) throws Exception {
        if (thisConfigObject == null) {
            return null;
        }

        Class thisConfigClass = thisConfigObject.getClass();
        if (shouldProxy(thisConfigClass, new Class[0]) == RETURN_SAME) {
            return thisConfigObject;
        }

        Class<?> otherConfigClass = classloader.loadClass(thisConfigClass.getName());
        if (isQuorumFunctionImplementation(thisConfigClass)) {
            return cloneQuorumFunctionImplementation(thisConfigObject, otherConfigClass);
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

        Object otherConfigObject = ClassLoaderUtil.newInstance(otherConfigClass.getClassLoader(), otherConfigClass.getName());

        if (isConfig(thisConfigClass)) {
            cloneGroupConfig(thisConfigObject, otherConfigObject);
        }

        for (Method method : thisConfigClass.getMethods()) {
            if (!isGetter(method)) {
                continue;
            }
            Class returnType = method.getReturnType();
            Class<?> otherReturnType;
            try {
                otherReturnType = getOtherReturnType(classloader, returnType);
            } catch (ClassNotFoundException e) {
                // new configuration option, return type was not found in target classloader
                debug("Configuration option %s is not available in target classloader: %s", method.getName(), e.getMessage());
                continue;
            }

            Method setter = getSetter(otherConfigClass, otherReturnType, createSetterName(method));
            if (setter != null) {
                String returnTypeName = returnType.getName();
                if (isMapStoreConfig(thisConfigClass) && method.getName().equals("getImplementation")) {
                    proxyMapStoreImplementations(thisConfigObject, otherConfigObject);
                } else if (Properties.class.isAssignableFrom(returnType)) {
                    Properties original = (Properties) method.invoke(thisConfigObject, null);
                    updateConfig(setter, otherConfigObject, copy(original));
                } else if (Map.class.isAssignableFrom(returnType) || ConcurrentMap.class.isAssignableFrom(returnType)) {
                    Map map = (Map) method.invoke(thisConfigObject, null);
                    Map otherMap = ConcurrentMap.class.isAssignableFrom(returnType) ? new ConcurrentHashMap() : new HashMap();
                    copyMap(map, otherMap, classloader);
                    updateConfig(setter, otherConfigObject, otherMap);
                } else if (returnType.equals(List.class)) {
                    List list = (List) method.invoke(thisConfigObject, null);
                    List otherList = new ArrayList();
                    for (Object item : list) {
                        Object otherItem = cloneConfig(item, classloader);
                        otherList.add(otherItem);
                    }
                    updateConfig(setter, otherConfigObject, otherList);
                } else if (returnType.isEnum()) {
                    Enum thisSubConfigObject = (Enum) method.invoke(thisConfigObject, null);
                    Class otherEnumClass = classloader.loadClass(thisSubConfigObject.getClass().getName());
                    Object otherEnumValue = Enum.valueOf(otherEnumClass, thisSubConfigObject.name());
                    updateConfig(setter, otherConfigObject, otherEnumValue);
                } else if (returnTypeName.startsWith("java") || returnType.isPrimitive()) {
                    Object thisSubConfigObject = method.invoke(thisConfigObject, null);
                    updateConfig(setter, otherConfigObject, thisSubConfigObject);
                } else if (returnTypeName.equals("com.hazelcast.core.RingbufferStore")
                        || returnTypeName.equals("com.hazelcast.core.RingbufferStoreFactory")
                        || returnTypeName.equals("com.hazelcast.core.QueueStore")
                        || returnTypeName.equals("com.hazelcast.core.QueueStoreFactory")) {
                    cloneStoreInstance(classloader, method, setter, thisConfigObject, otherConfigObject, returnTypeName);
                } else if (returnTypeName.startsWith("com.hazelcast.memory.MemorySize")) {
                    // ignore
                } else if (returnTypeName.startsWith("com.hazelcast")) {
                    Object thisSubConfigObject = method.invoke(thisConfigObject, null);
                    Object otherSubConfig = cloneConfig(thisSubConfigObject, classloader);
                    updateConfig(setter, otherConfigObject, otherSubConfig);
                }
            }
        }
        return otherConfigObject;
    }

    private static void copyMap(Map source, Map destination, ClassLoader classLoader) throws Exception {
        for (Object entry : source.entrySet()) {
            // keys are either Strings or, since 3.12, EndpointQualifiers
            Object key = ((Map.Entry) entry).getKey();
            Object mappedKey = proxyObjectForStarter(classLoader, key);
            Object value = ((Map.Entry) entry).getValue();
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

    private static Class<?> getOtherReturnType(ClassLoader classloader, Class returnType) throws Exception {
        String returnTypeName = returnType.getName();
        if (returnTypeName.startsWith("com.hazelcast")) {
            return classloader.loadClass(returnTypeName);
        }
        return returnType;
    }

    private static Method getSetter(Class<?> otherConfigClass, Class parameterType, String setterName) {
        try {
            return otherConfigClass.getMethod(setterName, parameterType);
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    /**
     * Creates a proxy class for a store implementation from the current
     * classloader for the proxied classloader.
     */
    private static void cloneStoreInstance(ClassLoader classloader, Method method, Method setter, Object thisConfigObject,
                                           Object otherConfigObject, String targetStoreClass) throws Exception {
        Object thisStoreObject = method.invoke(thisConfigObject);
        if (thisStoreObject == null) {
            return;
        }
        Class<?> thisStoreClass = thisStoreObject.getClass();
        if (isProxyClass(thisStoreClass) || classloader.equals(thisStoreClass.getClassLoader())) {
            updateConfig(setter, otherConfigObject, thisStoreObject);
        } else {
            Class<?> otherStoreClass = classloader.loadClass(targetStoreClass);
            Object otherStoreObject = generateProxyForInterface(thisStoreObject, classloader, otherStoreClass);
            updateConfig(setter, otherConfigObject, otherStoreObject);
        }
    }

    private static void updateConfig(Method setterMethod, Object otherConfigObject, Object value) {
        try {
            setterMethod.invoke(otherConfigObject, value);
        } catch (IllegalAccessException e) {
            debug("Could not update config via %s: %s", setterMethod.getName(), e.getMessage());
        } catch (InvocationTargetException e) {
            debug("Could not update config via %s: %s", setterMethod.getName(), e.getMessage());
        } catch (IllegalArgumentException e) {
            debug("Could not update config via %s: %s", setterMethod.getName(), e.getMessage());
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
     * Clones the built-in QuorumFunction implementations.
     */
    private static Object cloneQuorumFunctionImplementation(Object quorumFunction, Class<?> targetClass) throws Exception {
        if (targetClass.getName().equals("com.hazelcast.quorum.impl.ProbabilisticQuorumFunction")) {
            int size = (Integer) getFieldValueReflectively(quorumFunction, "quorumSize");
            double suspicionThreshold = (Double) getFieldValueReflectively(quorumFunction, "suspicionThreshold");
            int maxSampleSize = (Integer) getFieldValueReflectively(quorumFunction, "maxSampleSize");
            long minStdDeviationMillis = (Long) getFieldValueReflectively(quorumFunction, "minStdDeviationMillis");
            long acceptableHeartbeatPauseMillis = (Long) getFieldValueReflectively(quorumFunction,
                    "acceptableHeartbeatPauseMillis");
            long heartbeatIntervalMillis = (Long) getFieldValueReflectively(quorumFunction, "heartbeatIntervalMillis");

            Constructor<?> constructor = targetClass.getConstructor(Integer.TYPE, Long.TYPE, Long.TYPE, Integer.TYPE, Long.TYPE,
                    Double.TYPE);

            return constructor.newInstance(size, heartbeatIntervalMillis, acceptableHeartbeatPauseMillis,
                    maxSampleSize, minStdDeviationMillis, suspicionThreshold);
        } else if (targetClass.getName().equals("com.hazelcast.quorum.impl.RecentlyActiveQuorumFunction")) {
            int size = (Integer) getFieldValueReflectively(quorumFunction, "quorumSize");
            int heartbeatToleranceMillis = (Integer) getFieldValueReflectively(quorumFunction, "heartbeatToleranceMillis");

            Constructor<?> constructor = targetClass.getConstructor(Integer.TYPE, Integer.TYPE);
            return constructor.newInstance(size, heartbeatToleranceMillis);
        } else {
            debug("Did not handle configured QuorumFunction implementation %s", targetClass.getName());
            return null;
        }
    }

    /**
     * Clones the built-in QuorumFunction implementations.
     */
    private static Object cloneEvictionConfig(Object thisConfigObject, Class<?> otherConfigClass) throws Exception {
        // doesn't support comparator instances
        int size = getFieldValueReflectively(thisConfigObject, "size");
        Object maxSizePolicy = getFieldValueReflectively(thisConfigObject, "maxSizePolicy");
        Object evictionPolicy = getFieldValueReflectively(thisConfigObject, "evictionPolicy");
        String comparatorClassName = getFieldValueReflectively(thisConfigObject, "comparatorClassName");

        Constructor<?> constructor = otherConfigClass.getConstructor();
        Object thatConfigObject = constructor.newInstance();

        updateConfig(getSetter(thatConfigObject.getClass(), int.class, "setSize"), thatConfigObject, size);

        Class otherMaxSizePolicyClass;
        String methodName;
        try {
            // are we transferring from 3.12 to 4.0?
            otherMaxSizePolicyClass = otherConfigClass.getClassLoader()
                                                      .loadClass("com.hazelcast.config.MaxSizePolicy");
            methodName = "setMaxSizePolicy";
        } catch (ClassNotFoundException e) {
            // we are transferring from 4.0 to 3.12
            otherMaxSizePolicyClass
                    = otherConfigClass.getClassLoader()
                                      .loadClass("com.hazelcast.config.EvictionConfig$MaxSizePolicy");
            methodName = "setMaximumSizePolicy";
        }
        Enum otherMaxSizePolicy = Enum.valueOf(otherMaxSizePolicyClass, maxSizePolicy.toString());
        Method otherMaxSizePolicySetter = getSetter(thatConfigObject.getClass(), otherMaxSizePolicyClass, methodName);
        updateConfig(otherMaxSizePolicySetter, thatConfigObject, otherMaxSizePolicy);

        Class<?> otherEvictionPolicyClass = otherConfigClass.getClassLoader().loadClass(evictionPolicy.getClass().getName());
        updateConfig(getSetter(thatConfigObject.getClass(), otherEvictionPolicyClass, "setEvictionPolicy"), thatConfigObject, evictionPolicy);

        updateConfig(getSetter(thatConfigObject.getClass(), String.class, "setComparatorClassName"), thatConfigObject, comparatorClassName);

        return thatConfigObject;
    }

    /**
     * Clones the WanReplicationRef configuration.
     */
    private static Object cloneWanReplicationRef(Object wanReplicationRef, Class<?> targetClass) throws Exception {
        String name = getFieldValueReflectively(wanReplicationRef, "name");
        boolean republishingEnabled = getFieldValueReflectively(wanReplicationRef, "republishingEnabled");
        boolean is4_0 = hasField(wanReplicationRef.getClass(), "mergePolicyClassName");
        String mergePolicyClassName;
        if (is4_0) {
            // transforming from 4.0 to 3.12
            mergePolicyClassName = getFieldValueReflectively(wanReplicationRef, "mergePolicyClassName");
        } else {
            // transforming from 3.12 to 4.0
            mergePolicyClassName = getFieldValueReflectively(wanReplicationRef, "mergePolicy");
            if ("com.hazelcast.map.merge.HigherHitsMapMergePolicy".equals(mergePolicyClassName)
                    || "com.hazelcast.cache.merge.HigherHitsCacheMergePolicy".equals(mergePolicyClassName)) {
                mergePolicyClassName = HigherHitsMergePolicy.class.getName();
            } else if ("com.hazelcast.map.merge.LatestUpdateMapMergePolicy".equals(mergePolicyClassName)) {
                mergePolicyClassName = LatestUpdateMergePolicy.class.getName();
            } else if ("com.hazelcast.cache.merge.LatestAccessCacheMergePolicy".equals(mergePolicyClassName)) {
                mergePolicyClassName = LatestAccessMergePolicy.class.getName();
            } else if ("com.hazelcast.map.merge.PassThroughMergePolicy".equals(mergePolicyClassName)
                    || "com.hazelcast.cache.merge.PassThroughCacheMergePolicy".equals(mergePolicyClassName)) {
                mergePolicyClassName = PassThroughMergePolicy.class.getName();
            } else if ("com.hazelcast.map.merge.PutIfAbsentMapMergePolicy".equals(mergePolicyClassName)
                    || "com.hazelcast.cache.merge.PutIfAbsentCacheMergePolicy".equals(mergePolicyClassName)) {
                mergePolicyClassName = PutIfAbsentMergePolicy.class.getName();
            }
        }

        List<String> filters = getFieldValueReflectively(wanReplicationRef, "filters");
        Constructor<?> constructor = targetClass.getConstructor(String.class, String.class, List.class, Boolean.TYPE);

        return constructor.newInstance(name, mergePolicyClassName, filters, republishingEnabled);
    }

    /**
     * Clones the WanReplicationConfig configuration.
     */
    private static Object cloneWanReplicationConfig(Object wanReplicationConfig, Class<?> targetClass) throws Exception {
        String name = getFieldValueReflectively(wanReplicationConfig, "name");
        Object otherConfig = ClassLoaderUtil.newInstance(targetClass.getClassLoader(), targetClass.getName());
        updateConfig(getSetter(otherConfig.getClass(), String.class, "setName"), otherConfig, name);
        boolean is4_0 = hasField(wanReplicationConfig.getClass(), "batchPublisherConfigs");

        if (is4_0) {
            Object consumerConfig = getFieldValueReflectively(wanReplicationConfig, "consumerConfig");

            if (consumerConfig != null) {
                Object convertedConsumer = cloneConfig(consumerConfig, targetClass.getClassLoader());
                Method setter = getSetter(otherConfig.getClass(), convertedConsumer.getClass(), "setWanConsumerConfig");
                updateConfig(setter, otherConfig, convertedConsumer);
            }

            List<Object> customPublisherConfigs = getFieldValueReflectively(wanReplicationConfig, "customPublisherConfigs");
            List<Object> batchPublisherConfigs = getFieldValueReflectively(wanReplicationConfig, "batchPublisherConfigs");
            ArrayList<Object> convertedPublishers = new ArrayList<Object>(batchPublisherConfigs.size());
            for (Object publisherConfig : batchPublisherConfigs) {
                // leftovers: publisherId, implementation
                String clusterName = getFieldValueReflectively(publisherConfig, "clusterName");
                boolean snapshotEnabled = getFieldValueReflectively(publisherConfig, "snapshotEnabled");
                Object initialPublisherState = getFieldValueReflectively(publisherConfig, "initialPublisherState");
                int queueCapacity = getFieldValueReflectively(publisherConfig, "queueCapacity");
                int batchSize = getFieldValueReflectively(publisherConfig, "batchSize");
                int batchMaxDelayMillis = getFieldValueReflectively(publisherConfig, "batchMaxDelayMillis");
                int responseTimeoutMillis = getFieldValueReflectively(publisherConfig, "responseTimeoutMillis");
                Object queueFullBehavior = getFieldValueReflectively(publisherConfig, "queueFullBehavior");
                Object acknowledgeType = getFieldValueReflectively(publisherConfig, "acknowledgeType");
                int discoveryPeriodSeconds = getFieldValueReflectively(publisherConfig, "discoveryPeriodSeconds");
                int maxTargetEndpoints = getFieldValueReflectively(publisherConfig, "maxTargetEndpoints");
                int maxConcurrentInvocations = getFieldValueReflectively(publisherConfig, "maxConcurrentInvocations");
                boolean useEndpointPrivateAddress = getFieldValueReflectively(publisherConfig, "useEndpointPrivateAddress");
                long idleMinParkNs = getFieldValueReflectively(publisherConfig, "idleMinParkNs");
                long idleMaxParkNs = getFieldValueReflectively(publisherConfig, "idleMaxParkNs");
                String targetEndpoints = getFieldValueReflectively(publisherConfig, "targetEndpoints");
                Object awsConfig = getFieldValueReflectively(publisherConfig, "awsConfig");
                Object gcpConfig = getFieldValueReflectively(publisherConfig, "gcpConfig");
                Object azureConfig = getFieldValueReflectively(publisherConfig, "azureConfig");
                Object kubernetesConfig = getFieldValueReflectively(publisherConfig, "kubernetesConfig");
                Object eurekaConfig = getFieldValueReflectively(publisherConfig, "eurekaConfig");
                Object discoveryConfig = getFieldValueReflectively(publisherConfig, "discoveryConfig");
                Object syncConfig = getFieldValueReflectively(publisherConfig, "syncConfig");
                String endpoint = getFieldValueReflectively(publisherConfig, "endpoint");

                Object convertedPublisherConfig = ClassLoaderUtil.newInstance(targetClass.getClassLoader(), "com.hazelcast.config.WanPublisherConfig");
                updateConfig(getSetter(convertedPublisherConfig.getClass(), String.class, "setGroupName"), convertedPublisherConfig, clusterName);

                updateConfig(getSetter(convertedPublisherConfig.getClass(), String.class, "setClassName"), convertedPublisherConfig,
                        "com.hazelcast.enterprise.wan.replication.WanBatchReplication");

                HashMap<Object, Object> props = new HashMap<Object, Object>();
                props.put("group.password", "dev-pass");
                props.put("endpoints", targetEndpoints);

                updateConfig(getSetter(convertedPublisherConfig.getClass(), Map.class, "setProperties"), convertedPublisherConfig, props);
                convertedPublishers.add(convertedPublisherConfig);
            }
            updateConfig(getSetter(otherConfig.getClass(), List.class, "setWanPublisherConfigs"), otherConfig, convertedPublishers);
        } else {
            // copying from 3.12 to 4.0
            Object consumerConfig = getFieldValueReflectively(wanReplicationConfig, "wanConsumerConfig");
            List<Object> wanPublisherConfigs = getFieldValueReflectively(wanReplicationConfig, "wanPublisherConfigs");
            ArrayList<Object> convertedPublishers = new ArrayList<Object>(wanPublisherConfigs.size());

            if (consumerConfig != null) {
                Object convertedConsumer = cloneConfig(consumerConfig, targetClass.getClassLoader());
                Method setter = getSetter(otherConfig.getClass(), convertedConsumer.getClass(), "setConsumerConfig");
                updateConfig(setter, otherConfig, convertedConsumer);
            }

            for (Object publisherConfig : wanPublisherConfigs) {
                // leftovers: implementation
                String groupName = getFieldValueReflectively(publisherConfig, "groupName");
                String publisherId = getFieldValueReflectively(publisherConfig, "publisherId");
                int queueCapacity = getFieldValueReflectively(publisherConfig, "queueCapacity");
                Object queueFullBehavior = getFieldValueReflectively(publisherConfig, "queueFullBehavior");
                Object initialPublisherState = getFieldValueReflectively(publisherConfig, "initialPublisherState");
                Map<String, Comparable> properties = getFieldValueReflectively(publisherConfig, "properties");
                String className = getFieldValueReflectively(publisherConfig, "className");
                Object awsConfig = getFieldValueReflectively(publisherConfig, "awsConfig");
                Object gcpConfig = getFieldValueReflectively(publisherConfig, "gcpConfig");
                Object azureConfig = getFieldValueReflectively(publisherConfig, "azureConfig");
                Object kubernetesConfig = getFieldValueReflectively(publisherConfig, "kubernetesConfig");
                Object eurekaConfig = getFieldValueReflectively(publisherConfig, "eurekaConfig");
                Object discoveryConfig = getFieldValueReflectively(publisherConfig, "discoveryConfig");
                Object wanSyncConfig = getFieldValueReflectively(publisherConfig, "wanSyncConfig");
                String endpoint = getFieldValueReflectively(publisherConfig, "endpoint");

//                boolean snapshotEnabled = (boolean) getFieldValueReflectively(publisherConfig, "snapshotEnabled");
//                int batchSize = (int) getFieldValueReflectively(publisherConfig, "batchSize");
//                int batchMaxDelayMillis = (int) getFieldValueReflectively(publisherConfig, "batchMaxDelayMillis");
//                int responseTimeoutMillis = (int) getFieldValueReflectively(publisherConfig, "responseTimeoutMillis");
//                Object acknowledgeType = getFieldValueReflectively(publisherConfig, "acknowledgeType");
//                int discoveryPeriodSeconds = (int) getFieldValueReflectively(publisherConfig, "discoveryPeriodSeconds");
//                int maxTargetEndpoints = (int) getFieldValueReflectively(publisherConfig, "maxTargetEndpoints");
//                int maxConcurrentInvocations = (int) getFieldValueReflectively(publisherConfig, "maxConcurrentInvocations");
//                boolean useEndpointPrivateAddress = (boolean) getFieldValueReflectively(publisherConfig, "useEndpointPrivateAddress");
//                long idleMinParkNs = (long) getFieldValueReflectively(publisherConfig, "idleMinParkNs");
//                long idleMaxParkNs = (long) getFieldValueReflectively(publisherConfig, "idleMaxParkNs");
//                String targetEndpoints = (String) getFieldValueReflectively(publisherConfig, "targetEndpoints");

                if (!className.equals("com.hazelcast.enterprise.wan.replication.WanBatchReplication")) {
                    // not copying custom replication
                    continue;
                }

                Object convertedConfig = ClassLoaderUtil.newInstance(targetClass.getClassLoader(),
                        "com.hazelcast.config.WanBatchPublisherConfig");
                updateConfig(getSetter(convertedConfig.getClass(), String.class, "setClusterName"), convertedConfig, groupName);

                updateConfig(getSetter(convertedConfig.getClass(), String.class, "setTargetEndpoints"),
                        convertedConfig, properties.get("endpoints"));

                convertedPublishers.add(convertedConfig);
            }
            updateConfig(getSetter(otherConfig.getClass(), List.class, "setBatchPublisherConfigs"), otherConfig, convertedPublishers);
        }

        return otherConfig;
    }

    /**
     * TODO
     *
     * @param thisConfigObject
     * @param otherConfigObject
     * @throws Exception
     */
    private static void cloneGroupConfig(Object thisConfigObject, Object otherConfigObject)
            throws Exception {
        boolean is4_0 = hasField(thisConfigObject.getClass(), "clusterName");
        if (is4_0) {
            // copying from 4.0 to 3.12
            String clusterName = getFieldValueReflectively(thisConfigObject, "clusterName");
            Object groupConfig = getFieldValueReflectively(otherConfigObject, "groupConfig");
            updateConfig(getSetter(groupConfig.getClass(), String.class, "setName"),
                    groupConfig, clusterName);
        } else {
            // copying from 3.12 to 4.0
            Object groupConfig = getFieldValueReflectively(thisConfigObject, "groupConfig");
            String clusterName = getFieldValueReflectively(groupConfig, "name");
            updateConfig(getSetter(otherConfigObject.getClass(), String.class, "setClusterName"),
                    otherConfigObject, clusterName);
        }
    }

    private static void proxyMapStoreImplementations(Object thisConfigObject, Object otherConfigObject) throws Exception {
        Class<?> otherClass = otherConfigObject.getClass();
        ClassLoader otherClassLoader = otherClass.getClassLoader();
        Method getter = thisConfigObject.getClass().getMethod("getImplementation");
        Class<?> returnType = getter.getReturnType();
        Class<?> otherParameterType = getOtherReturnType(otherClassLoader, returnType);
        cloneStoreInstance(otherClassLoader,
                getter,
                otherConfigObject.getClass().getMethod("setImplementation", otherParameterType),
                thisConfigObject, otherConfigObject, "com.hazelcast.map.MapStore");
    }

    private static boolean isEvictionConfig(Class<?> klass) throws Exception {
        ClassLoader classLoader = klass.getClassLoader();
        Class<?> evictionConfigClass = classLoader.loadClass("com.hazelcast.config.EvictionConfig");
        return evictionConfigClass.isAssignableFrom(klass);
    }

    private static boolean isQuorumFunctionImplementation(Class<?> klass) throws Exception {
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

    private static boolean isWanReplicationRef(Class<?> klass) throws Exception {
        ClassLoader classLoader = klass.getClassLoader();
        Class<?> wanReplicationRefClass = classLoader.loadClass("com.hazelcast.config.WanReplicationRef");
        return wanReplicationRefClass.isAssignableFrom(klass);
    }

    private static boolean isWanReplicationConfig(Class<?> klass) throws Exception {
        ClassLoader classLoader = klass.getClassLoader();
        Class<?> wanReplicationConfigClass = classLoader.loadClass("com.hazelcast.config.WanReplicationConfig");
        return wanReplicationConfigClass.isAssignableFrom(klass);
    }

    private static boolean isConfig(Class<?> klass) throws Exception {
        ClassLoader classLoader = klass.getClassLoader();
        Class<?> configClass = classLoader.loadClass("com.hazelcast.config.Config");
        return configClass.isAssignableFrom(klass);
    }

    private static boolean isMapStoreConfig(Class<?> klass) throws Exception {
        ClassLoader classLoader = klass.getClassLoader();
        Class<?> configClass = classLoader.loadClass("com.hazelcast.config.MapStoreConfig");
        return configClass.isAssignableFrom(klass);
    }

}
