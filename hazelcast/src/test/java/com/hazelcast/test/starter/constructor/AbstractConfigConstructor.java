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

package com.hazelcast.test.starter.constructor;

import com.hazelcast.internal.nio.ClassLoaderUtil;

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

        Class<?> thisConfigClass = thisConfigObject.getClass();
        if (shouldProxy(thisConfigClass, new Class[0]) == RETURN_SAME) {
            return thisConfigObject;
        }

        Class<?> otherConfigClass = classloader.loadClass(thisConfigClass.getName());
        if (isSplitBrainProtectionFunctionImplementation(thisConfigClass)) {
            return cloneSplitBrainProtectionFunctionImplementation(thisConfigObject, otherConfigClass);
        }

        if (thisConfigClass.getName().equals("com.hazelcast.jet.impl.config.DelegatingInstanceConfig")) {
            otherConfigClass = classloader.loadClass("com.hazelcast.jet.config.InstanceConfig");
        }

        Object otherConfigObject = ClassLoaderUtil.newInstance(otherConfigClass.getClassLoader(), otherConfigClass.getName());

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
                debug("Configuration option %s is not available in target classloader: %s",
                        method.getName(), e.getMessage());
                continue;
            }

            Method setter = getSetter(otherConfigClass, otherReturnType, createSetterName(method));
            if (setter != null) {
                String returnTypeName = returnType.getName();
                if (Properties.class.isAssignableFrom(returnType)) {
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
                } else if (returnTypeName.equals("com.hazelcast.ringbuffer.RingbufferStore")
                        || returnTypeName.equals("com.hazelcast.ringbuffer.RingbufferStoreFactory")
                        || returnTypeName.equals("com.hazelcast.collection.QueueStore")
                        || returnTypeName.equals("com.hazelcast.collection.QueueStoreFactory")) {
                    cloneStoreInstance(classloader, method, setter, thisConfigObject, otherConfigObject, returnTypeName);
                } else if (returnTypeName.startsWith("com.hazelcast.memory.MemorySize")
                        || returnTypeName.startsWith("com.hazelcast.memory.Capacity")) {
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

    private static Method getSetter(Class<?> otherConfigClass, Class returnType, String setterName) {
        try {
            return otherConfigClass.getMethod(setterName, returnType);
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
     * Clones the built-in SplitBrainProtectionFunction implementations.
     */
    private static Object cloneSplitBrainProtectionFunctionImplementation(Object splitBrainProtectionFunction,
                                                                          Class<?> targetClass) throws Exception {
        if (targetClass.getName().equals("com.hazelcast.splitbrainprotection.impl.ProbabilisticSplitBrainProtectionFunction")) {
            int size = (Integer) getFieldValueReflectively(splitBrainProtectionFunction, "minimumClusterSize");
            double suspicionThreshold = (Double) getFieldValueReflectively(splitBrainProtectionFunction,
                    "suspicionThreshold");
            int maxSampleSize = (Integer) getFieldValueReflectively(splitBrainProtectionFunction, "maxSampleSize");
            long minStdDeviationMillis = (Long) getFieldValueReflectively(splitBrainProtectionFunction,
                    "minStdDeviationMillis");
            long acceptableHeartbeatPauseMillis = (Long) getFieldValueReflectively(splitBrainProtectionFunction,
                    "acceptableHeartbeatPauseMillis");
            long heartbeatIntervalMillis =
                    (Long) getFieldValueReflectively(splitBrainProtectionFunction, "heartbeatIntervalMillis");

            Constructor<?> constructor = targetClass.getConstructor(Integer.TYPE, Long.TYPE, Long.TYPE, Integer.TYPE, Long.TYPE,
                    Double.TYPE);

            return constructor.newInstance(size, heartbeatIntervalMillis, acceptableHeartbeatPauseMillis,
                    maxSampleSize, minStdDeviationMillis, suspicionThreshold);
        } else if (targetClass.getName()
                .equals("com.hazelcast.splitbrainprotection.impl.RecentlyActiveSplitBrainProtectionFunction")) {
            int size = (Integer) getFieldValueReflectively(splitBrainProtectionFunction, "minimumClusterSize");
            int heartbeatToleranceMillis =
                    (Integer) getFieldValueReflectively(splitBrainProtectionFunction, "heartbeatToleranceMillis");

            Constructor<?> constructor = targetClass.getConstructor(Integer.TYPE, Integer.TYPE);
            return constructor.newInstance(size, heartbeatToleranceMillis);
        } else {
            debug("Did not handle configured SplitBrainProtectionFunction implementation %s", targetClass.getName());
            return null;
        }
    }

    private static boolean isSplitBrainProtectionFunctionImplementation(Class<?> klass) throws Exception {
        ClassLoader classLoader = klass.getClassLoader();
        Class<?> splitBrainProtectionFunctionInterface =
                classLoader.loadClass("com.hazelcast.splitbrainprotection.SplitBrainProtectionFunction");
        return splitBrainProtectionFunctionInterface.isAssignableFrom(klass);
    }
}
