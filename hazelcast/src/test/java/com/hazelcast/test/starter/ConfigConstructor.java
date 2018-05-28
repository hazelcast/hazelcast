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

package com.hazelcast.test.starter;

import com.hazelcast.nio.ClassLoaderUtil;

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

import static com.hazelcast.test.starter.HazelcastProxyFactory.isJDKClass;
import static com.hazelcast.test.starter.HazelcastStarterUtils.debug;
import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;

/**
 * Clones the configuration from {@code mainConfig} to a new configuration object loaded in the
 * target {@code classloader}. The returned configuration has its classloader set to the target classloader.
 */
public class ConfigConstructor extends AbstractStarterObjectConstructor {

    public ConfigConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate) throws Exception {
        ClassLoader classloader = targetClass.getClassLoader();
        Object otherConfig = cloneConfig(delegate, classloader);

        Method setClassLoaderMethod = targetClass.getMethod("setClassLoader", ClassLoader.class);
        setClassLoaderMethod.invoke(otherConfig, classloader);
        return otherConfig;
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

    private static Object cloneConfig(Object thisConfigObject, ClassLoader classloader) throws Exception {
        if (thisConfigObject == null) {
            return null;
        }

        Class thisConfigClass = thisConfigObject.getClass();
        if (thisConfigClass.isPrimitive() || isJDKClass(thisConfigClass)) {
            return thisConfigObject;
        }

        Class<?> otherConfigClass = classloader.loadClass(thisConfigClass.getName());

        if (isQuorumFunctionImplementation(thisConfigClass)) {
            return cloneQuorumFunctionImplementation(thisConfigObject, otherConfigClass);
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
                debug("Configuration option %s is not available in target classloader: %s", method.getName(), e.getMessage());
                continue;
            }

            Method setter = getSetter(otherConfigClass, otherReturnType, createSetterName(method));
            if (setter != null) {
                if (Properties.class.isAssignableFrom(returnType)) {
                    Properties original = (Properties) method.invoke(thisConfigObject, null);
                    updateConfig(setter, otherConfigObject, copy(original));
                } else if (Map.class.isAssignableFrom(returnType) || ConcurrentMap.class.isAssignableFrom(returnType)) {
                    Map map = (Map) method.invoke(thisConfigObject, null);
                    Map otherMap = ConcurrentMap.class.isAssignableFrom(returnType) ? new ConcurrentHashMap() : new HashMap();
                    for (Object entry : map.entrySet()) {
                        String key = (String) ((Map.Entry) entry).getKey();
                        Object value = ((Map.Entry) entry).getValue();
                        Object otherMapItem = cloneConfig(value, classloader);
                        otherMap.put(key, otherMapItem);
                    }
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
                } else if (returnType.getName().startsWith("java") || returnType.isPrimitive()) {
                    Object thisSubConfigObject = method.invoke(thisConfigObject, null);
                    updateConfig(setter, otherConfigObject, thisSubConfigObject);
                } else if (returnType.getName().startsWith("com.hazelcast.memory.MemorySize")) {
                    // ignore
                } else if (returnType.getName().startsWith("com.hazelcast")) {
                    Object thisSubConfigObject = method.invoke(thisConfigObject, null);
                    Object otherSubConfig = cloneConfig(thisSubConfigObject, classloader);
                    updateConfig(setter, otherConfigObject, otherSubConfig);
                }
            }
        }
        return otherConfigObject;
    }

    private static Class<?> getOtherReturnType(ClassLoader classloader, Class returnType) throws ClassNotFoundException {
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

    public static Object getValue(Object obj, String getter)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method method = obj.getClass().getMethod(getter, null);
        return method.invoke(obj, null);
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

    // clones built-in QuorumFunction implementations
    private static Object cloneQuorumFunctionImplementation(Object quorumFunction, Class<?> targetClass)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException,
            InstantiationException {

        if (targetClass.getName().equals("com.hazelcast.quorum.impl.ProbabilisticQuorumFunction")) {
            int size = (Integer) getFieldValueReflectively(quorumFunction, "quorumSize");
            double suspicionThreshold = (Double) getFieldValueReflectively(quorumFunction, "suspicionThreshold");
            int maxSampleSize = (Integer) getFieldValueReflectively(quorumFunction, "maxSampleSize");
            long minStdDeviationMillis = (Long) getFieldValueReflectively(quorumFunction, "minStdDeviationMillis");
            long acceptableHeartbeatPauseMillis = (Long) getFieldValueReflectively(quorumFunction,
                    "acceptableHeartbeatPauseMillis");
            long heartbeatIntervalMillis = (Long) getFieldValueReflectively(quorumFunction, "heartbeatIntervalMillis");


            Constructor<?> ctor = targetClass.getConstructor(Integer.TYPE, Long.TYPE,
                    Long.TYPE, Integer.TYPE, Long.TYPE, Double.TYPE);

            return ctor.newInstance(size, heartbeatIntervalMillis, acceptableHeartbeatPauseMillis,
                    maxSampleSize, minStdDeviationMillis, suspicionThreshold);
        } else if (targetClass.getName().equals("com.hazelcast.quorum.impl.RecentlyActiveQuorumFunction")) {
            int size = (Integer) getFieldValueReflectively(quorumFunction, "quorumSize");
            int heartbeatToleranceMillis = (Integer) getFieldValueReflectively(quorumFunction, "heartbeatToleranceMillis");

            Constructor<?> ctor = targetClass.getConstructor(Integer.TYPE, Integer.TYPE);
            return ctor.newInstance(size, heartbeatToleranceMillis);
        } else {
            debug("Did not handle configured QuorumFunction implementation %s", targetClass.getName());
            return null;
        }
    }

    private static boolean isQuorumFunctionImplementation(Class<?> klass) throws ClassNotFoundException {
        ClassLoader classLoader = klass.getClassLoader();
        Class<?> quorumFunctionInterface = classLoader.loadClass("com.hazelcast.quorum.QuorumFunction");
        return quorumFunctionInterface.isAssignableFrom(klass);
    }
}
