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

package com.hazelcast.test.starter.hz3;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.impl.deployment.ChildFirstClassLoader;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.test.starter.HazelcastVersionLocator.HAZELCAST_JAR_INDEX;
import static com.hazelcast.test.starter.HazelcastVersionLocator.locateVersion;

/**
 * Creates new Hazelcast 3 proxy
 */
public class Hazelcast3Proxy {

    private static final Map<String, String> TYPES_4_TO_3 = new HashMap<String, String>() {{
        put("com.hazelcast.topic.ITopic", "com.hazelcast.core.ITopic");
        put("com.hazelcast.topic.MessageListener", "com.hazelcast.core.MessageListener");
        put("com.hazelcast.topic.Message", "com.hazelcast.core.Message");
    }};

    private static final Map<String, String> TYPES_3_TO_4 = new HashMap<String, String>() {{
        put("com.hazelcast.core.ITopic", "com.hazelcast.topic.ITopic");
        put("com.hazelcast.core.MessageListener", "com.hazelcast.topic.MessageListener");
        put("com.hazelcast.core.Message", "com.hazelcast.topic.Message");
    }};

    public HazelcastInstance newHazelcastInstance(String xmlConfig) {
        try {
            File hazelcastJar = locateVersion("3.12.12", new File("target"), false)[HAZELCAST_JAR_INDEX];
            URLClassLoader classLoader = new ChildFirstClassLoader(
                    new URL[]{hazelcastJar.toURI().toURL()},
                    Hazelcast3Proxy.class.getClassLoader()
            );
            Object config = buildHz3Config(xmlConfig, classLoader);
            Object hz3Instance = newHazelcast3Instance(classLoader, config);

            return proxyInstance(hz3Instance, HazelcastInstance.class);

        } catch (MalformedURLException | ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private Object buildHz3Config(String xmlConfig, URLClassLoader classLoader) throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        ByteArrayInputStream is = new ByteArrayInputStream(xmlConfig.getBytes(StandardCharsets.UTF_8));
        Class<?> xmlConfigBuilderClass = classLoader.loadClass("com.hazelcast.config.XmlConfigBuilder");
        Constructor<?> constructor = xmlConfigBuilderClass.getDeclaredConstructor(InputStream.class);
        Object configBuilder = constructor.newInstance(is);
        Object config = invokeMethod(configBuilder, "build");
        return config;
    }

    private Object newHazelcast3Instance(URLClassLoader classLoader, Object config) throws ClassNotFoundException {
        Class<?> hz3InstanceClass = classLoader.loadClass("com.hazelcast.core.Hazelcast");
        Object hz3Instance = invokeStaticMethod(hz3InstanceClass, "newHazelcastInstance", config);
        return hz3Instance;
    }

    private Object invokeMethod(Object object, String methodName) {
        try {
            Method method = object.getClass().getMethod(methodName);
            return method.invoke(object);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Could not find method on class ", e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException("Could not invoke method on class ", e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Could not access method on class ", e);
        }
    }

    private Object invokeStaticMethod(Class<?> clazz, String methodName, Object... params) {
        try {
            Class<?>[] paramTypes = paramsToTypes(params);
            Method method = clazz.getMethod(methodName, paramTypes);
            return method.invoke(null, params);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Could not find method on class ", e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException("Could not invoke method on class ", e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Could not access method on class ", e);
        }
    }

    private Class<?>[] paramsToTypes(Object[] params) {
        if (params == null) {
            return null;
        }
        Class<?>[] paramTypes = new Class[params.length];
        for (int i = 0; i < params.length; i++) {
            paramTypes[i] = params[i].getClass();
        }
        return paramTypes;
    }

    public <T> T proxyInstance(Object target, Class<T> iface) {
        Object proxy = Mockito.mock(iface, new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Method method = invocation.getMethod();
                Object[] objects = invocation.getArguments();

                Class<?>[] translatedTypes = translateParameterTypes(method.getParameterTypes());
                Object[] proxiedParams = proxyParameters(objects, translatedTypes);
                Method targetMethod = target.getClass().getMethod(method.getName(), translatedTypes);
                Object result = targetMethod.invoke(target, proxiedParams);

                if (method.getReturnType().getPackage() != null
                    && method.getReturnType().getPackage().getName().startsWith("com.hazelcast")) {

                    return proxyInstance(result, method.getReturnType());
                } else {
                    if (method.getReturnType().equals(UUID.class) && targetMethod.getReturnType().equals(String.class)) {
                        return UUID.fromString((String) result);
                    } else {
                        return result;
                    }
                }
            }

            private Class<?>[] translateParameterTypes(Class<?>[] types) throws ClassNotFoundException {
                Class<?>[] translated = new Class<?>[types.length];
                for (int i = 0; i < types.length; i++) {
                    if (types[i].getPackage() != null
                            && types[i].getPackage().getName().startsWith("com.hazelcast")) {

                        String typeName = types[i].getName();
                        String name = TYPES_4_TO_3.get(typeName);
                        if (name == null) {
                            name = TYPES_3_TO_4.get(typeName);
                        }
                        if (name == null) {
                            name = typeName;
                        }
                        translated[i] = target.getClass().getClassLoader().loadClass(name);
                    } else {
                        translated[i] = types[i];
                    }
                }
                return translated;
            }

            private Object[] proxyParameters(Object[] params, Class<?>[] types) {
                assert params.length == types.length;

                Object[] proxied = new Object[params.length];
                for (int i = 0; i < params.length; i++) {
                    if (types[i].getPackage() != null
                            && types[i].getPackage().getName().startsWith("com.hazelcast")) {
                        proxied[i] = proxyInstance(params[i], types[i]);
                    } else {
                        proxied[i] = params[i];
                    }
                }
                return proxied;
            }
        });

        return (T) proxy;
    }
}
