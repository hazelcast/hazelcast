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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

import static com.hazelcast.test.starter.HazelcastProxyFactory.proxyArgumentsIfNeeded;
import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;
import static java.lang.reflect.Proxy.newProxyInstance;

public class NodeConstructor extends AbstractStarterObjectConstructor {

    public NodeConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate) throws Exception {
        ClassLoader classLoader = targetClass.getClassLoader();
        Class<?> hzInstanceImplClass = classLoader.loadClass("com.hazelcast.instance.HazelcastInstanceImpl");
        Class<?> configClass = classLoader.loadClass("com.hazelcast.config.Config");
        Class<?> nodeContextClass = classLoader.loadClass("com.hazelcast.instance.NodeContext");
        Class<?> addressPickerClass = classLoader.loadClass("com.hazelcast.instance.AddressPicker");
        Constructor<?> constructor = targetClass.getDeclaredConstructor(hzInstanceImplClass, configClass, nodeContextClass);

        final Object nodeExtension = getFieldValueReflectively(delegate, "nodeExtension");
        final Object address = getFieldValueReflectively(delegate, "address");
        final Object connectionManager = getFieldValueReflectively(delegate, "connectionManager");
        final Object joiner = getFieldValueReflectively(delegate, "joiner");

        final Object addressPicker = newProxyInstance(classLoader, new Class[]{addressPickerClass}, new InvocationHandler() {
            public Object invoke(Object proxy, Method method, Object[] args) {
                String name = method.getName();
                if (name.equals("pickAddress")) {
                    return null;
                } else if (name.equals("getBindAddress")) {
                    return address;
                } else if (name.equals("getPublicAddress")) {
                    return address;
                } else if (name.equals("getServerSocketChannel")) {
                    return null;
                }
                throw new UnsupportedOperationException("Method is not implemented in InvocationHandler of AddressPicker: "
                        + name);
            }
        });

        Object nodeContext = newProxyInstance(classLoader, new Class[]{nodeContextClass}, new InvocationHandler() {
            public Object invoke(Object proxy, Method method, Object[] args) {
                String name = method.getName();
                if (name.equals("createNodeExtension")) {
                    return nodeExtension;
                } else if (name.equals("createAddressPicker")) {
                    return addressPicker;
                } else if (name.equals("createJoiner")) {
                    return joiner;
                } else if (name.equals("createConnectionManager")) {
                    return connectionManager;
                }
                throw new UnsupportedOperationException("Method is not implemented in InvocationHandler of NodeContext: " + name);
            }
        });

        Object hz = getFieldValueReflectively(delegate, "hazelcastInstance");
        Object config = getFieldValueReflectively(delegate, "config");
        Object[] args = new Object[]{hz, config, nodeContext};

        Object[] proxiedArgs = proxyArgumentsIfNeeded(args, classLoader);
        return constructor.newInstance(proxiedArgs);
    }
}
