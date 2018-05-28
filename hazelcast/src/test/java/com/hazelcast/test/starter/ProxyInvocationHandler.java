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

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;

import static com.hazelcast.test.starter.HazelcastProxyFactory.newCollectionFor;
import static com.hazelcast.test.starter.HazelcastStarterUtils.debug;
import static com.hazelcast.test.starter.HazelcastStarterUtils.isDebugEnabled;
import static com.hazelcast.test.starter.HazelcastStarterUtils.rethrowGuardianException;
import static com.hazelcast.test.starter.HazelcastStarterUtils.transferThrowable;

class ProxyInvocationHandler implements InvocationHandler, Serializable {

    private final Object delegate;

    ProxyInvocationHandler(Object delegate) {
        this.delegate = delegate;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        ClassLoader targetClassLoader = proxy.getClass().getClassLoader();
        debug("Proxy %s called. Method: %s", this, method);
        Class<?> delegateClass = delegate.getClass();
        Method methodDelegate = getMethodDelegate(method, delegateClass);

        Type returnType = method.getGenericReturnType();
        Type delegateReturnType = methodDelegate.getGenericReturnType();
        ClassLoader delegateClassClassLoader = delegateClass.getClassLoader();
        Object[] newArgs = HazelcastProxyFactory.proxyArgumentsIfNeeded(args, delegateClassClassLoader);
        Object delegateResult = invokeMethodDelegate(methodDelegate, newArgs);
        if (!shouldProxy(method, methodDelegate, delegateResult)) {
            return delegateResult;
        }

        if (returnType instanceof ParameterizedType) {
            ParameterizedType parameterizedReturnType = (ParameterizedType) returnType;
            ParameterizedType parameterizedDelegateReturnType = (ParameterizedType) delegateReturnType;

            if (Collection.class.isAssignableFrom((Class) parameterizedDelegateReturnType.getRawType())) {
                Collection result;
                Collection delegateCollectionResult = (Collection) delegateResult;
                // check if the raw types are equal: if yes, then return a collection of the same type
                // otherwise proxy it
                if (parameterizedDelegateReturnType.getRawType().equals(parameterizedReturnType.getRawType())) {
                    result = delegateCollectionResult;
                } else {
                    result = (Collection) proxyReturnObject(targetClassLoader, delegateResult);
                }

                // if the parameter type is not equal, need to proxy it
                Type returnParameterType = parameterizedReturnType.getActualTypeArguments()[0];
                Type delegateParameterType = parameterizedDelegateReturnType.getActualTypeArguments()[0];
                // if the type argument is equal, just return the result, otherwise proxy each item in the collection
                if (returnParameterType.equals(delegateParameterType)) {
                    return result;
                } else {
                    Collection temp = newCollectionFor((Class) parameterizedDelegateReturnType.getRawType());
                    for (Object o : delegateCollectionResult) {
                        temp.add(proxyReturnObject(targetClassLoader, o));
                    }
                    try {
                        result.clear();
                        result.addAll(temp);
                        return result;
                    } catch (UnsupportedOperationException e) {
                        return temp;
                    }
                }
            } else {
                return proxyReturnObject(targetClassLoader, delegateResult);
            }
        } else {
            // at this point we know the delegate returned something loaded by
            // different classloader than the proxy -> we need to proxy the result
            return proxyReturnObject(targetClassLoader, delegateResult);
        }
    }

    /**
     * @param targetClassLoader the classloader on which the proxy will be created
     * @param delegate          the object to be delegated to by the proxy
     * @return a proxy to delegate
     */
    private Object proxyReturnObject(ClassLoader targetClassLoader, Object delegate) {
        Object resultingProxy;
        try {
            resultingProxy = HazelcastProxyFactory.proxyObjectForStarter(targetClassLoader, delegate);
        } catch (Exception e) {
            throw new GuardianException(e);
        }
        printInfoAboutResultProxy(resultingProxy);
        return resultingProxy;
    }

    private Object invokeMethodDelegate(Method methodDelegate, Object[] args) throws Throwable {
        Object delegateResult;
        try {
            methodDelegate.setAccessible(true);
            delegateResult = methodDelegate.invoke(delegate, args);
        } catch (IllegalAccessException e) {
            throw rethrowGuardianException(e);
        } catch (InvocationTargetException e) {
            throw transferThrowable(e.getTargetException());
        }
        return delegateResult;
    }

    private Method getMethodDelegate(Method method, Class<?> delegateClass) {
        Method methodDelegate;
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes != null) {
            for (int i = 0; i < parameterTypes.length; i++) {
                Class<?> parameterType = parameterTypes[i];
                ClassLoader parameterTypeClassloader = parameterType.getClassLoader();
                ClassLoader delegateClassLoader = delegateClass.getClassLoader();
                if (parameterTypeClassloader != String.class.getClassLoader()
                        && parameterTypeClassloader != delegateClassLoader) {
                    try {
                        Class<?> delegateParameterType = delegateClassLoader.loadClass(parameterType.getName());
                        parameterTypes[i] = delegateParameterType;
                    } catch (ClassNotFoundException e) {
                        throw rethrowGuardianException(e);
                    }
                }
            }
        }
        try {
            methodDelegate = delegateClass.getMethod(method.getName(), parameterTypes);
        } catch (NoSuchMethodException e) {
            throw rethrowGuardianException(e);
        }
        return methodDelegate;
    }

    private static void printInfoAboutResultProxy(Object resultingProxy) {
        if (!isDebugEnabled()) {
            return;
        }
        debug("Returning proxy %s, loaded by %s", resultingProxy, resultingProxy.getClass().getClassLoader());
        debug("The proxy implements interfaces:");
        for (Class<?> interfaceClass : resultingProxy.getClass().getInterfaces()) {
            debug("%s, loaded by %s", interfaceClass, interfaceClass.getClassLoader());
        }
    }

    private boolean shouldProxy(Method proxyMethod, Method delegateMethod, Object delegateResult) {
        if (delegateResult == null) {
            return false;
        }

        Type returnType = proxyMethod.getGenericReturnType();
        if (returnType instanceof ParameterizedType) {
            return true;
        }

        // if there return types are equals -> they are loaded
        // by the same classloader -> no need to proxy what it returns
        Class<?> returnClass = proxyMethod.getReturnType();
        Class<?> delegateReturnClass = delegateMethod.getReturnType();
        return !(returnClass.equals(delegateReturnClass));
    }
}
