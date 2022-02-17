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

package com.hazelcast.test.starter.answer;

import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.test.starter.HazelcastAPIDelegatingClassloader;
import com.hazelcast.test.starter.ReflectionUtils;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.test.starter.HazelcastProxyFactory.proxyArgumentsIfNeeded;
import static com.hazelcast.test.starter.HazelcastProxyFactory.proxyObjectForStarter;
import static com.hazelcast.test.starter.ReflectionUtils.getMethod;
import static com.hazelcast.internal.util.Preconditions.checkInstanceOf;
import static com.hazelcast.internal.util.Preconditions.checkNotInstanceOf;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.RootCauseMatcher.getRootCause;
import static java.lang.reflect.Modifier.isFinal;
import static java.lang.reflect.Modifier.isPrivate;
import static java.lang.reflect.Modifier.isProtected;
import static org.mockito.Mockito.mock;

/**
 * Abstract superclass for {@link org.mockito.stubbing.Answer} to create a mock
 * with a default answer of a proxied class for the target classloader.
 * <p>
 * The goal of the mocking approach is to create proxy instances for the target
 * classloader as lazy as possible. This makes the access to internals much
 * easier, since we don't need to be able to construct all internal states as
 * soon as e.g. {@link java.util.LinkedList.Node} is retrieved. We can just
 * follow the usage and create mocks until we really need to access some data.
 */
abstract class AbstractAnswer implements Answer {

    static final ClassLoader targetClassloader = AbstractAnswer.class.getClassLoader();

    final Object delegate;
    final Class<?> delegateClass;
    final ClassLoader delegateClassloader;

    AbstractAnswer(Object delegate) {
        this(delegate, delegate.getClass().getClassLoader());
    }

    AbstractAnswer(Object delegate, ClassLoader delegateClassloader) {
        this.delegate = checkNotNull(delegate, "delegate object cannot be null");
        this.delegateClass = delegate.getClass();
        this.delegateClassloader = delegateClassloader;

        // these constraints are necessary to prevent very hard to debug classloading issues
        checkNotInstanceOf(HazelcastAPIDelegatingClassloader.class, Thread.currentThread().getContextClassLoader(),
                "The TCCL cannot be an instance of HazelcastAPIDelegatingClassloader."
                        + " If you are executing a task from the test classloader on a proxied Hazelcast instance,"
                        + " you have to set the TCCL to the test classloader, to prevent classloading issues,"
                        + " see AbstractClassLoaderAwareCallable");
        checkInstanceOf(HazelcastAPIDelegatingClassloader.class, delegateClassloader,
                "The delegateClassloader should be an instance of HazelcastAPIDelegatingClassloader"
                        + " (delegateClassloader: " + delegateClassloader + ") (delegateClass: " + delegateClass + ")");
        if (delegateClassloader.equals(targetClassloader)) {
            throw new IllegalArgumentException("The delegateClassloader cannot be the same as the targetClassloader"
                    + " (delegateClassloader: " + delegateClassloader + ") (targetClassloader: " + targetClassloader + ")");
        }

        // disable asserts in the proxied classloader, which might fail due to instanceOf checks
        delegateClassloader.setDefaultAssertionStatus(false);
    }

    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
        String methodName = invocation.getMethod().getName();
        Object[] arguments = invocation.getArguments();
        if (arguments.length > 0) {
            arguments = proxyArgumentsIfNeeded(arguments, delegateClassloader);
        }
        return answer(invocation, methodName, arguments);
    }

    /**
     * Answers the given {@link InvocationOnMock}.
     * <p>
     * The given method name is extracted from the invoked method.
     * The given arguments are already proxied if needed.
     *
     * @param invocation the {@link InvocationOnMock} to invoke
     * @param methodName the name of the invoked method
     * @param arguments  the (proxied) arguments to use for the invocation
     * @return the (proxied) result of the invocation
     * @throws Exception if the invocation fails
     */
    abstract Object answer(InvocationOnMock invocation, String methodName, Object[] arguments) throws Exception;

    /**
     * Calls the getLogger(String) method of the delegate instance, regardless
     * if the argument was of type String or Class.
     *
     * @param arguments the arguments to use for the invocation
     * @return the (proxied) Logger instance from the invocation
     * @throws Exception if the invocation fails
     */
    Object getLogger(Object... arguments) throws Exception {
        // we always call the getLogger(String) method, to avoid the lookup of the correct target class
        Method delegateMethod = getDelegateMethod("getLogger", String.class);
        String className = (arguments[0] instanceof String) ? (String) arguments[0] : ((Class) arguments[0]).getName();
        return invoke(delegateMethod, className);
    }

    /**
     * Returns the delegate {@link Method} of the delegate class, which
     * matches the given method name and parameter types.
     *
     * @param methodName     the method name to lookup
     * @param parameterTypes the parameter types to lookup
     * @return the found {@link Method} on the delegate class
     * @throws NoSuchMethodException when no method could be found
     */
    Method getDelegateMethod(String methodName, Class<?>... parameterTypes) throws NoSuchMethodException {
        return getMethod(delegateClass, methodName, parameterTypes);
    }

    /**
     * Invokes the given {@link InvocationOnMock} and returns a non-proxied
     * result.
     * <p>
     * This method is used to retrieve the result to create another mock.
     *
     * @param invocation the {@link InvocationOnMock} to invoke
     * @param arguments  the arguments to use for the invocation
     * @return the plain result from the invocation
     * @throws Exception if the invocation fails
     */
    Object invokeForMock(InvocationOnMock invocation, Object... arguments) throws Exception {
        return invoke(false, invocation, arguments);
    }

    /**
     * Invokes the given {@link InvocationOnMock} and returns a proxied result,
     * if needed.
     * <p>
     * This method is used for most invocations and tries to resolve the
     * delegate method to call on its own.
     *
     * @param invocation the {@link InvocationOnMock} to invoke
     * @param arguments  the arguments to use for the invocation
     * @return the (proxied) result from the invocation
     * @throws Exception if the invocation fails
     */
    Object invoke(InvocationOnMock invocation, Object... arguments) throws Exception {
        return invoke(true, invocation, arguments);
    }

    /**
     * Invokes the given {@link Method} and returns a proxied result, if
     * needed.
     * <p>
     * This method is used for invocations which need their own method lookup.
     *
     * @param delegateMethod the {@link Method} to invoke
     * @param arguments      the arguments to use for the invocation
     * @return the (proxied) result from the invocation
     * @throws Exception if the invocation fails
     */
    Object invoke(Method delegateMethod, Object... arguments) throws Exception {
        return invoke(true, delegateMethod, arguments);
    }

    /**
     * Invokes the given {@link InvocationOnMock} and returns a proxied or
     * non-proxied result as requested.
     *
     * @param proxyResult {@code true} if the result should be proxied,
     *                    {@code false} otherwise
     * @param invocation  the {@link InvocationOnMock} to invoke
     * @param arguments   the arguments to use for the invocation
     * @return the (proxied) result from the invocation
     * @throws Exception if the invocation fails
     */
    Object invoke(boolean proxyResult, InvocationOnMock invocation, Object... arguments) throws Exception {
        Method originalMethod = invocation.getMethod();
        Object[] originalArguments = invocation.getArguments();
        // if an argument was proxied, we need to replace the parameterType for a correct method lookup
        Class<?>[] parameterTypes = originalMethod.getParameterTypes();
        for (int i = 0; i < originalArguments.length; i++) {
            if (arguments[i] == null) {
                continue;
            }
            Class<?> argumentClass = arguments[i].getClass();
            if (parameterTypes[i].isPrimitive()
                    || Object.class.equals(parameterTypes[i])
                    || Collection.class.isAssignableFrom(parameterTypes[i])
                    || Map.class.isAssignableFrom(parameterTypes[i])
                    || argumentClass.getClassLoader() == null
                    || argumentClass.getClassLoader().equals(originalArguments[i].getClass().getClassLoader())
                    || !argumentClass.getName().startsWith("com.hazelcast")) {
                continue;
            }
            parameterTypes[i] = argumentClass.getClassLoader().loadClass(parameterTypes[i].getName());
        }
        Method delegateMethod = getDelegateMethod(originalMethod.getName(), parameterTypes);
        return invoke(proxyResult, delegateMethod, arguments);
    }

    /**
     * Invokes the given {@link Method} and returns a proxied or non-proxied
     * result as requested.
     *
     * @param proxyResult    {@code true} if the result should be proxied,
     *                       {@code false} otherwise
     * @param delegateMethod the {@link Method} to invoke
     * @param arguments      the arguments to use for the invocation
     * @return the (proxied) result from the invocation
     * @throws Exception if the invocation fails
     */
    Object invoke(boolean proxyResult, Method delegateMethod, Object... arguments) throws Exception {
        Object result = invokeDelegateMethod(delegateMethod, arguments);
        if (result == null) {
            return null;
        }

        // collection and map like return types need to return a specialized mock,
        // to resolve their items and entries as late as possible, and to apply
        // updates on the original data structure to the mocked structure as well
        Class<?> resultClass = ReflectionUtils.getClass(result);
        Class<?> delegateReturnType = delegateMethod.getReturnType();
        if (Map.Entry.class.isAssignableFrom(delegateReturnType)) {
            return mock(Map.Entry.class, new MapEntryAnswer(result, delegateClassloader));
        } else if (Map.class.isAssignableFrom(delegateReturnType)) {
            return createMapMock(resultClass, result);
        } else if (Collection.class.isAssignableFrom(delegateReturnType)) {
            return createCollectionMock(resultClass, result);
        } else if (Iterator.class.isAssignableFrom(delegateReturnType)) {
            return createIteratorMock(resultClass, result);
        }
        // some Hazelcast return types need to return a specialized mock
        if (!resultClass.isArray() && resultClass.getPackage().getName().contains("com.hazelcast")) {
            String resultClassName = resultClass.getName();
            if (resultClassName.contains("Container") || resultClassName.contains("MultiMapValue")) {
                return createMockForTargetClass(result, new DataStructureContainerAnswer(result));
            } else if ((resultClassName.contains("Item") || resultClassName.contains("Record"))
                    && !resultClassName.contains("HiDensity")) {
                return createMockForTargetClass(result, new DataStructureElementAnswer(result));
            }
        }
        return (proxyResult && !resultClass.isPrimitive())
                ? proxyObjectForStarter(targetClassloader, result) : result;
    }

    /**
     * Invokes the given {@link Method} and returns the result.
     * <p>
     * Handles some exceptions for debugging or root cause extraction.
     *
     * @param delegateMethod the {@link Method} to invoke
     * @param arguments      the arguments to use for the invocation
     * @return the proxied result from the invocation
     * @throws Exception if the invocation fails
     */
    private Object invokeDelegateMethod(Method delegateMethod, Object... arguments) throws Exception {
        try {
            return delegateMethod.invoke(delegate, arguments);
        } catch (IllegalArgumentException e) {
            // this is a poor man's debugging for argument type mismatch errors
            if ("argument type mismatch".equals(e.getMessage())) {
                StringBuilder sb = new StringBuilder("argument type mismatch when calling ")
                        .append(delegateMethod.getName()).append("()\n");
                Class<?>[] parameterTypes = delegateMethod.getParameterTypes();
                for (int i = 0; i < parameterTypes.length; i++) {
                    sb.append("parameterType: ").append(parameterTypes[i])
                            .append(" with argument ").append(arguments[i] == null ? "null" : arguments[i].getClass())
                            .append("\n");
                }
                System.err.println(sb.toString());
            }
            throw e;
        } catch (InvocationTargetException e) {
            // some methods throw an IllegalStateException, which is asserted in tests
            // we could maybe use ExceptionUtil.peel() to improve this
            Throwable cause = getRootCause(e);
            if (cause instanceof IllegalStateException) {
                throw (IllegalStateException) cause;
            }
            throw e;
        }
    }

    private Object createMapMock(Class<?> targetClass, Object map) {
        if (isNotMockable(targetClass)) {
            targetClass = ConcurrentHashMap.class;
        }
        return mock(targetClass, new MapAnswer(map, delegateClassloader));
    }

    private Object createCollectionMock(Class<?> targetClass, Object collection) {
        if (isNotMockable(targetClass)) {
            if (Set.class.isAssignableFrom(targetClass)) {
                targetClass = HashSet.class;
            } else {
                targetClass = LinkedList.class;
            }
        }
        return mock(targetClass, new CollectionAnswer(collection, delegateClassloader));
    }

    private Object createIteratorMock(Class<?> targetClass, Object iterator) {
        if (isNotMockable(targetClass)) {
            targetClass = Iterator.class;
        }
        return mock(targetClass, new IteratorAnswer(iterator, delegateClassloader));
    }

    /**
     * Creates a mock with the given default {@link Answer} and the class of
     * the given delegate instance in the target classloader.
     * <p>
     * Can be used if the target class is not constant, but depends on the
     * given delegate instance (e.g. to create the correct mock for
     * {@link NodeEngine#getService(String)}).
     *
     * @param delegate the delegate to retrieve the class from
     * @param answer   the default {@link Answer} to create the mock with
     * @return the created mock for the target class
     * @throws Exception if the mocking fails
     */
    static Object createMockForTargetClass(Object delegate, AbstractAnswer answer) throws Exception {
        Class<?> delegateClass = ReflectionUtils.getClass(delegate);
        Class<?> targetClass = targetClassloader.loadClass(delegateClass.getName());
        return mock(targetClass, answer);
    }

    /**
     * Checks if the given class is mockable or not.
     *
     * @param targetClass the class to check
     * @return {@code true} if the class is mockable, {@code false} otherwise
     */
    private static boolean isNotMockable(Class<?> targetClass) {
        int modifiers = targetClass.getModifiers();
        return isFinal(modifiers) || isPrivate(modifiers) || isProtected(modifiers);
    }
}
