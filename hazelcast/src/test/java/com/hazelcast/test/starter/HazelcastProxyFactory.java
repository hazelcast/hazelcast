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

package com.hazelcast.test.starter;

import com.hazelcast.internal.util.ConcurrentReferenceHashMap;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.test.starter.constructor.EnumConstructor;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.method.MethodList;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.Transformer;
import net.bytebuddy.dynamic.scaffold.MethodRegistry;
import net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy;
import net.bytebuddy.implementation.InvocationHandlerAdapter;
import net.bytebuddy.implementation.SuperMethodCall;
import net.bytebuddy.implementation.attribute.MethodAttributeAppender;
import net.bytebuddy.matcher.ElementMatchers;
import net.bytebuddy.matcher.LatentMatcher;
import org.reflections.Reflections;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.internal.nio.ClassLoaderUtil.getAllInterfaces;
import static com.hazelcast.internal.util.ConcurrentReferenceHashMap.ReferenceType.STRONG;
import static com.hazelcast.test.starter.HazelcastAPIDelegatingClassloader.DELEGATION_WHITE_LIST;
import static com.hazelcast.test.starter.HazelcastProxyFactory.ProxyPolicy.RETURN_SAME;
import static com.hazelcast.test.starter.HazelcastStarterUtils.debug;
import static com.hazelcast.test.starter.HazelcastStarterUtils.newCollectionFor;
import static com.hazelcast.test.starter.ReflectionUtils.getConstructor;
import static com.hazelcast.test.starter.ReflectionUtils.getReflectionsForTestPackage;
import static java.util.Arrays.asList;
import static java.util.Arrays.copyOf;
import static net.bytebuddy.jar.asm.Opcodes.ACC_PUBLIC;
import static net.bytebuddy.matcher.ElementMatchers.is;
import static net.bytebuddy.matcher.ElementMatchers.isConstructor;

public class HazelcastProxyFactory {

    // classes in this whitelist will not be proxied, instead instances of the same class (by name)
    // are constructed on target classloader
    private static final Map<String, Constructor<ConstructorFunction<Object, Object>>> NO_PROXYING_WHITELIST;

    // classes in this whitelist are explicitly selected for subclass proxying
    private static final Set<String> SUBCLASS_PROXYING_WHITELIST;

    // interfaces that have been refactored in the current version
    // must be mapped both ways (old -> new name and vice versa) in this map
    private static final Map<String, String> REFACTORED_INTERFACES;

    // <Class toProxy, ClassLoader targetClassLoader> -> Class<?> proxy mapping for subclass proxies
    // java.lang.reflect.Proxy already maintains its own cache
    private static final ConcurrentReferenceHashMap<ProxySource, Class<?>> PROXIES
            = new ConcurrentReferenceHashMap<ProxySource, Class<?>>(16, STRONG, STRONG);

    // <Class targetClass, ClassLoader targetClassLoader> -> ConstructorFunction<?>
    private static final ConcurrentReferenceHashMap<Class<?>, ConstructorFunction<Object, Object>> CONSTRUCTORS
            = new ConcurrentReferenceHashMap<Class<?>, ConstructorFunction<Object, Object>>(16, STRONG, STRONG);

    static {
        Map<String, Constructor<ConstructorFunction<Object, Object>>> notProxiedClasses
                = new HashMap<String, Constructor<ConstructorFunction<Object, Object>>>();
        Set<String> subclassProxiedClasses = new HashSet<String>();
        Map<String, String> refactoredInterfaces = new HashMap<String, String>();

        Reflections reflections = getReflectionsForTestPackage("com.hazelcast.test.starter.constructor");
        Set<Class<?>> constructorClasses = reflections.getTypesAnnotatedWith(HazelcastStarterConstructor.class);
        for (Class<?> constructorClass : constructorClasses) {
            HazelcastStarterConstructor annotation = constructorClass.getAnnotation(HazelcastStarterConstructor.class);
            String[] classNames = annotation.classNames();
            switch (annotation.proxyPolicy()) {
                case NO_PROXY:
                    // we need to check that this is a valid ConstructorFunction class
                    if (!ConstructorFunction.class.isAssignableFrom(constructorClass)) {
                        throw new IllegalArgumentException("HazelcastStarterConstructor annotated class with ProxyPolicy.NO_PROXY"
                                + " has to implement ConstructorFunction<Object, Object>: " + constructorClass);
                    }
                    // we lookup the constructor of the class and store it for each supported className
                    Constructor<ConstructorFunction<Object, Object>> constructor = getConstructor(constructorClass, Class.class);
                    for (String className : classNames) {
                        notProxiedClasses.put(className, constructor);
                    }
                    break;
                case SUBCLASS_PROXY:
                    subclassProxiedClasses.addAll(asList(classNames));
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported ProxyPolicy " + annotation.proxyPolicy());
            }
        }

        NO_PROXYING_WHITELIST = notProxiedClasses;
        SUBCLASS_PROXYING_WHITELIST = subclassProxiedClasses;
        REFACTORED_INTERFACES = refactoredInterfaces;
    }

    /**
     * Decides whether the given {@code delegateClass} should be proxied by
     * subclassing, dynamic JDK proxy or not proxied at all.
     *
     * @param delegateClass class of object to be proxied
     * @param ifaces        interfaces implemented by delegateClass
     */
    public static ProxyPolicy shouldProxy(Class<?> delegateClass, Class<?>[] ifaces) {
        if (delegateClass.isPrimitive() || isJDKClass(delegateClass) || isHazelcastAPIDelegatingClassloader(delegateClass)) {
            return ProxyPolicy.RETURN_SAME;
        }
        String className = delegateClass.getName();
        if (DELEGATION_WHITE_LIST.contains(className)) {
            return RETURN_SAME;
        }
        if (NO_PROXYING_WHITELIST.containsKey(className) || delegateClass.isEnum()) {
            return ProxyPolicy.NO_PROXY;
        }
        if (SUBCLASS_PROXYING_WHITELIST.contains(className) || ifaces.length == 0) {
            return ProxyPolicy.SUBCLASS_PROXY;
        }
        return ProxyPolicy.JDK_PROXY;
    }

    /**
     * Main entry point to obtain proxies for a target class loader.
     * <p>
     * Creates an Object valid for the Hazelcast version started with
     * {@code targetClassLoader} that proxies the given {@code arg},
     * which is valid in the current Hazelcast version.
     */
    public static Object proxyObjectForStarter(ClassLoader targetClassLoader, Object arg) throws ClassNotFoundException {
        if (arg == null) {
            return null;
        }

        // handle JDK collections (e.g. ArrayList)
        if (isJDKClass(arg.getClass()) && Collection.class.isAssignableFrom(arg.getClass())) {
            Collection<Object> targetCollection = newCollectionFor(arg.getClass());
            for (Object item : (Collection) arg) {
                targetCollection.add(proxyObjectForStarter(targetClassLoader, item));
            }
            return targetCollection;
        } else if (isJDKClass(arg.getClass()) && Map.class.isAssignableFrom(arg.getClass())) {
            Map<Object, Object> targetMap = new ConcurrentHashMap<Object, Object>();
            Map mapArg = (Map) arg;
            for (Object entry : mapArg.entrySet()) {
                Object key = proxyObjectForStarter(targetClassLoader, ((Map.Entry) entry).getKey());
                Object value = proxyObjectForStarter(targetClassLoader, ((Map.Entry) entry).getValue());
                targetMap.put(key, value);
            }
            return targetMap;
        } else if (arg.getClass().isArray()) {
            return toArray(targetClassLoader, arg);
        }

        if (arg.getClass().getClassLoader() == targetClassLoader) {
            return arg;
        }

        Class<?>[] ifaces = getAllInterfacesIncludingSelf(arg.getClass());
        Class<?>[] delegateIfaces = new Class<?>[ifaces.length];
        Object newArg;
        ProxyPolicy proxyPolicy = shouldProxy(arg.getClass(), ifaces);
        debug("Proxy policy for %s is %s", arg.getClass(), proxyPolicy);
        switch (proxyPolicy) {
            case NO_PROXY:
                newArg = constructWithoutProxy(targetClassLoader, arg);
                break;
            case SUBCLASS_PROXY:
                newArg = constructWithSubclassProxy(targetClassLoader, arg);
                break;
            case JDK_PROXY:
                newArg = constructWithJdkProxy(targetClassLoader, arg, ifaces, delegateIfaces);
                break;
            case RETURN_SAME:
                newArg = arg;
                break;
            default:
                throw new GuardianException("Unsupported proxy policy: " + proxyPolicy);
        }
        return newArg;
    }

    /**
     * Converts an array of arguments to the {@code targetClassLoader}, so they
     * can be passed conveniently as an argument to a method of a class in the
     * another classloader.
     */
    public static Object[] proxyArgumentsIfNeeded(Object[] args, ClassLoader targetClassLoader) throws ClassNotFoundException {
        if (args == null) {
            return null;
        }

        Object[] newArgs = new Object[args.length];
        for (int i = 0; i < args.length; i++) {
            Object arg = args[i];
            if (arg == null || (isJDKClass(arg.getClass()) && !isParameterizedType(arg.getClass()))) {
                newArgs[i] = arg;
            } else {
                newArgs[i] = proxyObjectForStarter(targetClassLoader, arg);
            }
        }
        return newArgs;
    }

    /**
     * Generates a JDK dynamic proxy implementing the expected interfaces.
     */
    @SuppressWarnings("unchecked")
    public static <T> T generateProxyForInterface(Object delegate, ClassLoader proxyTargetClassloader,
                                                  Class<?>... expectedInterfaces) {
        InvocationHandler myInvocationHandler = new ProxyInvocationHandler(delegate);
        return (T) Proxy.newProxyInstance(proxyTargetClassloader, expectedInterfaces, myInvocationHandler);
    }

    private static boolean isJDKClass(Class clazz) {
        return clazz.getClassLoader() == String.class.getClassLoader();
    }

    private static boolean isHazelcastAPIDelegatingClassloader(Class clazz) {
        return HazelcastAPIDelegatingClassloader.class.equals(clazz);
    }

    private static Object constructWithJdkProxy(ClassLoader targetClassLoader, Object arg, Class<?>[] ifaces,
                                                Class<?>[] delegateIfaces) throws ClassNotFoundException {
        for (int j = 0; j < ifaces.length; j++) {
            Class<?> clazz = ifaces[j];
            String className = clazz.getName();
            String classNameOnTargetClassLoader = className;
            if (REFACTORED_INTERFACES.containsKey(className)) {
                classNameOnTargetClassLoader = REFACTORED_INTERFACES.get(className);
            }
            Class<?> delegateInterface = targetClassLoader.loadClass(classNameOnTargetClassLoader);
            delegateIfaces[j] = delegateInterface;
        }
        return generateProxyForInterface(arg, targetClassLoader, delegateIfaces);
    }

    private static Object constructWithSubclassProxy(ClassLoader targetClassLoader, Object arg) throws ClassNotFoundException {
        // proxy class via subclassing the existing class implementation in the target targetClassLoader
        targetClassLoader.loadClass(arg.getClass().getName());
        return proxyWithSubclass(targetClassLoader, arg);
    }

    private static Object constructWithoutProxy(ClassLoader targetClassLoader, Object arg) throws ClassNotFoundException {
        if (isJDKClass(arg.getClass())) {
            return arg;
        }

        // obtain class in targetClassLoader
        Class<?> targetClass = targetClassLoader.loadClass(arg.getClass().getName());
        return construct(targetClass, arg);
    }

    private static Object proxyWithSubclass(ClassLoader targetClassLoader, final Object delegate) {
        ProxySource proxySource = ProxySource.of(delegate.getClass(), targetClassLoader);
        Class<?> targetClass = PROXIES.applyIfAbsent(proxySource,
                input -> new ByteBuddy().subclass(input.getToProxy(), AllAsPublicConstructorStrategy.INSTANCE)
                        .method(ElementMatchers.isDeclaredBy(input.getToProxy()))
                        .intercept(InvocationHandlerAdapter.of(new ProxyInvocationHandler(delegate)))
                        .make()
                        .load(input.getTargetClassLoader())
                        .getLoaded());
        return construct(targetClass, delegate);
    }

    private static Object construct(Class<?> clazz, Object delegate) {
        ConstructorFunction<Object, Object> constructorFunction = CONSTRUCTORS.applyIfAbsent(clazz, input -> {
            String className = input.getName();
            Constructor<ConstructorFunction<Object, Object>> constructor = NO_PROXYING_WHITELIST.get(className);
            if (constructor != null) {
                try {
                    return constructor.newInstance(input);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            } else if (input.isEnum()) {
                return new EnumConstructor(input);
            }
            throw new UnsupportedOperationException("Cannot construct target object for target " + input
                    + " on classloader " + input.getClassLoader());
        });

        return constructorFunction.createNew(delegate);
    }

    private static Object toArray(ClassLoader targetClassLoader, Object arg) throws ClassNotFoundException {
        if (arg instanceof byte[]) {
            return copyOf((byte[]) arg, ((byte[]) arg).length);
        } else if (arg instanceof int[]) {
            return copyOf((int[]) arg, ((int[]) arg).length);
        } else if (arg instanceof long[]) {
            return copyOf((long[]) arg, ((long[]) arg).length);
        } else if (arg instanceof boolean[]) {
            return copyOf((boolean[]) arg, ((boolean[]) arg).length);
        } else if (arg instanceof short[]) {
            return copyOf((short[]) arg, ((short[]) arg).length);
        } else if (arg instanceof float[]) {
            return copyOf((float[]) arg, ((float[]) arg).length);
        } else if (arg instanceof double[]) {
            return copyOf((double[]) arg, ((double[]) arg).length);
        } else if (arg instanceof char[]) {
            return copyOf((char[]) arg, ((char[]) arg).length);
        }
        Object[] srcArray = ((Object[]) arg);
        Class<?> targetClass = targetClassLoader.loadClass(srcArray.getClass().getComponentType().getName());
        Object[] targetArray = (Object[]) Array.newInstance(targetClass, srcArray.length);
        for (int i = 0; i < srcArray.length; i++) {
            targetArray[i] = proxyObjectForStarter(targetClassLoader, srcArray[i]);
        }
        return targetArray;
    }

    /**
     * Returns all interfaces implemented by {@code type}, along with
     * {@code type} itself if it's an interface.
     */
    private static Class<?>[] getAllInterfacesIncludingSelf(Class<?> type) {
        Set<Class<?>> interfaces = new HashSet<Class<?>>(Arrays.asList(getAllInterfaces(type)));
        //if the return type itself is an interface then we have to add it
        //to the list of interfaces implemented by the proxy
        if (type.isInterface()) {
            interfaces.add(type);
        }
        return interfaces.toArray(new Class<?>[0]);
    }

    private static boolean isParameterizedType(Class<?> clazz) {
        return clazz.getTypeParameters().length > 0;
    }

    /**
     * Tuple of {@link Class} {@code toProxy} and {@link ClassLoader}
     * {@code targetClassloader} that is used as a key for caching the
     * generated proxy class for {@code toProxy} on the
     * {@code targetClassloader}.
     */
    private static class ProxySource {

        private final Class<?> toProxy;
        private final ClassLoader targetClassLoader;

        ProxySource(Class<?> toProxy, ClassLoader targetClassLoader) {
            this.toProxy = toProxy;
            this.targetClassLoader = targetClassLoader;
        }

        Class<?> getToProxy() {
            return toProxy;
        }

        ClassLoader getTargetClassLoader() {
            return targetClassLoader;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ProxySource that = (ProxySource) o;
            if (!toProxy.equals(that.toProxy)) {
                return false;
            }
            return targetClassLoader.equals(that.targetClassLoader);
        }

        @Override
        public int hashCode() {
            int result = toProxy.hashCode();
            result = 31 * result + targetClassLoader.hashCode();
            return result;
        }

        public static ProxySource of(Class<?> klass, ClassLoader targetClassLoader) {
            return new ProxySource(klass, targetClassLoader);
        }
    }

    public enum ProxyPolicy {
        /**
         * Indicates that a class can be proxied by a JDK proxy implementing its interfaces
         */
        JDK_PROXY,
        /**
         * Proxy class by creating a subclass of delegate's class on target class loader
         */
        SUBCLASS_PROXY,
        /**
         * Do not proxy class, instead construct an instance of delegate's class on target class loader
         */
        NO_PROXY,
        /**
         * Do not proxy, neither attempt locating class at target classloader; instead return the object itself
         */
        RETURN_SAME,
    }

    public static class AllAsPublicConstructorStrategy implements ConstructorStrategy {

        public static final AllAsPublicConstructorStrategy INSTANCE = new AllAsPublicConstructorStrategy();

        @Override
        public MethodRegistry inject(TypeDescription instrumentedType, MethodRegistry methodRegistry) {
            return methodRegistry.append(new LatentMatcher.Resolved<MethodDescription>(isConstructor()),
                    new MethodRegistry.Handler.ForImplementation(SuperMethodCall.INSTANCE),
                    MethodAttributeAppender.NoOp.INSTANCE,
                    Transformer.NoOp.<MethodDescription>make());
        }

        @Override
        public List<MethodDescription.Token> extractConstructors(TypeDescription instrumentedType) {
            List<MethodDescription.Token> tokens = doExtractConstructors(instrumentedType);
            List<MethodDescription.Token> stripped = new ArrayList<MethodDescription.Token>(tokens.size());
            for (MethodDescription.Token token : tokens) {
                stripped.add(new MethodDescription.Token(token.getName(),
                        ACC_PUBLIC,
                        token.getTypeVariableTokens(),
                        token.getReturnType(),
                        token.getParameterTokens(),
                        token.getExceptionTypes(),
                        token.getAnnotations(),
                        token.getDefaultValue(),
                        TypeDescription.Generic.UNDEFINED));
            }
            return stripped;
        }

        List<MethodDescription.Token> doExtractConstructors(TypeDescription instrumentedType) {
            TypeDescription.Generic superClass = instrumentedType.getSuperClass();
            return (superClass == null
                    ? new MethodList.Empty<MethodDescription.InGenericShape>()
                    : superClass.getDeclaredMethods().filter(isConstructor())).asTokenList(is(instrumentedType));
        }
    }
}
