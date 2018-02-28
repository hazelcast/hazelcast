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

import com.hazelcast.core.IFunction;
import com.hazelcast.util.ConcurrentReferenceHashMap;
import com.hazelcast.util.ConstructorFunction;
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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.hazelcast.nio.ClassLoaderUtil.getAllInterfaces;
import static com.hazelcast.test.starter.HazelcastAPIDelegatingClassloader.DELEGATION_WHITE_LIST;
import static com.hazelcast.test.starter.HazelcastProxyFactory.ProxyPolicy.RETURN_SAME;
import static com.hazelcast.util.ConcurrentReferenceHashMap.ReferenceType.STRONG;
import static net.bytebuddy.jar.asm.Opcodes.ACC_PUBLIC;
import static net.bytebuddy.matcher.ElementMatchers.is;
import static net.bytebuddy.matcher.ElementMatchers.isConstructor;

public class HazelcastProxyFactory {

    // classes in this whitelist will not be proxied, instead instances of the same class (by name)
    // are constructed on target classloader
    private static final Set<String> NO_PROXYING_WHITELIST;

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

    private static final String CLASS_NAME_ENTRY_EVENT = "com.hazelcast.core.EntryEvent";
    private static final String CLASS_NAME_LIFECYCLE_EVENT = "com.hazelcast.core.LifecycleEvent";
    private static final String CLASS_NAME_DATA_AWARE_ENTRY_EVENT = "com.hazelcast.map.impl.DataAwareEntryEvent";
    private static final String CLASS_NAME_MAP_EVENT = "com.hazelcast.core.MapEvent";
    private static final String CLASS_NAME_CONFIG = "com.hazelcast.config.Config";
    private static final String CLASS_NAME_CLIENT_CONFIG = "com.hazelcast.client.config.ClientConfig";
    private static final String CLASS_NAME_ADDRESS = "com.hazelcast.nio.Address";
    private static final String CLASS_NAME_VERSION = "com.hazelcast.version.Version";
    private static final String CLASS_NAME_EVENT_JOURNAL_READER_39 = "com.hazelcast.journal.EventJournalReader";
    private static final String CLASS_NAME_EVENT_JOURNAL_READER = "com.hazelcast.internal.journal.EventJournalReader";

    static {
        Set<String> notProxiedClasses = new HashSet<String>();
        notProxiedClasses.add(CLASS_NAME_DATA_AWARE_ENTRY_EVENT);
        notProxiedClasses.add(CLASS_NAME_MAP_EVENT);
        notProxiedClasses.add(CLASS_NAME_CONFIG);
        notProxiedClasses.add(CLASS_NAME_CLIENT_CONFIG);
        notProxiedClasses.add(CLASS_NAME_ADDRESS);
        notProxiedClasses.add(CLASS_NAME_VERSION);
        NO_PROXYING_WHITELIST = notProxiedClasses;

        Set<String> subclassProxiedClasses = new HashSet<String>();
        subclassProxiedClasses.add(CLASS_NAME_ENTRY_EVENT);
        subclassProxiedClasses.add(CLASS_NAME_LIFECYCLE_EVENT);
        SUBCLASS_PROXYING_WHITELIST = subclassProxiedClasses;

        // RU_COMPAT_3_9 revise refactored interfaces mapping in 3.11 development cycle
        Map<String, String> refactoredInterfaces = new HashMap<String, String>();
        refactoredInterfaces.put(CLASS_NAME_EVENT_JOURNAL_READER, CLASS_NAME_EVENT_JOURNAL_READER_39);
        refactoredInterfaces.put(CLASS_NAME_EVENT_JOURNAL_READER_39, CLASS_NAME_EVENT_JOURNAL_READER);
        REFACTORED_INTERFACES = refactoredInterfaces;
    }

    /**
     * This is the main entry point to obtain proxies for a target class loader.
     * Create an Object valid for the Hazelcast version started with {@code targetClassLoader} that proxies
     * the given {@code arg} which is valid in the current Hazelcast version.
     */
    public static Object proxyObjectForStarter(ClassLoader targetClassLoader, Object arg) throws ClassNotFoundException {
        // handle JDK collections (eg ArrayList etc)
        if (isJDKClass(arg.getClass()) && Collection.class.isAssignableFrom(arg.getClass())) {
            Collection targetCollection = newCollectionFor(arg.getClass());
            Collection collectionArg = (Collection) arg;
            for (Object o : collectionArg) {
                targetCollection.add(proxyObjectForStarter(targetClassLoader, o));
            }
            return targetCollection;
        }

        if (arg.getClass().getClassLoader() == targetClassLoader) {
            return arg;
        }

        Class<?>[] ifaces = getAllInterfacesIncludingSelf(arg.getClass());
        Class<?>[] delegateIfaces = new Class<?>[ifaces.length];
        Object newArg;
        ProxyPolicy proxyPolicy = shouldProxy(arg.getClass(), ifaces);
        Utils.debug("Proxy policy for " + arg.getClass() + " is " + proxyPolicy);
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
     * Convenience method to proxy an array of objects to be passed as arguments to a method on a class that is
     * loaded by {@code targetClassLoader}
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
     * @return a new Collection object of a class that is assignable from the given type
     */
    static Collection newCollectionFor(Class type) {
        if (Set.class.isAssignableFrom(type)) {
            // original set might be ordered
            return new LinkedHashSet();
        } else if (List.class.isAssignableFrom(type)) {
            return new ArrayList();
        } else if (Queue.class.isAssignableFrom(type)) {
            return new ConcurrentLinkedQueue();
        } else if (Collection.class.isAssignableFrom(type)) {
            return new LinkedList();
        } else {
            throw new UnsupportedOperationException("Cannot locate collection type for " + type);
        }
    }

    static boolean isJDKClass(Class clazz) {
        return clazz.getClassLoader() == String.class.getClassLoader();
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
        Class<?> delegateClass = targetClassLoader.loadClass(arg.getClass().getName());
        return proxyWithSubclass(targetClassLoader, arg, delegateClass);
    }

    private static Object constructWithoutProxy(ClassLoader targetClassLoader, Object arg) throws ClassNotFoundException {
        if (isJDKClass(arg.getClass())) {
            return arg;
        }

        // obtain class in targetClassLoader
        Class<?> targetClass = targetClassLoader.loadClass(arg.getClass().getName());
        return construct(targetClass, arg);
    }

    /**
     * Generate a JDK dynamic proxy implementing the expected interfaces.
     */
    private static <T> T generateProxyForInterface(Object delegate, ClassLoader proxyTargetClassloader,
                                                   Class<?>... expectedInterfaces) {
        InvocationHandler myInvocationHandler = new ProxyInvocationHandler(delegate);
        return (T) Proxy.newProxyInstance(proxyTargetClassloader, expectedInterfaces, myInvocationHandler);
    }

    private static Object proxyWithSubclass(ClassLoader targetClassLoader, final Object arg, Class<?> delegateClass) {
        Class<?> targetClass;
        ProxySource proxySource = ProxySource.of(arg.getClass(), targetClassLoader);
        targetClass = PROXIES.applyIfAbsent(proxySource, new IFunction<ProxySource, Class<?>>() {
            @Override
            public Class<?> apply(ProxySource input) {
                return new ByteBuddy().subclass(input.getToProxy(), AllAsPublicConstructorStrategy.INSTANCE)
                        .method(ElementMatchers.isDeclaredBy(input.getToProxy()))
                        .intercept(InvocationHandlerAdapter.of(new ProxyInvocationHandler(arg)))
                        .make()
                        .load(input.getTargetClassLoader())
                        .getLoaded();
            }
        });
        return construct(targetClass, arg);
    }

    /**
     * Decide whether given {@code delegateClass} should be proxied by subclassing, dynamic JDK proxy or not
     * proxied at all.
     *
     * @param delegateClass class of object to be proxied
     * @param ifaces        interfaces implemented by delegateClass
     */
    private static ProxyPolicy shouldProxy(Class<?> delegateClass, Class<?>[] ifaces) {
        if (delegateClass.isPrimitive() || isJDKClass(delegateClass)) {
            return ProxyPolicy.RETURN_SAME;
        }

        String className = delegateClass.getName();
        if (DELEGATION_WHITE_LIST.contains(className)) {
            return RETURN_SAME;
        }

        if (NO_PROXYING_WHITELIST.contains(className) || delegateClass.isEnum()) {
            return ProxyPolicy.NO_PROXY;
        }

        if (SUBCLASS_PROXYING_WHITELIST.contains(className) || ifaces.length == 0) {
            return ProxyPolicy.SUBCLASS_PROXY;
        }

        return ProxyPolicy.JDK_PROXY;
    }

    private static Object construct(Class<?> klass, Object delegate) {
        ConstructorFunction<Object, Object> constructorFunction = CONSTRUCTORS.applyIfAbsent(klass,
                new IFunction<Class<?>, ConstructorFunction<Object, Object>>() {
                    @Override
                    public ConstructorFunction<Object, Object> apply(Class<?> input) {
                        String className = input.getName();
                        if (className.equals(CLASS_NAME_DATA_AWARE_ENTRY_EVENT)) {
                            return new DataAwareEntryEventConstructor(input);
                        } else if (className.equals(CLASS_NAME_MAP_EVENT)) {
                            return new MapEventConstructor(input);
                        } else if (className.equals(CLASS_NAME_LIFECYCLE_EVENT)) {
                            return new LifecycleEventConstructor(input);
                        } else if (className.equals(CLASS_NAME_ADDRESS)) {
                            return new AddressConstructor(input);
                        } else if (className.equals(CLASS_NAME_CONFIG)
                                || className.equals(CLASS_NAME_CLIENT_CONFIG)) {
                            return new ConfigConstructor(input);
                        } else if (className.equals(CLASS_NAME_VERSION)) {
                            return new VersionConstructor(input);
                        } else if (input.isEnum()) {
                            return new EnumConstructor(input);
                        } else {
                            throw new UnsupportedOperationException("Cannot construct target object "
                                    + "for target class" + input + " on classloader " + input.getClassLoader());
                        }
                    }
                });

        return constructorFunction.createNew(delegate);
    }

    /**
     * Return all interfaces implemented by {@code type}, along with {@code type} itself if it is an interface
     */
    private static Class<?>[] getAllInterfacesIncludingSelf(Class<?> type) {
        Set<Class<?>> interfaces = new HashSet<Class<?>>();
        interfaces.addAll(Arrays.asList(getAllInterfaces(type)));
        //if the return type itself is an interface then we have to add it
        //to the list of interfaces implemented by the proxy
        if (type.isInterface()) {
            interfaces.add(type);
        }
        return interfaces.toArray(new Class<?>[0]);
    }

    private static boolean isParameterizedType(Class<?> klass) {
        return klass.getTypeParameters().length > 0;
    }

    /**
     * (Class toProxy, ClassLoader targetClassLoader) tuple that is used as a key for caching the generated
     * proxy class for {@code toProxy} on {@code targetClassloader}.
     */
    private static class ProxySource {
        private final Class<?> toProxy;
        private final ClassLoader targetClassLoader;

        public ProxySource(Class<?> toProxy, ClassLoader targetClassLoader) {
            this.toProxy = toProxy;
            this.targetClassLoader = targetClassLoader;
        }

        public Class<?> getToProxy() {
            return toProxy;
        }

        public ClassLoader getTargetClassLoader() {
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
        public MethodRegistry inject(MethodRegistry methodRegistry) {
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

        protected List<MethodDescription.Token> doExtractConstructors(TypeDescription instrumentedType) {
            TypeDescription.Generic superClass = instrumentedType.getSuperClass();
            return (superClass == null
                    ? new MethodList.Empty<MethodDescription.InGenericShape>()
                    : superClass.getDeclaredMethods().filter(isConstructor())).asTokenList(is(instrumentedType));
        }
    }
}
