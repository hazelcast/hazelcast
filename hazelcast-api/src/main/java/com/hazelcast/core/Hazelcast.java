/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.core;

import com.hazelcast.config.Config;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Set;

/**
 * Factory for {@link HazelcastInstance}'s, a node in a cluster.
 */
public final class Hazelcast {

    private static final Method SHUTDOWN_ALL;
    private static final Method NEW_HAZELCAST_INSTANCE;
    private static final Method GET_HAZELCAST_INSTANCE;
    private static final Method GET_OR_CREATE_HAZELCAST_INSTANCE;
    private static final Method GET_ALL_HAZELCAST_INSTANCES;

    static {
        try {
            ClassLoader classLoader = Hazelcast.class.getClassLoader();
            Class instanceFactoryClass = classLoader.loadClass("com.hazelcast.instance.HazelcastInstanceFactory");
            SHUTDOWN_ALL = instanceFactoryClass.getMethod("shutdownAll");
            NEW_HAZELCAST_INSTANCE = instanceFactoryClass.getMethod("newHazelcastInstance", Config.class);
            GET_HAZELCAST_INSTANCE = instanceFactoryClass.getMethod("getHazelcastInstance", String.class);
            GET_OR_CREATE_HAZELCAST_INSTANCE = instanceFactoryClass.getMethod("getOrCreateHazelcastInstance", Config.class);
            GET_ALL_HAZELCAST_INSTANCES = instanceFactoryClass.getMethod("getAllHazelcastInstances");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Hazelcast() {
    }

    /**
     * Shuts down all running Hazelcast Instances on this JVM.
     * It doesn't shutdown all members of the
     * cluster but just the ones running on this JVM.
     *
     * @see #newHazelcastInstance(Config)
     */
    public static void shutdownAll() {
        invoke(SHUTDOWN_ALL);
    }

    /**
     * Creates a new HazelcastInstance (a new node in a cluster).
     * This method allows you to create and run multiple instances
     * of Hazelcast cluster members on the same JVM.
     * <p/>
     * To shutdown all running HazelcastInstances (all members on this JVM)
     * call {@link #shutdownAll()}.
     *
     * @param config Configuration for the new HazelcastInstance (member)
     * @return the new HazelcastInstance
     * @see #shutdownAll()
     * @see #getHazelcastInstanceByName(String)
     */
    public static HazelcastInstance newHazelcastInstance(Config config) {
        return invoke(NEW_HAZELCAST_INSTANCE, config);
    }

    /**
     * Creates a new HazelcastInstance (a new node in a cluster).
     * This method allows you to create and run multiple instances
     * of Hazelcast cluster members on the same JVM.
     * <p/>
     * To shutdown all running HazelcastInstances (all members on this JVM)
     * call {@link #shutdownAll()}.
     *
     * Hazelcast will look into two places for the configuration file:
     * <ol>
     * <li>
     * System property: Hazelcast will first check if "hazelcast.config" system property is set to a file path.
     * Example: -Dhazelcast.config=C:/myhazelcast.xml.
     * </li>
     * <li>
     * Classpath: If config file is not set as a system property, Hazelcast will check classpath for hazelcast.xml file.
     * </li>
     * </ol>
     * If Hazelcast doesn't find any config file, it will start with the default configuration (hazelcast-default.xml)
     * located in hazelcast.jar.
     *
     * @return the new HazelcastInstance
     * @see #shutdownAll()
     * @see #getHazelcastInstanceByName(String)
     */
    public static HazelcastInstance newHazelcastInstance() {
        return newHazelcastInstance(null);
    }

    /**
     * Returns an existing HazelcastInstance with instanceName.
     * <p/>
     * To shutdown all running HazelcastInstances (all members on this JVM)
     * call {@link #shutdownAll()}.
     *
     * @param instanceName Name of the HazelcastInstance (member)
     * @return an existing HazelcastInstance
     * @see #newHazelcastInstance(Config)
     * @see #shutdownAll()
     */
    public static HazelcastInstance getHazelcastInstanceByName(String instanceName) {
        return invoke(GET_HAZELCAST_INSTANCE, instanceName);
    }

    /**
     * Gets or creates the HazelcastInstance with a certain name.
     *
     * If a Hazelcast instance with the same name as the configuration exists, then it is returned, otherwise it is created.
     *
     * @param config the Config.
     * @return the HazelcastInstance
     * @throws NullPointerException     if config is null.
     * @throws IllegalArgumentException if the instance name of the config is null or empty.
     */
    public static HazelcastInstance getOrCreateHazelcastInstance(Config config) {
        return invoke(GET_OR_CREATE_HAZELCAST_INSTANCE, config);
    }


    /**
     * Returns all active/running HazelcastInstances on this JVM.
     * <p/>
     * To shutdown all running HazelcastInstances (all members on this JVM)
     * call {@link #shutdownAll()}.
     *
     * @return all active/running HazelcastInstances on this JVM
     * @see #newHazelcastInstance(Config)
     * @see #getHazelcastInstanceByName(String)
     * @see #shutdownAll()
     */
    public static Set<HazelcastInstance> getAllHazelcastInstances() {
        return invoke(GET_ALL_HAZELCAST_INSTANCES);
    }

    private static <T> T invoke(Method method, Object... args) {
        try {
            return (T) method.invoke(null, args);
        } catch (IllegalAccessException e) {
            throw new HazelcastException(e);
        } catch (InvocationTargetException e) {
            sneakyThrow(e.getTargetException());
            // won't get executed.
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T sneakyThrow(Throwable t) {
        Hazelcast.<RuntimeException>sneakyThrowInternal(t);
        return (T) t;
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrowInternal(Throwable t) throws T {
        throw (T) t;
    }

    /**
     * Sets <tt>OutOfMemoryHandler</tt> to be used when an <tt>OutOfMemoryError</tt>
     * is caught by Hazelcast threads.
     *
     * <p>
     * <b>Warning: </b> <tt>OutOfMemoryHandler</tt> may not be called although JVM throws
     * <tt>OutOfMemoryError</tt>.
     * Because error may be thrown from an external (user thread) thread
     * and Hazelcast may not be informed about <tt>OutOfMemoryError</tt>.
     * </p>
     *
     * @param outOfMemoryHandler set when an <tt>OutOfMemoryError</tt> is caught by Hazelcast threads
     * @see OutOfMemoryError
     * @see OutOfMemoryHandler
     */
    public static void setOutOfMemoryHandler(OutOfMemoryHandler outOfMemoryHandler) {
        //OutOfMemoryErrorDispatcher.setServerHandler(outOfMemoryHandler);
        //TODO:
    }
}
