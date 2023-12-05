/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.namespace.impl;

import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.jet.impl.deployment.MapResourceClassLoader;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.lang.reflect.Method;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.ServiceLoader;

/**
 * A wrapper around {@link DriverManager} to allow accessing registered {@link Driver}s from a {@link ClassLoader} other than
 * the one that registered it
 */
public class NamespaceAwareDriverManagerInterface {
    private static final ILogger LOGGER = Logger.getLogger(NamespaceAwareDriverManagerInterface.class);

    private final MapResourceClassLoader classLoader = (MapResourceClassLoader) getClass().getClassLoader();

    public static void initializeJdbcDrivers(String nsName, MapResourceClassLoader classLoader) {
        ServiceLoader<? extends Driver> driverLoader = ServiceLoader.load(Driver.class, classLoader);

        LOGGER.finest("Initializing driverLoader=%s in namespace %s", driverLoader, nsName);

        for (Driver d : driverLoader) {
            if (d.getClass().getClassLoader() == classLoader) {
                LOGGER.finest("Registering driver %s from classloader for namespace %s", d.getClass(), nsName);

                try {
                    DriverManager.registerDriver(d);
                } catch (SQLException e) {
                    LOGGER.warning("Failed to register driver " + d + " in namespace " + nsName, e);
                }
            } else {
                LOGGER.finest("Skipping %s because it's classloader (%s) differs from classloader (%s)", d.getClass(),
                        d.getClass().getClassLoader(), classLoader);
            }
        }
    }

    /**
     * cleanup any JDBC drivers that were registered from that classloader
     * <p>
     * Reloads this class inside the supplied {@code classLoader}, so that when {@link #cleanupJdbcDrivers(String)} calls
     * {@link DriverManager#deregisterDriver(Driver)}, it's called from within the required {@link ClassLoader} that allows it
     * to operate correctly.
     */
    public static void cleanupJdbcDrivers(String nsName, MapResourceClassLoader classLoader) {
        try {
            // This is a "NamespaceAwareDriverManagerInterface", but from the other ClassLoader, so no casting
            Class<?> clazz = classLoader.loadClassFromThisLoader(NamespaceAwareDriverManagerInterface.class);
            Method cleanupJdbcDriversMethod = clazz.getDeclaredMethod("cleanupJdbcDrivers", String.class);
            cleanupJdbcDriversMethod.invoke(clazz.getDeclaredConstructor().newInstance(), nsName);
        } catch (ReflectiveOperationException e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }

    /** Public only to allow easy reflection call - not actually sensible to call externally */
    public void cleanupJdbcDrivers(String nsName) {
        Enumeration<Driver> registeredDrivers = DriverManager.getDrivers();

        LOGGER.finest("Cleaning up registeredDrivers=%s, in namespace %s", registeredDrivers, nsName);

        while (registeredDrivers.hasMoreElements()) {
            Driver d = registeredDrivers.nextElement();

            if (d.getClass().getClassLoader() == classLoader) {
                try {
                    LOGGER.finest("Deregistering %s from removed classloader for namespace %s", d.getClass(), nsName);
                    DriverManager.deregisterDriver(d);
                } catch (SQLException e) {
                    LOGGER.warning("Failed to deregister driver " + d + " in namespace " + nsName, e);
                }
            } else {
                LOGGER.finest("Skipping %s because it's classloader (%s) differs from removedClassLoader (%s)", d.getClass(),
                        d.getClass().getClassLoader(), classLoader);
            }
        }
    }
}
