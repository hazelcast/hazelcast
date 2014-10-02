/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl;

import javax.cache.CacheException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Set;

/**
 * Provides utility functionality for the MXBean.
 */
//TODO Move this into ManagementService
public final class MXBeanUtil {

    //ensure everything gets put in one MBeanServer
    private static MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

    private MXBeanUtil() {
    }

    public static void registerCacheObject(Object mxbean, String cacheManagerName, String name, boolean stats) {
        //these can change during runtime, so always look it up
        ObjectName registeredObjectName = calculateObjectName(cacheManagerName, name, stats);
        try {
            if (!isRegistered(cacheManagerName, name, stats)) {
                mBeanServer.registerMBean(mxbean, registeredObjectName);
            }
        } catch (Exception e) {
            throw new CacheException(
                    "Error registering cache MXBeans for CacheManager " + registeredObjectName + " . Error was " + e.getMessage(),
                    e);
        }
    }

    static boolean isRegistered(String cacheManagerName, String name, boolean stats) {
        Set<ObjectName> registeredObjectNames = null;

        ObjectName objectName = calculateObjectName(cacheManagerName, name, stats);
        registeredObjectNames = mBeanServer.queryNames(objectName, null);

        return !registeredObjectNames.isEmpty();
    }

    public static void unregisterCacheObject(String cacheManagerName, String name, boolean stats) {
        Set<ObjectName> registeredObjectNames = null;

        ObjectName objectName = calculateObjectName(cacheManagerName, name, stats);
        registeredObjectNames = mBeanServer.queryNames(objectName, null);

        //should just be one
        for (ObjectName registeredObjectName : registeredObjectNames) {
            try {
                mBeanServer.unregisterMBean(registeredObjectName);
            } catch (Exception e) {
                throw new CacheException(
                        "Error unregistering object instance " + registeredObjectName + " . Error was " + e.getMessage(), e);
            }
        }
    }

    /**
     * Creates an object name using the scheme
     * "javax.cache:type=Cache&lt;Statistics|Configuration&gt;,name=&lt;cacheName&gt;"
     */
    public static ObjectName calculateObjectName(String cacheManagerName, String name, boolean stats) {
        String cacheManagerNameSafe = mbeanSafe(cacheManagerName);
        String cacheName = mbeanSafe(name);

        try {
            String objectNameType = stats ? "Statistics" : "Configuration";
            return new ObjectName(
                    "javax.cache:type=Cache" + objectNameType + ",CacheManager=" + cacheManagerNameSafe + ",Cache=" + cacheName);
        } catch (MalformedObjectNameException e) {
            throw new CacheException(
                    "Illegal ObjectName for Management Bean. " + "CacheManager=[" + cacheManagerNameSafe + "], Cache=["
                            + cacheName + "]", e);
        }
    }

    private static String mbeanSafe(String string) {
        return string == null ? "" : string.replaceAll(",|:|=|\n", ".");
    }
}
