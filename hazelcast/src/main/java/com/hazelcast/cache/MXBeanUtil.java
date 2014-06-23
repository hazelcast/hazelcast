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

package com.hazelcast.cache;

import javax.cache.CacheException;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.util.Set;

//FIXME Move this into ManagementService
public class MXBeanUtil {

    //ensure everything gets put in one MBeanServer
    private static MBeanServer mBeanServer = MBeanServerFactory.createMBeanServer();

    public static void registerCacheObject(ICache cache, boolean stats) {
        //these can change during runtime, so always look it up
        ObjectName registeredObjectName = calculateObjectName(cache, stats);
        try {
            if (!stats) {
                if (!isRegistered(cache, stats)) {
                    mBeanServer.registerMBean(cache.getCacheMXBean(), registeredObjectName);
                }
            } else if (stats) {
                if (!isRegistered(cache, stats)) {
                    mBeanServer.registerMBean(cache.getCacheStatisticsMXBean(), registeredObjectName);
                }
            }
        } catch (Exception e) {
            throw new CacheException("Error registering cache MXBeans for CacheManager "
                    + registeredObjectName + " . Error was " + e.getMessage(), e);
        }
    }


    static boolean isRegistered(ICache cache, boolean stats) {

        Set<ObjectName> registeredObjectNames = null;

        ObjectName objectName = calculateObjectName(cache, stats);
        registeredObjectNames = mBeanServer.queryNames(objectName, null);

        return !registeredObjectNames.isEmpty();
    }


    public static void unregisterCacheObject(ICache cache, boolean stats) {

        Set<ObjectName> registeredObjectNames = null;

        ObjectName objectName = calculateObjectName(cache, stats);
        registeredObjectNames = mBeanServer.queryNames(objectName, null);

        //should just be one
        for (ObjectName registeredObjectName : registeredObjectNames) {
            try {
                mBeanServer.unregisterMBean(registeredObjectName);
            } catch (Exception e) {
                throw new CacheException("Error unregistering object instance "
                        + registeredObjectName + " . Error was " + e.getMessage(), e);
            }
        }
    }

    /**
     * Creates an object name using the scheme
     * "javax.cache:type=Cache&lt;Statistics|Configuration&gt;,CacheManager=&lt;cacheManagerName&gt;,name=&lt;cacheName&gt;"
     */
    private static ObjectName calculateObjectName(ICache cache, boolean stats) {
        String cacheManagerName = mbeanSafe(cache.getCacheManager().getURI().toString());
        String cacheName = mbeanSafe(cache.getName());

        try {
            String objectNameType = stats?"Statistics":"Configuration";
            return new ObjectName("javax.cache:type=Cache" + objectNameType + ",CacheManager="
                    + cacheManagerName + ",Cache=" + cacheName);
        } catch (MalformedObjectNameException e) {
            throw new CacheException("Illegal ObjectName for Management Bean. " +
                    "CacheManager=[" + cacheManagerName + "], Cache=[" + cacheName + "]", e);
        }
    }

    private static String mbeanSafe(String string) {
        return string == null ? "" : string.replaceAll(",|:|=|\n", ".");
    }
}
