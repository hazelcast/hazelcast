/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * A basic filtering ObjectInputStream that will apply a blacklist and/or a
 * whitelist on deserialization. A class will be deserialized if and only if it
 * appears in the whitelist and not the blacklist. If the whitelist is empty,
 * whitelisting will be disabled and all classes are considered to appear in it.
 * The blacklist is always checked first.
 */
public class FilteringObjectInputStream extends ObjectInputStream {

    /**
     * A default list of classes usable in deserialization exploit gadgets
     */
    private static final Set<String> DEFAULT_SERIALIATION_BLACKLIST = new HashSet<String>(
            Arrays.asList(
                    "com.sun.org.apache.xalan.internal.xsltc.trax.TemplatesImpl",
                    "bsh.XThis", "org.apache.commons.beanutils.BeanComparator",
                    "org.apache.commons.collections.functors",
                    "org.apache.commons.collections4.functors",
                    "org.codehaus.groovy.runtime.MethodClosure",
                    "org.springframework.beans.factory.ObjectFactory"));

    private static final String DESERIALIZATION_ERROR = "Unable to deserialize class ";

    /**
     * Serialization blacklist cache, populated by getSerializationBlackList()
     */
    private static Set<String> serializationBlacklistCache;

    /**
     * Serialization black/whitelisting suppression
     */
    private static boolean serializationListSuppressed;

    /**
     * Serialization whitelist cache, populated by getSerializationWhiteList()
     */
    private static Set<String> serializationWhitelistCache;

    /**
     * Basic constructor
     * @param is
     * @throws IOException
     */
    FilteringObjectInputStream(InputStream is) throws IOException {
        super(is);
    }

    /**
     * Generate and cache the serialization blacklist
     */
    private static Set<String> getSerializationBlackList() throws ClassNotFoundException {
        Set<String> defaultList = serializationBlacklistCache;

        if (defaultList == null) {
            defaultList = DEFAULT_SERIALIATION_BLACKLIST;

            try {
                String suppressProp = System.getProperty("hazelcast.serialization.suppressCheck");
                String blacklistProp = System.getProperty("hazelcast.serialization.blacklist");

                if (suppressProp != null) {
                    if (Boolean.parseBoolean(suppressProp)) {
                        serializationBlacklistCache = null;
                        return null;
                    }
                }

                if (blacklistProp != null) {
                    String[] blacklistClasses = blacklistProp.split(",");
                    defaultList.addAll(Arrays.asList(blacklistClasses));
                }
            } catch (SecurityException e) {
                throw new ClassNotFoundException("Security exception reading blacklist configuration", e);
            }

            serializationBlacklistCache = defaultList;
        }

        return serializationBlacklistCache;
    }

    /**
     * Generate and cache the serialization whitelist
     */
    private static Set<String> getSerializationWhiteList() throws ClassNotFoundException {
        if (serializationWhitelistCache == null) {
            Set<String> defaultList = new HashSet<String>();

            try {
                String suppressProp = System
                        .getProperty("hazelcast.serialization.suppressCheck");
                String whitelistProp = System
                        .getProperty("hazelcast.serialization.whitelist");

                if (suppressProp != null) {
                    if (Boolean.parseBoolean(suppressProp)) {
                        serializationListSuppressed = true;
                        serializationWhitelistCache = null;
                        return null;
                    }
                }

                if (whitelistProp != null) {
                    String[] whitelistClasses = whitelistProp.split(",");
                    defaultList.addAll(Arrays.asList(whitelistClasses));
                }
            } catch (SecurityException e) {
                throw new ClassNotFoundException("Security exception reading whitelist configuration", e);
            }

            serializationWhitelistCache = defaultList;
        }

        return serializationWhitelistCache;
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException,
            ClassNotFoundException {
        if (!serializationListSuppressed) {
            checkBlackAndWhiteLists(desc);
        }

        // Not necessary to return anything here
        return null;
    }

    /**
     * Throws an exception if the class appears on the blacklist or does not appear
     * on a non-empty whitelist.
     * @param desc The class to check
     * @throws ClassNotFoundException if the class is disallowed
     */
    private void checkBlackAndWhiteLists(ObjectStreamClass desc)
            throws ClassNotFoundException {
        String className = desc.getName();
        Set<String> blackList = getSerializationBlackList();
        Set<String> whiteList = getSerializationWhiteList();

        if (blackList.contains(className)) {
            throw new ClassNotFoundException(DESERIALIZATION_ERROR
                    + className);
        }

        int dotPosition = className.lastIndexOf(".");
        if (dotPosition > 0) {
            String packageName = className.substring(0, dotPosition);

            if (blackList.contains(packageName)) {
                throw new ClassNotFoundException(DESERIALIZATION_ERROR
                        + className);
            }

            if (whiteList.size() > 0) {
                if (!whiteList.contains(className)
                        && !whiteList.contains(packageName)) {
                    throw new ClassNotFoundException(DESERIALIZATION_ERROR
                            + className);
                }
            }
        } else {
            if (whiteList.size() > 0) {
                if (!whiteList.contains(className)) {
                    throw new ClassNotFoundException(DESERIALIZATION_ERROR
                            + className);
                }
            }
        }
    }

}
