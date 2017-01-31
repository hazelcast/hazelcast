/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.usercodedeployment.impl;

import com.hazelcast.config.UserCodeDeploymentConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.IOUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.nio.IOUtil.toByteArray;

/**
 * Provides {@link ClassData} to remote members.
 *
 * It may consult a local class cache when enabled and then it delegates to a local classloader.
 */
public final class ClassDataProvider {
    private final UserCodeDeploymentConfig.ProviderMode providerMode;
    private final ClassLoader parent;
    private final ConcurrentMap<String, ClassSource> classSourceMap;
    private final ILogger logger;

    public ClassDataProvider(UserCodeDeploymentConfig.ProviderMode providerMode,
                             ClassLoader parent,
                             ConcurrentMap<String, ClassSource> classSourceMap,
                             ILogger logger) {
        this.providerMode = providerMode;
        this.parent = parent;
        this.classSourceMap = classSourceMap;
        this.logger = logger;
    }

    public ClassData getClassDataOrNull(String className) {
        if (providerMode == UserCodeDeploymentConfig.ProviderMode.OFF) {
            return null;
        }
        ClassData classData = null;
        byte[] bytecode = loadBytecodeFromParent(className);
        if (bytecode == null && providerMode == UserCodeDeploymentConfig.ProviderMode.LOCAL_AND_CACHED_CLASSES) {
            bytecode = loadBytecodeFromCache(className);
        }
        if (bytecode != null) {
            classData = new ClassData(bytecode);
        }
        return classData;
    }

    private byte[] loadBytecodeFromCache(String className) {
        ClassSource classSource = classSourceMap.get(className);
        if (classSource == null) {
            return null;
        }
        return classSource.getBytecode();
    }

    private byte[] loadBytecodeFromParent(String className) {
        String resource = className.replace('.', '/').concat(".class");
        InputStream is = null;
        try {
            is = parent.getResourceAsStream(resource);
            if (is != null) {
                try {
                    return toByteArray(is);
                } catch (IOException e) {
                    logger.severe(e);
                }
            }
        } finally {
            IOUtil.closeResource(is);
        }
        return null;
    }
}
