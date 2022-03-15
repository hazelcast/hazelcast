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

package com.hazelcast.internal.usercodedeployment;

import com.hazelcast.cluster.Member;
import com.hazelcast.config.UserCodeDeploymentConfig;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.usercodedeployment.impl.ClassData;
import com.hazelcast.internal.usercodedeployment.impl.ClassDataProvider;
import com.hazelcast.internal.usercodedeployment.impl.ClassLocator;
import com.hazelcast.internal.usercodedeployment.impl.ClassSource;
import com.hazelcast.internal.util.filter.Filter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.usercodedeployment.impl.filter.ClassNameFilterParser.parseClassNameFilters;
import static com.hazelcast.internal.usercodedeployment.impl.filter.MemberProviderFilterParser.parseMemberFilter;
import static com.hazelcast.jet.impl.util.Util.CONFIG_CHANGE_TEMPLATE;
import static java.lang.String.format;

public final class UserCodeDeploymentService implements ManagedService {

    public static final String SERVICE_NAME = "user-code-deployment-service";

    private volatile boolean enabled;

    private ClassDataProvider provider;
    private ClassLocator locator;

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        UserCodeDeploymentConfig config = nodeEngine.getConfig().getUserCodeDeploymentConfig();
        if (!config.isEnabled()) {
            return;
        }

        ClassLoader parent = nodeEngine.getConfigClassLoader().getParent();
        Filter<String> classNameFilter = parseClassNameFilters(config);
        Filter<Member> memberFilter = parseMemberFilter(config.getProviderFilter());
        ConcurrentMap<String, ClassSource> classMap = new ConcurrentHashMap<String, ClassSource>();
        ConcurrentMap<String, ClassSource> clientClassMap = new ConcurrentHashMap<String, ClassSource>();

        UserCodeDeploymentConfig.ProviderMode providerMode = config.getProviderMode();
        ILogger providerLogger = nodeEngine.getLogger(ClassDataProvider.class);
        provider = new ClassDataProvider(providerMode, parent, classMap, clientClassMap, providerLogger);

        UserCodeDeploymentConfig.ClassCacheMode classCacheMode = config.getClassCacheMode();
        locator = new ClassLocator(classMap, clientClassMap, parent, classNameFilter, memberFilter, classCacheMode, nodeEngine);
        enabled = config.isEnabled();
    }

    public void defineClasses(List<Map.Entry<String, byte[]>> classDefinitions) {
        if (!enabled) {
            throw new IllegalStateException("User Code Deployment is not enabled. "
                    + "To enable User Code Deployment, do one of the following:\n"
                    + format(CONFIG_CHANGE_TEMPLATE,
                        "config.getUserCodeDeploymentConfig().setEnabled(true);",
                        "hazelcast.user-code-deployment.enabled to true",
                        "-Dhz.user-code-deployment.enabled=true",
                        "HZ_USERCODEDEPLOYMENT_ENABLED=true"));
        }
        locator.defineClassesFromClient(classDefinitions);
    }

    // called by operations sent by other members
    public ClassData getClassDataOrNull(String className) {
        if (!enabled) {
            return null;
        }
        return provider.getClassDataOrNull(className);
    }

    // called by User Code Deployment classloader on this member
    public Class<?> handleClassNotFoundException(String name)
            throws ClassNotFoundException {
        if (!enabled) {
            throw new ClassNotFoundException("User Code Deployment is not enabled. Cannot find class " + name);
        }
        return locator.handleClassNotFoundException(name);
    }

    // called by User Code Deployment classloader on this member
    public Class<?> findLoadedClass(String name) {
        if (!enabled) {
            return null;
        }
        return locator.findLoadedClass(name);
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
    }
}
