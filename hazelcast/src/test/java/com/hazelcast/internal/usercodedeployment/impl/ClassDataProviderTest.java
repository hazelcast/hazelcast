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

package com.hazelcast.internal.usercodedeployment.impl;

import com.hazelcast.config.UserCodeDeploymentConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.config.UserCodeDeploymentConfig.ProviderMode.LOCAL_AND_CACHED_CLASSES;
import static com.hazelcast.config.UserCodeDeploymentConfig.ProviderMode.LOCAL_CLASSES_ONLY;
import static com.hazelcast.config.UserCodeDeploymentConfig.ProviderMode.OFF;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClassDataProviderTest {

    @Test
    public void givenProviderModeSetToOFF_whenMapClassContainsClass_thenReturnNull() throws Exception {
        UserCodeDeploymentConfig.ProviderMode providerMode = OFF;
        String className = "className";
        ClassSource classSource = newMockClassSource();
        ClassLoader parent = getClass().getClassLoader();
        ClassDataProvider provider = createClassDataProvider(providerMode, className, classSource, parent);
        ClassData classData = provider.getClassDataOrNull(className);
        assertNull(classData);
    }

    @Test
    public void givenProviderModeSetToLOCAL_CLASSES_ONLY_whenMapClassContainsClass_thenReturnNull() throws Exception {
        UserCodeDeploymentConfig.ProviderMode providerMode = LOCAL_CLASSES_ONLY;
        String className = "className";
        ClassSource classSource = newMockClassSource();
        ClassLoader parent = getClass().getClassLoader();
        ClassDataProvider provider = createClassDataProvider(providerMode, className, classSource, parent);
        ClassData classData = provider.getClassDataOrNull(className);
        assertNull(classData);
    }

    @Test
    public void givenProviderModeSetToLOCAL_AND_CACHED_CLASSES_whenMapClassContainsClass_thenReturnIt() throws Exception {
        UserCodeDeploymentConfig.ProviderMode providerMode = LOCAL_AND_CACHED_CLASSES;
        String className = "className";
        ClassSource classSource = newMockClassSource();
        ClassLoader parent = getClass().getClassLoader();
        ClassDataProvider provider = createClassDataProvider(providerMode, className, classSource, parent);
        ClassData classData = provider.getClassDataOrNull(className);
        assertNotNull(classData.getInnerClassDefinitions());
    }

    private ClassDataProvider createClassDataProvider(UserCodeDeploymentConfig.ProviderMode providerMode,
                                                      String className, ClassSource classSource, ClassLoader parent) {
        ILogger logger = mock(ILogger.class);
        ConcurrentMap<String, ClassSource> classSourceMap = new ConcurrentHashMap<String, ClassSource>();
        ConcurrentMap<String, ClassSource> clientClassSourceMap = new ConcurrentHashMap<String, ClassSource>();
        classSourceMap.put(className, classSource);
        return new ClassDataProvider(providerMode, parent, classSourceMap, clientClassSourceMap, logger);
    }

    private static ClassSource newMockClassSource() {
        ClassSource classSource = new ClassSource(null, null, Collections.EMPTY_MAP);
        classSource.addClassDefinition("className", new byte[4]);
        return classSource;
    }
}
