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

package com.hazelcast.instance.impl;

import com.hazelcast.config.NamespaceConfig;
import com.hazelcast.internal.namespace.impl.NamespaceAwareClassLoader;
import com.hazelcast.internal.usercodedeployment.UserCodeDeploymentClassLoader;
import com.hazelcast.test.annotation.NamespaceTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@Category(NamespaceTest.class)
public class NodeTest extends ConfigClassLoaderTest {

    @Test
    public void testConfigClassLoader_whenNoNamespaceExists_andUCDDisabled() {
        createHazelcastInstanceWithConfig();
        assertSame(config.getClassLoader(), engineConfigClassLoader);
    }

    @Test
    public void testConfigClassLoader_whenNoNamespaceExists_andUCDEnabled_thenIsUCDClassLoader() {
        config.getUserCodeDeploymentConfig().setEnabled(true);

        createHazelcastInstanceWithConfig();
        assertTrue(engineConfigClassLoader instanceof UserCodeDeploymentClassLoader);
        assertSame(config.getClassLoader(), engineConfigClassLoader.getParent());
    }

    @Test
    public void testConfigClassLoader_whenNamespaceExists_andUCDEnabled_thenIsNsAwareWithUCDParent() {
        config.getUserCodeDeploymentConfig().setEnabled(true);
        config.getNamespacesConfig().setEnabled(true);
        config.getNamespacesConfig().addNamespaceConfig(new NamespaceConfig("namespace"));

        createHazelcastInstanceWithConfig();
        assertTrue(engineConfigClassLoader instanceof NamespaceAwareClassLoader);
        assertTrue(engineConfigClassLoader.getParent() instanceof UserCodeDeploymentClassLoader);
        assertSame(config.getClassLoader(), engineConfigClassLoader.getParent().getParent());
    }
}
