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

package com.hazelcast.util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ServiceLoaderTest {

    @Test
    public void selectingSimpleSingleClassLoader()
            throws Exception {

        Set<ClassLoader> classLoaders = ServiceLoader.selectClassLoaders(null);
        assertEquals(1, classLoaders.size());
    }

    @Test
    public void selectingSimpleGivenClassLoader()
            throws Exception {

        Set<ClassLoader> classLoaders = ServiceLoader.selectClassLoaders(new URLClassLoader(new URL[0]));
        assertEquals(2, classLoaders.size());
    }

    @Test
    public void selectingSimpleDifferentThreadContextClassLoader()
            throws Exception {

        Thread currentThread = Thread.currentThread();
        ClassLoader tccl = currentThread.getContextClassLoader();
        currentThread.setContextClassLoader(new URLClassLoader(new URL[0]));
        Set<ClassLoader> classLoaders = ServiceLoader.selectClassLoaders(null);
        currentThread.setContextClassLoader(tccl);
        assertEquals(2, classLoaders.size());
    }

    @Test
    public void selectingTcclAndGivenClassLoader()
            throws Exception {

        Thread currentThread = Thread.currentThread();
        ClassLoader tccl = currentThread.getContextClassLoader();
        currentThread.setContextClassLoader(new URLClassLoader(new URL[0]));
        Set<ClassLoader> classLoaders = ServiceLoader.selectClassLoaders(new URLClassLoader(new URL[0]));
        currentThread.setContextClassLoader(tccl);
        assertEquals(3, classLoaders.size());
    }

}
