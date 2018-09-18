/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.probing;

import java.lang.management.ClassLoadingMXBean;
import java.lang.management.ManagementFactory;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ProbeClassLoadingTest extends AbstractProbeTest {

    private static final ClassLoadingMXBean BEAN = ManagementFactory.getClassLoadingMXBean();

    @Before
    public void setup() {
        registry.register(Probing.OS);
    }

    @Test
    public void loadedClassesCount() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertProbed("classloading.loadedClassesCount", BEAN.getLoadedClassCount(), 100);
            }
        });
    }

    @Test
    public void totalLoadedClassesCount() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertProbed("classloading.totalLoadedClassesCount", BEAN.getTotalLoadedClassCount(), 100);
            }
        });
    }

    @Test
    public void unloadedClassCount() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertProbed("classloading.unloadedClassCount", BEAN.getUnloadedClassCount(), 100);
            }
        });
    }

}
