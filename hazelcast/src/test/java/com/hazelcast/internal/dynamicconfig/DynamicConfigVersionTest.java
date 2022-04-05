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

package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.Config;
import com.hazelcast.config.DeviceConfig;
import com.hazelcast.config.DynamicConfigurationConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.internal.dynamicconfig.ClusterWideConfigurationService.CONFIG_TO_VERSION;
import static java.lang.String.format;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DynamicConfigVersionTest {

    // config classes not supported by dynamic data structure config
    private static final Set<Class<?>> NON_DYNAMIC_CONFIG_CLASSES;

    static {
        Set<Class<?>> nonDynamicConfigClasses = new HashSet<Class<?>>();
        nonDynamicConfigClasses.add(WanReplicationConfig.class);
        nonDynamicConfigClasses.add(SplitBrainProtectionConfig.class);
        nonDynamicConfigClasses.add(ListenerConfig.class);
        nonDynamicConfigClasses.add(DeviceConfig.class);
        nonDynamicConfigClasses.add(DynamicConfigurationConfig.class);
        NON_DYNAMIC_CONFIG_CLASSES = nonDynamicConfigClasses;
    }

    @Test
    public void test_allConfigClasses_areAssignedToVersion() {
        Class<Config> topLevelConfigClass = Config.class;
        Method[] allConfigMethods = topLevelConfigClass.getDeclaredMethods();
        for (Method method : allConfigMethods) {
            String methodName = method.getName();
            if (methodName.startsWith("add") && methodName.endsWith("Config")) {
                assert method.getParameterTypes().length == 1;
                Class klass = method.getParameterTypes()[0];
                boolean isMappedToVersion = CONFIG_TO_VERSION.get(klass) != null
                        || NON_DYNAMIC_CONFIG_CLASSES.contains(klass);
                assertTrue(format("Config class %s does not have a minimum Version set", klass.getName()), isMappedToVersion);
            }
        }
    }
}
