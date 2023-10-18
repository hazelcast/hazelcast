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

package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.Config;
import com.hazelcast.config.DeviceConfig;
import com.hazelcast.config.DynamicConfigurationConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Category({QuickTest.class, ParallelJVMTest.class})
public class DynamicConfigVersionTest {
    /** config classes not supported by dynamic data structure config */
    private static Set<Class<?>> nonDynamicConfigClasses;

    @BeforeAll
    public static void setUp() {
        nonDynamicConfigClasses = Set.of(WanReplicationConfig.class, SplitBrainProtectionConfig.class, ListenerConfig.class,
                DeviceConfig.class, DynamicConfigurationConfig.class);
    }

    private static Stream<Method> getConfigMethods() {
        return Arrays.stream(Config.class.getDeclaredMethods())
                .filter(method -> method.getName().startsWith("add") && method.getName().endsWith("Config"));
    }

    @ParameterizedTest
    @MethodSource("getConfigMethods")
    void test_allConfigClasses_areAssignedToVersion(Method method) {
        assertEquals(1, method.getParameterCount(), method::toString);

        Class<?> klass = method.getParameterTypes()[0];
        boolean isMappedToVersion = (ClusterWideConfigurationService.CONFIG_TO_VERSION.get(klass) != null)
                || nonDynamicConfigClasses.contains(klass);
        assertTrue(isMappedToVersion,
                () -> String.format("Config class %s does not have a minimum Version set", klass.getName()));
    }
}
