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
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DynamicConfigurationAwareConfigTest {

    protected Class<? extends Config> getDynamicConfigClass() {
        return DynamicConfigurationAwareConfig.class;
    }

    @Test
    public void testDecorateAllPublicMethodsFromTest() {
        // this test makes sure when you add a new method into Config class
        // then you also adds it into the Dynamic Configuration Aware decorator.

        // in other words: if this test is failing then update the class returned by
        // getDynamicConfigClass()
        Class<? extends Config> dynamicConfigClass = getDynamicConfigClass();
        Method[] methods = dynamicConfigClass.getMethods();
        for (Method method : methods) {
            if (isMethodStatic(method)) {
                continue;
            }
            if (isMethodDeclaredByClass(method, Object.class)) {
                //let's skip methods like wait() or notify() - declared directly in the Object class
                continue;
            }

            //all other public method should be overridden by the dynamic config aware decorator
            if (!isMethodDeclaredByClass(method, dynamicConfigClass)) {
                Class<?> declaringClass = method.getDeclaringClass();
                fail("Method " + method + " is declared by " + declaringClass + " whilst it should be"
                        + " declared by " + dynamicConfigClass);
            }
        }
    }

    private static boolean isMethodStatic(Method method) {
        return Modifier.isStatic(method.getModifiers());
    }

    private static boolean isMethodDeclaredByClass(Method method, Class<?> expectedDeclaringClass) {
        Class<?> actualDeclaringClass = method.getDeclaringClass();
        return expectedDeclaringClass.equals(actualDeclaringClass);
    }
}
