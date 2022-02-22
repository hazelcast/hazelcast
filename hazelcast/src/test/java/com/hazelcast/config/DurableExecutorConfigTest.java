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

package com.hazelcast.config;

import com.hazelcast.internal.config.DurableExecutorConfigReadOnly;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static com.hazelcast.test.HazelcastTestSupport.assumeDifferentHashCodes;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DurableExecutorConfigTest {

    @Test
    public void testReadOnly() throws Exception {
        Config config = new Config();
        DurableExecutorConfig durableExecutorConfig = config.getDurableExecutorConfig(randomString());
        DurableExecutorConfig readOnly = new DurableExecutorConfigReadOnly(durableExecutorConfig);

        Method[] methods = DurableExecutorConfig.class.getMethods();
        for (Method method : methods) {
            if (method.getName().startsWith("set")) {
                try {
                    Object param = newParameter(method);
                    method.invoke(readOnly, param);
                    fail();
                } catch (InvocationTargetException e) {
                    assertTrue(e.getCause() instanceof UnsupportedOperationException);
                }
            }
        }
    }

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(DurableExecutorConfig.class)
                      .suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS)
                      .withPrefabValues(DurableExecutorConfigReadOnly.class,
                              new DurableExecutorConfigReadOnly(new DurableExecutorConfig("red")),
                              new DurableExecutorConfigReadOnly(new DurableExecutorConfig("black")))
                      .verify();
    }

    private static Object newParameter(Method method) throws Exception {
        Class<?>[] parameterTypes = method.getParameterTypes();
        assertEquals(1, parameterTypes.length);
        Class<?> parameterType = parameterTypes[0];
        if (!parameterType.isPrimitive()) {
            return parameterType.newInstance();
        }
        if ("int".equals(parameterType.getName())) {
            return 0;
        }
        if ("long".equals(parameterType.getName())) {
            return 0L;
        }
        if ("boolean".equals(parameterType.getName())) {
            return false;
        }
        if ("short".equals(parameterType.getName())) {
            return (short) 0;
        }
        if ("double".equals(parameterType.getName())) {
            return (double) 0;
        }
        if ("float".equals(parameterType.getName())) {
            return (float) 0;
        }
        if ("byte".equals(parameterType.getName())) {
            return (byte) 0;
        }
        if ("char".equals(parameterType.getName())) {
            return (char) 0;
        }
        throw new IllegalArgumentException();
    }
}
