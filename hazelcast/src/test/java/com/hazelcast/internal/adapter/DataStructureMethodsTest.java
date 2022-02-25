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

package com.hazelcast.internal.adapter;

import com.hazelcast.internal.adapter.DataStructureAdapter.DataStructureMethods;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.junit.Assert.fail;

/**
 * Checks that {@link DataStructureMethods} contains unique method signatures which are defined in {@link DataStructureAdapter}.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DataStructureMethodsTest {

    @Test
    public void testThatAllDataStructureMethodsAreFoundAndReferencedJustOnce() {
        String adapterClassName = DataStructureAdapter.class.getSimpleName();
        String methodsClassName = DataStructureMethods.class.getSimpleName();

        Map<Method, DataStructureMethods> knownMethods = new HashMap<Method, DataStructureMethods>();
        for (DataStructureMethods method : DataStructureMethods.values()) {
            try {
                Method adapterMethod = DataStructureAdapter.class.getMethod(method.getMethodName(), method.getParameterTypes());
                if (knownMethods.containsKey(adapterMethod)) {
                    fail(format("%s::%s(%s) is referenced by %s.%s, but was already referenced by %s.%s",
                            adapterClassName, method.getMethodName(), method.getParameterTypeString(), methodsClassName, method,
                            methodsClassName, knownMethods.get(adapterMethod)));
                }
                knownMethods.put(adapterMethod, method);
            } catch (NoSuchMethodException e) {
                fail(format("%s::%s(%s) is referenced by %s.%s, but could not be found!",
                        adapterClassName, method.getMethodName(), method.getParameterTypeString(), methodsClassName, method));
            }
        }
    }
}
