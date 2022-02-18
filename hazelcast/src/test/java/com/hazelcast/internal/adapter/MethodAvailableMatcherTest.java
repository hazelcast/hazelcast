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
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MethodAvailableMatcherTest {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    private Class<? extends DataStructureAdapter> adapterClass = ReplicatedMapDataStructureAdapter.class;

    @Test
    public void assertThat_withAvailableMethod() {
        assertThat(adapterClass, new MethodAvailableMatcher(DataStructureMethods.CLEAR));
    }

    @Test
    public void assertThat_withAvailableMethod_withParameter() {
        assertThat(adapterClass, new MethodAvailableMatcher(DataStructureMethods.GET));
    }

    @Test
    public void assertThat_withAvailableMethod_withMultipleParameters() {
        assertThat(adapterClass, new MethodAvailableMatcher(DataStructureMethods.PUT));
    }

    @Test
    public void assertThat_withAvailableMethod_withParameterMismatch() {
        expected.expect(AssertionError.class);
        expected.expectMessage("Could not find method " + adapterClass.getSimpleName() + ".put(Integer, String)");
        assertThat(adapterClass, new MethodAvailableMatcher(new DataStructureAdapterMethod() {
            @Override
            public String getMethodName() {
                return "put";
            }

            @Override
            public Class<?>[] getParameterTypes() {
                return new Class[]{Integer.class, String.class};
            }

            @Override
            public String getParameterTypeString() {
                return "Integer, String";
            }
        }));
    }

    @Test
    public void assertThat_withUnavailableMethod_withParameter() {
        expected.expect(AssertionError.class);
        expected.expectMessage("removeAsync(Object) to be available");
        assertThat(adapterClass, new MethodAvailableMatcher(DataStructureMethods.REMOVE_ASYNC));
    }

    @Test
    public void assertThat_withUnavailableMethod_withMultipleParameters() {
        expected.expect(AssertionError.class);
        expected.expectMessage("putIfAbsentAsync(Object, Object) to be available");
        assertThat(adapterClass, new MethodAvailableMatcher(DataStructureMethods.PUT_IF_ABSENT_ASYNC));
    }

    @Test
    public void assertThat_withNull() {
        expected.expect(AssertionError.class);
        expected.expectMessage("clear() to be available");
        assertThat(null, new MethodAvailableMatcher(DataStructureMethods.CLEAR));
    }
}
