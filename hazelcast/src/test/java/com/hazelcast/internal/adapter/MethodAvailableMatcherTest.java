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

import static com.hazelcast.internal.adapter.MethodAvailableMatcher.methodAvailable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MethodAvailableMatcherTest {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    private final Class<? extends DataStructureAdapter> adapterClass = ReplicatedMapDataStructureAdapter.class;

    @Test
    public void assertThat_withAvailableMethod() {
        assertThat(adapterClass).is(methodAvailable(DataStructureMethods.CLEAR));
    }

    @Test
    public void assertThat_withAvailableMethod_withParameter() {
        assertThat(adapterClass).is(methodAvailable(DataStructureMethods.GET));
    }

    @Test
    public void assertThat_withAvailableMethod_withMultipleParameters() {
        assertThat(adapterClass).is(methodAvailable(DataStructureMethods.PUT));
    }

    @Test
    public void assertThat_withAvailableMethod_withParameterMismatch() {
        expected.expect(AssertionError.class);
        expected.expectMessage("Could not find method " + adapterClass.getSimpleName() + ".put(Integer, String)");
        assertThat(adapterClass).is(methodAvailable(new DataStructureAdapterMethod() {
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
        assertThat(adapterClass).is(methodAvailable(DataStructureMethods.REMOVE_ASYNC));
    }

    @Test
    public void assertThat_withUnavailableMethod_withMultipleParameters() {
        expected.expect(AssertionError.class);
        expected.expectMessage("putIfAbsentAsync(Object, Object) to be available");
        assertThat(adapterClass).is(methodAvailable(DataStructureMethods.PUT_IF_ABSENT_ASYNC));
    }

    @Test
    public void assertThat_withNull() {
        var ex = assertThrows(AssertionError.class, () -> {
            methodAvailable(DataStructureMethods.CLEAR).matches(null);
        });
        assertThat(ex).hasMessageContaining("clear() to be available");
    }
}
