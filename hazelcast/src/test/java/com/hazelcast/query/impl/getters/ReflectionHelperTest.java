/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl.getters;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.util.RootCauseMatcher.rootCause;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReflectionHelperTest {

    @Test
    public void extractValue_whenIntermediateFieldIsInterfaceAndDoesNotContainField_thenThrowIllegalArgumentException() {
        // createGetter() method is catching everything throwable and wraps it in QueryException
        // I don't think it's the right thing to do, but I don't want to change this behaviour.
        // Hence, I have to check the root cause instead of declaring IllegalArgumentException as expected exception.
        assertThatThrownBy(() -> extractValue("emptyInterface.doesNotExist", true))
                .describedAs("Non-existing field has been ignored")
                .has(rootCause(IllegalArgumentException.class));
    }

    @Test
    public void extractValue_whenIntermediateFieldIsInterfaceAndDoesNotContainField_returnsNull()
            throws Exception {
        assertNull(extractValue("emptyInterface.doesNotExist", false));
    }

    @SuppressWarnings("unused")
    private static class OuterObject {
        private EmptyInterface emptyInterface;
    }

    private interface EmptyInterface {
    }

    private static Object extractValue(String attributeName, boolean failOnMissingAttribute) throws Exception {
        OuterObject object = new OuterObject();
        return ReflectionHelper.createGetter(object, attributeName, failOnMissingAttribute).getValue(object);
    }
}
