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

package com.hazelcast.query.impl.getters;

import com.hazelcast.query.QueryException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReflectionHelperTest {

    @Test
    public void extractValue_whenIntermediateFieldIsInterfaceAndDoesNotContainField_thenThrowIllegalArgumentException()
            throws Exception {
        OuterObject object = new OuterObject();
        try {
            ReflectionHelper.extractValue(object, "emptyInterface.doesNotExist", true);
            fail("Non-existing field has been ignored");
        } catch (QueryException e) {
            // createGetter() method is catching everything throwable and wraps it in QueryException
            // I don't think it's the right thing to do, but I don't want to change this behaviour.
            // Hence I have to use try/catch in this test instead of just declaring
            // IllegalArgumentException as expected exception.
            assertEquals(IllegalArgumentException.class, e.getCause().getClass());
        }
    }

    @Test
    public void extractValue_whenIntermediateFieldIsInterfaceAndDoesNotContainField_returnsNull()
            throws Exception {
        OuterObject object = new OuterObject();

        assertNull(ReflectionHelper.extractValue(object, "emptyInterface.doesNotExist", false));
    }

    @SuppressWarnings("unused")
    private static class OuterObject {
        private EmptyInterface emptyInterface;
    }

    private interface EmptyInterface {
    }
}
