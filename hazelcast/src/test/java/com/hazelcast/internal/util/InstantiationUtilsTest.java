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

package com.hazelcast.internal.util;


import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InstantiationUtilsTest extends HazelcastTestSupport {

    @Test
    public void newInstanceOrNull_createInstanceWithNoArguments() {
        ClassWithNonArgConstructor instance = InstantiationUtils.newInstanceOrNull(ClassWithNonArgConstructor.class);
        assertNotNull(instance);
    }

    @Test
    public void newInstanceOrNull_createInstanceWithSingleArgument() {
        ClassWithStringConstructor instance = InstantiationUtils.newInstanceOrNull(ClassWithStringConstructor.class,
                "foo");
        assertNotNull(instance);
    }

    @Test
    public void newInstanceOrNull_createInstanceWithSubclass() {
        ClassWithObjectConstructor instance = InstantiationUtils.newInstanceOrNull(ClassWithObjectConstructor.class,
                "foo");
        assertNotNull(instance);
    }

    @Test
    public void newInstanceOrNull_noMatchingConstructorFound_different_length() {
        ClassWithNonArgConstructor instance = InstantiationUtils.newInstanceOrNull(ClassWithNonArgConstructor.class,
                "foo");
        assertNull(instance);
    }

    @Test
    public void newInstanceOrNull_noMatchingConstructorFound_different_types() {
        ClassWithStringConstructor instance = InstantiationUtils.newInstanceOrNull(ClassWithStringConstructor.class,
                43);
        assertNull(instance);
    }

    @Test
    public void newInstanceOrNull_nullIsMatchingAllTypes() {
        ClassWithTwoArgConstructorConstructor instance = InstantiationUtils.newInstanceOrNull(
                ClassWithTwoArgConstructorConstructor.class,
                "foo", null);
        assertNotNull(instance);
    }

    @Test
    public void newInstanceOrNull_primitiveArgInConstructor() {
        ClassWithPrimitiveArgConstructor instance = InstantiationUtils.newInstanceOrNull(
                ClassWithPrimitiveArgConstructor.class,
                true, (byte) 0, 'c', 42d, 42f, 42, (long) 43, (short) 42);
        assertNotNull(instance);
    }

    @Test
    public void newInstanceOrNull_primitiveArgInConstructorPassingNull() {
        ClassWithTwoConstructorsIncludingPrimitives instance = InstantiationUtils.newInstanceOrNull(
                ClassWithTwoConstructorsIncludingPrimitives.class,
                42, null);
        assertNotNull(instance);
    }

    @Test(expected = AmbiguousInstantiationException.class)
    public void newInstanceOrNull_ambigiousConstructor() {
        InstantiationUtils.newInstanceOrNull(ClassWithTwoConstructors.class, "foo");
    }

    @Test
    public void newInstanceOrNull_primitiveArrayArgInConstructor() {
        ClassWithPrimitiveArrayInConstructor instance = InstantiationUtils.newInstanceOrNull(
                ClassWithPrimitiveArrayInConstructor.class,
                "foo", new int[]{42, 42});
        assertNotNull(instance);
    }

    public static class ClassWithNonArgConstructor {

    }

    public static class ClassWithStringConstructor {
        public ClassWithStringConstructor(String ignored) {

        }
    }

    public static class ClassWithObjectConstructor {
        public ClassWithObjectConstructor(Object ignored) {

        }
    }

    public static class ClassWithTwoArgConstructorConstructor {
        public ClassWithTwoArgConstructorConstructor(String arg0, String arg1) {

        }
    }

    public static class ClassWithTwoConstructorsIncludingPrimitives {
        public ClassWithTwoConstructorsIncludingPrimitives(int arg0, int arg1) {

        }

        public ClassWithTwoConstructorsIncludingPrimitives(int arg0, Object arg1) {

        }
    }

    public static class ClassWithPrimitiveArgConstructor {
        public ClassWithPrimitiveArgConstructor(boolean arg0, byte arg1, char arg2, double arg3, float arg4, int arg5,
                                                long arg6, short arg7) {

        }
    }

    public static class ClassWithTwoConstructors {
        public ClassWithTwoConstructors(String ignored) {

        }

        public ClassWithTwoConstructors(Object ignored) {

        }
    }

    public static class ClassWithPrimitiveArrayInConstructor {
        public ClassWithPrimitiveArrayInConstructor(String ignored, int[] a) {

        }
    }


}
