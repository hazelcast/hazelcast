/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClassLoaderUtilTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(ClassLoaderUtil.class);
    }

    @Test
    public void testImplementsIntefaceWithSameName_whenInterfaceIsDirectlyImplemented() {
        assertTrue(ClassLoaderUtil.implementsInterfaceWithSameName(DirectlyImplementingInterface.class, MyInterface.class));
    }

    @Test
    public void testDoNotImplementInterface() {
        assertFalse(ClassLoaderUtil.implementsInterfaceWithSameName(Object.class, MyInterface.class));
    }

    @Test
    public void testImplementsIntefaceWithSameName_whenInterfaceIsImplementedBySuperClass() {
        assertTrue(ClassLoaderUtil.implementsInterfaceWithSameName(ExtendingClassImplementingInterface.class, MyInterface.class));
    }

    @Test
    public void testImplementsIntefaceWithSameName_whenDirectlyImplementingSubInterface() {
        assertTrue(ClassLoaderUtil.implementsInterfaceWithSameName(DirectlyImplementingSubInterfaceInterface.class, MyInterface.class));
    }

    @Test
    public void testImplementsIntefaceWithSameName_whenExtendingClassImplementingSubinterface() {
        assertTrue(ClassLoaderUtil.implementsInterfaceWithSameName(ExtendingClassImplementingSubInterface.class, MyInterface.class));
    }


    private static class ExtendingClassImplementingSubInterface extends DirectlyImplementingSubInterfaceInterface {

    }

    private static class ExtendingClassImplementingInterface extends DirectlyImplementingInterface {

    }

    private static class DirectlyImplementingInterface implements MyInterface {

    }

    private static class DirectlyImplementingSubInterfaceInterface implements SubInterface {

    }

    private interface SubInterface extends MyInterface {

    }

    private interface MyInterface {

    }

}
