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

package com.hazelcast.nio;

import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClassLoaderUtilTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(ClassLoaderUtil.class);
    }

    @Test
    public void testImplementsInterfaceWithSameName_whenInterfaceIsDirectlyImplemented() {
        assertTrue(ClassLoaderUtil.implementsInterfaceWithSameName(DirectlyImplementingInterface.class, MyInterface.class));
    }

    @Test
    public void testDoNotImplementInterface() {
        assertFalse(ClassLoaderUtil.implementsInterfaceWithSameName(Object.class, MyInterface.class));
    }

    @Test
    public void testImplementsInterfaceWithSameName_whenInterfaceIsImplementedBySuperClass() {
        assertTrue(ClassLoaderUtil.implementsInterfaceWithSameName(ExtendingClassImplementingInterface.class, MyInterface.class));
    }

    @Test
    public void testImplementsInterfaceWithSameName_whenDirectlyImplementingSubInterface() {
        assertTrue(ClassLoaderUtil.implementsInterfaceWithSameName(DirectlyImplementingSubInterfaceInterface.class, MyInterface.class));
    }

    @Test
    public void testImplementsInterfaceWithSameName_whenExtendingClassImplementingSubinterface() {
        assertTrue(ClassLoaderUtil.implementsInterfaceWithSameName(ExtendingClassImplementingSubInterface.class, MyInterface.class));
    }

    @Test
    public void testIssue13509() throws Exception {
        // see https://github.com/hazelcast/hazelcast/issues/13509
        ClassLoader testCL = new ClassLoader() {
            @Override
            protected Class<?> findClass(String name) throws ClassNotFoundException {
                if (name.equals("mock.Class")) {
                    try {
                        byte[] classData = IOUtil.toByteArray(getClass().getResourceAsStream("mock-class-data.dat"));
                        return defineClass(name, classData, 0, classData.length);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                throw new ClassNotFoundException(name);
            }
        };

        ClassLoader previousCL = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(testCL);
        try {
            Thread.currentThread().setContextClassLoader(testCL);
            Object o = ClassLoaderUtil.newInstance(null, "mock.Class");
            assertNotNull("no object created", o);
        } finally {
            Thread.currentThread().setContextClassLoader(previousCL);
        }

        // now the context class loader is reset back, new instance should fail
        try {
            ClassLoaderUtil.newInstance(null, "mock.Class");
            fail("call did not fail, class probably incorrectly returned from CONSTRUCTOR_CACHE");
        } catch (ClassNotFoundException expected) { }
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
