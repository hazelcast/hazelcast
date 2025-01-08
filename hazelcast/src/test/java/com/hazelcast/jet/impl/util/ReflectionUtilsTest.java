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

package com.hazelcast.jet.impl.util;

import com.hazelcast.jet.impl.util.ReflectionUtils.ClassResource;
import com.hazelcast.jet.impl.util.ReflectionUtils.Resources;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.jet.impl.util.ReflectionUtils.findPropertyField;
import static com.hazelcast.jet.impl.util.ReflectionUtils.findPropertySetter;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.substringAfterLast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReflectionUtilsTest {

    @Test
    public void when_loadClass_then_returnsClass() {
        // When
        Class<?> clazz = ReflectionUtils.loadClass(getClass().getClassLoader(), getClass().getName());

        // Then
        assertThat(clazz).isEqualTo(getClass());
    }

    @Test
    public void when_newInstance_then_returnsInstance() {
        // When
        OuterClass instance = ReflectionUtils.newInstance(OuterClass.class.getClassLoader(), OuterClass.class.getName());

        // Then
        assertThat(instance).isNotNull();
    }

    @Test
    public void readStaticFieldOrNull_whenClassDoesNotExist_thenReturnNull() {
        Object field = ReflectionUtils.readStaticFieldOrNull("foo.bar.nonExistingClass", "field");
        assertNull(field);
    }

    @Test
    public void readStaticFieldOrNull_whenFieldDoesNotExist_thenReturnNull() {
        Object field = ReflectionUtils.readStaticFieldOrNull(MyClass.class.getName(), "nonExistingField");
        assertNull(field);
    }

    @Test
    public void readStaticFieldOrNull_readFromPrivateField() {
        String field = ReflectionUtils.readStaticFieldOrNull(MyClass.class.getName(), "staticPrivateField");
        assertEquals("staticPrivateFieldContent", field);
    }

    @Test
    public void readStaticFieldOrNull_readFromPublicField() {
        String field = ReflectionUtils.readStaticFieldOrNull(MyClass.class.getName(), "staticPublicField");
        assertEquals("staticPublicFieldContent", field);
    }

    @Test
    public void when_findPropertySetter_public_then_returnsIt() {
        assertNotNull(findPropertySetter(JavaProperties.class, "publicField", int.class));
    }

    @Test
    public void when_findPropertySetter_public_wrongType_then_returnsNull() {
        assertNull(findPropertySetter(JavaProperties.class, "publicField", long.class));
    }

    @Test
    public void when_findPropertySetter_public_wrongReturnType_then_returnsNull() {
        assertNull(findPropertySetter(JavaProperties.class, "intWithParameter", int.class));
    }

    @Test
    public void when_findPropertySetter_public_builderStyleReturnType_then_returnsIt() {
        assertNotNull(findPropertySetter(JavaProperties.class, "builderStyleSetter", int.class));
    }

    @Test
    public void when_findPropertySetter_publicStatic_then_returnsNull() {
        assertNull(findPropertySetter(JavaProperties.class, "publicStaticField", int.class));
    }

    @Test
    public void when_findPropertySetter_publicDefault_then_returnsNull() {
        assertNull(findPropertySetter(JavaProperties.class, "defaultField", int.class));
    }

    @Test
    public void when_findPropertySetter_protected_then_returnsNull() {
        assertNull(findPropertySetter(JavaProperties.class, "protectedField", int.class));
    }

    @Test
    public void when_findPropertySetter_private_then_returnsNull() {
        assertNull(findPropertySetter(JavaProperties.class, "privateField", int.class));
    }

    @Test
    public void when_findPropertySetter_nonExistent_then_returnsNull() {
        assertNull(findPropertySetter(JavaProperties.class, "nonExistentField", int.class));
    }

    @Test
    public void when_findPropertyField_public_then_returnsIt() {
        assertNotNull(findPropertyField(JavaFields.class, "publicField"));
    }

    @Test
    public void when_findPropertyField_default_then_returnsNull() {
        assertNull(findPropertyField(JavaFields.class, "defaultField"));
    }

    @Test
    public void when_findPropertyField_protected_then_returnsNull() {
        assertNull(findPropertyField(JavaFields.class, "protectedField"));
    }

    @Test
    public void when_findPropertyField_private_then_returnsNull() {
        assertNull(findPropertyField(JavaFields.class, "privateField"));
    }

    @Test
    public void when_findPropertyField_publicStatic_then_returnsNull() {
        assertNull(findPropertyField(JavaFields.class, "publicStaticField"));
    }

    @Test
    public void when_findPropertyField_nonExistent_then_returnsNull() {
        assertNull(findPropertyField(JavaFields.class, "nonExistentField"));
    }

    @Test
    public void when_nestedClassesOf_then_returnsAllNestedClasses() throws ClassNotFoundException {
        // When
        Collection<Class<?>> classes = ReflectionUtils.nestedClassesOf(OuterClass.class);

        // Then
        assertThat(classes).containsExactlyInAnyOrder(
                OuterClass.class,
                OuterClass.NestedClass.class,
                OuterClass.NestedClass.DeeplyNestedClass.class,
                Class.forName(OuterClass.class.getName() + "$1"),
                Class.forName(OuterClass.NestedClass.DeeplyNestedClass.class.getName() + "$1")
        );
    }

    @Test
    public void when_resourcesOf_then_returnsAllResources() throws ClassNotFoundException {
        // When
        Resources resources = ReflectionUtils.resourcesOf(OuterClass.class.getPackage().getName());

        // Then
        Collection<ClassResource> classes = resources.classes().collect(toList());
        assertThat(classes).hasSizeGreaterThan(5);
        assertThat(classes).contains(classResource(OuterClass.class));
        assertThat(classes).contains(classResource(OuterClass.NestedClass.class));
        assertThat(classes).contains(classResource(OuterClass.NestedClass.DeeplyNestedClass.class));
        assertThat(classes).contains(classResource(Class.forName(OuterClass.class.getName() + "$1")));
        assertThat(classes).contains(
                classResource(Class.forName(OuterClass.NestedClass.DeeplyNestedClass.class.getName() + "$1"))
        );

        List<URL> nonClasses = resources.nonClasses().collect(toList());
        assertThat(nonClasses).hasSize(5);
        assertThat(nonClasses).map(v -> substringAfterLast(v.toString(), "/"))
                              .contains("file.json")
                              .contains("file_list.json")
                              .contains("file_pretty_printed.json")
                              .contains("file_list_pretty_printed.json")
                              .contains("package.properties");
    }

    @Test
    public void testGetStackTrace() {
        String stackTrace = ReflectionUtils.getStackTrace(Thread.currentThread());
        String[] stackTraceLines = stackTrace.split(System.lineSeparator());

        // Because the stack trace contains line numbers, and those are implementation specific (i.e. within the
        // ReflectionUtils or ReflectionUtilsTest classes, or even in the JVM, we can't use a typical assertion

        // Instead assert the first and last elements look as expected
        assertThat(stackTraceLines[0]).startsWith("com.hazelcast.jet.impl.util.ReflectionUtilsTest.testGetStackTrace");
        assertThat(stackTraceLines[stackTraceLines.length - 1]).startsWith("\tjava.base/java.lang.Thread.run");
    }

    @Test
    public void testAllConstantTagsReadable_whenReadingInternalBinaryName() {
        // To read an internal binary name, we need to read (and skip values for) all constant pool bytes
        //    in the class file, so we need to make sure all 17 (as of JDK 21) are handled correctly
        byte[] classBytes = generateClassFileHeaderWithAllConstants();
        assertEquals("com.hazelcast.test.FakeClass", ReflectionUtils.getInternalBinaryName(classBytes));
    }

    private static final byte[] CONSTANT_POOL_TAGS = new byte[]      {1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 15, 16, 17, 18, 19, 20};
    private static final int[] CONSTANT_POOL_PAYLOAD_SIZE = new int[]{0, 4, 4, 8, 8, 2, 2, 4, 4,  4,  4,  3,  2,  4,  4,  2,  2};

    private static byte[] generateClassFileHeaderWithAllConstants() {
        // We need to define all bytes up to the `this_class` definition
        ByteBuffer buffer = ByteBuffer.allocate(154);
        buffer.order(ByteOrder.BIG_ENDIAN);

        // 4 bytes of magic
        fillBuffer(buffer, 4);
        // 4 bytes of versioning
        fillBuffer(buffer, 4);
        // constant pool length (for all our constants + 2 for 2x 8 byte payloads + 1 because the index starts at 1)
        buffer.putShort((short) (CONSTANT_POOL_TAGS.length + 2 + 1));
        // constant pool definition
        for (int k = 0; k < CONSTANT_POOL_TAGS.length; k++) {
            byte tagId = CONSTANT_POOL_TAGS[k];
            buffer.put(tagId);
            // Special handling for CONSTANT_Class to point at our Utf8 index (1)
            if (tagId == 7) {
                buffer.putShort((short) 1);
            } else if (tagId == 1) {
                // Special handling for CONSTANT_Utf8 to write our fake class name
                byte[] bytes = "com.hazelcast.test.FakeClass".getBytes(StandardCharsets.UTF_8);
                buffer.putShort((short) bytes.length);
                buffer.put(bytes);
            } else {
                int payloadByteLength = CONSTANT_POOL_PAYLOAD_SIZE[k];
                fillBuffer(buffer, payloadByteLength);
            }
        }
        // 2 bytes of access flags
        fillBuffer(buffer, 2);
        // this_class definition (point to our CONSTANT_Class index)
        buffer.putShort((short) 8);
        // extra noise for completeness
        fillBuffer(buffer, 32);
        return buffer.array();
    }

    private static void fillBuffer(ByteBuffer buffer, int bytes) {
        for (int k = 0; k < bytes; k++) {
            buffer.put((byte) 0);
        }
    }

    private static ClassResource classResource(Class<?> clazz) {
        URL url = clazz.getClassLoader().getResource(ReflectionUtils.toClassResourceId(clazz));
        return new ClassResource(clazz.getName(), url);
    }

    @SuppressWarnings("unused")
    private static class JavaProperties {

        public static int getPublicStaticField() {
            return 0;
        }

        public static void setPublicStaticField() {
        }

        public int getField() {
            return 0;
        }

        public int getPublicField() {
            return 0;
        }

        public void setPublicField(int i) {
        }

        int getDefaultField() {
            return 0;
        }

        void setDefaultField(int i) {
        }

        protected int getProtectedField() {
            return 0;
        }

        protected void setProtectedField(int i) {
        }

        private int getPrivateField() {
            return 0;
        }

        private void setPrivateField(int i) {
        }

        public boolean isBooleanIsField() {
            return true;
        }

        public void setBooleanIsField(boolean b) {
        }

        public boolean getBooleanGetField() {
            return true;
        }

        public void setBooleanGetField(boolean b) {
        }

        public Boolean isBooleanNonPrimitiveField() {
            return true;
        }

        public void setBooleanNonPrimitiveField(Boolean b) {
        }

        public void getVoidField() {
        }

        public Void getVoid() {
            return null;
        }

        public void isVoidIntegerPrimitive() {
        }

        public Void isVoidInteger() {
            return null;
        }

        public void isVoidPrimitive() {
        }

        public Void isVoid() {
            return null;
        }

        public int getIntWithParameter(int parameter) {
            return 0;
        }

        public int setIntWithParameter(int parameter) {
            return 0;
        }

        public JavaProperties setBuilderStyleSetter(int parameter) {
            return this;
        }
    }

    @SuppressWarnings("unused")
    private static class JavaFields {

        public static int publicStaticField;
        public int publicField;
        protected int protectedField;
        int defaultField;
        private int privateField;
    }

    @SuppressWarnings("unused")
    public static final class MyClass {
        public static String staticPublicField = "staticPublicFieldContent";
        private static String staticPrivateField = "staticPrivateFieldContent";
    }

    @SuppressWarnings("unused")
    private static class OuterClass {
        private void method() {
            new Object() {
            };
        }

        private static class NestedClass {

            private static class DeeplyNestedClass {

                private void method() {
                    new Object() {
                    };
                }
            }
        }
    }
}
