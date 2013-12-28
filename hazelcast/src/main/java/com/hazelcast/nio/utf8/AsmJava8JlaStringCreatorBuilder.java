/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.utf8;

import com.hazelcast.nio.UTFUtil;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import sun.misc.SharedSecrets;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

class AsmJava8JlaStringCreatorBuilder implements Opcodes, StringCreatorBuilder {

    @Override
    public UTFUtil.StringCreator build() throws Exception {
        final int id = StringCreatorUtil.CLASS_ID_COUNTER.getAndIncrement();
        final String className = "com/hazelcast/nio/utf8/AsmStringAccessor" + id;

        ClassWriter cw = new ClassWriter(0);
        cw.visit(V1_6, ACC_PUBLIC + ACC_FINAL, className, null,
                "java/lang/Object", new String[]{"com/hazelcast/nio/UTFUtil$StringCreator"});

        FieldVisitor fv = cw.visitField(ACC_FINAL + ACC_PRIVATE, "jla", "Lsun/misc/JavaLangAccess;", null, null);
        fv.visitEnd();

        MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "<init>", "(Lsun/misc/JavaLangAccess;)V", null, null);
        mv.visitCode();
        mv.visitVarInsn(ALOAD, 0);
        mv.visitMethodInsn(INVOKESPECIAL, "java/lang/Object", "<init>", "()V");
        mv.visitVarInsn(ALOAD, 0);
        mv.visitVarInsn(ALOAD, 1);
        mv.visitFieldInsn(PUTFIELD, className, "jla", "Lsun/misc/JavaLangAccess;");
        mv.visitInsn(Opcodes.RETURN);
        mv.visitMaxs(2, 2);
        mv.visitEnd();

        mv = cw.visitMethod(ACC_PUBLIC, "buildString", "([C)Ljava/lang/String;", null, null);
        mv.visitCode();
        mv.visitVarInsn(ALOAD, 0);
        mv.visitFieldInsn(GETFIELD, className, "jla", "Lsun/misc/JavaLangAccess;");
        mv.visitVarInsn(ALOAD, 1);
        mv.visitMethodInsn(INVOKEINTERFACE, "sun/misc/JavaLangAccess", "newStringUnsafe", "([C)Ljava/lang/String;");
        mv.visitInsn(Opcodes.ARETURN);
        mv.visitMaxs(3, 2);
        mv.visitEnd();

        cw.visitEnd();
        final byte[] impl = cw.toByteArray();

        AnonClassLoader cl = new AnonClassLoader(getClass().getClassLoader());
        Class<?> clazz = cl.loadClass("com.hazelcast.nio.utf8.AsmStringAccessor" + id, impl);
        Constructor constructor = clazz.getConstructor(sun.misc.JavaLangAccess.class);

        Field jlaField = SharedSecrets.class.getDeclaredField("javaLangAccess");
        jlaField.setAccessible(true);

        return (UTFUtil.StringCreator) constructor.newInstance(jlaField.get(SharedSecrets.class));
    }
}
