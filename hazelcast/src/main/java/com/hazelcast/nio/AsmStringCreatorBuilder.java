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

package com.hazelcast.nio;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import sun.invoke.anon.AnonymousClassLoader;

import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.util.function.Function;

class AsmStringCreatorBuilder implements Opcodes, UTFUtil.StringCreatorBuilder {

    public UTFUtil.StringCreator build() throws ReflectiveOperationException {
        ClassWriter cw = new ClassWriter(0);
        cw.visit(V1_6, ACC_PUBLIC + ACC_FINAL, "sun/reflect/AsmString", null,
                "sun/reflect/MagicAccessorImpl", new String[]{"java/util/function/Function"});

        MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
        mv.visitCode();
        mv.visitVarInsn(ALOAD, 0);
        mv.visitMethodInsn(INVOKESPECIAL, "java/lang/Object", "<init>", "()V");
        mv.visitInsn(Opcodes.RETURN);
        mv.visitMaxs(1, 1);
        mv.visitEnd();

        mv = cw.visitMethod(ACC_PUBLIC, "apply", "(Ljava/lang/Object;)Ljava/lang/Object;", null, null);
        mv.visitCode();
        mv.visitVarInsn(ALOAD, 0);
        mv.visitTypeInsn(NEW, "java/lang/String");
        mv.visitInsn(DUP);
        mv.visitVarInsn(ALOAD, 1);
        mv.visitTypeInsn(CHECKCAST, "[C");
        mv.visitInsn(ICONST_1);
        mv.visitMethodInsn(INVOKESPECIAL, "java/lang/String", "<init>", "([CZ)V");
        mv.visitInsn(Opcodes.ARETURN);
        mv.visitMaxs(5, 5);
        mv.visitEnd();
        final byte[] data = cw.toByteArray();

        final sun.misc.Unsafe unsafe = UnsafeHelper.UNSAFE;
        Class clazz = AccessController.doPrivileged(new PrivilegedAction<Class>() {
            @Override
            public Class run() {
                Class<?> stringClass = String.class;

                ClassLoader cl = sun.reflect.ConstructorAccessor.class.getClassLoader();
                return unsafe.defineClass("sun/reflect/AsmString", data, 0, data.length, cl, null);
            }
        });

        final Function function = (Function) clazz.newInstance();
        return new UTFUtil.StringCreator() {
            @Override
            public String buildString(char[] chars) {
                return (String) function.apply(chars);
            }
        };
    }

}
