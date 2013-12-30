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
import com.hazelcast.nio.UnsafeHelper;
import javassist.bytecode.*;
import javassist.bytecode.ClassFileWriter.ConstPoolWriter;
import javassist.bytecode.ClassFileWriter.MethodWriter;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;

public class JavassistMagicAccessorStringCreatorBuilder implements Opcode, StringCreatorBuilder {

    @Override
    public UTFUtil.StringCreator build() throws Exception {
        final int id = StringCreatorUtil.CLASS_ID_COUNTER.getAndIncrement();
        final String className = "sun/reflect/JavassistString" + id;

        ClassFileWriter cfw = new ClassFileWriter(ClassFile.JAVA_6, 0);
        ConstPoolWriter cpw = cfw.getConstPool();

        int thisClass = cpw.addClassInfo(className);
        int superClass = cpw.addClassInfo("sun/reflect/MagicAccessorImpl");
        int interfaceClass = cpw.addClassInfo("java/util/Map");
        int stringClass = cpw.addClassInfo("java/lang/String");
        int charArrayClass = cpw.addClassInfo("[C");

        MethodWriter mw = cfw.getMethodWriter();
        mw.begin(AccessFlag.PUBLIC, MethodInfo.nameInit, "()V", null, null);
        mw.add(ALOAD_0);
        mw.add(INVOKESPECIAL);
        int signature = cpw.addNameAndTypeInfo(MethodInfo.nameInit, "()V");
        mw.add16(cpw.addMethodrefInfo(superClass, signature));
        mw.add(RETURN);
        mw.codeEnd(1, 1);
        mw.end(null, null);

        mw.begin(AccessFlag.PUBLIC, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", null, null);
        mw.add(ALOAD_0);
        mw.add(NEW);
        mw.add16(stringClass);
        mw.add(DUP);
        if (StringCreatorUtil.useOldStringConstructor()) {
            mw.add(ICONST_0);
            mw.add(ALOAD_1);
            mw.add(CHECKCAST);
            mw.add16(charArrayClass);
            mw.add(ARRAYLENGTH);
            mw.add(ALOAD_1);
            mw.add(CHECKCAST);
            mw.add16(charArrayClass);
            mw.add(INVOKESPECIAL);
            signature = cpw.addNameAndTypeInfo(MethodInfo.nameInit, "(II[C)V");
            mw.add16(cpw.addMethodrefInfo(stringClass, signature));
            mw.add(ARETURN);
            mw.codeEnd(6, 6);
        } else {
            mw.add(ALOAD_1);
            mw.add(CHECKCAST);
            mw.add16(charArrayClass);
            mw.add(ICONST_1);
            mw.add(INVOKESPECIAL);
            signature = cpw.addNameAndTypeInfo(MethodInfo.nameInit, "([CZ)V");
            mw.add16(cpw.addMethodrefInfo(stringClass, signature));
            mw.add(ARETURN);
            mw.codeEnd(5, 5);
        }
        mw.end(null, null);

        final byte[] impl = cfw.end(AccessFlag.PUBLIC, thisClass, superClass, new int[]{interfaceClass}, null);

        final sun.misc.Unsafe unsafe = UnsafeHelper.UNSAFE;
        Class clazz = AccessController.doPrivileged(new PrivilegedAction<Class>() {
            @Override
            public Class run() {
                ClassLoader cl = StringCreatorUtil.MAGIC_CLASSLOADER;
                return unsafe.defineClass("sun/reflect/JavassistString" + id, impl, 0, impl.length, cl, null);
            }
        });

        final Map accessor = (Map) clazz.newInstance();
        return new UTFUtil.StringCreator() {
            @Override
            public String buildString(char[] chars) {
                return (String) accessor.get(chars);
            }
        };
    }
}
