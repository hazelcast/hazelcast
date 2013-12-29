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
import javassist.bytecode.*;
import javassist.bytecode.ClassFileWriter.ConstPoolWriter;
import javassist.bytecode.ClassFileWriter.FieldWriter;
import javassist.bytecode.ClassFileWriter.MethodWriter;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

public class JavassistJava8JlaStringCreatorBuilder implements Opcode, StringCreatorBuilder {

    @Override
    public UTFUtil.StringCreator build() throws Exception {
        final int id = StringCreatorUtil.CLASS_ID_COUNTER.getAndIncrement();
        final String className = "com/hazelcast/nio/utf8/JavassistStringAccessor" + id;

        ClassFileWriter cfw = new ClassFileWriter(ClassFile.JAVA_6, 0);
        ConstPoolWriter cpw = cfw.getConstPool();

        int thisClass = cpw.addClassInfo(className);
        int superClass = cpw.addClassInfo("java/lang/Object");
        int interfaceClass = cpw.addClassInfo("com/hazelcast/nio/UTFUtil$StringCreator");
        int jlaClass = cpw.addClassInfo("sun/misc/JavaLangAccess");
        int fieldName = cpw.addUtf8Info("jla");
        int fieldDesc = cpw.addUtf8Info("Lsun/misc/JavaLangAccess;");
        int fieldRef = cpw.addNameAndTypeInfo(fieldName, fieldDesc);
        int fieldRefInfo = cpw.addFieldrefInfo(thisClass, fieldRef);

        FieldWriter fw = cfw.getFieldWriter();
        fw.add(AccessFlag.FINAL, fieldName, fieldDesc, null);

        MethodWriter mw = cfw.getMethodWriter();
        mw.begin(AccessFlag.PUBLIC, MethodInfo.nameInit, "(Lsun/misc/JavaLangAccess;)V", null, null);
        mw.add(ALOAD_0);
        mw.add(INVOKESPECIAL);
        int signature = cpw.addNameAndTypeInfo(MethodInfo.nameInit, "()V");
        mw.add16(cpw.addMethodrefInfo(superClass, signature));
        mw.add(ALOAD_0);
        mw.add(ALOAD_1);
        mw.add(PUTFIELD);
        mw.add16(fieldRefInfo);
        mw.add(RETURN);
        mw.codeEnd(2, 2);
        mw.end(null, null);

        mw.begin(AccessFlag.PUBLIC, "buildString", "([C)Ljava/lang/String;", null, null);
        mw.add(ALOAD_0);
        mw.add(GETFIELD);
        mw.add16(fieldRefInfo);
        mw.add(ALOAD_1);
        mw.add(INVOKEINTERFACE);
        signature = cpw.addNameAndTypeInfo("newStringUnsafe", "([C)Ljava/lang/String;");
        mw.add16(cpw.addInterfaceMethodrefInfo(jlaClass, signature));
        mw.add(2);
        mw.add(0);
        mw.add(ARETURN);
        mw.codeEnd(3, 2);
        mw.end(null, null);

        final byte[] impl = cfw.end(AccessFlag.PUBLIC, thisClass, superClass, new int[]{interfaceClass}, null);

        AnonClassLoader cl = new AnonClassLoader(getClass().getClassLoader());
        Class<?> clazz = cl.loadClass("com.hazelcast.nio.utf8.JavassistStringAccessor" + id, impl);
        Constructor constructor = clazz.getConstructor(StringCreatorUtil.JAVA_LANG_ACCESS_CLASS);

        Field jlaField = StringCreatorUtil.SHARED_SECRET_CLASS.getDeclaredField("javaLangAccess");
        jlaField.setAccessible(true);

        return (UTFUtil.StringCreator) constructor.newInstance(jlaField.get(StringCreatorUtil.SHARED_SECRET_CLASS));
    }
}
