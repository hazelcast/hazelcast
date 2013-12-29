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
import org.apache.bcel.Constants;
import org.apache.bcel.generic.*;
import sun.misc.JavaLangAccess;
import sun.misc.SharedSecrets;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

class IBMBcelJava8JlaStringCreatorBuilder implements Constants, StringCreatorBuilder {

    @Override
    public UTFUtil.StringCreator build() throws Exception {
        final int id = StringCreatorUtil.CLASS_ID_COUNTER.getAndIncrement();
        final String className = "com.hazelcast.nio.utf8.InternalBcelStringAccessor" + id;

        ClassGen classGen = new ClassGen(className, "java.lang.Object", "<generated>", ACC_PUBLIC | ACC_FINAL,
                new String[]{"com.hazelcast.nio.UTFUtil$StringCreator"});

        Type jlaType = Type.getType(JavaLangAccess.class);
        Type charArrayType = new ArrayType(Type.CHAR, 1);

        FieldGen fg = new FieldGen(ACC_FINAL | ACC_PRIVATE, jlaType, "jla", classGen.getConstantPool());
        classGen.addField(fg.getField());

        InstructionFactory ilf = new InstructionFactory(classGen);
        InstructionList il = new InstructionList();
        il.append(InstructionConstants.ALOAD_0);
        il.append(ilf.createInvoke("java.lang.Object", CONSTRUCTOR_NAME, Type.VOID, new Type[0], INVOKESPECIAL));
        il.append(InstructionConstants.ALOAD_0);
        il.append(InstructionConstants.ALOAD_1);
        il.append(ilf.createPutField(className, "jla", jlaType));
        il.append(InstructionConstants.RETURN);

        MethodGen mg = new MethodGen(ACC_PUBLIC, Type.VOID, new Type[]{jlaType}, new String[]{"jla"},
                CONSTRUCTOR_NAME, className, il, classGen.getConstantPool());
        mg.setMaxStack(2);
        mg.setMaxLocals(2);
        classGen.addMethod(mg.getMethod());

        il = new InstructionList();
        il.append(InstructionConstants.ALOAD_0);
        il.append(ilf.createGetField(className, "jla", jlaType));
        il.append(InstructionConstants.ALOAD_1);
        il.append(ilf.createInvoke("sun.misc.JavaLangAccess", "newStringUnsafe", Type.STRING,
                new Type[]{charArrayType}, INVOKEINTERFACE));
        il.append(InstructionConstants.ARETURN);

        mg = new MethodGen(ACC_PUBLIC, Type.STRING, new Type[]{charArrayType}, new String[]{"data"},
                "buildString", className, il, classGen.getConstantPool());
        mg.setMaxStack(3);
        mg.setMaxLocals(2);
        classGen.addMethod(mg.getMethod());

        final byte[] impl = classGen.getJavaClass().getBytes();

        AnonClassLoader cl = new AnonClassLoader(getClass().getClassLoader());
        Class<?> clazz = cl.loadClass(className, impl);
        Constructor constructor = clazz.getConstructor(JavaLangAccess.class);

        Field jlaField = SharedSecrets.class.getDeclaredField("javaLangAccess");
        jlaField.setAccessible(true);

        return (UTFUtil.StringCreator) constructor.newInstance(jlaField.get(SharedSecrets.class));
    }
}
