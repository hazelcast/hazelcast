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
import org.apache.bcel.Constants;
import org.apache.bcel.generic.*;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;

class BcelMagicAccessorStringCreatorBuilder implements Constants, StringCreatorBuilder {

    @Override
    public UTFUtil.StringCreator build() throws Exception {
        final int id = StringCreatorUtil.CLASS_ID_COUNTER.getAndIncrement();
        final String className = "sun.reflect.BcelString" + id;

        ClassGen classGen = new ClassGen(className,
                "sun.reflect.MagicAccessorImpl", "<generated>", ACC_PUBLIC | ACC_FINAL,
                new String[]{"java/util/Map" });

        classGen.addEmptyConstructor(ACC_PUBLIC);

        InstructionFactory ilf = new InstructionFactory(classGen);
        InstructionList il = new InstructionList();
        il.append(ilf.createNew("java.lang.String"));
        il.append(InstructionConstants.DUP);
        if (StringCreatorUtil.useOldStringConstructor()) {
            il.append(InstructionConstants.ICONST_0);
            il.append(InstructionConstants.ALOAD_1);
            il.append(ilf.createCast(Type.OBJECT, new ArrayType(Type.CHAR, 1)));
            il.append(InstructionConstants.ARRAYLENGTH);
            il.append(InstructionConstants.ALOAD_1);
            il.append(ilf.createCast(Type.OBJECT, new ArrayType(Type.CHAR, 1)));
            il.append(ilf.createInvoke("java.lang.String", "<init>", Type.VOID,
                    new Type[]{Type.INT, Type.INT, new ArrayType(Type.CHAR, 1)}, INVOKESPECIAL));
            il.append(ilf.createReturn(Type.OBJECT));
        } else {
            il.append(InstructionConstants.ALOAD_1);
            il.append(ilf.createCast(Type.OBJECT, new ArrayType(Type.CHAR, 1)));
            il.append(InstructionConstants.ICONST_1);
            il.append(ilf.createInvoke("java.lang.String", "<init>", Type.VOID,
                    new Type[]{new ArrayType(Type.CHAR, 1), Type.BOOLEAN}, INVOKESPECIAL));
            il.append(ilf.createReturn(Type.OBJECT));
        }

        MethodGen mg = new MethodGen(ACC_PUBLIC, Type.OBJECT, new Type[]{Type.OBJECT}, new String[]{"key" },
                "get", className, il, classGen.getConstantPool());
        if (StringCreatorUtil.useOldStringConstructor()) {
            mg.setMaxStack(6);
            mg.setMaxLocals(6);
        } else {
            mg.setMaxStack(5);
            mg.setMaxLocals(5);
        }
        classGen.addMethod(mg.getMethod());

        final byte[] impl = classGen.getJavaClass().getBytes();

        final sun.misc.Unsafe unsafe = UnsafeHelper.UNSAFE;
        Class clazz = AccessController.doPrivileged(new PrivilegedAction<Class>() {
            @Override
            public Class run() {
                ClassLoader cl = StringCreatorUtil.MAGIC_CLASSLOADER;
                return unsafe.defineClass("sun/reflect/BcelString" + id, impl, 0, impl.length, cl, null);
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
