/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.usercodedeployment.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Classloader created on a local member to define a class from a bytecode loaded from a remote source.
 *
 * We use a classloader per each class loaded from a remote source as it allows us to discard the classloader
 * and reload the class via another classloader as we see fit.
 *
 * bytecodes of inner/anonymous classes are kept with their parent classes. If there is nested inner classes,
 * they are also kept in the same class loader.
 *
 * Delegation model:
 * 1. When the request matches the specific classname then it will provide the class on its own
 * 2. Then it delegates to the parent classloader - that's usually a regular classloader loading classes
 * from a local classpath only
 * 3. Finally it delegates to {@link ClassLocator} which may initiate a remote lookup
 */
public final class ClassSource extends ClassLoader {

    private final Map<String, Class> classes = new ConcurrentHashMap<String, Class>();
    private final Map<String, byte[]> classDefinitions = new ConcurrentHashMap<String, byte[]>();
    private final ClassLocator classLocator;

    public ClassSource(ClassLoader parent, ClassLocator classLocator) {
        super(parent);
        this.classLocator = classLocator;
    }

    public Class<?> define(String name, byte[] bytecode) {
        Class clazz = defineClass(name, bytecode, 0, bytecode.length);
        classDefinitions.put(name, bytecode);
        classes.put(name, clazz);
        return clazz;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        Class aClass = classes.get(name);
        if (aClass != null) {
            return aClass;
        }
        try {
            return super.loadClass(name, resolve);
        } catch (ClassNotFoundException e) {
            return classLocator.handleClassNotFoundException(name);
        }
    }

    byte[] getClassDefinition(String name) {
        return classDefinitions.get(name);
    }

    Class getClazz(String name) {
        return classes.get(name);
    }

    ClassData getClassData(String className) {
        ClassData classData = new ClassData();
        HashMap<String, byte[]> innerClassDefinitions = new HashMap<String, byte[]>(this.classDefinitions);
        byte[] mainClassDefinition = innerClassDefinitions.remove(className);
        classData.setInnerClassDefinitions(innerClassDefinitions);
        classData.setMainClassDefinition(mainClassDefinition);
        return classData;
    }
}
