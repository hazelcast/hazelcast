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

package com.hazelcast.internal.usercodedeployment.impl;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Classloader created on a local member to define a class from a bytecode loaded from a remote source.
 *
 * We use a classloader per each class loaded from a remote source as it allows us to discard the classloader
 * and reload the class via another classloader as we see fit.
 *
 * Delegation model:
 * 1. When the request matches the specific classname then it will provide the class on its own
 * 2. Then it delegates to the parent classloader - that's usually a regular classloader loading classes
 *    from a local classpath only
 * 3. Finally it delegates to {@link ClassLocator} which may initiate a remote lookup
 */
public final class ClassSource extends ClassLoader {

    private final byte[] bytecode;
    private final ClassLocator classLocator;
    private Class clazz;
    private final String name;

    @SuppressFBWarnings({"MS_EXPOSE_REP", "EI_EXPOSE_REP"})
    public ClassSource(String classname, byte[] bytecode, ClassLoader parent, ClassLocator classLocator) {
        super(parent);
        this.bytecode = bytecode;
        this.name = classname;
        this.classLocator = classLocator;
    }

    public Class<?> define() {
        clazz = defineClass(name, bytecode, 0, bytecode.length);
        return clazz;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (name.equals(this.name)) {
            return clazz;
        }
        try {
            return super.loadClass(name, resolve);
        } catch (ClassNotFoundException e) {
            return classLocator.handleClassNotFoundException(name);
        }
    }

    @SuppressFBWarnings({"MS_EXPOSE_REP", "EI_EXPOSE_REP"})
    public byte[] getBytecode() {
        return bytecode;
    }

    public Class getClazz() {
        return clazz;
    }
}
