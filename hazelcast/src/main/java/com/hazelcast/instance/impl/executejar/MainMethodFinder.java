/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance.impl.executejar;

import com.hazelcast.internal.util.StringUtil;

import java.lang.reflect.Method;

import static java.lang.reflect.Modifier.isPublic;
import static java.lang.reflect.Modifier.isStatic;

class MainMethodFinder {

    Method mainMethod;

    private String errorMessage;

    public String getErrorMessage() {
        return errorMessage;
    }

    public Method getMainMethod() {
        return mainMethod;
    }

    boolean hasError() {
        return !StringUtil.isNullOrEmpty(errorMessage);
    }

    public void findMainMethod(ClassLoader classLoader, String mainClassName) throws ClassNotFoundException {
        Class<?> clazz = classLoader.loadClass(mainClassName);
        getMainMethodOfClass(clazz);
    }

    void getMainMethodOfClass(Class<?> clazz) {
        try {
            mainMethod = clazz.getDeclaredMethod("main", String[].class);

            if (!isPublicAndStatic()) {
                errorMessage = "Class " + clazz.getName()
                               + " has a main(String[] args) method which is not public static";
            }
        } catch (NoSuchMethodException exception) {
            errorMessage = "Class " + clazz.getName()
                           + " does not have a main(String[] args) method";
        }
    }

    boolean isPublicAndStatic() {
        int modifiers = mainMethod.getModifiers();
        return isPublic(modifiers) && isStatic(modifiers);
    }
}
