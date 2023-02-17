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

package com.hazelcast.instance.impl;

import java.lang.reflect.Method;

import static java.lang.reflect.Modifier.isPublic;
import static java.lang.reflect.Modifier.isStatic;

public class MainMethodFinder {

    private String errorMessage;

    private Method mainMethod;

    public String getErrorMessage() {
        return errorMessage;
    }

    public Method getMainMethod() {
        return mainMethod;
    }

    // Public for testing purpose
    public void setMainMethod(Method mainMethod) {
        this.mainMethod = mainMethod;
    }

    public void findMainMethod(ClassLoader classLoader, String mainClassName) {
        try {
            Class<?> clazz = classLoader.loadClass(mainClassName);
            getMainMethodOfClass(clazz);
        } catch (ClassNotFoundException e) {
            errorMessage = "Cannot find or load main class: " + mainClassName;
        }
    }

    void getMainMethodOfClass(Class<?> clazz) {
        try {
            // If main method does not exist, throws NoSuchMethodException
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
