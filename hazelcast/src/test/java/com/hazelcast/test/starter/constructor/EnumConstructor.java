/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.starter.constructor;

import java.lang.reflect.Method;

/**
 * Constructs enum instances using its {@link Enum#name()} and
 * {@link Enum#valueOf(Class, String)} methods.
 */
public class EnumConstructor extends AbstractStarterObjectConstructor {

    public EnumConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate) throws Exception {
        // obtain delegate as string
        Method nameMethod = delegate.getClass().getMethod("name");
        String delegateAsString = (String) nameMethod.invoke(delegate);
        // construct new instance at target classloader via valueOf()
        Method valueOf = targetClass.getMethod("valueOf", String.class);

        return valueOf.invoke(null, delegateAsString);
    }
}
