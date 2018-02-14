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

package com.hazelcast.test.starter;

import java.lang.reflect.Constructor;

import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;

public class AddressConstructor extends AbstractStarterObjectConstructor {

    public AddressConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate)
            throws Exception {
        // obtain reference to constructor Address(String host, int port)
        Constructor<?> constructor = targetClass.getDeclaredConstructor(String.class, Integer.TYPE);

        Object host = getFieldValueReflectively(delegate, "host");
        Integer port = (Integer) getFieldValueReflectively(delegate, "port");
        Object[] args = new Object[]{host, port.intValue()};

        return constructor.newInstance(args);
    }
}
