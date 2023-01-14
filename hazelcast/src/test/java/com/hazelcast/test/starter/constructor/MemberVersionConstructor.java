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

package com.hazelcast.test.starter.constructor;

import com.hazelcast.test.starter.HazelcastStarterConstructor;

import java.lang.reflect.Constructor;

import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;

@HazelcastStarterConstructor(classNames = {"com.hazelcast.version.MemberVersion"})
public class MemberVersionConstructor extends AbstractStarterObjectConstructor {

    public MemberVersionConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate) throws Exception {
        // obtain reference to constructor MemberVersion(int major, int minor, int patch)
        Constructor<?> constructor = targetClass.getDeclaredConstructor(Integer.TYPE, Integer.TYPE, Integer.TYPE);

        Byte major = (Byte) getFieldValueReflectively(delegate, "major");
        Byte minor = (Byte) getFieldValueReflectively(delegate, "minor");
        Byte patch = (Byte) getFieldValueReflectively(delegate, "patch");
        Object[] args = new Object[]{major, minor, patch};

        return constructor.newInstance(args);
    }
}
