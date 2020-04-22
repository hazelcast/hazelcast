/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;

import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;

@HazelcastStarterConstructor(classNames = "com.hazelcast.cp.internal.RaftGroupId")
public class RaftGroupIdConstructor extends AbstractStarterObjectConstructor {

    public RaftGroupIdConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate)
            throws Exception {
        MethodType ctorType = MethodType.methodType(void.class, String.class, long.class, long.class);
        MethodHandle constructor = PUBLIC_LOOKUP.findConstructor(targetClass, ctorType);
        String name = getFieldValueReflectively(delegate, "name");
        long seed = getFieldValueReflectively(delegate, "seed");
        long groupId = getFieldValueReflectively(delegate, "groupId");
        try {
            return constructor.invoke(name, seed, groupId);
        } catch (Throwable throwable) {
            throw sneakyThrow(throwable);
        }
    }
}
