/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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
import java.util.UUID;

import static com.hazelcast.test.starter.HazelcastProxyFactory.proxyArgumentsIfNeeded;
import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;

@HazelcastStarterConstructor(classNames = "com.hazelcast.cp.internal.CPMemberInfo")
public class CPMemberInfoConstructor extends AbstractStarterObjectConstructor {

    public CPMemberInfoConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate) throws Exception {
        UUID    uuid    = (UUID)   getFieldValueReflectively(delegate, "uuid");
        Object  address =    getFieldValueReflectively(delegate, "address");
        Boolean auto    = null;
        try {
            auto = (Boolean) getFieldValueReflectively(delegate, "autoStepDownWhenLeader");
        } catch (NoSuchFieldError ignored) {
            // older versions don’t have that flag
        }

        Class<?> cls       = targetClass;
        ClassLoader loader = cls.getClassLoader();
        Class<?> eeAddrCl  = loader.loadClass("com.hazelcast.cluster.Address");

        Constructor<?> ctor;
        Object[]    args;

        try {
            ctor = cls.getDeclaredConstructor(UUID.class, eeAddrCl, boolean.class, boolean.class);
            boolean exists = auto != null;
            boolean value  = exists && auto;
            args = new Object[]{ uuid, address, exists, value };
        } catch (NoSuchMethodException e1) {
            try {
                ctor = cls.getDeclaredConstructor(UUID.class, eeAddrCl, boolean.class);
                boolean flag = auto != null && auto;
                args = new Object[]{ uuid, address, flag };
            } catch (NoSuchMethodException e2) {
                ctor = cls.getDeclaredConstructor(UUID.class, eeAddrCl);
                args = new Object[]{ uuid, address };
            }
        }

        ctor.setAccessible(true);
        Object[] proxied = proxyArgumentsIfNeeded(args, loader);
        return ctor.newInstance(proxied);
    }
}
