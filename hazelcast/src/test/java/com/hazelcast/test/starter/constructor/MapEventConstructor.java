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

package com.hazelcast.test.starter.constructor;

import java.lang.reflect.Constructor;

import static com.hazelcast.test.starter.HazelcastProxyFactory.proxyArgumentsIfNeeded;
import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;

public class MapEventConstructor extends AbstractStarterObjectConstructor {

    public MapEventConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate) throws Exception {
        ClassLoader starterClassLoader = targetClass.getClassLoader();
        Class<?> memberClass = starterClassLoader.loadClass("com.hazelcast.core.Member");
        Constructor<?> constructor = targetClass.getConstructor(Object.class, memberClass, Integer.TYPE, Integer.TYPE);

        Object source = getFieldValueReflectively(delegate, "source");
        Object member = getFieldValueReflectively(delegate, "member");
        Object entryEventType = getFieldValueReflectively(delegate, "entryEventType");
        Integer eventTypeId = (Integer) entryEventType.getClass().getMethod("getType").invoke(entryEventType);
        Object numberOfKeysAffected = getFieldValueReflectively(delegate, "numberOfEntriesAffected");

        Object[] args = new Object[]{source, member, eventTypeId, numberOfKeysAffected};

        Object[] proxiedArgs = proxyArgumentsIfNeeded(args, starterClassLoader);

        return constructor.newInstance(proxiedArgs);
    }
}
