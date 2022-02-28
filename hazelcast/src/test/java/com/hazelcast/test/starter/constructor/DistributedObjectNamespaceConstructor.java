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

import com.hazelcast.test.starter.HazelcastStarterConstructor;

import java.lang.reflect.Constructor;

import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;

@HazelcastStarterConstructor(classNames = {"com.hazelcast.internal.services.DistributedObjectNamespace"})
public class DistributedObjectNamespaceConstructor extends AbstractStarterObjectConstructor {

    public DistributedObjectNamespaceConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate) throws Exception {
        // obtain reference to constructor DistributedObjectNamespace(String serviceName, String objectName)
        Constructor<?> constructor = targetClass.getDeclaredConstructor(String.class, String.class);

        Object service = getFieldValueReflectively(delegate, "service");
        Object objectName = getFieldValueReflectively(delegate, "objectName");
        Object[] args = new Object[]{service, objectName};

        return constructor.newInstance(args);
    }
}
