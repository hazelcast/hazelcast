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
import java.util.UUID;

import static com.hazelcast.test.starter.HazelcastProxyFactory.proxyArgumentsIfNeeded;
import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;

@HazelcastStarterConstructor(classNames = {"com.hazelcast.scheduledexecutor.impl.ScheduledTaskHandlerImpl"})
public class ScheduledTaskHandlerImplConstructor extends AbstractStarterObjectConstructor {

    public ScheduledTaskHandlerImplConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate) throws Exception {
        Object uuid = getFieldValueReflectively(delegate, "uuid");
        Integer partitionId = getFieldValueReflectively(delegate, "partitionId");
        String schedulerName = getFieldValueReflectively(delegate, "schedulerName");
        String taskName = getFieldValueReflectively(delegate, "taskName");

        ClassLoader targetClassloader = targetClass.getClassLoader();

        Constructor<?> constructor = targetClass.getDeclaredConstructor(UUID.class, Integer.TYPE,
                String.class, String.class);
        constructor.setAccessible(true);

        if (uuid != null) {
            Object[] args = new Object[]{uuid, -1, schedulerName, taskName};
            Object[] proxiedArgs = proxyArgumentsIfNeeded(args, targetClassloader);
            return constructor.newInstance(proxiedArgs);
        } else {
            Object[] args = new Object[]{null, partitionId, schedulerName, taskName};
            return constructor.newInstance(args);
        }
    }
}
