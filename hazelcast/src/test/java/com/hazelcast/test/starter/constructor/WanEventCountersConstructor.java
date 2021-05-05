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
import com.hazelcast.wan.WanEventCounters.DistributedObjectWanEventCounters;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.test.starter.ReflectionUtils.copyFieldValuesReflectively;
import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;

@HazelcastStarterConstructor(classNames = {"com.hazelcast.wan.WanEventCounters",
        "com.hazelcast.wan.impl.DistributedServiceWanEventCounters"})
public class WanEventCountersConstructor extends AbstractStarterObjectConstructor {

    public WanEventCountersConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate) throws Exception {
        Constructor<?> constructor = targetClass.getConstructor();
        Object targetInstance = constructor.newInstance();
        Map<String, Object> targetCounterMap = getFieldValueReflectively(targetInstance, "eventCounterMap");
        Map<String, Object> delegateCounterMap = getFieldValueReflectively(delegate, "eventCounterMap");
        ClassLoader targetClassLoader = targetClass.getClassLoader();

        Constructor<?> targetClassConstructor = getCounterClass(targetClassLoader)
                .getDeclaredConstructor();
        targetClassConstructor.setAccessible(true);

        for (Entry<String, Object> delegateCounterEntry : delegateCounterMap.entrySet()) {
            String key = delegateCounterEntry.getKey();
            Object delegateCounter = delegateCounterEntry.getValue();
            Object targetCounter = targetClassConstructor.newInstance();
            copyFieldValuesReflectively(delegateCounter, targetCounter,
                    "syncCount", "updateCount", "removeCount", "droppedCount");
            targetCounterMap.put(key, targetCounter);
        }

        return targetInstance;
    }

    private Class<?> getCounterClass(ClassLoader targetClassLoader) throws ClassNotFoundException {
        try {
            return targetClassLoader.loadClass(DistributedObjectWanEventCounters.class.getName());
        } catch (ClassNotFoundException e) {
            // target classloader is 3.x
            String className = "com.hazelcast.wan.impl.DistributedServiceWanEventCounters$DistributedObjectWanEventCounters";
            return targetClassLoader.loadClass(className);
        }
    }
}
