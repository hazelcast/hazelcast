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

import java.lang.reflect.Constructor;
import java.util.UUID;

import static com.hazelcast.test.starter.HazelcastProxyFactory.proxyArgumentsIfNeeded;
import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;

@HazelcastStarterConstructor(classNames = {"com.hazelcast.internal.partition.PartitionReplica"})
public class PartitionReplicaConstructor extends AbstractStarterObjectConstructor {

    public PartitionReplicaConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate)
            throws Exception {
        ClassLoader classloader = targetClass.getClassLoader();
        Class<?> replicaClass = classloader.loadClass("com.hazelcast.internal.partition.PartitionReplica");
        Class<?> addressClass = classloader.loadClass("com.hazelcast.cluster.Address");
        Constructor<?> constructor = targetClass.getDeclaredConstructor(addressClass, UUID.class);

        Object address = getFieldValueReflectively(delegate, "address");
        Object uuid = getFieldValueReflectively(delegate, "uuid");

        Object[] args = new Object[] {address, uuid};
        Object[] proxiedArgs = proxyArgumentsIfNeeded(args, classloader);

        return constructor.newInstance(proxiedArgs);
    }
}
