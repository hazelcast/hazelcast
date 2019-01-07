/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;

import static com.hazelcast.internal.partition.PartitionReplica.UNKNOWN_UID;
import static com.hazelcast.test.starter.HazelcastProxyFactory.proxyArgumentsIfNeeded;
import static com.hazelcast.test.starter.ReflectionUtils.getAllFieldsByName;
import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;

@HazelcastStarterConstructor(classNames = {"com.hazelcast.internal.partition.impl.InternalPartitionImpl"})
public class InternalPartitionImplConstructor extends AbstractStarterObjectConstructor {

    public InternalPartitionImplConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate) throws Exception {
        ClassLoader classloader = targetClass.getClassLoader();
        Class<?> partitionListenerClass = classloader.loadClass("com.hazelcast.internal.partition.PartitionListener");
        Class<?> replicaClass = classloader.loadClass("com.hazelcast.internal.partition.PartitionReplica");
        Class<?> addressClass = classloader.loadClass("com.hazelcast.nio.Address");
        Class<?> replicaArrayClass = Array.newInstance(replicaClass, 0).getClass();
        // obtain reference to constructor InternalPartitionImpl(int partitionId, PartitionListener listener,
        //                                                       PartitionReplica localReplica, PartitionReplica[] replicas)
        Constructor<?> constructor = targetClass.getDeclaredConstructor(Integer.TYPE, partitionListenerClass, replicaClass,
                replicaArrayClass);

        // obtain reference to constructor PartitionReplica(Address address, String uuid)
        Constructor<?> replicaConstructor = replicaClass.getConstructor(addressClass, String.class);

        Integer partitionId = (Integer) getFieldValueReflectively(delegate, "partitionId");
        Object partitionListener = getFieldValueReflectively(delegate, "partitionListener");
        Object localReplica = getLocalReplica(delegate, classloader, replicaConstructor);
        Object replicas = getReplicas(delegate, classloader, replicaConstructor);

        Object[] args = new Object[]{partitionId, partitionListener, localReplica, replicas};
        Object[] proxiedArgs = proxyArgumentsIfNeeded(args, classloader);

        return constructor.newInstance(proxiedArgs);
    }

    private Object getReplicas(Object delegate,
                               ClassLoader classloader,
                               Constructor<?> replicaConstructor) throws Exception {
        // RU_COMPAT_3_11
        boolean is311Instance = is311Instance(delegate);
        if (is311Instance) {
            Object[] addresses = (Object[]) getFieldValueReflectively(delegate, "addresses");
            Object[] replicas = (Object[]) Array.newInstance(replicaConstructor.getDeclaringClass(), addresses.length);
            for (int i = 0; i < addresses.length; i++) {
                replicas[i] = toPartitionReplica(addresses[i], replicaConstructor, classloader);
            }
            return replicas;
        } else {
            return getFieldValueReflectively(delegate, "replicas");
        }
    }

    private Object getLocalReplica(Object delegate, ClassLoader classloader, Constructor<?> replicaConstructor) throws Exception {
        // RU_COMPAT_3_11
        boolean is311Instance = is311Instance(delegate);
        if (is311Instance) {
            Object thisAddress = getFieldValueReflectively(delegate, "thisAddress");
            return toPartitionReplica(thisAddress, replicaConstructor, classloader);
        } else {
            return getFieldValueReflectively(delegate, "localReplica");
        }
    }

    private static boolean is311Instance(Object delegate) {
        return getAllFieldsByName(delegate.getClass()).containsKey("thisAddress");
    }

    private Object toPartitionReplica(Object address,
                                      Constructor<?> replicaConstructor,
                                      ClassLoader targetClassloader) throws Exception {
        if (address == null) {
            return null;
        }
        Object[] args = {address, UNKNOWN_UID};
        Object[] proxiedArgs = proxyArgumentsIfNeeded(args, targetClassloader);
        return replicaConstructor.newInstance(proxiedArgs);
    }
}
