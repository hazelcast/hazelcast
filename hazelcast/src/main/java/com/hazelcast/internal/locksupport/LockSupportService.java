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

package com.hazelcast.internal.locksupport;

import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.util.ConstructorFunction;

import java.util.Collection;

public interface LockSupportService {

    String SERVICE_NAME = "hz:impl:lockService";

    void registerLockStoreConstructor(String serviceName,
                                      ConstructorFunction<ObjectNamespace, LockStoreInfo> constructorFunction);

    LockStore createLockStore(int partitionId, ObjectNamespace namespace);

    void clearLockStore(int partitionId, ObjectNamespace namespace);

    Collection<LockResource> getAllLocks();

    long getMaxLeaseTimeInMillis();
}
