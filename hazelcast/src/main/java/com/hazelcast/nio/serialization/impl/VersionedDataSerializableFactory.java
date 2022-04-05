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

package com.hazelcast.nio.serialization.impl;


import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;

/**
 * VersionedDataSerializableFactory is used to create IdentifiedDataSerializable instances during de-serialization.
 * This factory is version-aware - meaning it can create specific versions of the given objects.
 * May be used for the evolution of classes.
 * <p>
 * It extends the DataSerializableFactory so that it may also produce the newest/default objects outside the versioned-scope.
 * If the creation of the object is done in the context of no versioning the default create(int typeId) method will be used.
 *
 * @see IdentifiedDataSerializable
 * @see DataSerializableFactory
 * @see MemberVersion
 */
public interface VersionedDataSerializableFactory extends DataSerializableFactory {

    /**
     * Creates an IdentifiedDataSerializable instance using given type ID and object version
     *
     * @param typeId             IdentifiedDataSerializable type ID
     * @param clusterVersion     version of the object it should create - it's cluster version bound, since
     *                           objects change between release only. May be {@link Version#UNKNOWN} if the
     *                           WAN protocol version is set or cluster version is not available
     * @param wanProtocolVersion WAN protocol version. May be {@link Version#UNKNOWN} if the
     *                           cluster version is set or WAN protocol version is not available
     * @return IdentifiedDataSerializable instance or null if type ID is not known by this factory
     * @see MemberVersion
     */
    IdentifiedDataSerializable create(int typeId, Version clusterVersion, Version wanProtocolVersion);
}
