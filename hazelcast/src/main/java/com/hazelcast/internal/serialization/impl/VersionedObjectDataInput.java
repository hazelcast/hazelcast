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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.version.Version;

import java.io.InputStream;

/**
 * Base class for ObjectDataInput that is VersionAware and allows mutating
 * the version.
 * <p>
 * What the version means it's up to the Serializer/Deserializer.
 * If the serializer supports versioning it may set the version to use for
 * the serialization on this object.
 */
abstract class VersionedObjectDataInput extends InputStream implements ObjectDataInput {

    protected Version version = Version.UNKNOWN;
    protected Version wanProtocolVersion = Version.UNKNOWN;

    @Override
    public Version getWanProtocolVersion() {
        return wanProtocolVersion;
    }

    @Override
    public void setWanProtocolVersion(Version version) {
        this.wanProtocolVersion = version;
    }

    /**
     * {@inheritDoc}
     * If the serializer supports versioning it may set the version to use for
     * the intra-cluster message serialization on this object.
     *
     * @return the version of {@link Version#UNKNOWN} if the version is unknown to the object.
     */
    @Override
    public Version getVersion() {
        return version;
    }

    /**
     * {@inheritDoc}
     * If the serializer supports versioning it may set the version to use for
     * the intra-cluster message serialization on this object.
     *
     * @param version version to set
     */
    @Override
    public void setVersion(Version version) {
        this.version = version;
    }
}
