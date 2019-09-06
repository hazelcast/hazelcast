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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.VersionAware;
import com.hazelcast.version.Version;

import java.io.OutputStream;

import static com.hazelcast.version.Version.UNKNOWN;

/**
 * Base class for ObjectDataInput that is {@link VersionAware} and allows
 * mutating the version.
 * What the version means it's up to the Serializer/Deserializer.
 * If the serializer supports versioning it may set the version to use for
 * the serialization on this object.
 */
abstract class VersionedObjectDataOutput extends OutputStream implements ObjectDataOutput, VersionAware {
    /**
     * Used for both WAN protocol version and intra-cluster message versioning.
     * Negative major version numbers indicate that the WAN protocol version is
     * set while positive major version numbers indicate that the intra-cluster
     * message version is set.
     */
    protected Version version = Version.UNKNOWN;

    /**
     * {@inheritDoc}
     * If the serializer supports versioning it may set the version to use for
     * the intra-cluster message serialization on this object.
     * The {@link #version} field is used for both the WAN protocol version and
     * intra-cluster message versioning so the output stream can only have one
     * of the {@link #setVersion(Version)} or
     * {@link #setWanProtocolVersion(Version)} set at a time.
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
     * The {@link #version} field is used for both the WAN protocol version and
     * intra-cluster message versioning so the output stream can only have one
     * of the {@link #setVersion(Version)} or
     * {@link #setWanProtocolVersion(Version)} set at a time.
     *
     * @param version version to set
     */
    @Override
    public void setVersion(Version version) {
        this.version = version;
    }

    /**
     * Returns the raw, unformatted version set on this instance. On the other
     * hand, both {@link #getVersion()} and {@link #getWanProtocolVersion()}
     * will return conditional and formatted versions.
     *
     * @return the raw, unformatted version set on this instance
     */
    public Version getRawVersion() {
        return version;
    }

    /**
     * {@inheritDoc}
     * The {@link #version} field is used for both the WAN protocol version and
     * intra-cluster message versioning so the output stream can only have one
     * of the {@link #setVersion(Version)} or
     * {@link #setWanProtocolVersion(Version)} set at a time.
     */
    @Override
    public void setWanProtocolVersion(Version version) {
        this.version = Version.of(-1 * version.getMajor(), version.getMinor());
    }

    @Override
    public Version getWanProtocolVersion() {
        // WAN protocol version is set when major version is negative
        return isWanProtocolVersionSet()
                ? Version.of(-1 * version.getMajor(), version.getMinor())
                : UNKNOWN;
    }

    /**
     * Returns {@code true} if WAN protocol version is set, {@code false} otherwise.
     */
    public boolean isWanProtocolVersionSet() {
        return version.getMajor() < 0;
    }
}
