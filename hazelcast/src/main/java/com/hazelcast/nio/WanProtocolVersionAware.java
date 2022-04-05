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

package com.hazelcast.nio;

import com.hazelcast.version.Version;

/**
 * An instance which is used in WAN and supports versioning. The version
 * indicates the version of the WAN protocol.
 */
public interface WanProtocolVersionAware {

    /**
     * Returns the WAN protocol version. This method is similar to
     * {@link VersionAware#getVersion()} but is used in WAN replication, not in
     * message exchange inside a single cluster.
     *
     * @return the WAN protocol version or {@link Version#UNKNOWN} if not set
     * @see VersionAware#getVersion()
     */
    Version getWanProtocolVersion();

    /**
     * Sets the WAN protocol version. This method is similar to
     * {@link VersionAware#setVersion(Version)} but is used in WAN replication,
     * not in message exchange inside a single cluster.
     *
     * @param version the WAN protocol version
     * @see VersionAware#setVersion(Version)
     */
    void setWanProtocolVersion(Version version);
}
