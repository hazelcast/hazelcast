/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.version.Version;

/**
 * An extension to {@link IdentifiedDataSerializable}, which makes it possible
 * to report different class IDs depending on the cluster version.
 *
 * @since 5.4
 */
public interface VersionedIdentifiedDataSerializable extends IdentifiedDataSerializable, Versioned {

    int getClassId(Version clusterVersion);

    @Override
    default int getClassId() {
        throw new UnsupportedOperationException();
    }
}
