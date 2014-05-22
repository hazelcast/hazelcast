/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.serialization;

final class PortableVersionHelper {

    private PortableVersionHelper() {
    }

    public static int getVersion(Portable portable, int defaultVersion) {
        int version = defaultVersion;
        if (portable instanceof VersionedPortable) {
            VersionedPortable versionedPortable = (VersionedPortable) portable;
            version = versionedPortable.getClassVersion();
            if (version < 0) {
                throw new IllegalArgumentException("Version cannot be negative!");
            }
        }
        return version;
    }

}
