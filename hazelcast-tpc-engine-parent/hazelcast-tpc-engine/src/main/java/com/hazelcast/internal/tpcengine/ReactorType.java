/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

/**
 * The Type of {@link Reactor}.
 * <p/>
 * Idea:
 * Add an IN_MEMORY type that can only be used for in memory communication.
 * The primary purpose would to speed up testing.
 */
public enum ReactorType {

    // Can be used on any OS that can run Java
    NIO,
    // Can only be used on Linux 5.7+
    IOURING;

    public static ReactorType fromString(String type) {
        checkNotNull(type, "type");

        if (type.equalsIgnoreCase("io_uring")
                || type.equalsIgnoreCase("iouring")
                || type.equalsIgnoreCase("uring")) {
            return IOURING;
        } else if (type.equalsIgnoreCase("nio")) {
            return NIO;
        } else {
            throw new IllegalArgumentException("Reactor type [" + type + "] is not recognized.");
        }
    }
}