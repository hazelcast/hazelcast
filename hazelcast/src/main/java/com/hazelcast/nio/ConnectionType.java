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

package com.hazelcast.nio;

/**
 * An enumeration of all possible Connection types.
 *
* @author mdogan 7/3/13
*/
public enum ConnectionType {

    NONE(false, false),
    MEMBER(true, true),
    JAVA_CLIENT(false, true),
    CSHARP_CLIENT(false, true),
    CPP_CLIENT(false, true),
    PYTHON_CLIENT(false, true),
    RUBY_CLIENT(false, true),
    BINARY_CLIENT(false, true),
    REST_CLIENT(false, false),
    MEMCACHE_CLIENT(false, false);

    final boolean member;
    final boolean binary;

    ConnectionType(boolean member, boolean binary) {
        this.member = member;
        this.binary = binary;
    }

    public boolean isBinary() {
        return binary;
    }

    public boolean isClient() {
        return !member;
    }
}
