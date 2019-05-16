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
package com.hazelcast.security;

import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.nio.serialization.Data;

/**
 * {@link Credentials} type for custom authentication (token based).
 */
public interface TokenCredentials extends Credentials {

    /**
     * Returns the token as a byte array.
     */
    byte[] getToken();

    /**
     * Returns the token represented as {@link Data}. The default implementation wraps the token bytes into a on-heap Data
     * instance.
     */
    default Data asData() {
        return new HeapData(getToken());
    }
}
