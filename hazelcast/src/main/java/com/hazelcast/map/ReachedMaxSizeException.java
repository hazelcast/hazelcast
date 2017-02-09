/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import com.hazelcast.nio.serialization.impl.BinaryInterface;

import static com.hazelcast.nio.serialization.impl.BinaryInterface.Reason.OTHER_CONVENTION;

/**
 * Exception thrown when a write-behind {@link com.hazelcast.core.MapStore} rejects to accept a new element.
 * Used when {@link com.hazelcast.config.MapStoreConfig#writeCoalescing} is set to {@code false}.
 */
@BinaryInterface(reason = OTHER_CONVENTION)
public class ReachedMaxSizeException extends RuntimeException {

    private static final long serialVersionUID = -2352370861668557606L;

    public ReachedMaxSizeException(String msg) {
        super(msg);
    }
}
