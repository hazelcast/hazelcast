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

package com.hazelcast.core;

import com.hazelcast.spi.properties.ClusterProperty;

/**
 * A {@link com.hazelcast.core.HazelcastException} that is thrown when the system won't handle more load due to
 * an overload.
 *
 * This exception is thrown when backpressure is enabled. For more information see
 * {@link ClusterProperty#BACKPRESSURE_ENABLED}.
 */
public class HazelcastOverloadException extends HazelcastException {

    public HazelcastOverloadException(String message) {
        super(message);
    }

    public HazelcastOverloadException(String message, Throwable cause) {
        super(message, cause);
    }
}
