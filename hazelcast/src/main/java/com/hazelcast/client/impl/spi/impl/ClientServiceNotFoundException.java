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

package com.hazelcast.client.impl.spi.impl;

/**
 * A {@link IllegalArgumentException} that indicates that a requested client service doesn't exist.
 *
 * The previous implementation was throwing {@link IllegalArgumentException}
 * but we need a specific exception type for client service not available case.
 * Therefore, for keeping backward compatibility throwing exception is still an {@link IllegalArgumentException}
 * by extending {@link ClientServiceNotFoundException} from {@link IllegalArgumentException}.
 */
public class ClientServiceNotFoundException extends IllegalArgumentException {

    public ClientServiceNotFoundException(String message) {
        super(message);
    }
}
