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

package com.hazelcast.client.impl.protocol;

/**
 * Status codes of authentication results
 */
public enum AuthenticationStatus {

    AUTHENTICATED(0),
    CREDENTIALS_FAILED(1),
    SERIALIZATION_VERSION_MISMATCH(2),
    NOT_ALLOWED_IN_CLUSTER(3);

    private final byte id;

    AuthenticationStatus(int status) {
        this.id = (byte) status;
    }

    public byte getId() {
        return id;
    }

    public static AuthenticationStatus getById(int id) {
        for (AuthenticationStatus as : values()) {
            if (as.getId() == id) {
                return as;
            }
        }
        throw new IllegalArgumentException("Unsupported ID value");
    }
}
