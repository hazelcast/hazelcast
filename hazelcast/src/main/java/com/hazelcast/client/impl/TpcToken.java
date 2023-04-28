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

package com.hazelcast.client.impl;

import com.hazelcast.internal.util.ThreadLocalRandomProvider;

import java.security.SecureRandom;
import java.util.Arrays;

/**
 * Represents the secure-randomly generated tokens associated
 * to the clients so that the client can proof its identity
 * while authenticating with the TPC channels.
 */
public final class TpcToken {
    private static final int CONTENT_LENGTH = 64;
    private final byte[] content;

    public TpcToken() {
        SecureRandom secureRandom = ThreadLocalRandomProvider.getSecure();
        byte[] content = new byte[CONTENT_LENGTH];
        secureRandom.nextBytes(content);
        this.content = content;
    }

    /**
     * @return The content of the token.
     */
    public byte[] getContent() {
        return content;
    }

    /**
     * @param tokenContent Token to check.
     * @return Whether the given token matches with this or not.
     */
    public boolean matches(byte[] tokenContent) {
        return Arrays.equals(this.content, tokenContent);
    }
}
