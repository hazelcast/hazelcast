/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import com.hazelcast.internal.util.ThreadLocalRandomProvider;
import com.hazelcast.nio.Address;

import java.security.SecureRandom;
import java.util.Random;
import java.util.UUID;

/**
 * Util class to generate type 4 (pseudo randomly generated) {@link UUID}.
 * <p/>
 * Creates {@link UUID} of different quality of randomness,
 * which are based on cryptographically weak {@link Random}
 * and strong {@link SecureRandom} pseudo random number generators.
 */
@SuppressWarnings("unused")
public final class UuidUtil {

    private UuidUtil() {
    }

    /**
     * Creates a new cluster {@link UUID} string,
     * based on a cryptographically weak pseudo random number generator.
     *
     * @return a new cluster {@link UUID} string
     */
    public static String createClusterUuid() {
        return newUnsecureUuidString();
    }

    /**
     * Creates a new member {@link UUID} string,
     * based on a cryptographically weak pseudo random number generator.
     *
     * @return a new member {@link UUID} string
     */
    public static String createMemberUuid(Address endpoint) {
        return newUnsecureUuidString();
    }

    /**
     * Creates a new client {@link UUID} string,
     * based on a cryptographically weak pseudo random number generator.
     *
     * @return a new client {@link UUID} string
     */
    public static String createClientUuid(Address endpoint) {
        return newUnsecureUuidString();
    }

    /**
     * Static factory to retrieve a type 4 (pseudo randomly generated) UUID based string.
     *
     * The {@code UUID} string is generated using a cryptographically weak pseudo random number generator.
     *
     * @return A randomly generated {@code UUID} base string
     */
    public static String newUnsecureUuidString() {
        return newUnsecureUUID().toString();
    }

    /**
     * Static factory to retrieve a type 4 (pseudo randomly generated) UUID based string.
     *
     * The {@code UUID} string is generated using a cryptographically strong pseudo random number generator.
     *
     * @return A randomly generated {@code UUID} base string
     */
    public static String newSecureUuidString() {
        return newSecureUUID().toString();
    }

    /**
     * Static factory to retrieve a type 4 (pseudo randomly generated) UUID.
     *
     * The {@code UUID} is generated using a cryptographically weak pseudo random number generator.
     *
     * @return A randomly generated {@code UUID}
     */
    public static UUID newUnsecureUUID() {
        return getUUID(ThreadLocalRandomProvider.get());
    }

    /**
     * Static factory to retrieve a type 4 (pseudo randomly generated) UUID.
     *
     * The {@code UUID} is generated using a cryptographically strong pseudo random number generator.
     *
     * @return A randomly generated {@code UUID}
     */
    public static UUID newSecureUUID() {
        return getUUID(ThreadLocalRandomProvider.getSecure());
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private static UUID getUUID(Random random) {
        byte[] data = new byte[16];
        random.nextBytes(data);

        // clear version
        data[6] &= 0x0f;
        // set to version 4
        data[6] |= 0x40;
        // clear variant
        data[8] &= 0x3f;
        // set to IETF variant
        data[8] |= 0x80;

        long mostSigBits = 0;
        long leastSigBits = 0;
        assert data.length == 16 : "data must be 16 bytes in length";
        for (int i = 0; i < 8; i++) {
            mostSigBits = (mostSigBits << 8) | (data[i] & 0xff);
        }
        for (int i = 8; i < 16; i++) {
            leastSigBits = (leastSigBits << 8) | (data[i] & 0xff);
        }
        return new UUID(mostSigBits, leastSigBits);
    }
}
