/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.Address;

import java.security.SecureRandom;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Util class to generate random unique identifiers
 */
public final class UuidUtil {

    private UuidUtil() {
    }

    public static String createMemberUuid(Address endpoint) {
        return buildRandomUUID().toString();
    }

    public static String createClientUuid(Address endpoint) {
        return buildRandomUUID().toString();
    }

    public static String buildRandomUuidString() {
        return buildRandomUUID().toString();
    }

    public static UUID buildRandomUUID() {
        //CHECKSTYLE:OFF  suppressed because of magic numbers
        byte[] data = new byte[16];
        new SecureRandom().nextBytes(data);
        /* clear version        */
        data[6] &= 0x0f;
        /* set to version 4     */
        data[6] |= 0x40;
        /* clear variant        */
        data[8] &= 0x3f;
        /* set to IETF variant  */
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
        //CHECKSTYLE:ON
    }
}
