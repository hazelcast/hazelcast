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

package com.hazelcast.util;

import com.hazelcast.nio.Address;

import java.util.Random;
import java.util.UUID;

/**
 * @author mdogan 2/1/13
 */
public class UuidUtil {

    private static final ThreadLocal<Random> randomizers = new ThreadLocal<Random>() {
        @Override
        protected Random initialValue()
        {
            return new Random( -System.nanoTime() );
        }
    };

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
        byte[] data = new byte[16];
        randomizers.get().nextBytes(data);
        data[6]  &= 0x0f;  /* clear version        */
        data[6]  |= 0x40;  /* set to version 4     */
        data[8]  &= 0x3f;  /* clear variant        */
        data[8]  |= 0x80;  /* set to IETF variant  */

        long mostSigBits = 0;
        long leastSigBits = 0;
        assert data.length == 16 : "data must be 16 bytes in length";
        for (int i=0; i<8; i++)
            mostSigBits = (mostSigBits << 8) | (data[i] & 0xff);
        for (int i=8; i<16; i++)
            leastSigBits = (leastSigBits << 8) | (data[i] & 0xff);
        return new UUID(mostSigBits, leastSigBits);
    }

    private UuidUtil(){}

}
