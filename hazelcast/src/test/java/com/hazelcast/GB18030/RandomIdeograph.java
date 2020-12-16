package com.hazelcast.GB18030;

import static org.apache.commons.lang3.RandomStringUtils.random;
import static org.apache.commons.lang3.RandomUtils.nextInt;

final class RandomIdeograph {

    /**
     * @return random CJK Unified Ideographs in unicode range of 4E00-9FFF for Common set
     * @see <a href=https://en.wikipedia.org/wiki/CJK_Unified_Ideographs>CJK Unified Ideographs</a></href>
     */
    static String generate() {
        final int count = nextInt(1, 42);
        return random(count, 0x4e00, 0x9fa5, true, true);
    }
}
