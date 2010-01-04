/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.util;

public class ByteUtil {
    private final static byte[] POWERS = new byte[8];

    static {
        POWERS[0] = 1;
        POWERS[1] = 2;
        POWERS[2] = 4;
        POWERS[3] = 8;
        POWERS[4] = 16;
        POWERS[5] = 32;
        POWERS[6] = 64;
        POWERS[7] = -128;
    }

    public static byte setTrue(byte number, int index) {
        return (byte) (number | POWERS[index]);
    }

    public static byte setFalse(byte number, int index) {
        return (byte) (number & ~(POWERS[index]));
    }

    public static boolean isTrue(byte number, int index) {
        return (byte) (number & (POWERS[index])) != 0;
    }

    public static boolean isFalse(byte number, int index) {
        return (byte) (number & (POWERS[index])) == 0;
    }

    public static void main(String[] args) {
        byte x = 0;
        x = setTrue(x, 1);
        x = setTrue(x, 3);
        x = setTrue(x, 6);
        for (byte i = 0; i < 8; i++) {
            boolean get = isTrue(x, i);
            System.out.println(i + " " + get);
        }
        for (byte i = 0; i < 8; i++) {
            boolean get = isTrue(x, i);
            System.out.println(i + " " + get);
        }
    }
}
