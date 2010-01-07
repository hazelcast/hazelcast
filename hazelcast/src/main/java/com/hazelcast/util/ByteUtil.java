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

/**
 * This class allows to set, clear and check bits of a byte value.
 */
public final class ByteUtil {

	private final static byte[] POWERS = new byte[] {1, 2, 4, 8, 16, 32, 64, -128};

	/**
	 * All members are static and there should never be an instance of this class.
	 */
	private ByteUtil() {
	}

	/**
	 * Sets a bit to 1.
	 * 
	 * @param number the original byte value
	 * @param index the bit to set
	 * @return the modified byte value
	 */
    public static byte setTrue(final byte number, final int index) {
        return (byte) (number | POWERS[index]);
    }

    /**
     * Clears a bit, by setting it to 0.
     * 
     * @param number the original byte value
     * @param index the bit to set
     * @return the modified byte value
     */
    public static byte setFalse(final byte number, final int index) {
        return (byte) (number & ~(POWERS[index]));
    }

    /**
     * Checks if the index-th bit of number is set.
     * 
     * @param number the byte value
     * @param index the bit to check
     * @return true if the bit is set, false otherwise
     */
    public static boolean isTrue(final byte number, final int index) {
        return (number & (POWERS[index])) != 0;
    }

    /**
     * Checks if the index-th bit of number is NOT set. 
     * 
     * @param number the byte value
     * @param index the bit to check
     * @return true if the bit is NOT set, false otherwise
     */
    public static boolean isFalse(final byte number, final int index) {
        return (number & (POWERS[index])) == 0;
    }

}
