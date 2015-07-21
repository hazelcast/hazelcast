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

import java.nio.charset.Charset;
import java.util.Locale;

/**
 * Utility class for Strings.
 */
public final class StringUtil {

    private static final Locale LOCALE_INTERNAL = Locale.ENGLISH;
    private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");

    private StringUtil() {
    }

    /**
     * Creates a UTF8_CHARSET string from a byte array.
     *
     * @param bytes the byte array.
     * @param offset the index of the first byte to decode
     * @param length the number of bytes to decode
     * @return the string created from the byte array.
     */
    public static String bytesToString(byte[] bytes, int offset, int length) {
        return new String(bytes, offset, length, UTF8_CHARSET);
    }

    /**
     * Creates a UTF8_CHARSET string from a byte array.
     *
     * @param bytes the byte array.
     * @return the string created from the byte array.
     */
    public static String bytesToString(byte[] bytes) {

        return new String(bytes, UTF8_CHARSET);
    }

    /**
     * Creates a byte array from a string.
     *
     * @param s the string.
     * @return the byte array created from the string.
     */
    public static byte[] stringToBytes(String s) {
        return s.getBytes(UTF8_CHARSET);
    }

    /**
     * Checks if a string is empty or not.
     *
     * @param s the string to check.
     * @return true if the string is null or empty, false otherwise
     */

    public static boolean isNullOrEmpty(String s) {
        if (s == null) {
            return true;
        }
        return s.isEmpty();
    }

    /**
     * HC specific settings, operands etc. use this method.
     * Creates an uppercase string from the given string.
     *
     * @param s the given string
     * @return an uppercase string, or null/empty if the string is null/empty
     */
    public static String upperCaseInternal(String s) {
        if (isNullOrEmpty(s)) {
            return s;
        }
        return s.toUpperCase(LOCALE_INTERNAL);
    }

    /**
     * HC specific settings, operands etc. use this method.
     * Creates a lowercase string from the given string.
     *
     * @param s the given string
     * @return a lowercase string, or null/empty if the string is null/empty
     */
    public static String lowerCaseInternal(String s) {
        if (isNullOrEmpty(s)) {
            return s;
        }
        return s.toLowerCase(LOCALE_INTERNAL);
    }

    /**
     * Returns system property "line.seperator"
     * @return line seperator for the specific OS
     */
    public static String getLineSeperator() {
        return System.getProperty("line.separator");
    }
}
