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

import java.nio.charset.Charset;
import java.util.Locale;

/**
 * Utility class for Strings.
 */
public final class StringUtil {

    private static final Locale LOCALE_INTERNAL = Locale.ENGLISH;

    private StringUtil() {
    }

    public static String bytesToString(byte[] bytes, int offset, int length) {
        return new String(bytes, offset, length, Charset.forName("UTF-8"));
    }

    public static String bytesToString(byte[] bytes) {
        return new String(bytes, Charset.forName("UTF-8"));
    }

    public static byte[] stringToBytes(String s) {
        return s.getBytes(Charset.forName("UTF-8"));
    }

    public static boolean isNullOrEmpty(String s) {
        if (s == null) {
            return true;
        }
        return s.isEmpty();
    }

    /**
     * HC specific settings, operands etc. use this method.
     */
    public static String upperCaseInternal(String s) {
        if (isNullOrEmpty(s)) {
            return s;
        }
        return s.toUpperCase(LOCALE_INTERNAL);
    }

    /**
     * HC specific settings, operands etc. use this method.
     */
    public static String lowerCaseInternal(String s) {
        if (isNullOrEmpty(s)) {
            return s;
        }
        return s.toLowerCase(LOCALE_INTERNAL);
    }

}
