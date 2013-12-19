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

import java.util.Locale;

/**
 * User: ahmetmircik
 * Date: 11/14/13
 */
public final class StringUtil {

    private static final Locale LOCALE_INTERNAL = Locale.ENGLISH;

    private StringUtil() {
    }

    /**
     *
     * HC specific settings, operands etc. use this method.
     *
     * */
    public static String upperCaseInternal(String str) {
        if (str == null || str.length() == 0) {
            return str;
        }
        return str.toUpperCase(LOCALE_INTERNAL);
    }

    /**
     *
     * HC specific settings, operands etc. use this method.
     *
     * */
    public static String lowerCaseInternal(String str) {
        if (str == null || str.length() == 0) {
            return str;
        }
        return str.toLowerCase(LOCALE_INTERNAL);
    }

}
