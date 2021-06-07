/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.ascii;

import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Locale;

/**
 * Utility for parsing Content-Type HTTP header
 *
 * TODO: This started as very simplistic thing and evolved into a relatively complex and inefficient thing.
 * Moreover very very far from being bullet proof. Consider using a proper parser, for example we could use
 * one from the Apache HTTP client project.
 *
 */
public final class HttpContentTypeParser {
    private static final String CHARSET_PARAMETER_KEY_NAME = "charset=";
    private static final int    CHARSET_PARAMETER_KEY_NAME_LENGTH = CHARSET_PARAMETER_KEY_NAME.length();
    private static final char   PARAMETER_DELIMITER = ';';
    private static final char   SUBTYPE_DELIMITER = '/';
    // use US locale to prevent oddities in some other locales
    private static final Locale LOCALE = Locale.US;

    private HttpContentTypeParser() {

    }

    /**
     * Parse charset from HTTP content-type header.
     * When charset is not present or is unknown the method returns null.
     * It also returns null when charset parameter is malformed.
     *
     * @param contentTypeValue content type value
     * @return charset if found and is a valid charset otherwise null
     */
    public static Charset parseCharset(String contentTypeValue) {
        if (contentTypeValue == null) {
            return null;
        }
        // parameter name tokens are case insensitive
        contentTypeValue = contentTypeValue.toLowerCase(LOCALE);

        // find the charset parameter token and remove it
        int i = contentTypeValue.indexOf(CHARSET_PARAMETER_KEY_NAME);
        if (i == -1 || i == 0) {
            // content type cannot start with charset. there must be media type first
            return null;
        }
        char charBeforeParamKey = contentTypeValue.charAt(i - 1);
        if (charBeforeParamKey != PARAMETER_DELIMITER && charBeforeParamKey != ' ') {
            return null;
        }
        String charset = contentTypeValue.substring(i + CHARSET_PARAMETER_KEY_NAME_LENGTH);

        // remove possible trailing space or semicolons
        i = charset.indexOf(' ');
        if (i != -1) {
            charset = charset.substring(0, i);
        }
        i = charset.indexOf(PARAMETER_DELIMITER);
        if (i != -1) {
            charset = charset.substring(0, i);
        }

        // parameters are allowed to be in double-quotes. let's remove them.
        i = charset.indexOf('"');
        if (i != -1) {
            if (i != 0) {
                return null;
            }
            if (charset.length() == 1) {
                return null;
            }
            int i2 = charset.indexOf('"', 1);
            if (i2 != charset.length() - 1) {
                return null;
            }
            charset = charset.substring(i + 1, i2);
        }

        if (charset.isEmpty()) {
            return null;
        }
        try {
            return Charset.forName(charset);
        } catch (IllegalCharsetNameException | UnsupportedCharsetException e) {
            return null;
        }
    }

    /**
     * TODO
     *
     * @param contentTypeValue
     * @return
     */
    public static String parseMediaType(String contentTypeValue) {
        if (contentTypeValue == null) {
            return null;
        }
        contentTypeValue = contentTypeValue.trim().toLowerCase(LOCALE);

        int i = contentTypeValue.indexOf(SUBTYPE_DELIMITER);
        if (i == -1) {
            return null;
        }

        // remove trailing semicolon
        i = contentTypeValue.indexOf(PARAMETER_DELIMITER);
        if (i != -1) {
            contentTypeValue = contentTypeValue.substring(0, i);
        }

        // remove trailing space
        i = contentTypeValue.indexOf(' ');
        if (i != -1) {
            contentTypeValue = contentTypeValue.substring(0, i);
        }

        i = contentTypeValue.indexOf(SUBTYPE_DELIMITER);
        if (i == -1 || i == 0 || i == contentTypeValue.length() - 1) {
            return null;
        }

        return contentTypeValue;
    }
}
