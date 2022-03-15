/*
 * Copyright (c) 2010-2012 JBoss by Red Hat
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.buildutils;

import java.util.ArrayList;
import java.util.List;

/**
 * Originally taken from JBOSS project (thanks guys, great work)
 * Original class: org.jboss.osgi.metadata.spi.ElementParser -> JBossOSGi Resolver Metadata
 *
 * @author Thomas.Diesler@jboss.com
 */
public final class ElementParser {

    private static final int CHAR = 1;
    private static final int DELIMITER = 2;
    private static final int START_QUOTE = 4;
    private static final int END_QUOTE = 8;

    private ElementParser() {
    }

    public static List<String> parseDelimitedString(String value, char delimiter) {
        return parseDelimitedString(value, delimiter, true);
    }

    /**
     * Parses delimited string and returns an array containing the tokens. This parser obeys quotes, so the delimiter character
     * will be ignored if it is inside of a quote. This method assumes that the quote character is not included in the set of
     * delimiter characters.
     *
     * @param value     the delimited string to parse.
     * @param delimiter the characters delimiting the tokens.
     * @param trim      whether to trim the parts.
     * @return an array of string tokens or null if there were no tokens.
     */
    public static List<String> parseDelimitedString(String value, char delimiter, boolean trim) {
        if (value == null) {
            value = "";
        }

        List<String> list = new ArrayList<String>();
        StringBuilder sb = new StringBuilder();

        int expecting = (CHAR | DELIMITER | START_QUOTE);
        for (int i = 0; i < value.length(); i++) {
            char character = value.charAt(i);

            boolean isEscaped = isEscaped(value, i);
            boolean isDelimiter = isDelimiter(delimiter, character, isEscaped);
            boolean isQuote = isQuote(character, isEscaped);

            if (isDelimiter && ((expecting & DELIMITER) > 0)) {
                addPart(list, sb, trim);
                sb.delete(0, sb.length());
                expecting = (CHAR | DELIMITER | START_QUOTE);
            } else if (isQuote && ((expecting & START_QUOTE) > 0)) {
                sb.append(character);
                expecting = CHAR | END_QUOTE;
            } else if (isQuote && ((expecting & END_QUOTE) > 0)) {
                sb.append(character);
                expecting = (CHAR | START_QUOTE | DELIMITER);
            } else if ((expecting & CHAR) > 0) {
                sb.append(character);
            } else {
                String message = String.format("Invalid delimited string [%s] for delimiter: %s", value, delimiter);
                throw new IllegalArgumentException(message);
            }
        }

        if (sb.length() > 0) {
            addPart(list, sb, trim);
        }

        return list;
    }

    private static boolean isEscaped(String value, int index) {
        return (index > 0 && value.charAt(index - 1) == '\\');
    }

    private static boolean isQuote(char character, boolean isEscaped) {
        return (!isEscaped && (character == '"' || character == '\''));
    }

    private static boolean isDelimiter(char delimiter, char character, boolean isEscaped) {
        return (!isEscaped && character == delimiter);
    }

    private static void addPart(List<String> list, StringBuilder sb, boolean trim) {
        list.add(trim ? sb.toString().trim() : sb.toString());
    }
}
