/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 * Copyright (C) 2010 - 2012 JBoss by Red Hat
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

    private ElementParser() {
    }

    public static List<String> parseDelimitedString(String value, char delim) {
        return parseDelimitedString(value, delim, true);
    }

    /**
     * Parses delimited string and returns an array containing the tokens. This parser obeys quotes, so the delimiter character
     * will be ignored if it is inside of a quote. This method assumes that the quote character is not included in the set of
     * delimiter characters.
     *
     * @param value the delimited string to parse.
     * @param delim the characters delimiting the tokens.
     * @param trim  whether to trim the parts.
     * @return an array of string tokens or null if there were no tokens.
     */
    public static List<String> parseDelimitedString(String value, char delim, boolean trim) {
        if (value == null) {
            value = "";
        }

        List<String> list = new ArrayList<String>();

        int CHAR = 1;
        int DELIMITER = 2;
        int STARTQUOTE = 4;
        int ENDQUOTE = 8;

        StringBuilder sb = new StringBuilder();

        int expecting = (CHAR | DELIMITER | STARTQUOTE);

        for (int i = 0; i < value.length(); i++) {
            char p = i > 0 ? value.charAt(i - 1) : 0;
            char c = value.charAt(i);

            boolean isDelimiter = (delim == c) && (p != '\\');
            boolean isQuote = ((c == '"') || (c == '\'')) && (p != '\\');

            if (isDelimiter && ((expecting & DELIMITER) > 0)) {
                addPart(list, sb, trim);
                sb.delete(0, sb.length());
                expecting = (CHAR | DELIMITER | STARTQUOTE);
            } else if (isQuote && ((expecting & STARTQUOTE) > 0)) {
                sb.append(c);
                expecting = CHAR | ENDQUOTE;
            } else if (isQuote && ((expecting & ENDQUOTE) > 0)) {
                sb.append(c);
                expecting = (CHAR | STARTQUOTE | DELIMITER);
            } else if ((expecting & CHAR) > 0) {
                sb.append(c);
            } else {
                String message = String.format("Invalid delimited string [%s] for delimiter: %s", value, delim);
                throw new IllegalArgumentException(message);
            }
        }

        if (sb.length() > 0) {
            addPart(list, sb, trim);
        }

        return list;
    }

    private static void addPart(List<String> list, StringBuilder sb, boolean trim) {
        list.add(trim ? sb.toString().trim() : sb.toString());
    }
}
