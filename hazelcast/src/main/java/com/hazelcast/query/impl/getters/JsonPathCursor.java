/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl.getters;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.charset.Charset;

/**
 * Represents a query path for Json querying. Parsing of a query path
 * is lazy. This cursor splits parts of an attribute path by "."s and
 * "["s.
 */
public class JsonPathCursor {

    private static final Charset UTF8_CHARSET = Charset.forName("UTF8");

    private String attributePath;
    private String current;
    private byte[] currentAsUtf8;
    private int currentArrayIndex = -1;
    private boolean isArray;
    private boolean isAny;
    private int cursor;

    /**
     * Creates a new cursor from given attribute path.
     * @param attributePath
     */
    public JsonPathCursor(String attributePath) {
        this.attributePath = attributePath;
        cursor = 0;
    }


    public String getAttributePath() {
        return attributePath;
    }

    /**
     * Returns the next attribute path part. The returned string is either
     * the name of the next attribute or the string representation of an
     * array index. {@code null} indicates the end of input.
     *
     * @return either {@code null} or the next part of the attribute path
     */
    public String getNext() {
        next();
        return current;
    }

    /**
     * Returns the current attribute path part. The returned string is either
     * the name of the current attribute or the string representation of an
     * array index. {@code null} indicates the end of input.
     *
     * @return either {@code null} or the current part of the attribute path
     */
    public String getCurrent() {
        return current;
    }

    /**
     * Returns byte array of UTF8 encoded {@link #getCurrent()}. This
     * method caches the UTF8 encoded byte array, so it is more
     * efficient to call repeteadly.
     *
     * The returned byte array must not be modified!
     * @return
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "Making a copy reverses the benefit of this method")
    public byte[] getCurrentAsUTF8() {
        if (currentAsUtf8 == null) {
            currentAsUtf8 = current.getBytes(UTF8_CHARSET);
        }
        return currentAsUtf8;
    }

    /**
     *
     * @return true if the current item is an array
     */
    public boolean isArray() {
        return isArray;
    }

    /**
     *
     * @return true if the current item is "any"
     */
    public boolean isAny() {
        return isAny;
    }

    /**
     * Returns array index if the current item is array and not "any".
     * @return -1 if the current item is "any" or non-array.
     */
    public int getArrayIndex() {
        return currentArrayIndex;
    }

    public void reset() {
        current = null;
        currentArrayIndex = -1;
        isArray = false;
        isAny = false;
        cursor = 0;
    }

    private void next() {
        currentAsUtf8 = null;
        currentArrayIndex = -1;
        isAny = false;
        isArray = false;
        int nextCursor = iterate();
        if (nextCursor == -1) {
            return;
        }
        current = attributePath.substring(cursor, nextCursor);
        cursor = nextCursor + 1;
        if (isArray) {
            if ("any".equals(current)) {
                isAny = true;
            } else {
                try {
                    currentArrayIndex = Integer.parseInt(current);
                } catch (NumberFormatException e) {
                    throw createIllegalArgumentException();
                }
            }
        }
    }

    private int iterate() {
        if (cursor < attributePath.length()) {
            try {
                while (attributePath.charAt(cursor) == '[' || attributePath.charAt(cursor) == '.') {
                    cursor++;
                }
            } catch (IndexOutOfBoundsException e) {
                throw createIllegalArgumentException();
            }
        } else {
            current = null;
            return -1;
        }
        for (int i = cursor; i < attributePath.length(); i++) {
            switch (attributePath.charAt(i)) {
                case '.':
                case '[':
                    return i;
                case ']':
                    isArray = true;
                    return i;
                default:
                    // no-op
            }
        }
        return attributePath.length();
    }

    private IllegalArgumentException createIllegalArgumentException() {
        return new IllegalArgumentException("Malformed query path " + attributePath);
    }
}
