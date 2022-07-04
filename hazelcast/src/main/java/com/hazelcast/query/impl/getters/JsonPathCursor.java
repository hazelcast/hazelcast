/*
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

package com.hazelcast.query.impl.getters;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a query path for Json querying. Parsing of a query path
 * is lazy. This cursor splits parts of an attribute path by "."s and
 * "["s.
 */
public class JsonPathCursor {

    private static final int DEFAULT_PATH_ELEMENT_COUNT = 5;

    private List<Triple> triples;

    private String attributePath;
    private String current;
    private byte[] currentAsUtf8;
    private int currentArrayIndex = -1;
    private boolean isArray;
    private boolean isAny;
    private int cursor;

    private JsonPathCursor(String originalPath, List<Triple> triples) {
        this.attributePath = originalPath;
        this.triples = triples;
    }

    /**
     * Creates a shallow copy of this object
     *
     * @param other
     */
    JsonPathCursor(JsonPathCursor other) {
        this.attributePath = other.attributePath;
        this.triples = other.triples;
    }

    /**
     * Creates a new cursor from given attribute path.
     *
     * @param attributePath
     */
    public static JsonPathCursor createCursor(String attributePath) {
        ArrayList<Triple> triples = new ArrayList<Triple>(DEFAULT_PATH_ELEMENT_COUNT);
        int start = 0;
        int end;
        while (start < attributePath.length()) {
            boolean isArray = false;
            try {
                while (attributePath.charAt(start) == '[' || attributePath.charAt(start) == '.') {
                    start++;
                }
            } catch (IndexOutOfBoundsException e) {
                throw createIllegalArgumentException(attributePath);
            }
            end = start + 1;
            while (end < attributePath.length()) {
                char c = attributePath.charAt(end);
                if ('.' == c || '[' == c) {
                    break;
                } else if (']' == c) {
                    isArray = true;
                    break;
                }
                end++;
            }
            String part = attributePath.substring(start, end);

            Triple triple = new Triple(part, part.getBytes(StandardCharsets.UTF_8), isArray);
            triples.add(triple);
            start = end + 1;
        }
        return new JsonPathCursor(attributePath, triples);
    }

    private static IllegalArgumentException createIllegalArgumentException(String attributePath) {
        return new IllegalArgumentException("Malformed query path " + attributePath);
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
     *
     * @return
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "Making a copy reverses the benefit of this method")
    public byte[] getCurrentAsUTF8() {
        return currentAsUtf8;
    }

    /**
     * @return true if the current item is an array
     */
    public boolean isArray() {
        return isArray;
    }

    /**
     * @return true if the current item is "any"
     */
    public boolean isAny() {
        return isAny;
    }

    /**
     * Returns array index if the current item is array and not "any".
     *
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

    public boolean hasNext() {
        return cursor < triples.size();
    }

    private void next() {
        if (cursor < triples.size()) {
            Triple triple = triples.get(cursor);
            current = triple.string;
            currentAsUtf8 = triple.stringAsUtf8;
            isArray = triple.isArray;
            currentArrayIndex = -1;
            isAny = false;
            if (isArray) {
                if ("any".equals(current)) {
                    isAny = true;
                } else {
                    isAny = false;
                    currentArrayIndex = Integer.parseInt(current);
                }
            }
            cursor++;
        } else {
            current = null;
            currentAsUtf8 = null;
            currentArrayIndex = -1;
            isAny = false;
            isArray = false;
        }
    }

    private static final class Triple {
        private final String string;
        private final byte[] stringAsUtf8;
        private final boolean isArray;

        private Triple(String string, byte[] stringAsUtf8, boolean isArray) {
            this.string = string;
            this.stringAsUtf8 = stringAsUtf8;
            this.isArray = isArray;
        }
    }
}
