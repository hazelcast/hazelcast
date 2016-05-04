/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl;

import java.util.regex.Pattern;

/**
 * Mutable cursor that allows iterating over path tokens split by a dot (.)
 * It may be reused, just init it with the init() method and then reset() to tear it down after the usage.
 */
final class PortablePathCursor {

    private static final Pattern NESTED_PATH_SPLITTER = Pattern.compile("\\.");

    private String[] pathTokens;
    private String path;
    private int index;

    PortablePathCursor() {
    }

    /**
     * Inits the cursor with the given path and sets the current position to the first token.
     *
     * @param path path to initialise the cursor with
     */
    void init(String path) {
        this.path = path;
        this.pathTokens = NESTED_PATH_SPLITTER.split(path);
        this.index = 0;
    }

    /**
     * Resets the cursor to a null state.
     */
    void reset() {
        this.path = null;
        this.pathTokens = null;
        this.index = -1;
    }

    boolean isLastToken() {
        return index == pathTokens.length - 1;
    }

    String token() {
        return pathTokens[index];
    }

    int length() {
        return pathTokens.length;
    }

    String path() {
        return path;
    }

    boolean advanceToNextToken() {
        return ++index <= pathTokens.length - 1;
    }

    /**
     * Sets the index to the given index without validating. If the index is out of bound the consecutive token() call
     * will throw a runtime exception.
     *
     * @param index value to set the cursor's index to.
     */
    void index(int index) {
        this.index = index;
    }

    int index() {
        return index;
    }

    boolean isAnyPath() {
        return path.contains("[any]");
    }
}
