/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl.portable;

import com.hazelcast.internal.util.StringUtil;

import static com.hazelcast.internal.util.Preconditions.checkHasText;

/**
 * Mutable cursor that allows iterating over path tokens split by a dot (.)
 * It may be reused, just init it with the init() method and then reset() to tear it down after the usage.
 */
final class PortablePathCursor {

    private String path;
    private int index;

    private int offset;
    private int nextSplit;
    private String token;

    PortablePathCursor() {
    }

    /**
     * Inits the cursor with the given path and sets the current position to the first token.
     *
     * @param path path to initialise the cursor with
     */
    void init(String path) {
        this.path = checkHasText(path, "path cannot be null or empty");
        this.index = 0;
        this.offset = 0;
        this.nextSplit = StringUtil.indexOf(path, '.', 0);
        this.token = null;
        if (nextSplit == 0) {
            throw new IllegalArgumentException("The path cannot begin with a dot: " + path);
        }
    }

    /**
     * Inits the cursor with the given assuming it's a path with single token (and without validating it).
     * It's an optimisation for the common execution path where there's only a single field beeing read.
     *
     * @param path path to initialise the cursor with
     */
    void initWithSingleTokenPath(String path) {
        this.path = path;
        this.index = 0;
        this.offset = 0;
        this.nextSplit = -1;
        this.token = path;
    }

    /**
     * Resets the cursor to a null state.
     */
    void reset() {
        this.path = null;
        this.index = -1;
        this.offset = 0;
        this.token = null;
    }

    boolean isLastToken() {
        return nextSplit == -1;
    }

    String token() {
        if (token != null) {
            return token;
        }
        int endIndex = (nextSplit < 0 ? path.length() : nextSplit);
        if (endIndex <= offset) {
            throw new IllegalArgumentException("The token's length cannot be zero: " + path);
        }
        token = checkHasText(path.substring(offset, endIndex), "Token cannot be null or empty");
        return token;
    }

    String path() {
        return path;
    }

    boolean advanceToNextToken() {
        if (nextSplit == -1) {
            return false;
        }
        token = null;
        int oldNextSplit = nextSplit;
        nextSplit = StringUtil.indexOf(path, '.', oldNextSplit + 1);
        offset = oldNextSplit + 1;
        index++;
        return true;
    }

    /**
     * Sets the index to the given index without validating. If the index is out of bound the consecutive token() call
     * will throw a runtime exception.
     *
     * @param indexToNavigateTo value to set the cursor's index to.
     */
    void index(int indexToNavigateTo) {
        this.index = 0;
        this.offset = 0;
        this.nextSplit = StringUtil.indexOf(path, '.', 0);
        this.token = null;

        for (int i = 1; i <= indexToNavigateTo; i++) {
            if (!advanceToNextToken()) {
                throw new IndexOutOfBoundsException("Index out of bound " + indexToNavigateTo + " in " + path);
            }
        }
    }

    int index() {
        return index;
    }

    boolean isAnyPath() {
        return path.contains("[any]");
    }
}
