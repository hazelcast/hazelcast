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

package com.hazelcast.jet.pipeline.file;

import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;

/**
 * Internal util class to detect glob wildcards in a path
 */
final class WildcardMatcher {

    private static final char BACKSLASH = '\\';
    private static final Set<Character> WILDCARDS = new HashSet<>(asList(
            '*', '?', '{', '['
    ));

    /**
     * Utility class
     */
    private WildcardMatcher() {
    }

    public static boolean hasWildcard(String path) {
        for (int i = 0; i < path.length(); i++) {
            char ch = path.charAt(i);
            // check if the current char is wildcard AND if it isn't escaped by backslash
            if (WILDCARDS.contains(ch) && !(i > 0 && path.charAt(i - 1) == BACKSLASH)) {
                return true;
            }
        }
        return false;
    }
}
