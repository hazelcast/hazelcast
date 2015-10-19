/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

/**
 * TODO: This has to be refactored and hardened.
 *
 */
public final class SuffixModifierUtils {
    private static final String ANY_TOKEN = "any";

    public static final int DO_NOT_REDUCE = -1;
    public static final int REDUCE_EVERYTHING = -2;

    private SuffixModifierUtils() {

    }


    public static String removeModifierSuffix(String name) {
        int indexOfOpeningBracket = name.indexOf('[');
        if (indexOfOpeningBracket == -1) {
            return name;
        }
        return name.substring(0, indexOfOpeningBracket);
    }


    public static String getModifierSuffix(String fullName, String baseName) {
        if (baseName == fullName) {
            return null;
        }
        int indexOfOpeningBracket = fullName.indexOf('[');
        return fullName.substring(indexOfOpeningBracket, fullName.length());
    }


    public static int parseModifier(String modifier) {
        String stringValue = modifier.substring(1, modifier.length() - 1);
        if (ANY_TOKEN.equals(stringValue)) {
            return REDUCE_EVERYTHING;
        } else {
            return Integer.parseInt(stringValue);
        }
    }
}
