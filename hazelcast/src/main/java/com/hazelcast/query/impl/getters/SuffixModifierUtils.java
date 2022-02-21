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

/**
 * Convenient utilities to parse attribute suffix modifiers from attributes.
 *
 * Example:
 * Attribute <code>foo[*]</code> consists of baseName <code>foo</code>
 * and a modifier suffix <code>[*]</code>.
 *
 * Getters can use modifier suffixes to adjust getter behaviour.
 *
 */
public final class SuffixModifierUtils {
    private static final char MODIFIER_OPENING_TOKEN = '[';
    private static final char MODIFIER_CLOSING_TOKEN = ']';

    private SuffixModifierUtils() {

    }


    /**
     * Remove modifier suffix from given fullName.
     *
     * @param fullName
     * @return
     */
    public static String removeModifierSuffix(String fullName) {
        int indexOfFirstOpeningToken = fullName.indexOf(MODIFIER_OPENING_TOKEN);
        if (indexOfFirstOpeningToken == -1) {
            return fullName;
        }
        int indexOfSecondOpeningToken = fullName.lastIndexOf(MODIFIER_OPENING_TOKEN);
        if (indexOfSecondOpeningToken != indexOfFirstOpeningToken) {
            throw new IllegalArgumentException("Attribute name '" + fullName
                    + "' is not valid as it contains more than one " + MODIFIER_OPENING_TOKEN);
        }
        int indexOfFirstClosingToken = fullName.indexOf(MODIFIER_CLOSING_TOKEN);
        if (indexOfFirstClosingToken != fullName.length() - 1) {
            throw new IllegalArgumentException("Attribute name '" + fullName
                    + "' is not valid as the last character is not " + MODIFIER_CLOSING_TOKEN);
        }

        return fullName.substring(0, indexOfFirstOpeningToken);
    }


    /**
     * Get modifier suffix if fullName contains any otherwise returns null.
     *
     * In contains no validation of input parameters as it assumes the validation
     * has been already done by {@link #removeModifierSuffix(String)}
     *
     * @param fullName
     * @param baseName as returned by {@link #removeModifierSuffix(String)}
     * @return modifier suffix or null if no suffix is present
     */
    public static String getModifierSuffix(String fullName, String baseName) {
        if (fullName.equals(baseName)) {
            return null;
        }
        int indexOfOpeningBracket = fullName.indexOf(MODIFIER_OPENING_TOKEN);
        return fullName.substring(indexOfOpeningBracket, fullName.length());
    }

}
