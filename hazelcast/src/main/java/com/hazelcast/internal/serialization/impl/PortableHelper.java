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

import javax.annotation.Nullable;
import java.util.regex.Pattern;

public final class PortableHelper {

    private static final String NO_SQUARE_BRACKETS_EXP = "[^\\Q[]\\E]";
    private static final String SQUARE_BRACKETS_EXP = "\\[([^\\Q[]\\E])*\\]";
    private static final Pattern COLLECTION_ARGS_PATTERN = Pattern.compile(
            String.format("^((%s)+(%s))+$", NO_SQUARE_BRACKETS_EXP, SQUARE_BRACKETS_EXP));

    private PortableHelper() {
    }

    @Nullable
    static String extractArgumentsFromAttributeName(String attributeNameWithArguments) {
        int start = attributeNameWithArguments.lastIndexOf('[');
        int end = attributeNameWithArguments.lastIndexOf(']');
        if (COLLECTION_ARGS_PATTERN.matcher(attributeNameWithArguments).matches()) {
            return attributeNameWithArguments.substring(start + 1, end);
        } else if (start < 0 && end < 0) {
            return null;
        }
        throw new IllegalArgumentException("Wrong argument input passed to extractor " + attributeNameWithArguments);
    }

    static String extractAttributeNameNameWithoutArguments(String attributeNameWithArguments) {
        if (COLLECTION_ARGS_PATTERN.matcher(attributeNameWithArguments).matches()) {
            int start = attributeNameWithArguments.lastIndexOf('[');
            return attributeNameWithArguments.substring(0, start);
        } else {
            return attributeNameWithArguments;
        }
    }


}
