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

package com.hazelcast.query.impl;

import com.hazelcast.query.extractor.Arguments;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ExtractorsContext {

    private static final int MAX_SIZE = 10000;

    // We are using the default argument parser if not overridden by the map config
    private final DefaultArgumentsParser defaultArgumentsParser;

    // extractorAttributeName WITH the arguments -> extractorAttributeName WITHOUT the arguments
    // TODO: I don't think this is worth it. put/get is probably more expensive than calculating it again.
    private final Map<String, String> withToWithoutArgumentsAttributeNames;

    // extractorAttributeName (with the arguments) -> Arguments
    private final Map<String, Arguments> arguments;

    public ExtractorsContext() {
        withToWithoutArgumentsAttributeNames = new ConcurrentHashMap<String, String>();
        arguments = new ConcurrentHashMap<String, Arguments>();
        defaultArgumentsParser = new DefaultArgumentsParser();
    }

    public String getAttributeNameWithoutArguments(String attributeNameWithArguments) {
        String result = withToWithoutArgumentsAttributeNames.get(attributeNameWithArguments);
        // this is not fully thread safe - may lead to extra allocations - but it's consistent and fast
        if (result == null) {
            result = extractArgumentNameWithoutLastSquareBracketArguments(attributeNameWithArguments);
            withToWithoutArgumentsAttributeNames.put(attributeNameWithArguments, result);
            // evict on put only - so only once a new query is executed
            evictAttributeNames();
        }
        return result;
    }

    public Arguments getArguments(String attributeNameWithArguments) {
        Arguments result = arguments.get(attributeNameWithArguments);
        // this is not fully thread safe - may lead to extra allocations - but it's consistent and fast
        if (result == null) {
            result = defaultArgumentsParser.parse(extractArgumentsFromLastSquareBracket(attributeNameWithArguments));
            arguments.put(attributeNameWithArguments, result);
            // evict on put only - so only once a new query is executed
            evictArguments();
        }
        return result;
    }

    @Nullable
    public static String extractArgumentsFromLastSquareBracket(String attributeNameWithArguments) {
        int start = attributeNameWithArguments.lastIndexOf("[");
        int end = attributeNameWithArguments.lastIndexOf("]");
        if (start > 0 && start < attributeNameWithArguments.length() && start < end) {
            return attributeNameWithArguments.substring(start + 1, end);
        }
        if (start < 0 && end < 0) {
            return null;
        }
        throw new IllegalArgumentException("Wrong argument input passed to extractor " + attributeNameWithArguments);
    }

    public static String extractArgumentNameWithoutLastSquareBracketArguments(String attributeNameWithArguments) {
        int lastOpeningSquareBracket = attributeNameWithArguments.lastIndexOf("[");
        if (lastOpeningSquareBracket > 0) {
            return attributeNameWithArguments.substring(0, lastOpeningSquareBracket);
        }
        return attributeNameWithArguments;
    }

    public void evictAttributeNames() {
        if (withToWithoutArgumentsAttributeNames.size() > MAX_SIZE) {
            withToWithoutArgumentsAttributeNames.clear();
        }
    }

    public void evictArguments() {
        if (arguments.size() > MAX_SIZE) {
            arguments.clear();
        }
    }

}
