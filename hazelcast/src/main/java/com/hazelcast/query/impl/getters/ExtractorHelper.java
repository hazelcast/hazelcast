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

import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.query.extractor.ValueExtractor;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

final class ExtractorHelper {

    private static final String NO_SQUARE_BRACKETS_EXP = "[^\\Q[]\\E]";
    private static final String SQUARE_BRACKETS_EXP = "\\[([^\\Q[]\\E])*\\]";

    private static final Pattern EXTRACTOR_ARGS_PATTERN = Pattern.compile(
            String.format("^(%s)+%s$", NO_SQUARE_BRACKETS_EXP, SQUARE_BRACKETS_EXP));

    private static final Pattern COLLECTION_ARGS_PATTERN = Pattern.compile(
            String.format("^((%s)+(%s){0,1})+$", NO_SQUARE_BRACKETS_EXP, SQUARE_BRACKETS_EXP));

    private ExtractorHelper() {
    }

    static Map<String, ValueExtractor> instantiateExtractors(List<MapAttributeConfig> mapAttributeConfigs) {
        Map<String, ValueExtractor> extractors = new HashMap<String, ValueExtractor>();
        for (MapAttributeConfig config : mapAttributeConfigs) {
            if (extractors.containsKey(config.getName())) {
                throw new IllegalArgumentException("Could not add " + config
                        + ". Extractor for this attribute name already added.");
            }
            extractors.put(config.getName(), instantiateExtractor(config));
        }
        return extractors;
    }

    static ValueExtractor instantiateExtractor(MapAttributeConfig config) {
        try {
            Class<?> clazz = Class.forName(config.getExtractor());
            Object extractor = clazz.newInstance();
            if (extractor instanceof ValueExtractor) {
                return (ValueExtractor) extractor;
            } else {
                throw new IllegalArgumentException("Extractor does not extend ValueExtractor class " + config);
            }
        } catch (IllegalAccessException ex) {
            throw new IllegalArgumentException("Could not initialize extractor " + config, ex);
        } catch (InstantiationException ex) {
            throw new IllegalArgumentException("Could not initialize extractor " + config, ex);
        } catch (ClassNotFoundException ex) {
            throw new IllegalArgumentException("Could not initialize extractor " + config, ex);
        }
    }

    @Nullable
    static String extractArgumentsFromAttributeName(String attributeNameWithArguments) {
        int start = attributeNameWithArguments.lastIndexOf('[');
        int end = attributeNameWithArguments.lastIndexOf(']');
        if (EXTRACTOR_ARGS_PATTERN.matcher(attributeNameWithArguments).matches()) {
            return attributeNameWithArguments.substring(start + 1, end);
        } else if (start < 0 && end < 0) {
            return null;
        }
        throw new IllegalArgumentException("Wrong argument input passed to extractor " + attributeNameWithArguments);
    }

    static String extractAttributeNameNameWithoutArguments(String attributeNameWithArguments) {
        if (EXTRACTOR_ARGS_PATTERN.matcher(attributeNameWithArguments).matches()) {
            int start = attributeNameWithArguments.lastIndexOf('[');
            return attributeNameWithArguments.substring(0, start);
        } else if (COLLECTION_ARGS_PATTERN.matcher(attributeNameWithArguments).matches()) {
            return attributeNameWithArguments;
        } else {
            throw new IllegalArgumentException("Wrong argument input passed to extractor " + attributeNameWithArguments);
        }
    }

}
