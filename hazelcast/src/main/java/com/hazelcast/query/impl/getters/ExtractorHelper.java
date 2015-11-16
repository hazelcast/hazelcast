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

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.query.extractor.ValueExtractor;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class ExtractorHelper {

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
            throw new InvalidConfigurationException("Could not initialize extractor " + config, ex);
        } catch (InstantiationException ex) {
            throw new InvalidConfigurationException("Could not initialize extractor " + config, ex);
        } catch (ClassNotFoundException ex) {
            throw new InvalidConfigurationException("Could not initialize extractor " + config, ex);
        }
    }

    @Nullable
    static String extractArgumentsFromAttributeName(String attributeNameWithArguments) {
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

    static String extractAttributeNameNameWithoutArguments(String attributeNameWithArguments) {
        int start = attributeNameWithArguments.lastIndexOf("[");
        int end = attributeNameWithArguments.lastIndexOf("]");
        if (start > 0 && start < attributeNameWithArguments.length() && start < end) {
            return attributeNameWithArguments.substring(0, start);
        }
        if (start < 0 && end < 0) {
            return attributeNameWithArguments;
        }
        throw new IllegalArgumentException("Wrong argument input passed to extractor " + attributeNameWithArguments);
    }

}
