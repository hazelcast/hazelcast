/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.AttributeConfig;
import com.hazelcast.logging.Logger;
import com.hazelcast.query.extractor.ValueExtractor;
import com.hazelcast.internal.util.StringUtil;

import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.util.MapUtil.createHashMap;

public final class ExtractorHelper {

    private ExtractorHelper() {
    }

    static Map<String, ValueExtractor> instantiateExtractors(List<AttributeConfig> attributeConfigs,
                                                             ClassLoader classLoader) {
        Map<String, ValueExtractor> extractors = createHashMap(attributeConfigs.size());
        for (AttributeConfig config : attributeConfigs) {
            if (extractors.containsKey(config.getName())) {
                throw new IllegalArgumentException("Could not add " + config
                        + ". Extractor for this attribute name already added.");
            }
            extractors.put(config.getName(), instantiateExtractor(config, classLoader));
        }
        return extractors;
    }

    static ValueExtractor instantiateExtractor(AttributeConfig config, ClassLoader classLoader) {
        ValueExtractor extractor = null;
        if (classLoader != null) {
            try {
                extractor = instantiateExtractorWithConfigClassLoader(config, classLoader);
            } catch (IllegalArgumentException ex) {
                // cached back-stage, initialised lazily since it's not a common case
                Logger.getLogger(ExtractorHelper.class)
                        .warning("Could not instantiate extractor with the config class loader", ex);
            }
        }

        if (extractor == null) {
            extractor = instantiateExtractorWithClassForName(config);
        }
        return extractor;
    }

    private static ValueExtractor instantiateExtractorWithConfigClassLoader(AttributeConfig config, ClassLoader classLoader) {
        try {
            Class<?> clazz = classLoader.loadClass(config.getExtractorClassName());
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

    private static ValueExtractor instantiateExtractorWithClassForName(AttributeConfig config) {
        try {
            Class<?> clazz = Class.forName(config.getExtractorClassName());
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

    public static String extractAttributeNameNameWithoutArguments(String attributeNameWithArguments) {
        int start = StringUtil.lastIndexOf(attributeNameWithArguments, '[');
        int end = StringUtil.lastIndexOf(attributeNameWithArguments, ']');
        if (start > 0 && end > 0 && end > start) {
            return attributeNameWithArguments.substring(0, start);
        }
        if (start < 0 && end < 0) {
            return attributeNameWithArguments;
        }
        throw new IllegalArgumentException("Wrong argument input passed " + attributeNameWithArguments);
    }

    public static String extractArgumentsFromAttributeName(String attributeNameWithArguments) {
        int start = StringUtil.lastIndexOf(attributeNameWithArguments, '[');
        int end = StringUtil.lastIndexOf(attributeNameWithArguments, ']');
        if (start > 0 && end > 0 && end > start) {
            return attributeNameWithArguments.substring(start + 1, end);
        }
        if (start < 0 && end < 0) {
            return null;
        }
        throw new IllegalArgumentException("Wrong argument input passed " + attributeNameWithArguments);
    }

}
