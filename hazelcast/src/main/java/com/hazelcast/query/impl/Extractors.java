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

import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.query.extractor.Arguments;
import com.hazelcast.query.extractor.ValueExtractor;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Extractors {

    private static final Extractors EMPTY = new Extractors(Collections.<MapAttributeConfig>emptyList());

    // Maps the extractorAttributeName WITHOUT the arguments to a ValueExtractor instance
    // The name does not contain the argument since it's not allowed to register an extractor under an attribute name
    // that contains an argument in square brackets.
    private final Map<String, ValueExtractor> extractors;
    private final ExtractorsContext context;

    public Extractors(List<MapAttributeConfig> mapAttributeConfigs) {
        extractors = new HashMap<String, ValueExtractor>();
        context = new ExtractorsContext();
        for (MapAttributeConfig config : mapAttributeConfigs) {
            if (extractors.containsKey(config.getName())) {
                throw new IllegalArgumentException("Could not add " + config
                        + ". Extractor for this attribute name already added.");
            }
            extractors.put(config.getName(), instantiateExtractor(config));
        }
    }

    private ValueExtractor instantiateExtractor(MapAttributeConfig config) {
        try {
            Class<?> clazz = Class.forName(config.getExtractor());
            Object extractor = clazz.newInstance();
            if (extractor instanceof ValueExtractor) {
                return (ValueExtractor) extractor;
            } else {
                throw new IllegalArgumentException("Extractor does not extend ValueExtractor class " + config);
            }
        } catch (IllegalAccessException ex) {
            throw new RuntimeException("Could not initialize extractor " + config, ex);
        } catch (InstantiationException ex) {
            throw new RuntimeException("Could not initialize extractor " + config, ex);
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException("Could not initialize extractor " + config, ex);
        }
    }

    @SuppressWarnings("unchecked")
    public <T, K> ValueExtractor<T, K> getExtractor(String attributeNameWithArguments) {
        return extractors.get(context.getAttributeNameWithoutArguments(attributeNameWithArguments));
    }

    @SuppressWarnings("unchecked")
    public <T> Arguments<T> getArguments(String attributeNameWithArguments) {
        return context.getArguments(attributeNameWithArguments);
    }

    public static Extractors empty() {
        return EMPTY;
    }

}
