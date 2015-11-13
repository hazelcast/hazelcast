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
import com.hazelcast.query.extractor.Arguments;
import com.hazelcast.query.extractor.ValueExtractor;
import com.hazelcast.query.impl.DefaultArgumentsParser;
import com.hazelcast.query.impl.DefaultValueCollector;
import com.hazelcast.util.ExceptionUtil;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.hazelcast.query.impl.getters.ExtractorHelper.extractArgumentsFromAttributeName;
import static com.hazelcast.query.impl.getters.ExtractorHelper.extractAttributeNameNameWithoutArguments;
import static com.hazelcast.query.impl.getters.ExtractorHelper.instantiateExtractors;

public class Extractors {

    private static final int MAX_CLASSES_IN_CACHE = 1000;
    private static final int MAX_GETTERS_PER_CLASS_IN_CACHE = 100;
    private static final float EVICTION_PERCENTAGE = 0.2f;
    private static final Extractors EMPTY = new Extractors(Collections.<MapAttributeConfig>emptyList());

    // Maps the extractorAttributeName WITHOUT the arguments to a ValueExtractor instance
    // The name does not contain the argument since it's not allowed to register an extractor under an attribute name
    // that contains an argument in square brackets.
    private final Map<String, ValueExtractor> extractors;
    private final EvictableGetterCache getterCache;
    private final DefaultArgumentsParser argumentsParser;

    public Extractors(List<MapAttributeConfig> mapAttributeConfigs) {
        this.extractors = instantiateExtractors(mapAttributeConfigs);
        this.getterCache = new EvictableGetterCache(MAX_CLASSES_IN_CACHE, MAX_GETTERS_PER_CLASS_IN_CACHE,
                EVICTION_PERCENTAGE);
        this.argumentsParser = new DefaultArgumentsParser();
    }

    public Object extract(Object targetObject, String attributeName) {
        if (targetObject != null) {
            Getter getter = getGetter(targetObject, attributeName);
            try {
                return getter.getValue(targetObject);
            } catch (Exception ex) {
                ExceptionUtil.sneakyThrow(ex);
            }
        }
        return null;
    }

    private Getter getGetter(Object targetObject, String attributeName) {
        Getter getter = getterCache.getGetter(targetObject.getClass(), attributeName);
        if (getter == null) {
            getter = instantiateGetter(targetObject, attributeName);
        }
        if (getter.isCacheable()) {
            getterCache.putGetter(targetObject.getClass(), attributeName, getter);
        }
        return getter;
    }

    private Getter instantiateGetter(Object targetObject, String attributeName) {
        String attributeNameWithoutArguments = extractAttributeNameNameWithoutArguments(attributeName);
        ValueExtractor valueExtractor = extractors.get(attributeNameWithoutArguments);
        if (valueExtractor != null) {
            Arguments arguments = argumentsParser.parse(extractArgumentsFromAttributeName(attributeName));
            return new ExtractorGetter(valueExtractor, arguments);
        } else {
            return ReflectionHelper.createGetter(targetObject, attributeName);
        }
    }

    public static Extractors empty() {
        return EMPTY;
    }

    @SuppressWarnings("unchecked")
    private static final class ExtractorGetter extends Getter {
        private final ValueExtractor extractor;
        private final Arguments arguments;

        private ExtractorGetter(ValueExtractor extractor, Arguments arguments) {
            super(null);
            this.extractor = extractor;
            this.arguments = arguments;
        }

        @Override
        Object getValue(Object target) throws Exception {
            // This part will be improved in 3.7 to avoid extra allocation
            DefaultValueCollector collector = new DefaultValueCollector();
            extractor.extract(target, arguments, collector);
            return collector.getResult();
        }

        @Override
        Class getReturnType() {
            throw new UnsupportedOperationException();
        }

        @Override
        boolean isCacheable() {
            return true;
        }
    }

}
