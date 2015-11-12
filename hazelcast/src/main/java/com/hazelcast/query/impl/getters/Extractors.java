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
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ExceptionUtil;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.query.impl.getters.ExtractorHelper.extractArgumentsFromAttributeName;
import static com.hazelcast.query.impl.getters.ExtractorHelper.extractAttributeNameNameWithoutArguments;
import static com.hazelcast.query.impl.getters.ExtractorHelper.instantiateExtractors;

public class Extractors {

    private static final int MAX_CACHE_SIZE = 10000;
    private static final Extractors EMPTY = new Extractors(Collections.<MapAttributeConfig>emptyList());

    // Maps the extractorAttributeName WITHOUT the arguments to a ValueExtractor instance
    // The name does not contain the argument since it's not allowed to register an extractor under an attribute name
    // that contains an argument in square brackets.
    private final Map<String, ValueExtractor> extractors;
    private final GetterCache getterCache;
    private final DefaultArgumentsParser argumentsParser;

    public Extractors(List<MapAttributeConfig> mapAttributeConfigs) {
        this.extractors = instantiateExtractors(mapAttributeConfigs);
        this.getterCache = new GetterCache();
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
        if (getter != null) {
            return getter;
        }
        String attributeNameWithoutArguments = extractAttributeNameNameWithoutArguments(attributeName);
        ValueExtractor valueExtractor = extractors.get(attributeNameWithoutArguments);
        if (valueExtractor != null) {
            Arguments arguments = argumentsParser.parse(extractArgumentsFromAttributeName(attributeName));
            ExtractorGetter extractorGetter = new ExtractorGetter(valueExtractor, arguments);
            getterCache.putGetter(targetObject.getClass(), attributeName, extractorGetter);
            return extractorGetter;
        } else {
            Getter reflectionGetter = ReflectionHelper.createGetter(targetObject, attributeName);
            if (reflectionGetter.isCacheable()) {
                getterCache.putGetter(targetObject.getClass(), attributeName, reflectionGetter);
            }
            return reflectionGetter;
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

    private static final class GetterCache {
        private final ConcurrentMap<Class, ConcurrentMap<String, Getter>> getterCache
                = new ConcurrentHashMap<Class, ConcurrentMap<String, Getter>>(1000);

        private final ConstructorFunction<Class, ConcurrentMap<String, Getter>> getterCacheConstructor
                = new ConstructorFunction<Class, ConcurrentMap<String, Getter>>() {
            @Override
            public ConcurrentMap<String, Getter> createNew(Class arg) {
                return new ConcurrentHashMap<String, Getter>();
            }
        };

        @Nullable
        private Getter getGetter(Class clazz, String attribute) {
            ConcurrentMap<String, Getter> cache = getterCache.get(clazz);
            if (cache == null) {
                return null;
            }
            return cache.get(attribute);
        }

        private Getter putGetter(Class clazz, String attribute, Getter getter) {
            ConcurrentMap<String, Getter> cache = ConcurrencyUtil.getOrPutIfAbsent(getterCache, clazz, getterCacheConstructor);
            Getter foundGetter = cache.putIfAbsent(attribute, getter);
            evictIfMaxSizeReached();
            return foundGetter == null ? getter : foundGetter;
        }

        public void evictIfMaxSizeReached() {
            if (getterCache.size() > MAX_CACHE_SIZE) {
                getterCache.clear();
            }
        }
    }

}
