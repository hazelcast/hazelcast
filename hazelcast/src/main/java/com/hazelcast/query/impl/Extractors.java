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
import com.hazelcast.query.QueryException;
import com.hazelcast.query.extractor.Arguments;
import com.hazelcast.query.extractor.ValueExtractor;
import com.hazelcast.query.impl.getters.ReflectionHelper;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Extractors {

    private static final Extractors EMPTY = new Extractors(Collections.<MapAttributeConfig>emptyList());

    // Maps the extractorAttributeName WITHOUT the arguments to a ValueExtractor instance
    // The name does not contain the argument since it's not allowed to register an extractor under an attribute name
    // that contains an argument in square brackets.
    private final Map<String, ValueExtractor> extractors;
    //TODO: Eviction
    private final Map<String, Getter> getterMap;
    private final ExtractorsContext context;

    public Extractors(List<MapAttributeConfig> mapAttributeConfigs) {
        extractors = new HashMap<String, ValueExtractor>();
        getterMap = new ConcurrentHashMap<String, Getter>();
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

    public Object extract(Object targetObject, String attributeName) {
        Getter extractorWrapper = getterMap.get(attributeName);
        if (extractorWrapper != null) {
            return extractorWrapper.extract(targetObject);
        }

        String attributeNameWithoutArguments = context.getAttributeNameWithoutArguments(attributeName);
        ValueExtractor valueExtractor = extractors.get(attributeNameWithoutArguments);
        if (valueExtractor == null) {
            ReflectionGetter reflectionGetter = new ReflectionGetter(attributeName);
            getterMap.put(attributeName, reflectionGetter);
            return reflectionGetter.extract(targetObject);
        }
        Arguments arguments = context.getArguments(attributeName);
        ExtractorGetter wrapper = new ExtractorGetter(valueExtractor, arguments);
        getterMap.put(attributeName, wrapper);
        return wrapper.extract(targetObject);
    }

    // This method is very inefficient because:
    // lot of time is spend on retrieving field/method and it isn't cached
    // the actual invocation on the Field, Method is also is quite expensive.
    private static Object extractViaReflection(String attributeName, Object obj) {
        try {
            return ReflectionHelper.extractValue(obj, attributeName);
            //TODO: *Query*Exception does not belong here.
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            //TODO: *Query*Exception does not belong here.
            throw new QueryException(e);
        }
    }


    @SuppressWarnings("unchecked")
    private <T> Arguments<T> getArguments(String attributeNameWithArguments) {
        return context.getArguments(attributeNameWithArguments);
    }

    public static Extractors empty() {
        return EMPTY;
    }

    //TODO: Silly name
    private interface Getter<T> {
        Object extract(T targetObject);
    }

    private final static class ReflectionGetter<T> implements Getter<T> {
        private final String attributeName;

        private ReflectionGetter(String attributeName) {
            this.attributeName = attributeName;
        }

        @Override
        public Object extract(T targetObject) {
            return extractViaReflection(attributeName, targetObject);
        }
    }

    private final static class ExtractorGetter<T, K> implements Getter<T>{
        private final ValueExtractor<T, K> extractor;
        private final Arguments<K> arguments;

        private ExtractorGetter(ValueExtractor<T, K> extractor, Arguments<K> arguments) {
            this.extractor = extractor;
            this.arguments = arguments;
        }

        public Object extract(T targetObject) {
            // This part will be improved in 3.7 to avoid extra allocation
            DefaultValueCollector collector = new DefaultValueCollector();
            extractor.extract(targetObject, arguments, collector);
            return collector.getResult();
        }
    }

}