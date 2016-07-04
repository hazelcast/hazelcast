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

package com.hazelcast.query.impl.getters;

import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.extractor.ValueExtractor;
import com.hazelcast.query.impl.DefaultArgumentParser;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.hazelcast.query.impl.getters.ExtractorHelper.extractArgumentsFromAttributeName;
import static com.hazelcast.query.impl.getters.ExtractorHelper.extractAttributeNameNameWithoutArguments;

// one instance per MapContainer
public final class Extractors {

    private static final int MAX_CLASSES_IN_CACHE = 1000;
    private static final int MAX_GETTERS_PER_CLASS_IN_CACHE = 100;
    private static final float EVICTION_PERCENTAGE = 0.2f;

    private volatile PortableGetter genericPortableGetter;

    /**
     * Maps the extractorAttributeName WITHOUT the arguments to a ValueExtractor instance.
     * The name does not contain the argument since it's not allowed to register an extractor under an attribute name
     * that contains an argument in square brackets.
     */
    private final Map<String, ValueExtractor> extractors;
    private final EvictableGetterCache getterCache;
    private final DefaultArgumentParser argumentsParser;

    // TODO InternalSerializationService should be passed in constructor
    public Extractors(List<MapAttributeConfig> mapAttributeConfigs, ClassLoader classLoader) {
        this.extractors = ExtractorHelper.instantiateExtractors(mapAttributeConfigs, classLoader);
        this.getterCache = new EvictableGetterCache(MAX_CLASSES_IN_CACHE, MAX_GETTERS_PER_CLASS_IN_CACHE,
                EVICTION_PERCENTAGE);
        this.argumentsParser = new DefaultArgumentParser();
    }

    public Object extract(InternalSerializationService serializationService, Object target, String attributeName) {
        Object targetObject = getTargetObject(serializationService, target);
        if (targetObject != null) {
            Getter getter = getGetter(serializationService, targetObject, attributeName);
            try {
                return getter.getValue(targetObject, attributeName);
            } catch (Exception ex) {
                throw new QueryException(ex);
            }
        }
        return null;
    }

    /**
     * @return Data (in this case it's portable) or Object (in this case it's non-portable)
     */
    private static Object getTargetObject(InternalSerializationService serializationService, Object target) {
        Data targetData;
        if (target instanceof Portable) {
            targetData = serializationService.toData(target);
            if (targetData.isPortable()) {
                return targetData;
            }
        }

        if (target instanceof Data) {
            targetData = (Data) target;
            if (targetData.isPortable()) {
                return targetData;
            } else {
                // convert non-portable Data to object
                return serializationService.toObject(target);
            }
        }

        return target;
    }

    Getter getGetter(InternalSerializationService serializationService, Object targetObject, String attributeName) {
        Getter getter = getterCache.getGetter(targetObject.getClass(), attributeName);
        if (getter == null) {
            getter = instantiateGetter(serializationService, targetObject, attributeName);
            if (getter.isCacheable()) {
                getterCache.putGetter(targetObject.getClass(), attributeName, getter);
            }
        }
        return getter;
    }

    private Getter instantiateGetter(InternalSerializationService serializationService,
                                     Object targetObject, String attributeName) {
        String attributeNameWithoutArguments = extractAttributeNameNameWithoutArguments(attributeName);
        ValueExtractor valueExtractor = extractors.get(attributeNameWithoutArguments);
        if (valueExtractor != null) {
            Object arguments = argumentsParser.parse(extractArgumentsFromAttributeName(attributeName));
            return new ExtractorGetter(serializationService, valueExtractor, arguments);
        } else {
            if (targetObject instanceof Data) {
                if (genericPortableGetter == null) {
                    // will be initialised a couple of times in the worst case
                    genericPortableGetter = new PortableGetter(serializationService);
                }
                return genericPortableGetter;
            } else {
                return ReflectionHelper.createGetter(targetObject, attributeName);
            }
        }
    }

    public static Extractors empty() {
        return new Extractors(Collections.<MapAttributeConfig>emptyList(), null);
    }

}
