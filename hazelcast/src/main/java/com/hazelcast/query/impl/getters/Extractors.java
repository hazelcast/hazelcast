/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.compact.CompactGenericRecord;
import com.hazelcast.internal.serialization.impl.portable.PortableGenericRecord;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.extractor.ValueExtractor;
import com.hazelcast.query.impl.DefaultArgumentParser;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static com.hazelcast.query.impl.getters.ExtractorHelper.extractArgumentsFromAttributeName;
import static com.hazelcast.query.impl.getters.ExtractorHelper.extractAttributeNameNameWithoutArguments;
import static com.hazelcast.query.impl.getters.ExtractorHelper.instantiateExtractors;
import static com.hazelcast.query.impl.getters.GetterCache.EVICTABLE_GETTER_CACHE_SUPPLIER;

// one instance per MapContainer
public final class Extractors {

    final GetterCache getterCache;

    private volatile PortableGetter portableGetter;
    private volatile JsonDataGetter jsonDataGetter;
    private volatile CompactGetter compactGetter;
    /**
     * Maps the extractorAttributeName WITHOUT the arguments to a
     * ValueExtractor instance. The name does not contain the argument
     * since it's not allowed to register an extractor under an
     * attribute name that contains an argument in square brackets.
     */
    private final Map<String, ValueExtractor> extractors;
    private final InternalSerializationService ss;
    private final DefaultArgumentParser argumentsParser;

    private Extractors(
            List<AttributeConfig> attributeConfigs,
            ClassLoader classLoader,
            InternalSerializationService ss,
            Supplier<GetterCache> getterCacheSupplier
    ) {
        this.extractors = attributeConfigs == null
                ? Collections.<String, ValueExtractor>emptyMap()
                : instantiateExtractors(attributeConfigs, classLoader);
        this.getterCache = getterCacheSupplier.get();
        this.argumentsParser = new DefaultArgumentParser();
        this.ss = ss;
    }

    public Object extract(Object target, String attributeName, Object metadata) {
        return extract(target, attributeName, metadata, true);
    }

    public Object extract(Object target, String attributeName, Object metadata, boolean failOnMissingReflectiveAttribute) {
        Object targetObject = getTargetObject(target);
        if (targetObject != null) {
            Getter getter = getGetter(targetObject, attributeName, failOnMissingReflectiveAttribute);
            try {
                return getter.getValue(targetObject, attributeName, metadata);
            } catch (Exception ex) {
                throw new QueryException(ex);
            }
        }
        return null;
    }

    /**
     * Returns the form of this data that is queryable.
     * Returns {@link Data} if {@code target} is
     * <ul>
     *     <li>a portable object either in Data form or Object form</li>
     *     <li>a {@link HazelcastJsonValue} in Data form</li>
     * </ul>
     * Otherwise, returns object form.
     *
     * @return Data or Object
     */
    private Object getTargetObject(Object target) {
        Data targetData;
        if (target instanceof Portable) {
            targetData = ss.toData(target);
            if (targetData.isPortable()) {
                return targetData;
            }
        }
        if (target instanceof Data) {
            targetData = (Data) target;
            if (targetData.isPortable() || targetData.isJson() || targetData.isCompact()) {
                return targetData;
            } else {
                // convert non-portable Data to object
                return ss.toObject(target);
            }
        }

        return target;
    }

    Getter getGetter(Object targetObject, String attributeName, boolean failOnMissingReflectiveAttribute) {
        Getter getter = getterCache.getGetter(targetObject.getClass(), attributeName);
        if (getter == null) {
            getter = instantiateGetter(targetObject, attributeName, failOnMissingReflectiveAttribute);
            if (getter.isCacheable()) {
                getterCache.putGetter(targetObject.getClass(), attributeName, getter);
            }
        }
        return getter;
    }

    private Getter instantiateGetter(Object targetObject, String attributeName, boolean failOnMissingReflectiveAttribute) {
        String attributeNameWithoutArguments = extractAttributeNameNameWithoutArguments(attributeName);
        ValueExtractor valueExtractor = extractors.get(attributeNameWithoutArguments);
        if (valueExtractor != null) {
            Object arguments = argumentsParser.parse(extractArgumentsFromAttributeName(attributeName));
            return new ExtractorGetter(ss, valueExtractor, arguments);
        } else if (targetObject instanceof Data) {
            return instantiateGetterForData((Data) targetObject);
        } else if (targetObject instanceof HazelcastJsonValue) {
            return JsonGetter.INSTANCE;
        } else if (targetObject instanceof PortableGenericRecord) {
            if (portableGetter == null) {
                // will be initialised a couple of times in the worst case
                portableGetter = new PortableGetter(ss);
            }
            return portableGetter;
        } else if (targetObject instanceof CompactGenericRecord) {
            if (compactGetter == null) {
                // will be initialised a couple of times in the worst case
                compactGetter = new CompactGetter(ss);
            }
            return compactGetter;
        } else {
            return ReflectionHelper.createGetter(targetObject, attributeName, failOnMissingReflectiveAttribute);
        }
    }

    private Getter instantiateGetterForData(Data data) {
        if (data.isPortable()) {
            if (portableGetter == null) {
                // will be initialised a couple of times in the worst case
                portableGetter = new PortableGetter(ss);
            }
            return portableGetter;
        }

        if (data.isJson()) {
            if (jsonDataGetter == null) {
                // will be initialised a couple of times in the worst case
                jsonDataGetter = new JsonDataGetter(ss);
            }
            return jsonDataGetter;
        }

        if (data.isCompact()) {
            if (compactGetter == null) {
                // will be initialised a couple of times in the worst case
                compactGetter = new CompactGetter(ss);
            }
            return compactGetter;
        }

        throw new HazelcastSerializationException("No Data getter found for type " + data.getType());
    }

    public static Extractors.Builder newBuilder(InternalSerializationService ss) {
        return new Extractors.Builder(ss);
    }

    /**
     * Builder which is used to create a new Extractors object.
     */
    public static final class Builder {
        private ClassLoader classLoader;
        private List<AttributeConfig> attributeConfigs;
        private Supplier<GetterCache> getterCacheSupplier = EVICTABLE_GETTER_CACHE_SUPPLIER;

        private final InternalSerializationService ss;

        public Builder(InternalSerializationService ss) {
            this.ss = Preconditions.checkNotNull(ss);
        }

        public Builder setGetterCacheSupplier(Supplier<GetterCache> getterCacheSupplier) {
            this.getterCacheSupplier = getterCacheSupplier;
            return this;
        }

        public Builder setAttributeConfigs(List<AttributeConfig> attributeConfigs) {
            this.attributeConfigs = attributeConfigs;
            return this;
        }

        public Builder setClassLoader(ClassLoader classLoader) {
            this.classLoader = classLoader;
            return this;
        }

        /**
         * @return a new instance of Extractors
         */
        public Extractors build() {
            return new Extractors(attributeConfigs, classLoader, ss, getterCacheSupplier);
        }
    }
}
