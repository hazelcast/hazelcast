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

package com.hazelcast.query.impl;

import com.hazelcast.core.TypeConverter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.query.impl.TypeConverters.NULL_CONVERTER;

/**
 * Maintains a cache of {@link TypeConverter} instances corresponding to
 * attributes of a single {@link Indexes} instance.
 */
public final class ConverterCache {

    // Marks UnresolvedConverter as fully unresolved, i.e. having no index
    // information attached or having a null/transient converter.
    private static final int FULLY_UNRESOLVED = -1;

    private final Indexes indexes;

    private final Map<String, TypeConverter> cache = new ConcurrentHashMap<String, TypeConverter>();

    /**
     * Constructs a new converters cache for the given indexes.
     *
     * @param indexes the indexes to construct a cache for.
     */
    public ConverterCache(Indexes indexes) {
        this.indexes = indexes;
    }

    /**
     * @return {@link TypeConverter} for the given attribute or {@code null} if
     * this cache doesn't aware of a converter for the given attribute.
     */
    public TypeConverter get(String attribute) {
        TypeConverter cached = cache.get(attribute);
        if (cached == null || cached instanceof UnresolvedConverter) {
            cached = tryResolve(attribute, (UnresolvedConverter) cached);
        }
        return cached;
    }

    /**
     * Invalidates this cache after the addition of the given index to the
     * {@link Indexes} for which this cache was constructed for.
     *
     * @param index the index added.
     */
    public void invalidate(InternalIndex index) {
        String[] components = index.getComponents();
        if (components.length == 1) {
            cache.remove(components[0]);
            return;
        }

        for (String component : components) {
            TypeConverter converter = cache.get(component);

            if (converter instanceof UnresolvedConverter) {
                cache.remove(component);
            }
        }
    }

    /**
     * Clears this cache by purging all entries.
     */
    public void clear() {
        cache.clear();
    }

    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity", "checkstyle:returncount"})
    private TypeConverter tryResolve(String attribute, UnresolvedConverter unresolved) {
        // The main idea here is to avoid scanning indexes on every invocation.
        // Unresolved converters are represented as UnresolvedConverter instances
        // and saved into the cache, so on the next invocation we don't need to
        // rescan the indexes.

        InternalIndex[] indexesSnapshot = indexes.getIndexes();
        if (indexesSnapshot.length == 0) {
            // no indexes at all
            return null;
        }

        if (unresolved != null) {
            // already marked as unresolved
            TypeConverter converter = unresolved.tryResolve();
            if (converter == null) {
                // still unresolved
                return null;
            }
            cache.put(attribute, converter);
            return converter;
        }

        // try non-composite index first, if any
        for (InternalIndex index : indexesSnapshot) {
            String[] components = index.getComponents();
            if (components.length != 1) {
                // composite index will be checked later.
                continue;
            }

            if (!components[0].equals(attribute)) {
                // not a component/attribute we are searching for
                continue;
            }

            TypeConverter converter = index.getConverter();
            if (isNull(converter)) {
                cache.put(attribute, new UnresolvedConverter(index, FULLY_UNRESOLVED));
                return null;
            } else {
                cache.put(attribute, converter);
                return converter;
            }
        }

        // scan composite indexes
        for (InternalIndex index : indexesSnapshot) {
            String[] components = index.getComponents();
            if (components.length == 1) {
                // not a composite index
                continue;
            }

            for (int i = 0; i < components.length; ++i) {
                String component = components[i];
                if (!component.equals(attribute)) {
                    // not a component/attribute we are searching for
                    continue;
                }

                CompositeConverter compositeConverter = (CompositeConverter) index.getConverter();
                if (compositeConverter == null) {
                    // no converter available, mark component as unresolved
                    cache.put(attribute, new UnresolvedConverter(index, i));
                    return null;
                }

                TypeConverter converter = compositeConverter.getComponentConverter(i);
                if (converter == NULL_CONVERTER) {
                    // null/transient converter available, mark component as unresolved
                    cache.put(attribute, new UnresolvedConverter(index, i));
                    return null;
                }

                // we found it
                cache.put(attribute, converter);
                return converter;
            }
        }

        // the attribute is not known by any index
        cache.put(attribute, new UnresolvedConverter(null, FULLY_UNRESOLVED));
        return null;
    }

    private static boolean isNull(TypeConverter converter) {
        return converter == null || converter == NULL_CONVERTER;
    }

    private static final class UnresolvedConverter implements TypeConverter {

        final InternalIndex index;
        final int component;

        UnresolvedConverter(InternalIndex index, int component) {
            this.index = index;
            this.component = component;
        }

        public TypeConverter tryResolve() {
            if (index == null) {
                // we don't known even an index that can provide a converter
                assert component == FULLY_UNRESOLVED;
                return null;
            }

            if (component == FULLY_UNRESOLVED) {
                // we got a non-composite index with a null/transient converter
                assert index.getComponents().length == 1;
                TypeConverter converter = index.getConverter();
                return isNull(converter) ? null : converter;
            }

            CompositeConverter compositeConverter = (CompositeConverter) index.getConverter();
            if (compositeConverter == null) {
                // still no converter
                return null;
            }
            TypeConverter converter = compositeConverter.getComponentConverter(component);
            return converter == NULL_CONVERTER ? null : converter;
        }

        @Override
        public Comparable convert(Comparable value) {
            throw new UnsupportedOperationException("must never be called");
        }

    }

}
