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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import static com.hazelcast.query.impl.AbstractIndex.NULL;
import static com.hazelcast.query.impl.CompositeValue.NEGATIVE_INFINITY;
import static com.hazelcast.query.impl.CompositeValue.POSITIVE_INFINITY;
import static com.hazelcast.query.impl.TypeConverters.NULL_CONVERTER;

/**
 * Represents a composite converter composed out of a converter tuple. Used
 * while converting {@link CompositeValue}s for the purpose of querying and
 * storing.
 * <p>
 * A composite converter may be marked as {@link #isTransient transient} if at
 * least one of its component converters is resolved to {@link
 * TypeConverters#NULL_CONVERTER NULL_CONVERTER}. That null converter set for a
 * certain component indicates that there are only {@code null} values present
 * in the index for the corresponding attribute and therefore the actual
 * attribute type is not resolved yet.
 */
public class CompositeConverter implements TypeConverter {

    private final TypeConverter[] converters;

    private final boolean isTransient;

    /**
     * Constructs a new composite converter from the given component converters.
     * <p>
     * For performance reasons, the ownership of the passed converters array is
     * transferred to the new composite converter instance.
     *
     * @param converters the component converters.
     */
    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public CompositeConverter(TypeConverter[] converters) {
        this.converters = converters;

        boolean isTransient = false;
        for (TypeConverter converter : converters) {
            assert converter != null;

            if (converter == NULL_CONVERTER) {
                isTransient = true;
                break;
            }
        }
        this.isTransient = isTransient;
    }

    /**
     * @return {@code true} if this composite converter instance contains
     * unresolved component converters, {@code false} if all component
     * converters are resolved.
     */
    public boolean isTransient() {
        return isTransient;
    }

    public int getComponentCount() {
        return converters.length;
    }

    /**
     * @return the converter for the given component.
     */
    public TypeConverter getComponentConverter(int component) {
        return converters[component];
    }

    @Override
    public Comparable convert(Comparable value) {
        if (!(value instanceof CompositeValue)) {
            throw new IllegalArgumentException("Cannot convert [" + value + "] to composite");
        }
        CompositeValue compositeValue = (CompositeValue) value;

        Comparable[] components = compositeValue.getComponents();
        Comparable[] converted = new Comparable[components.length];
        for (int i = 0; i < components.length; ++i) {
            Comparable component = components[i];
            if (component == NULL || component == NEGATIVE_INFINITY || component == POSITIVE_INFINITY) {
                converted[i] = component;
            } else {
                converted[i] = converters[i].convert(component);
            }
        }

        return new CompositeValue(converted);
    }

}
