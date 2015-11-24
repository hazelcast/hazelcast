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

package com.hazelcast.instance;

import com.hazelcast.spi.annotation.PrivateApi;

import java.util.concurrent.TimeUnit;

/**
 * Interface for Hazelcast Member and Client properties.
 */
@PrivateApi
public interface HazelcastProperty {

    /**
     * Gets the index of the property.
     *
     * @return index of the property
     */
    int getIndex();

    /**
     * Returns the property name.
     *
     * @return the property name
     */
    String getName();

    /**
     * Returns the default value of the property.
     *
     * @return the default value or <tt>null</tt> if none is defined
     */
    String getDefaultValue();

    /**
     * Returns the {@link TimeUnit} of the property.
     *
     * @return the {@link TimeUnit}
     * @throws IllegalArgumentException if no {@link TimeUnit} is defined
     */
    TimeUnit getTimeUnit();

    /**
     * Returns the parent {@link GroupProperty} of the property.
     *
     * @return the parent {@link GroupProperty} or <tt>null</tt> if none is defined
     */
    GroupProperty getParent();

    /**
     * Sets the environmental value of the property.
     *
     * @param value the value to set
     */
    void setSystemProperty(String value);

    /**
     * Gets the environmental value of the property.
     *
     * @return the value of the property
     */
    String getSystemProperty();

    /**
     * Clears the environmental value of the property.
     */
    String clearSystemProperty();
}
