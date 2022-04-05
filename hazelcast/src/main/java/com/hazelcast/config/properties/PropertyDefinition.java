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

package com.hazelcast.config.properties;

import com.hazelcast.core.TypeConverter;

/**
 * This interface describes an extended approach of the currently available
 * pure property based configuration inside Hazelcast and helps implementing
 * an automatic and transparent way to verify configuration as well as converting
 * types based on provided validators and converters.
 * <p>
 * All verification is done on property level which means that the configuration
 * overall might still be invalid and needs to be checked by the provider vendor
 * before actually using it.
 * <p>
 * All used {@link com.hazelcast.core.TypeConverter}s and
 * {@link com.hazelcast.config.properties.ValueValidator}s need to be fully thread-safe
 * and are recommended to be stateless to prevent any kind of unexpected concurrency
 * issues.
 */
public interface PropertyDefinition {

    /**
     * The {@link com.hazelcast.core.TypeConverter} to be used to convert the
     * string value read from XML to the expected type automatically.
     *
     * @return a defined type converter to convert from string to another type
     */
    TypeConverter typeConverter();

    /**
     * Returns the key (the name) of this property inside the configuration.
     *
     * @return returns the property key
     */
    String key();

    /**
     * Returns an optional validator to validate a value before finalizing the
     * configuration.
     *
     * @return the optional validator. Is allowed to be <code>null</code> in case
     * there is no validation required.
     */
    ValueValidator validator();

    /**
     * Defines if this property is optional or not. Optional properties don't
     * have to be set inside the configuration but if set they need to pass a
     * possibly defined {@link com.hazelcast.config.properties.ValueValidator}
     * test.
     *
     * @return {@code true} if this property is optional, {@code false} otherwise
     */
    boolean optional();
}
