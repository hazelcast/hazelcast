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

package com.hazelcast.config;

import com.hazelcast.query.QueryConstants;

import static com.hazelcast.util.Preconditions.checkHasText;

/**
 * Contains the configuration of a custom attribute that will be extracted from a Map's entry using a given ValueExtractor.
 * This class should be used in combination with the {@link MapConfig}.
 *
 * @see com.hazelcast.query.extractor.ValueExtractor
 */
public class MapAttributeConfig {

    private String name;
    private String extractor;

    private MapAttributeConfigReadOnly readOnly;

    /**
     * Creates an empty MapAttributeConfig.
     */
    public MapAttributeConfig() {
    }

    /**
     * Creates a MapAttributeConfig with the given attribute and ordered setting.
     *
     * @param name      the name given to an attribute that is going to be extracted.
     * @param extractor full class name of the extractor used to extract the value of the attribute.
     * @see #setName(String)
     * @see #setExtractor(String)
     */
    public MapAttributeConfig(String name, String extractor) {
        setName(name);
        setExtractor(extractor);
    }

    public MapAttributeConfig(MapAttributeConfig config) {
        name = config.getName();
        extractor = config.getExtractor();
    }

    public MapAttributeConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new MapAttributeConfigReadOnly(this);
        }
        return readOnly;
    }

    /**
     * Gets the name of the attribute extracted by the extractor.
     *
     * @return the name of the attribute extracted by the extractor.
     * @see #setName(String)
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the attribute extracted by the extractor.
     * The name cannot be equal to any of the query constants.
     *
     * @param name the name of the attribute extracted by the extractor.
     * @return the updated MapAttributeConfig.
     * @throws IllegalArgumentException if attribute is null,an empty or inappropriate string.
     * @see QueryConstants
     */
    public MapAttributeConfig setName(String name) {
        this.name = checkNotQueryConstant(checkHasText(name, "Map attribute name must contain text"));
        return this;
    }

    private static String checkNotQueryConstant(String name) {
        for (QueryConstants constant : QueryConstants.values()) {
            if (name.equals(constant.value())) {
                throw new IllegalArgumentException(String.format("Map attribute name must not contain query constant '%s'", constant.value()));
            }
        }
        return name;
    }

    /**
     * Gets the full class name of the extractor in a String format, e.g. com.example.car.SpeedExtractor
     *
     * @return the full class name of the extractor in a String format.
     * @see #setExtractor(String)
     */
    public String getExtractor() {
        return extractor;
    }

    /**
     * Sets the full class name of the extractor in a String format, e.g. com.example.car.SpeedExtractor
     *
     * @param extractor the full class name of the extractor in a String format.
     * @return the updated MapAttributeConfig.
     */
    public MapAttributeConfig setExtractor(String extractor) {
        this.extractor = checkHasText(extractor, "Map attribute extractor must contain text");
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MapAttributeConfig{");
        sb.append("name='").append(name).append('\'');
        sb.append("extractor='").append(extractor).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
