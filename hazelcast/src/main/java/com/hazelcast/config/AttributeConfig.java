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

package com.hazelcast.config;

import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.QueryConstants;

import java.io.IOException;
import java.util.regex.Pattern;

import static com.hazelcast.internal.util.Preconditions.checkHasText;
import static java.lang.String.format;

/**
 * Contains the configuration of a custom attribute that will be extracted from a Map's entry using a given ValueExtractor.
 * This class should be used in combination with the {@link MapConfig}.
 *
 * @see com.hazelcast.query.extractor.ValueExtractor
 */
public class AttributeConfig implements IdentifiedDataSerializable {

    private static final Pattern NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9][a-zA-Z0-9_]*$");

    private String name;
    private String extractorClassName;

    /**
     * Creates an empty AttributeConfig.
     */
    public AttributeConfig() {
    }

    /**
     * Creates a AttributeConfig with the given attribute and ordered setting.
     * <p>
     * Name may begin with an ascii letter [A-Za-z] or digit [0-9] and may contain ascii letters [A-Za-z], digits [0-9]
     * or underscores later on.
     *
     * @param name               the name given to an attribute that is going to be extracted
     * @param extractorClassName full class name of the extractor used to extract the value of the attribute
     * @see #setName(String)
     * @see #setExtractorClassName(String) (String)
     */
    public AttributeConfig(String name, String extractorClassName) {
        setName(name);
        setExtractorClassName(extractorClassName);
    }

    public AttributeConfig(AttributeConfig config) {
        name = config.getName();
        extractorClassName = config.getExtractorClassName();
    }

    /**
     * Gets the name of the attribute extracted by the extractor.
     *
     * @return the name of the attribute extracted by the extractor
     * @see #setName(String)
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the attribute extracted by the extractor.
     * The name cannot be equal to any of the query constants.
     *
     * @param name the name of the attribute extracted by the extractor
     * @return the updated AttributeConfig
     * @throws IllegalArgumentException if attribute is null,an empty or inappropriate string
     * @see QueryConstants
     */
    public AttributeConfig setName(String name) {
        this.name = checkName(name);
        return this;
    }

    private static String checkName(String name) {
        checkHasText(name, "Map attribute name must contain text");
        checkNameValid(name);
        checkNotQueryConstant(name);
        return name;
    }

    private static void checkNameValid(String name) {
        if (!NAME_PATTERN.matcher(name).matches()) {
            throw new IllegalArgumentException("Map attribute name is invalid. It may contain upper-case and lower-case"
                    + " letters, digits and underscores but an underscore may not be located at the first position).");
        }
    }

    private static void checkNotQueryConstant(String name) {
        for (QueryConstants constant : QueryConstants.values()) {
            if (name.equals(constant.value())) {
                throw new IllegalArgumentException(format("Map attribute name must not contain query constant '%s'",
                        constant.value()));
            }
        }
    }

    /**
     * Gets the full class name of the extractor in a String format, e.g. {@code com.example.car.SpeedExtractor}.
     *
     * @return the full class name of the extractor in a String format
     * @see #setExtractorClassName(String) (String)
     */
    public String getExtractorClassName() {
        return extractorClassName;
    }

    /**
     * Sets the full class name of the extractor in a String format, e.g. {@code com.example.car.SpeedExtractor}.
     *
     * @param extractorClassName the full class name of the extractor in a String format
     * @return the updated AttributeConfig
     */
    public AttributeConfig setExtractorClassName(String extractorClassName) {
        this.extractorClassName = checkHasText(extractorClassName, "Map attribute extractor must contain text");
        return this;
    }

    @Override
    public String toString() {
        return "AttributeConfig{"
                + "name='" + name + '\''
                + "extractorClassName='" + extractorClassName + '\''
                + '}';
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.MAP_ATTRIBUTE_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeString(extractorClassName);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        extractorClassName = in.readString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AttributeConfig that = (AttributeConfig) o;
        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        return extractorClassName != null
            ? extractorClassName.equals(that.extractorClassName) : that.extractorClassName == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (extractorClassName != null ? extractorClassName.hashCode() : 0);
        return result;
    }
}
