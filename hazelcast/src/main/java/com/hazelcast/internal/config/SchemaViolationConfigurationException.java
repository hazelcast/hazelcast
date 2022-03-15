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

package com.hazelcast.internal.config;

import com.hazelcast.config.InvalidConfigurationException;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.joining;

/**
 * Thrown by {@link com.hazelcast.internal.config.YamlConfigSchemaValidator} implementations.
 */
public class SchemaViolationConfigurationException
        extends InvalidConfigurationException {

    private final String keywordLocation;

    private final String instanceLocation;

    private final List<SchemaViolationConfigurationException> errors;

    public SchemaViolationConfigurationException(String message, String keywordLocation,
                                                 String instanceLocation,
                                                 List<SchemaViolationConfigurationException> errors) {
        super(message);
        this.keywordLocation = keywordLocation;
        this.instanceLocation = instanceLocation;
        this.errors = unmodifiableList(new ArrayList<>(errors));
    }

    /**
     * @return a JSON pointer denoting the path to the keyword in the schema on which the validation failed.
     */
    public String getKeywordLocation() {
        return keywordLocation;
    }

    /**
     * @return a JSON pointer denoting the path to the configuration element which failed to validate.
     */
    public String getInstanceLocation() {
        return instanceLocation;
    }

    /**
     * @return a possibly empty list of sub-errors that caused the validation failure. If a higher-level configuration element
     * has multiple sub-elements that violate the schema, then a {@code SchemaViolationConfigurationException} will be thrown with
     * the {@link #getInstanceLocation()} pointing to the higher-level configuration element, and multiple exceptions are returned
     * by {@link #getErrors()} for each invalid sub-element. The errors can form an arbitrarily deep tree structure.
     */
    public List<SchemaViolationConfigurationException> getErrors() {
        return errors;
    }

    private String prettyPrint(int indentLevel) {
        String linePrefix = IntStream.range(0, indentLevel).mapToObj(i -> "  ").collect(joining(""));
        String lineSeparator = System.lineSeparator();
        StringBuilder sb = new StringBuilder(linePrefix).append(getMessage()).append(lineSeparator)
                .append(linePrefix).append("  instance location: ").append(instanceLocation).append(lineSeparator)
                .append(linePrefix).append("  keyword location: ").append(keywordLocation).append(lineSeparator);
        errors.stream().map(err -> err.prettyPrint(indentLevel + 1)).forEach(sb::append);
        return sb.toString();
    }

    @Override
    public String toString() {
        return prettyPrint(0);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SchemaViolationConfigurationException that = (SchemaViolationConfigurationException) o;
        return getMessage().equals(that.getMessage())
                && keywordLocation.equals(that.keywordLocation)
                && instanceLocation.equals(that.instanceLocation)
                && errors.equals(that.errors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getMessage(), keywordLocation, instanceLocation, errors);
    }
}
