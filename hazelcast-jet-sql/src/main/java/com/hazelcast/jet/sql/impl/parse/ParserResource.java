/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.parse;

import org.apache.calcite.runtime.Resources;
import org.apache.calcite.runtime.Resources.BaseMessage;
import org.apache.calcite.runtime.Resources.ExInst;
import org.apache.calcite.sql.validate.SqlValidatorException;

public interface ParserResource {

    ParserResource RESOURCE = Resources.create(ParserResource.class);

    @BaseMessage("{0} is not supported for {1}")
    ExInst<SqlValidatorException> notSupported(String option, String statement);

    @BaseMessage("Column ''{0}'' specified more than once")
    ExInst<SqlValidatorException> duplicateColumn(String columnName);

    @BaseMessage("Option ''{0}'' specified more than once")
    ExInst<SqlValidatorException> duplicateOption(String optionName);

    @BaseMessage("SINK INTO clause is not supported for {0}")
    ExInst<SqlValidatorException> sinkIntoNotSupported(String connectorName);

    @BaseMessage("INSERT INTO clause is not supported for {0}")
    ExInst<SqlValidatorException> insertIntoNotSupported(String connectorName);

    @BaseMessage("The OR REPLACE option is required for CREATE SNAPSHOT")
    ExInst<SqlValidatorException> createSnapshotWithoutReplace();

    @BaseMessage("Unsupported value for {0}: {1}")
    ExInst<SqlValidatorException> processingGuaranteeBadValue(String key, String value);

    @BaseMessage("Invalid number for {0}: {1}")
    ExInst<SqlValidatorException> jobOptionIncorrectNumber(String key, String value);

    @BaseMessage("Unknown job option: {0}")
    ExInst<SqlValidatorException> unknownJobOption(String key);

    @BaseMessage("The mapping must be created in the \"public\" schema")
    ExInst<SqlValidatorException> mappingIncorrectSchema();

    @BaseMessage("Mapping does not exist: {0}")
    ExInst<SqlValidatorException> droppedMappingDoesNotExist(String mappingName);

    @BaseMessage("Writing to top-level fields of type OBJECT not supported")
    ExInst<SqlValidatorException> insertToTopLevelObject();
}
