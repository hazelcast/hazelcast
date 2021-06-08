/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorException;

public interface ParserResource {

    ParserResource RESOURCE = Resources.create(ParserResource.class);

    @BaseMessage("{0} is not supported for {1}")
    ExInst<SqlValidatorException> notSupported(String option, String statement);

    @BaseMessage("OR REPLACE in conjunction with IF NOT EXISTS not supported")
    ExInst<SqlValidatorException> orReplaceWithIfNotExistsNotSupported();

    @BaseMessage("The mapping must be created in the \"public\" schema")
    ExInst<SqlValidatorException> mappingIncorrectSchema();

    @BaseMessage("Column ''{0}'' specified more than once")
    ExInst<SqlValidatorException> duplicateColumn(String columnName);

    @BaseMessage("WATERMARK column specified more than once")
    Resources.ExInst<SqlValidatorException> multipleWatermarkColumns();

    @BaseMessage("WATERMARK column must be of TIMESTAMP WITH TIME ZONE type")
    Resources.ExInst<SqlValidatorException> invalidWatermarkColumnType();

    @BaseMessage("Lag cannot be expressed as ''{0}''")
    Resources.ExInst<SqlValidatorException> invalidLagInterval(SqlTypeName typeName);

    @BaseMessage("Lag must not be negative")
    Resources.ExInst<SqlValidatorException> negativeLag();

    @BaseMessage("Option ''{0}'' specified more than once")
    ExInst<SqlValidatorException> duplicateOption(String optionName);

    @BaseMessage("Mapping does not exist: {0}")
    ExInst<SqlValidatorException> droppedMappingDoesNotExist(String mappingName);

    @BaseMessage("INSERT INTO clause is not supported for {0}, use SINK INTO")
    ExInst<SqlValidatorException> insertIntoNotSupported(String connectorName);

    @BaseMessage("Writing to top-level fields of type OBJECT not supported")
    ExInst<SqlValidatorException> insertToTopLevelObject();

    @BaseMessage("Unsupported value for {0}: {1}")
    ExInst<SqlValidatorException> processingGuaranteeBadValue(String key, String value);

    @BaseMessage("Invalid number for {0}: {1}")
    ExInst<SqlValidatorException> jobOptionIncorrectNumber(String key, String value);

    @BaseMessage("Unknown job option: {0}")
    ExInst<SqlValidatorException> unknownJobOption(String key);

    @BaseMessage("The OR REPLACE option is required for CREATE SNAPSHOT")
    ExInst<SqlValidatorException> createSnapshotWithoutReplace();
}
