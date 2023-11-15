/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
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

    @BaseMessage("OR REPLACE in conjunction with IF NOT EXISTS not supported")
    ExInst<SqlValidatorException> orReplaceWithIfNotExistsNotSupported();

    @BaseMessage("The mapping must be created in the \"public\" schema")
    ExInst<SqlValidatorException> mappingIncorrectSchema();

    @BaseMessage("The data connection must be created in the \"public\" schema")
    ExInst<SqlValidatorException> dataConnectionIncorrectSchemaCreate();

    @BaseMessage("Data connections can exists only in the \"public\" schema")
    ExInst<SqlValidatorException> dataConnectionIncorrectSchemaUse();

    @BaseMessage("The view must be created in the \"public\" schema")
    ExInst<SqlValidatorException> viewIncorrectSchema();

    @BaseMessage("The type must be created in the \"public\" schema")
    ExInst<SqlValidatorException> typeIncorrectSchema();

    @BaseMessage("Column ''{0}'' specified more than once")
    ExInst<SqlValidatorException> duplicateColumn(String columnName);

    @BaseMessage("Index attribute ''{0}'' specified more than once")
    ExInst<SqlValidatorException> duplicateIndexAttribute(String columnName);

    @BaseMessage("Option ''{0}'' specified more than once")
    ExInst<SqlValidatorException> duplicateOption(String optionName);

    @BaseMessage("Mapping does not exist: {0}")
    ExInst<SqlValidatorException> droppedMappingDoesNotExist(String mappingName);

    @BaseMessage("Data connection does not exist: {0}")
    ExInst<SqlValidatorException> droppedDataConnectionDoesNotExist(String dataConnectionName);

    @BaseMessage("Index does not exist: {0}")
    ExInst<SqlValidatorException> droppedIndexDoesNotExist(String indexName);

    @BaseMessage("View does not exist: {0}")
    ExInst<SqlValidatorException> droppedViewDoesNotExist(String viewName);

    @BaseMessage("Type does not exist: {0}")
    ExInst<SqlValidatorException> droppedTypeDoesNotExist(String typeName);

    @BaseMessage("Writing to top-level fields of type OBJECT not supported")
    ExInst<SqlValidatorException> insertToTopLevelObject();

    @BaseMessage("Unknown option for {0} index: {1}")
    ExInst<SqlValidatorException> unsupportedIndexType(String indexType, String option);

    @BaseMessage("Unsupported value for {0}: {1}")
    ExInst<SqlValidatorException> processingGuaranteeBadValue(String key, String value);

    @BaseMessage("Invalid number for {0}: {1}")
    ExInst<SqlValidatorException> jobOptionIncorrectNumber(String key, String value);

    @BaseMessage("Unknown job option: {0}")
    ExInst<SqlValidatorException> unknownJobOption(String key);

    @BaseMessage("Job option is not supported for ANALYZE: {0}")
    ExInst<SqlValidatorException> unsupportedAnalyzeJobOption(String key);

    @BaseMessage("The OR REPLACE option is required for CREATE SNAPSHOT")
    ExInst<SqlValidatorException> createSnapshotWithoutReplace();
}
