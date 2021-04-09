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

import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.parse.SqlExtendedInsert.Keyword;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import org.apache.calcite.runtime.Resources.ExInst;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class SqlExtendedInsertTest {

    private static final String TABLE_NAME = "t";

    @Mock
    private SqlValidator validator;

    @Mock
    private SqlValidatorCatalogReader catalogReader;

    @Mock
    private SqlValidatorTable validatorTable;

    @Mock
    private HazelcastTable hazelcastTable;

    @Mock
    private SqlConnector connector;

    @Mock
    private SqlValidatorScope scope;

    @Before
    public void setUp() {
        given(validator.getCatalogReader()).willReturn(catalogReader);
        given(catalogReader.getTable(singletonList(TABLE_NAME))).willReturn(validatorTable);
        given(validatorTable.unwrap(HazelcastTable.class)).willReturn(hazelcastTable);
        given(hazelcastTable.getTarget()).willReturn(table(connector));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void when_sinkAndConnectorDoesNotSupportIt_then_throws() {
        SqlInsert insert = extendedInsert(TABLE_NAME, Keyword.SINK.symbol(SqlParserPos.ZERO));

        given(connector.supportsSink()).willReturn(false);
        given(validator.newValidationError(eq(insert), isA(ExInst.class)))
                .willThrow(new RuntimeException("expected test exception"));

        assertThatThrownBy(() -> insert.validate(validator, scope))
                .isInstanceOf(RuntimeException.class);
        verify(validator).validateInsert(insert);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void when_insertAndConnectorDoesNotSupportIt_then_throws() {
        SqlInsert insert = extendedInsert(TABLE_NAME);

        given(connector.supportsInsert()).willReturn(false);
        given(validator.newValidationError(eq(insert), isA(ExInst.class)))
                .willThrow(new RuntimeException("expected test exception"));

        assertThatThrownBy(() -> insert.validate(validator, scope))
                .isInstanceOf(RuntimeException.class);
        verify(validator).validateInsert(insert);
    }

    private static SqlInsert extendedInsert(String tableName, SqlNode... extendedKeywords) {
        return new SqlExtendedInsert(
                identifier(tableName),
                null,
                SqlNodeList.EMPTY,
                new SqlNodeList(asList(extendedKeywords), SqlParserPos.ZERO),
                SqlNodeList.EMPTY,
                SqlParserPos.ZERO
        );
    }

    private static SqlIdentifier identifier(String name) {
        return new SqlIdentifier(name, SqlParserPos.ZERO);
    }

    private static JetTable table(SqlConnector connector) {
        return new JetTable(connector, emptyList(), "schema", TABLE_NAME, new ConstantTableStatistics(0)) {
            @Override
            public PlanObjectKey getObjectKey() {
                return null;
            }
        };
    }
}

