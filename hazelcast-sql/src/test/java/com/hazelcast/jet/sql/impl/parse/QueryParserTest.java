/*
 * Copyright 2021 Hazelcast Inc.
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

import com.hazelcast.jet.sql.impl.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.sql.SqlNode;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.BDDMockito.given;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryParserTest {

    @InjectMocks
    private QueryParser parser;

    @Mock
    private HazelcastSqlValidator sqlValidator;

    @Mock
    private SqlNode validatedNode;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void unsupportedKeywordTest() {
        assertThatThrownBy(() -> parser.parse("show tables"))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Encountered \"show tables\" at line 1, column 1.");
    }

    @Test
    public void test_trailingSemicolon() {
        given(sqlValidator.validate(isA(SqlNode.class))).willReturn(validatedNode);

        parser.parse("SELECT * FROM t;");
        parser.parse("SELECT * FROM t;;");
    }

    @Test
    public void test_noFrom() {
        given(sqlValidator.validate(isA(SqlNode.class))).willReturn(validatedNode);

        parser.parse("SELECT 1");
        parser.parse("SELECT 'test'");
        parser.parse("SELECT TO_TIMESTAMP_TZ(1)");
        parser.parse("SELECT rand()");
    }

    @Test
    public void when_multipleStatements_then_fails() {
        assertThatThrownBy(() -> parser.parse("SELECT * FROM t; SELECT * FROM t"))
                .isInstanceOf(QueryException.class)
                .hasMessage("The command must contain a single statement");
    }
}
