/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.internal.util.phonehome;

import com.hazelcast.instance.impl.Node;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.impl.InternalSqlService;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.function.BiConsumer;

import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.SQL_QUERIES_SUBMITTED;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.SQL_STREAMING_QUERIES_EXECUTED;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
@Category({QuickTest.class})
public class SqlInfoCollectorTest {

    SqlInfoCollector sqlInfoCollector;

    @Mock
    BiConsumer<PhoneHomeMetrics, String> metricsConsumer;

    @Mock
    Node node;

    @Mock
    NodeEngineImpl nodeEngine;

    @Mock
    InternalSqlService sqlService;

    @Before
    public void setUp() throws Exception {
        sqlInfoCollector = new SqlInfoCollector();

        when(node.getNodeEngine()).thenReturn(nodeEngine);
        when(nodeEngine.getSqlService()).thenReturn(sqlService);
    }

    @Test
    public void test_SqlQueries() {
        // given
        when(sqlService.getSqlQueriesSubmittedCount()).thenReturn(5L);
        when(sqlService.getSqlStreamingQueriesExecutedCount()).thenReturn(3L);

        // when
        sqlInfoCollector.forEachMetric(node, metricsConsumer);

        // then
        verify(metricsConsumer).accept(SQL_QUERIES_SUBMITTED, "5");
        verify(metricsConsumer).accept(SQL_STREAMING_QUERIES_EXECUTED, "3");
        verifyNoMoreInteractions(metricsConsumer);
    }

}
