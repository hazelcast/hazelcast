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

package com.hazelcast.jet.sql.impl.aggregate;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class DistinctSqlAggregationTest {

    @Mock
    private SqlAggregation delegate;

    @Mock
    private SqlAggregation otherDelegate;

    @Test
    public void test_accumulate() {
        SqlAggregation aggregation = new DistinctSqlAggregation(delegate);
        aggregation.accumulate("1");
        aggregation.accumulate("2");
        aggregation.accumulate("1");

        verify(delegate).accumulate("1");
        verify(delegate).accumulate("2");
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void test_combine() {
        SqlAggregation left = new DistinctSqlAggregation(delegate);
        left.accumulate("1");
        left.accumulate("2");

        SqlAggregation right = new DistinctSqlAggregation(otherDelegate);
        right.accumulate("2");
        right.accumulate("3");

        left.combine(right);

        verify(delegate).accumulate("1");
        verify(delegate).accumulate("2");
        verify(delegate).accumulate("3");
        verifyNoMoreInteractions(delegate);
    }
}
