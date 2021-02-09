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
}
