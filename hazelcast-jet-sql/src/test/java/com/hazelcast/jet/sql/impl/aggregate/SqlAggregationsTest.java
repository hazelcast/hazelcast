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

package com.hazelcast.jet.sql.impl.aggregate;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class SqlAggregationsTest {

    private SqlAggregation[] aggregations;

    @Before
    public void setUp() {
        aggregations = new SqlAggregation[]{
                mock(SqlAggregation.class),
                mock(SqlAggregation.class),
                mock(SqlAggregation.class),
                mock(SqlAggregation.class)
        };
    }

    @Test
    public void test_accumulate() {
        SqlAggregations aggregations =
                new SqlAggregations(new SqlAggregation[]{this.aggregations[0], this.aggregations[1]});
        Object[] row = new Object[]{1};

        aggregations.accumulate(row);

        verify(this.aggregations[0]).accumulate(row);
        verify(this.aggregations[1]).accumulate(row);
    }

    @Test
    public void test_combine() {
        SqlAggregations left = new SqlAggregations(new SqlAggregation[]{aggregations[0], aggregations[1]});
        SqlAggregations right = new SqlAggregations(new SqlAggregation[]{aggregations[2], aggregations[3]});

        left.combine(right);

        verify(aggregations[0]).combine(aggregations[2]);
        verify(aggregations[1]).combine(aggregations[3]);
    }

    @Test
    public void test_collect() {
        SqlAggregations aggregations =
                new SqlAggregations(new SqlAggregation[]{this.aggregations[0], this.aggregations[1]});
        given(this.aggregations[0].collect()).willReturn(1L);
        given(this.aggregations[1].collect()).willReturn("v");

        assertThat(aggregations.collect()).isEqualTo(new Object[]{1L, "v"});
    }

    @Test
    public void test_serialization() {
        ValueSqlAggregation original = new ValueSqlAggregation(0, QueryDataType.VARCHAR);
        original.accumulate(new Object[]{"v"});

        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        ValueSqlAggregation serialized = ss.toObject(ss.toData(original));

        assertThat(serialized).isEqualTo(original);
    }
}
