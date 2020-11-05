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

package com.hazelcast.jet.sql.impl.opt;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.hazelcast.sql.impl.calcite.HazelcastRexBuilder;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class OptUtilsTest {

    private static final SqlTypeFactoryImpl TYPE_FACTORY = HazelcastTypeFactory.INSTANCE;
    private static final HazelcastRexBuilder REX_BUILDER = new HazelcastRexBuilder(TYPE_FACTORY);

    @Mock
    private RelOptCluster cluster;

    @Test
    public void test_convert() {
        Values values = values(ImmutableMap.of(0, "a", 1, "b"));

        List<Object[]> converted = OptUtils.convert(values);

        assertThat(converted).containsExactly(new Object[]{0, "a"}, new Object[]{1, "b"});
    }

    @Test
    public void test_convertWithRowType() {
        Values values = values(ImmutableMap.of(0, "a", 1, "b"));

        List<Object[]> converted = OptUtils.convert(values, rowType(BIGINT, VARCHAR));

        assertThat(converted).containsExactly(new Object[]{0L, "a"}, new Object[]{1L, "b"});
    }

    private Values values(Map<Integer, String> values) {
        RelDataType rowType = rowType(INTEGER, VARCHAR);

        ImmutableList.Builder<ImmutableList<RexLiteral>> tuplesBuilder = new ImmutableList.Builder<>();
        for (Entry<Integer, String> value : values.entrySet()) {
            tuplesBuilder.add(ImmutableList.of(
                    REX_BUILDER.makeExactLiteral(BigDecimal.valueOf(value.getKey()), TYPE_FACTORY.createSqlType(INTEGER)),
                    REX_BUILDER.makeLiteral(value.getValue())
            ));
        }
        ImmutableList<ImmutableList<RexLiteral>> tuples = tuplesBuilder.build();

        return new LogicalValues(
                cluster,
                RelTraitSet.createEmpty(),
                rowType,
                tuples
        );
    }

    private static RelDataType rowType(SqlTypeName... types) {
        List<RelDataTypeField> fields = new ArrayList<>();
        for (int i = 0; i < types.length; i++) {
            fields.add(new RelDataTypeFieldImpl("field" + i, i, TYPE_FACTORY.createSqlType(types[i])));
        }
        return new RelRecordType(StructKind.PEEK_FIELDS, fields, false);
    }
}
