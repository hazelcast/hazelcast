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

package com.hazelcast.jet.sql.impl.opt;

import com.google.common.collect.ImmutableList;
import com.hazelcast.sql.impl.calcite.HazelcastRexBuilder;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import org.apache.calcite.rex.RexLiteral;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.assertj.core.api.Assertions.assertThat;

public class OptUtilsTest {

    private static final HazelcastTypeFactory TYPE_FACTORY = HazelcastTypeFactory.INSTANCE;
    private static final HazelcastRexBuilder REX_BUILDER = new HazelcastRexBuilder(TYPE_FACTORY);

    @Test
    public void test_convert() {
        ImmutableList<ImmutableList<RexLiteral>> values = ImmutableList.of(literals(0, "a"), literals(1, "b"));

        List<Object[]> converted = OptUtils.convert(values);

        assertThat(converted).containsExactly(new Object[]{0, "a"}, new Object[]{1, "b"});
    }

    private static ImmutableList<RexLiteral> literals(int i, String s) {
        return ImmutableList.of(
                REX_BUILDER.makeExactLiteral(BigDecimal.valueOf(i), TYPE_FACTORY.createSqlType(INTEGER)),
                REX_BUILDER.makeLiteral(s)
        );
    }
}
