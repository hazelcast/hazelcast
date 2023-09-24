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

package org.apache.calcite.rex;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.sql.impl.inject.UpsertTarget;
import com.hazelcast.jet.sql.impl.opt.physical.visitor.RexToExpressionVisitor;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static com.hazelcast.sql.impl.expression.ConstantExpression.UNSET;

/**
 * A placeholder row expression that signifies an unset {@link TableField}.
 * <p>
 * When a mapping field is omitted in an {@code INSERT} statement, it is
 * defaulted to null by Calcite. However, {@link UpsertTarget} should be able
 * to {@linkplain UpsertTarget#createConverter(List) differentiate} null and
 * unset {@code __key}/{@code this} fields in order to decide the upsert mode.
 * If the value is {@link ConstantExpression#UNSET UNSET}, individual fields
 * are used for value injection, whereas null value nullifies the whole record.
 * <p>
 * Note that {@code RexUnset} is used for all mappings since if {@code __key}
 * and {@code this} fields are not explicitly specified in a mapping, they are
 * automatically added as hidden fields to make querying/updating whole records
 * possible.
 */
@SuppressWarnings("EqualsHashCode")
public class RexUnset extends RexLiteral {

    public RexUnset(RelDataType type) {
        super(null, type, SqlTypeName.NULL);
    }

    /**
     * @implNote {@link RexSimplify} always {@linkplain RexSimplify#simplify(RexNode,
     * RexUnknownAs) creates} new {@code RexLiteral}s when simplifying null expressions.
     * This causes {@code RexUnset} to be overwritten by an ordinary {@link RexLiteral}.
     */
    @Override
    public boolean isNull() {
        return false;
    }

    /**
     * @implNote {@link RexLiteral#equals} does not compare classes, and consequently,
     * it considers a {@code RexUnset} and a {@link RexLiteral} equal if they are
     * memberwise equal. This causes {@link RexProgramBuilder} to {@linkplain
     * RexProgramBuilder#exprMap eliminate} {@code RexUnset} if an <em>equal</em>
     * {@code RexLiteral} is already registered.
     */
    @Override
    public boolean equals(@Nullable Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        return getType() == ((RexUnset) obj).getType();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R> R accept(RexVisitor<R> visitor) {
        return visitor instanceof RexToExpressionVisitor
                ? (R) ConstantExpression.create(UNSET, QueryDataType.OBJECT)
                : super.accept(visitor);
    }

    /**
     * Eliminates {@code RexUnset}s from {@code projection} and returns a row projection
     * that inserts {@link ConstantExpression#UNSET UNSET}s at appropriate positions.
     */
    public static FunctionEx<Object[], Object[]> createRowProjection(List<RexNode> projection) {
        Object[] mask = new Object[projection.size()];
        for (int i = projection.size() - 1; i >= 0; i--) {
            if (projection.get(i) instanceof RexUnset) {
                mask[i] = UNSET;
                projection.remove(i);
            }
        }
        return row -> {
            Object[] projected = mask.clone();
            for (int i = 0, j = 0; i < projected.length; i++) {
                if (projected[i] != UNSET) {
                    projected[i] = row[j++];
                }
            }
            return projected;
        };
    }
}
