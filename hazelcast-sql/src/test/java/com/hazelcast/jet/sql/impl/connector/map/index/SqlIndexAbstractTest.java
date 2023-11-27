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

package com.hazelcast.jet.sql.impl.connector.map.index;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
import com.hazelcast.jet.sql.impl.opt.logical.FullScanLogicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.CalcPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.IndexScanMapPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.SortPhysicalRel;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionType;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionValue;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.assertj.core.api.JUnitSoftAssertions;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.base.Predicates.alwaysFalse;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionPredicates.and;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionPredicates.eq;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionPredicates.eq_2;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionPredicates.gt;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionPredicates.gt_2;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionPredicates.gte;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionPredicates.gte_2;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionPredicates.isNotNull;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionPredicates.isNotNull_2;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionPredicates.isNull;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionPredicates.isNull_2;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionPredicates.lt;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionPredicates.lt_2;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionPredicates.lte;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionPredicates.lte_2;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionPredicates.neq;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionPredicates.neq_2;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionPredicates.or;
import static com.hazelcast.sql.impl.schema.map.MapTableUtils.getPartitionedMapIndexes;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings({"unchecked", "rawtypes", "unused"})
public abstract class SqlIndexAbstractTest extends SqlIndexTestSupport {
    protected static final int DEFAULT_MEMBERS_COUNT = 2;
    private static final AtomicInteger MAP_NAME_GEN = new AtomicInteger();
    private static final String INDEX_NAME = "index";

    @Parameterized.Parameter
    public IndexType indexType;

    @Parameterized.Parameter(1)
    public boolean composite;

    @Parameterized.Parameter(2)
    public ExpressionType<?> f1;

    @Parameterized.Parameter(3)
    public ExpressionType<?> f2;

    @Rule
    public final JUnitSoftAssertions softly = new JUnitSoftAssertions();

    protected final String mapName = "map" + MAP_NAME_GEN.incrementAndGet();

    private IMap<Integer, ExpressionBiValue> map;
    private Map<Integer, ExpressionBiValue> localMap;
    private Class<? extends ExpressionBiValue> valueClass;
    private int runIdGen;

    @BeforeClass
    public static void beforeClass() {
        Config config = smallInstanceConfig();

        MapConfig mapConfig = new MapConfig();
        mapConfig.setName("map*")
                .setBackupCount(0);

        config.addMapConfig(mapConfig);

        initialize(DEFAULT_MEMBERS_COUNT, config);
    }

    @Before
    public void before() {
        // Start members if needed
        valueClass = ExpressionBiValue.createBiClass(f1, f2);

        createMapping(mapName, int.class, valueClass);
        map = instance().getMap(mapName);
        map.addIndex(getIndexConfig());

        fill();
    }

    @After
    public void after() {
        if (map != null) {
            // keep memory usage low, especially important for TS
            map.clear();
            map.destroy();
            map = null;
        }
    }

    @Test
    public void test() {
        checkFirstColumn();
        checkSecondColumn();
        checkBothColumns();
    }

    @Test
    public void testDisjunctionSameValue() {
        // Test for index scans with disjunctions that match the same row.
        // SQL query must not return duplicate rows in such case.

        // WHERE f1=? or f1=?
        check(query("(field1=? or field1=?)", f1.valueFrom(), f1.valueFrom()),
                c_notHashComposite(),
                eq(f1.valueFrom())
        );

        // WHERE f1=? or (f1=? and f2=?)
        check(query("((field1=? and field2=?) or (field1=? and field2=?))", f1.valueFrom(), f2.valueFrom(), f1.valueFrom(), f2.valueFrom()),
                // OR is currently not converted to union of index results or index scan with many ranges.
                // With literals Calcite produces Sarg which is single expression so is converted to a better plan.
                false,
                and(eq(f1.valueFrom()), eq_2(f2.valueFrom()))
        );
    }

    @Test
    public void testDisjunctionOverlappingRange() {
        // WHERE f1=? or (f1>=? and f1<=?) with eq value belonging to range
        check(query("field1=? or (field1>=? and field1<=?)", f1.valueFrom(), f1.valueFrom(), f1.valueTo()),
                // OR is currently not converted to union of index results or index scan with many ranges.
                // With literals Calcite produces Sarg which is single expression so is converted to a better plan.
                false,
                and(gte(f1.valueFrom()), lte(f1.valueTo()))
        );

        // this query might not use index also due to selectivity of predicates
        check(query("field1>=? or field1<=?", f1.valueFrom(), f1.valueTo()),
                c_sorted(),
                isNotNull()
        );
    }

    @Test
    public void testConjunctionOfRanges() {
        ///// Conjunction of ranges open on the same end
        check(query("field1>? and field1>?", f1.valueFrom(), f1.valueTo()),
                c_sorted(), gt(f1.valueTo()));
        check(query("field1>=? and field1>=?", f1.valueFrom(), f1.valueTo()),
                c_sorted(), gte(f1.valueTo()));
        check(query("field1<? and field1<?", f1.valueFrom(), f1.valueTo()),
                c_sorted(), lt(f1.valueFrom()));
        check(query("field1<=? and field1<=?", f1.valueFrom(), f1.valueTo()),
                c_sorted(), lte(f1.valueFrom()));
    }

    @Test
    public void testOrderBy() {
        check0(query("1=1 order by field1"), c_sorted(), v -> true);
        check0(query("1=1 order by field1 desc"), c_sorted(), v -> true);

        // even though we could get use composite index for ordering on prefix of columns
        // we do not do this currently
        check0(query("1=1 order by field1, field2"), c_sorted() && c_composite(), v -> true);
        check0(query("1=1 order by field1 desc, field2 desc"), c_sorted() && c_composite(), v -> true);

        // different ordering on columns cannot use composite index
        check0(query("1=1 order by field1 asc, field2 desc"), false, v -> true);
        check0(query("1=1 order by field1 desc, field2 asc"), false, v -> true);

        check0(query("1=1 order by field2"), false, v -> true);
        check0(query("1=1 order by field2 desc"), false, v -> true);
    }

        // Test helpers

    private void checkFirstColumn() {
        // WHERE f1 IS NULL
        check(query("field1 IS NULL"), c_notHashComposite(), isNull());

        // WHERE f1 IS NOT NULL
        // index with additional condition is not used due to cost estimation, full scan is slightly cheaper
        check(query("field1 IS NOT NULL"), c_sorted(), false, isNotNull());

        // WHERE f1=literal
        check(query("field1=" + toLiteral(f1, f1.valueFrom())), c_notHashComposite(), eq(f1.valueFrom()));
        check(query(toLiteral(f1, f1.valueFrom()) + "=field1"), c_notHashComposite(), eq(f1.valueFrom()));

        // WHERE f1=literal1 or f1=literal2
        check(query("field1=" + toLiteral(f1, f1.valueFrom()) + " or field1=" + toLiteral(f1, f1.valueTo())),
                c_notHashComposite(), or(eq(f1.valueFrom()), eq(f1.valueTo())));
        check(query(toLiteral(f1, f1.valueFrom()) + "=field1 or " + toLiteral(f1, f1.valueTo()) + "=field1"),
                c_notHashComposite(), or(eq(f1.valueFrom()), eq(f1.valueTo())));

        // WHERE f1=?
        check(query("field1=?", f1.valueFrom()), c_notHashComposite(), eq(f1.valueFrom()));
        check(query("?=field1", f1.valueFrom()), c_notHashComposite(), eq(f1.valueFrom()));

        // WHERE f1=? or f1=?
        check(query("field1=? or field1=?", f1.valueFrom(), f1.valueTo()),
                c_notHashComposite(), or(eq(f1.valueFrom()), eq(f1.valueTo())));
        check(query("?=field1 or ?=field1", f1.valueFrom(), f1.valueTo()),
                c_notHashComposite(), or(eq(f1.valueFrom()), eq(f1.valueTo())));

        // WHERE f1=? and f1=? - contradictory values
        check(query("field1=? and field1=?", f1.valueTo(), f1.valueFrom()),
                c_notHashComposite(), alwaysFalse());
        check(query("?=field1 and ?=field1", f1.valueFrom(), f1.valueTo()),
                c_notHashComposite(), alwaysFalse());

        // WHERE f1=? or f1 is null
        check(query("field1=? or field1 is null", f1.valueFrom()),
                c_notHashComposite(), or(eq(f1.valueFrom()), isNull()));

        // WHERE f1 is null or f1=?
        check(query("field1 is null or field1=?", f1.valueFrom()),
                c_notHashComposite(), or(eq(f1.valueFrom()), isNull()));

        // WHERE f1=literal or f1 is null
        check(query("field1=" + toLiteral(f1, f1.valueFrom()) + " or field1 is null"),
                c_notHashComposite(), or(eq(f1.valueFrom()), isNull()));

        // WHERE f1 is null or f1=literal
        check(query("field1 is null or field1=" + toLiteral(f1, f1.valueFrom())),
                c_notHashComposite(), or(eq(f1.valueFrom()), isNull()));

        // WHERE f1!=literal
        check(query("field1!=" + toLiteral(f1, f1.valueFrom())), c_booleanComponent() && c_notHashComposite(), neq(f1.valueFrom()));
        check(query(toLiteral(f1, f1.valueFrom()) + "!=field1"), c_booleanComponent() && c_notHashComposite(), neq(f1.valueFrom()));

        // WHERE f1!=?
        check(query("field1!=?", f1.valueFrom()), false, neq(f1.valueFrom()));
        check(query("?!=field1", f1.valueFrom()), false, neq(f1.valueFrom()));

        // WHERE f1>literal
        check(query("field1>" + toLiteral(f1, f1.valueFrom())), c_sorted() || c_booleanComponent() && c_notHashComposite(), gt(f1.valueFrom()));
        check(query(toLiteral(f1, f1.valueFrom()) + "<field1"), c_sorted() || c_booleanComponent() && c_notHashComposite(), gt(f1.valueFrom()));

        // WHERE f1>?
        check(query("field1>?", f1.valueFrom()), c_sorted(), gt(f1.valueFrom()));
        check(query("?<field1", f1.valueFrom()), c_sorted(), gt(f1.valueFrom()));
        check(query("field1>?", null), c_sorted(), alwaysFalse());
        check(query("?<field1", null), c_sorted(), alwaysFalse());

        // WHERE f1>=literal
        check(query("field1>=" + toLiteral(f1, f1.valueFrom())), c_sorted(), gte(f1.valueFrom()));
        check(query(toLiteral(f1, f1.valueFrom()) + "<=field1"), c_sorted(), gte(f1.valueFrom()));

        // WHERE f1>=?
        check(query("field1>=?", f1.valueFrom()), c_sorted(), gte(f1.valueFrom()));
        check(query("?<=field1", f1.valueFrom()), c_sorted(), gte(f1.valueFrom()));
        check(query("field1>=?", null), c_sorted(), alwaysFalse());
        check(query("?<=field1", null), c_sorted(), alwaysFalse());

        // WHERE f1<literal
        check(query("field1<" + toLiteral(f1, f1.valueFrom())), c_sorted(), lt(f1.valueFrom()));
        check(query(toLiteral(f1, f1.valueFrom()) + ">field1"), c_sorted(), lt(f1.valueFrom()));

        // WHERE f1<?
        check(query("field1<?", f1.valueFrom()), c_sorted(), lt(f1.valueFrom()));
        check(query("?>field1", f1.valueFrom()), c_sorted(), lt(f1.valueFrom()));
        check(query("field1<?", null), c_sorted(), alwaysFalse());
        check(query("?<field1", null), c_sorted(), alwaysFalse());

        // WHERE f1<=literal
        check(query("field1<=" + toLiteral(f1, f1.valueFrom())), c_sorted() || c_booleanComponent() && c_notHashComposite(), lte(f1.valueFrom()));
        check(query(toLiteral(f1, f1.valueFrom()) + ">=field1"), c_sorted() || c_booleanComponent() && c_notHashComposite(), lte(f1.valueFrom()));

        // WHERE f1<=?
        check(query("field1<=?", f1.valueFrom()), c_sorted(), lte(f1.valueFrom()));
        check(query("?>=field1", f1.valueFrom()), c_sorted(), lte(f1.valueFrom()));
        check(query("field1<=?", null), c_sorted(), alwaysFalse());
        check(query("?>=field1", null), c_sorted(), alwaysFalse());

        ///// single range from...to
        if (!(f1 instanceof ExpressionType.BooleanType)) {
            // WHERE f1>literal AND f1<literal
            check(
                    query("field1>" + toLiteral(f1, f1.valueFrom()) + " AND field1<" + toLiteral(f1, f1.valueTo())),
                    c_sorted(),
                    and(gt(f1.valueFrom()), lt(f1.valueTo()))
            );

            // WHERE f1>literal AND f1<=literal
            check(
                    query("field1>" + toLiteral(f1, f1.valueFrom()) + " AND field1<=" + toLiteral(f1, f1.valueTo())),
                    c_sorted(),
                    and(gt(f1.valueFrom()), lte(f1.valueTo()))
            );

            // WHERE f1>=literal AND f1<literal
            check(
                    query("field1>=" + toLiteral(f1, f1.valueFrom()) + " AND field1<" + toLiteral(f1, f1.valueTo())),
                    c_sorted(),
                    and(gte(f1.valueFrom()), lt(f1.valueTo()))
            );
        }

        // WHERE f1>=literal AND f1<=literal
        check(
                query("field1>=" + toLiteral(f1, f1.valueFrom()) + " AND field1<=" + toLiteral(f1, f1.valueTo())),
                c_sorted(),
                and(gte(f1.valueFrom()), lte(f1.valueTo()))
        );

        // WHERE f1>? AND f1<?
        check(
                query("field1>? AND field1<?", f1.valueFrom(), f1.valueTo()),
                c_sorted(),
                and(gt(f1.valueFrom()), lt(f1.valueTo()))
        );
        check(
                query("field1>? AND field1<?", f1.valueTo(), f1.valueFrom()),
                c_sorted(),
                alwaysFalse()
        );
        check(
                query("field1>? AND field1<?", f1.valueFrom(), f1.valueFrom()),
                c_sorted(),
                alwaysFalse()
        );

        // WHERE f1>? AND f1<=?
        check(
                query("field1>? AND field1<=?", f1.valueFrom(), f1.valueTo()),
                c_sorted(),
                and(gt(f1.valueFrom()), lte(f1.valueTo()))
        );
        check(
                query("field1>? AND field1<=?", f1.valueTo(), f1.valueFrom()),
                c_sorted(),
                alwaysFalse()
        );
        check(
                query("field1>? AND field1<=?", f1.valueFrom(), f1.valueFrom()),
                c_sorted(),
                alwaysFalse()
        );

        // WHERE f1>=? AND f1<?
        check(
                query("field1>=? AND field1<?", f1.valueFrom(), f1.valueTo()),
                c_sorted(),
                and(gte(f1.valueFrom()), lt(f1.valueTo()))
        );
        check(
                query("field1>=? AND field1<?", f1.valueTo(), f1.valueFrom()),
                c_sorted(),
                alwaysFalse()
        );
        check(
                query("field1>=? AND field1<?", f1.valueFrom(), f1.valueFrom()),
                c_sorted(),
                alwaysFalse()
        );

        // WHERE f1>=? AND f1<=?
        check(
                query("field1>=? AND field1<=?", f1.valueFrom(), f1.valueTo()),
                c_sorted(),
                and(gte(f1.valueFrom()), lte(f1.valueTo()))
        );
        check(
                query("field1>=? AND field1<=?", f1.valueTo(), f1.valueFrom()),
                c_sorted(),
                alwaysFalse()
        );
        check(
                query("field1>=? AND field1<=?", f1.valueFrom(), f1.valueFrom()),
                c_sorted(),
                eq(f1.valueFrom())
        );

        ///// 2 disjoint unlimited ranges

        // WHERE f1<literal OR f1>literal (range from -inf..val1 and val2..+inf)
        check(
                query("field1<" + toLiteral(f1, f1.valueFrom()) + " OR field1>" + toLiteral(f1, f1.valueTo())),
                c_sorted(),
                or(lt(f1.valueFrom()), gt(f1.valueTo()))
        );
        // WHERE f1<? OR f1>? (range from -inf..val1 and val2..+inf)
        check(
                query("field1<? OR field1>?", f1.valueFrom(), f1.valueTo()),
                c_sorted(),
                or(lt(f1.valueFrom()), gt(f1.valueTo()))
        );

        ///// 2 limited ranges - disjoint and overlapping

        if (!(f1 instanceof ExpressionType.BooleanType)) {
            // WHERE (f1>=literal and f1<=literal) OR f1=literal (range and equality)
            check(
                    query("(field1>=" + toLiteral(f1, f1.valueFrom()) + " AND field1<=" + toLiteral(f1, f1.valueMiddle()) +
                            ") OR field1=" + toLiteral(f1, f1.valueTo())),
                    c_sorted(),
                    or(and(gte(f1.valueFrom()), lte(f1.valueMiddle())), eq(f1.valueTo()))
            );
            // WHERE (f1>=? and f1<=?) OR f1=? (range and equality)
            check(
                    query("(field1>=? AND field1<=?) OR field1=?", f1.valueFrom(), f1.valueMiddle(), f1.valueTo()),
                    // OR is currently not converted to union of index results or index scan with many ranges.
                    // With literals Calcite produces Sarg which is single expression so is converted to a better plan.
                    false,
                    or(and(gte(f1.valueFrom()), lte(f1.valueMiddle())), eq(f1.valueTo()))
            );

            // WHERE (f1>=literal and f1<literal) OR (f1>literal and f1<=literal)  (non-overlapping ranges)
            check(
                    query("(field1>=" + toLiteral(f1, f1.valueFrom()) + " AND field1<" + toLiteral(f1, f1.valueMiddle()) +
                            ") OR (field1>" + toLiteral(f1, f1.valueMiddle()) + " AND field1<=" + toLiteral(f1, f1.valueTo()) +
                            ")"),
                    c_sorted(),
                    or(and(gte(f1.valueFrom()), lt(f1.valueMiddle())), and(gt(f1.valueMiddle()), lte(f1.valueTo())))
            );
            check(
                    query("(field1>=? AND field1<?) OR (field1>? AND field1<=?)",
                            f1.valueFrom(), f1.valueMiddle(), f1.valueMiddle(), f1.valueTo()),
                    // OR is currently not converted to union of index results or index scan with many ranges.
                    // With literals Calcite produces Sarg which is single expression so is converted to a better plan.
                    false,
                    or(and(gte(f1.valueFrom()), lt(f1.valueMiddle())), and(gt(f1.valueMiddle()), lte(f1.valueTo())))
            );

            // WHERE (f1>=literal and f1<literal) OR (f1>literal and f1<=literal)  (non-overlapping ranges, reverse order)
            check(
                    query("(field1>" + toLiteral(f1, f1.valueMiddle()) + " AND field1<=" + toLiteral(f1, f1.valueTo()) +
                            ") OR (field1>=" + toLiteral(f1, f1.valueFrom()) + " AND field1<" + toLiteral(f1, f1.valueMiddle()) +
                            ")"),
                    c_sorted(),
                    or(and(gte(f1.valueFrom()), lt(f1.valueMiddle())), and(gt(f1.valueMiddle()), lte(f1.valueTo())))
            );
            check(
                    query("(field1>? AND field1<=?) OR (field1>=? AND field1<?)",
                            f1.valueMiddle(), f1.valueTo(), f1.valueFrom(), f1.valueMiddle()),
                    // OR is currently not converted to union of index results or index scan with many ranges.
                    // With literals Calcite produces Sarg which is single expression so is converted to a better plan.
                    false,
                    or(and(gte(f1.valueFrom()), lt(f1.valueMiddle())), and(gt(f1.valueMiddle()), lte(f1.valueTo())))
            );
        }

        // IN
        check(
                query("field1=? OR field1=?", f1.valueFrom(), f1.valueTo()),
                c_notHashComposite(),
                or(eq(f1.valueFrom()), eq(f1.valueTo()))
        );

        // Special cases for boolean field
        if (f1 instanceof ExpressionType.BooleanType) {
            // WHERE f1
            check(query("field1"), c_notHashComposite(), eq(true));

            // WHERE f1 IS TRUE
            check(query("field1 IS TRUE"), c_notHashComposite(), eq(true));

            // WHERE f1 IS FALSE
            check(query("field1 IS FALSE"), c_notHashComposite(), eq(false));

            // WHERE f1 IS NOT TRUE
            check(query("field1 IS NOT TRUE"), c_notHashComposite(), or(eq(false), isNull()));

            // WHERE f1 IS NOT FALSE
            check(query("field1 IS NOT FALSE"), c_notHashComposite(), or(eq(true), isNull()));
        }
    }

    private void checkSecondColumn() {
        // WHERE f2 IS (NOT) NULL
        check(query("field2 IS NULL"), false, isNull_2());
        check(query("field2 IS NOT NULL"), false, isNotNull_2());

        // WHERE f2<cmp>?
        check(query("field2=?", f2.valueFrom()), false, eq_2(f2.valueFrom()));
        check(query("field2!=?", f2.valueFrom()), false, neq_2(f2.valueFrom()));
        check(query("field2>?", f2.valueFrom()), false, gt_2(f2.valueFrom()));
        check(query("field2>=?", f2.valueFrom()), false, gte_2(f2.valueFrom()));
        check(query("field2<?", f2.valueFrom()), false, lt_2(f2.valueFrom()));
        check(query("field2<=?", f2.valueFrom()), false, lte_2(f2.valueFrom()));

        // WHERE f2>(=)? AND f2<(=)?
        check(query("field2>? AND field2<?", f2.valueFrom(), f2.valueTo()), false, and(gt_2(f2.valueFrom()), lt_2(f2.valueTo())));
        check(query("field2>? AND field2<=?", f2.valueFrom(), f2.valueTo()), false, and(gt_2(f2.valueFrom()), lte_2(f2.valueTo())));
        check(query("field2>=? AND field2<?", f2.valueFrom(), f2.valueTo()), false, and(gte_2(f2.valueFrom()), lt_2(f2.valueTo())));
        check(query("field2>=? AND field2<=?", f2.valueFrom(), f2.valueTo()), false, and(gte_2(f2.valueFrom()), lte_2(f2.valueTo())));

        // Special cases for boolean field
        if (f2 instanceof ExpressionType.BooleanType) {
            check(query("field2"), false, eq_2(true));
            check(query("field2 IS TRUE"), false, eq_2(true));
            check(query("field2 IS FALSE"), false, eq_2(false));
            check(query("field2 IS NOT TRUE"), false, or(eq_2(false), isNull_2()));
            check(query("field2 IS NOT FALSE"), false, or(eq_2(true), isNull_2()));
        }
    }

    private void checkBothColumns() {
        // EQ + EQ
        check(
                query("field1=? AND field2=?", f1.valueFrom(), f2.valueFrom()),
                c_always(),
                and(eq(f1.valueFrom()), eq_2(f2.valueFrom()))
        );

        // EQ + IN
        check(
                query("field1=? AND (field2=? OR field2=?)", f1.valueFrom(), f2.valueFrom(), f2.valueTo()),
                c_always(),
                and(eq(f1.valueFrom()), or(eq_2(f2.valueFrom()), eq_2(f2.valueTo())))
        );

        // EQ + RANGE
        check(
                query("field1=? AND field2>? AND field2<?", f1.valueFrom(), f2.valueFrom(), f2.valueTo()),
                c_sorted() || c_notComposite(),
                and(eq(f1.valueFrom()), and(gt_2(f2.valueFrom()), lt_2(f2.valueTo())))
        );

        // IN + EQ
        check(
                query("(field1=? OR field1=?) AND field2=?", f1.valueFrom(), f1.valueTo(), f2.valueFrom()),
                c_sorted() || c_notComposite(),
                and(or(eq(f1.valueFrom()), eq(f1.valueTo())), eq_2(f2.valueFrom()))
        );

        // IN + IN
        check(
                query("(field1=? OR field1=?) AND (field2=? OR field2=?)", f1.valueFrom(), f1.valueTo(), f2.valueFrom(), f2.valueTo()),
                c_sorted() || c_notComposite(),
                and(or(eq(f1.valueFrom()), eq(f1.valueTo())), or(eq_2(f2.valueFrom()), eq_2(f2.valueTo())))
        );

        // IN + RANGE
        check(
                query("(field1=? OR field1=?) AND (field2>? AND field2<?)", f1.valueFrom(), f1.valueTo(), f2.valueFrom(), f2.valueTo()),
                c_sorted() || c_notComposite(),
                and(or(eq(f1.valueFrom()), eq(f1.valueTo())), and(gt_2(f2.valueFrom()), lt_2(f2.valueTo())))
        );
        check(
                // both field1 parameters have the same value
                query("(field1=? OR field1=?) AND (field2>? AND field2<?)", f1.valueFrom(), f1.valueFrom(), f2.valueFrom(), f2.valueTo()),
                c_sorted() || c_notComposite(),
                and(or(eq(f1.valueFrom()), eq(f1.valueFrom())), and(gt_2(f2.valueFrom()), lt_2(f2.valueTo())))
        );

        // RANGE + EQ
        check(
                query("(field1>? AND field1<?) AND field2=?", f1.valueFrom(), f1.valueTo(), f2.valueFrom()),
                c_sorted(),
                and(and(gt(f1.valueFrom()), lt(f1.valueTo())), eq_2(f2.valueFrom()))
        );

        // RANGE + IN
        check(
                query("(field1>? AND field1<?) AND (field2=? AND field2=?)", f1.valueFrom(), f1.valueTo(), f2.valueFrom(), f2.valueTo()),
                c_sorted(),
                and(and(gt(f1.valueFrom()), lt(f1.valueTo())), and(eq_2(f2.valueFrom()), eq_2(f2.valueTo())))
        );

        // RANGE + RANGE
        check(
                query("(field1>? AND field1<?) AND (field2>? AND field2<?)", f1.valueFrom(), f1.valueTo(), f2.valueFrom(), f2.valueTo()),
                c_sorted(),
                and(and(gt(f1.valueFrom()), lt(f1.valueTo())), and(gt_2(f2.valueFrom()), lt_2(f2.valueTo())))
        );

        // IS NOT NULL/IS NULL combinations
        check(
                query("field1 IS NULL AND field2 IS NULL"),
                // lookup on all index types is possible
                true,
                and(isNull(), isNull_2())
        );
        check(
                query("field1 IS NULL AND field2 IS NOT NULL"),
                // NOT NULL is range scan so composite hash index cannot be used at all.
                // Sorted index can be used at least for prefix (field1),
                // condition on field2 should use composite index.
                c_notHashComposite(),
                and(isNull(), isNotNull_2())
        );
        check(
                query("field1 IS NOT NULL AND field2 IS NULL"),
                // basic query does not use sorted index due to poor selectivity of IS NOT NULL
                // queries with ORDER BY will use sorted index due to high sort cost.
                false,
                and(isNotNull(), isNull_2())
        );
        check(
                query("field1 IS NOT NULL AND field2 IS NOT NULL"),
                // Note that with composite sorted index field2 will not be used in the lookup range
                // because it is not possible to express such scan as bounded number of iteration pointers
                // (and also it would not bring much value due to poor selectivity).
                false,
                and(isNotNull(), isNotNull_2())
        );
    }

    private boolean c_always() {
        return true;
    }

    private boolean c_never() {
        return false;
    }

    private boolean c_sorted() {
        return indexType == IndexType.SORTED;
    }

    private boolean c_hash() {
        return indexType == IndexType.HASH;
    }

    private boolean c_composite() {
        return composite;
    }

    private boolean c_notComposite() {
        return !composite;
    }

    // NOTE: Only sorted index or HASH non-composite index should be used. Used for equality conditions.
    private boolean c_notHashComposite() {
        return c_sorted() || c_notComposite();
    }

    private boolean c_booleanComponent() {
        return f1 instanceof ExpressionType.BooleanType;
    }

    private boolean c_booleanComponentAndNotHashComposite() {
        return c_booleanComponent() && c_notHashComposite();
    }

    private void check(Query query, boolean expectedUseIndex, Predicate<ExpressionValue> expectedKeysPredicate) {
        check(query, expectedUseIndex, expectedUseIndex, expectedKeysPredicate);

    }

    private void check(Query query, boolean expectedUseIndex, boolean expectedUseIndexWithAndCondition,
                       Predicate<ExpressionValue> expectedKeysPredicate) {
        // Prepare two additional queries with an additional AND/OR predicate
        String condition = "__key / 2 = 0";
        Query queryWithAnd = addConditionToQuery(query, condition, true);
        Query queryWithOr = addConditionToQuery(query, condition, false);
        Query queryWithOrderBy = new Query(query.sql + " ORDER BY field1", query.parameters);
        Query queryWithOrderByDesc = new Query(query.sql + " ORDER BY field1 DESC", query.parameters);

        Predicate<ExpressionValue> predicate = value -> value.key / 2 == 0;
        Predicate<ExpressionValue> expectedKeysPredicateWithAnd = and(expectedKeysPredicate, predicate);
        Predicate<ExpressionValue> expectedKeysPredicateWithOr = or(expectedKeysPredicate, predicate);

        // Run the original query
        check0(query, expectedUseIndex, expectedKeysPredicate);

        // Run query with AND, the same index should be used
        check0(queryWithAnd, expectedUseIndexWithAndCondition, expectedKeysPredicateWithAnd);

        // Run query with OR, no index should be used
        check0(queryWithOr, false, expectedKeysPredicateWithOr);

        // Sorting is so costly that index should be preferred regardless of predicates
        // For hash index sorting does not use index, but scan still can use it.
        check0(queryWithOrderBy, expectedUseIndex || c_sorted(), expectedKeysPredicate);
        check0(queryWithOrderByDesc, expectedUseIndex || c_sorted(), expectedKeysPredicate);
    }

    private void check0(Query query, boolean expectedUseIndex, Predicate<ExpressionValue> expectedKeysPredicate) {
        check0(query.sql, query.parameters, expectedUseIndex, expectedKeysPredicate);
    }

    private void check0(
            String sql,
            List<Object> params,
            boolean expectedUseIndex,
            Predicate<ExpressionValue> expectedKeysPredicate
    ) {
        softly.assertThatCode(() -> {
            int runId = runIdGen++;
            checkPlan(expectedUseIndex, sql);

            // SQL might return duplicates, expectedMapKeys never contains duplicates
            Multiset<Integer> sqlKeys = sqlKeys(expectedUseIndex, sql, params);
            Set<Integer> expectedMapKeys = expectedMapKeys(expectedKeysPredicate);

            if (!sqlKeys.equals(HashMultiset.create(expectedMapKeys))) {
                failOnDifference(
                        runId,
                        sql,
                        params,
                        sqlKeys,
                        expectedMapKeys,
                        "actual SQL keys differ from expected map keys",
                        "actual SQL keys",
                        "expected map keys"
                );
            }
        }).as("Test failed for query " + sql).doesNotThrowAnyException();
    }

    @SuppressWarnings("StringConcatenationInsideStringBufferAppend")
    private void failOnDifference(
            int runId,
            String sql,
            List<Object> params,
            Multiset<Integer> first,
            Set<Integer> second,
            String mainMessage,
            String firstCaption,
            String secondCaption
    ) {
        Set<Integer> firstOnly = new TreeSet<>(first);
        Set<Integer> secondOnly = new TreeSet<>(second);

        firstOnly.removeAll(second);
        secondOnly.removeAll(first);

        List<Integer> duplicates = first.entrySet().stream()
                .filter(e -> e.getCount() > 1)
                .map(Multiset.Entry::getElement)
                .collect(Collectors.toList());

        assertTrue(!firstOnly.isEmpty() || !secondOnly.isEmpty() || !duplicates.isEmpty());

        StringBuilder message = new StringBuilder();

        message.append("\nRun " + runId + " failed: " + mainMessage + "\n\n");
        message.append("SQL: " + sql + "\n");
        message.append("Parameters: " + params + "\n\n");

        if (!firstOnly.isEmpty()) {
            message.append("\t" + firstCaption + ":\n");

            for (Integer key : firstOnly) {
                message.append("\t\t" + key + " -> " + map.get(key) + "\n");
            }
        }

        if (!secondOnly.isEmpty()) {
            message.append("\t" + secondCaption + ":\n");

            for (Integer key : secondOnly) {
                message.append("\t\t" + key + " -> " + map.get(key) + "\n");
            }
        }

        if (!duplicates.isEmpty()) {
            message.append("\tduplicated " + firstCaption + ":\n");

            for (Integer key : duplicates) {
                message.append("\t\t" + key + " -> " + map.get(key) + " occurred " + first.count(key) + " times\n");
            }
        }

        fail(message.toString());
    }

    private void checkPlan(boolean withIndex, String sql) {
        List<QueryDataType> parameterTypes = asList(QueryDataType.INT, f1.getFieldConverterType(), f2.getFieldConverterType());
        List<TableField> mapTableFields = asList(
                new MapTableField("__key", QueryDataType.INT, false, QueryPath.KEY_PATH),
                new MapTableField("field1", f1.getFieldConverterType(), false, new QueryPath("field1", false)),
                new MapTableField("field2", f2.getFieldConverterType(), false, new QueryPath("field2", false))
        );
        HazelcastTable table = partitionedTable(
                mapName,
                mapTableFields,
                getPartitionedMapIndexes(mapContainer(map), mapTableFields),
                map.size()
        );
        OptimizerTestSupport.Result optimizationResult = optimizePhysical(sql, parameterTypes, table);
        if (sql.toLowerCase(Locale.ROOT).contains("order by")) {
            // for ORDER BY queries index:
            // 1) can be used for filtering only, with separate sort step afterwards
            // 2) can be used for filtering and sorting if the same column is used for both
            // 3) can be unused

            if (withIndex) {
                if (c_sorted()) {
                    assertPlan(
                            optimizationResult.getPhysical(),
                            plan(
                                    planRow(0, CalcPhysicalRel.class),
                                    planRow(1, IndexScanMapPhysicalRel.class))
                    );
                } else {
                    // hash index does not use index for sorting, only for filtering
                    assertPlan(
                            optimizationResult.getPhysical(),
                            plan(
                                    planRow(0, CalcPhysicalRel.class),
                                    planRow(1, SortPhysicalRel.class),
                                    planRow(2, IndexScanMapPhysicalRel.class))
                    );
                }
            } else {
                assertPlan(
                        optimizationResult.getPhysical(),
                        plan(
                                planRow(0, CalcPhysicalRel.class),
                                planRow(1, SortPhysicalRel.class),
                                planRow(2, FullScanPhysicalRel.class))
                );
            }
        } else {
            assertPlan(
                    optimizationResult.getLogical(),
                    plan(planRow(0, FullScanLogicalRel.class))
            );
            assertPlan(
                    optimizationResult.getPhysical(),
                    plan(planRow(0, withIndex ? IndexScanMapPhysicalRel.class : FullScanPhysicalRel.class))
            );
        }
    }

    protected MapConfig getMapConfig() {
        return new MapConfig().setName(mapName).setBackupCount(0).addIndexConfig(getIndexConfig());
    }

    protected static Collection<Object[]> parametersQuick() {
        List<Object[]> res = new ArrayList<>();

        for (IndexType indexType : Arrays.asList(IndexType.SORTED, IndexType.HASH)) {
            for (boolean composite : Arrays.asList(true, false)) {
                for (ExpressionType<?> firstType : baseTypes()) {
                    for (ExpressionType<?> secondType : baseTypes()) {
                        res.add(new Object[]{indexType, composite, firstType, secondType});
                    }
                }
            }
        }

        return res;
    }

    /**
     * It tests all non-base types. Base type interactions were tested by quick test suite.
     */
    protected static Collection<Object[]> parametersSlow() {
        List<Object[]> res = new ArrayList<>();

        for (IndexType indexType : Arrays.asList(IndexType.SORTED, IndexType.HASH)) {
            for (boolean composite : Arrays.asList(true, false)) {
                for (ExpressionType<?> firstType : nonBaseTypes()) {
                    for (ExpressionType<?> secondType : nonBaseTypes()) {
                        res.add(new Object[]{indexType, composite, firstType, secondType});
                    }
                }
            }
        }

        return res;
    }

    private void fill() {
        // Create an object with non-null fields to initialize converters
        for (int i = 0; i < instances().length; ++i) {
            int key = getLocalKeys(instances()[i], 1, value -> value).get(0);
            ExpressionBiValue value = ExpressionBiValue.createBiValue(valueClass, key, f1.valueFrom(), f2.valueFrom());

            map.put(key, value);
            map.remove(key);
        }

        // Fill with values
        int keyCounter = 0;

        localMap = new HashMap();

        for (Object firstField : f1.values()) {
            for (Object secondField : f2.values()) {
                // Put the same value twice intentionally to test index key with multiple values
                for (int i = 0; i < 2 * DEFAULT_MEMBERS_COUNT; i++) {
                    int key = keyCounter++;
                    ExpressionBiValue value = ExpressionBiValue.createBiValue(valueClass, key, firstField, secondField);

                    localMap.put(key, value);
                }
            }
        }
        map.putAll(localMap);
    }

    private String sql(String condition) {
        return "SELECT __key FROM " + mapName + " WHERE " + condition;
    }

    private Multiset<Integer> sqlKeys(boolean withIndex, String sql, List<Object> params) {
        SqlStatement query = new SqlStatement(sql);
        // with some bugs the queries could hang, however the CI tests sometimes are very slow
        query.setTimeoutMillis(60_000);
        query.setParameters(params);

        Multiset<Integer> keys = HashMultiset.create();

        try (SqlResult result = instance().getSql().execute(query)) {
            for (SqlRow row : result) {
                keys.add(row.getObject(0));
            }
        }

        return keys;
    }

    private Set<Integer> expectedMapKeys(Predicate<ExpressionValue> predicate) {
        Set<Integer> keys = new HashSet<>();

        for (Map.Entry<Integer, ExpressionBiValue> entry : localMap.entrySet()) {
            Integer key = entry.getKey();
            ExpressionBiValue value = entry.getValue();

            if (predicate.test(value)) {
                keys.add(key);
            }
        }

        return keys;
    }

    protected IndexConfig getIndexConfig() {
        IndexConfig config = new IndexConfig().setName(INDEX_NAME).setType(indexType);

        config.addAttribute("field1");

        if (composite) {
            config.addAttribute("field2");
        }

        return config;
    }

    private static Query addConditionToQuery(Query query, String condition, boolean conjunction) {
        String sql = query.sql;

        if (sql.contains("WHERE")) {
            int openPosition = sql.indexOf("WHERE") + 6;

            sql = sql.substring(0, openPosition) + "(" + sql.substring(openPosition) + ")";

            sql = sql + " " + (conjunction ? "AND" : "OR") + " " + condition;
        } else {
            sql = sql + " WHERE " + condition;
        }

        return new Query(sql, query.parameters);
    }

    private Query query(String condition, Object... parameters) {
        // parameters == null if single null argument was passed.
        // lack of arguments => parameters is emtpy array
        return new Query(sql(condition), parameters != null ? Arrays.asList(parameters) : Arrays.asList(new Object[]{null}));
    }

    private static class Query {

        private final String sql;
        private final List<Object> parameters;

        private Query(String sql, List<Object> parameters) {
            this.sql = sql;
            this.parameters = parameters;
        }
    }
}
