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

package com.hazelcast.sql.impl.expression;

import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.calcite.OptimizerContext;
import com.hazelcast.sql.impl.calcite.SqlToQueryType;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.RexToExpressionVisitor;
import com.hazelcast.sql.impl.calcite.parse.QueryParseResult;
import com.hazelcast.sql.impl.calcite.schema.HazelcastSchema;
import com.hazelcast.sql.impl.calcite.schema.HazelcastSchemaUtils;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.calcite.schema.MapTableStatistic;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.SqlDaySecondInterval;
import com.hazelcast.sql.impl.type.SqlYearMonthInterval;
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.sql.impl.type.converter.Converters;
import com.hazelcast.sql.impl.type.converter.StringConverter;
import com.hazelcast.test.TestStringUtils;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.junit.After;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

import static com.hazelcast.sql.impl.QueryUtils.SCHEMA_NAME_REPLICATED;
import static com.hazelcast.sql.impl.calcite.SqlToQueryType.mapRowType;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.narrowestTypeFor;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.withHigherPrecedence;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.withHigherPrecedenceForLiterals;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;
import static org.apache.calcite.sql.type.SqlTypeName.ANY;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;
import static org.apache.calcite.sql.type.SqlTypeName.CHAR_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.DATE;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.FRACTIONAL_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_DAY_SECOND;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_YEAR_MONTH;
import static org.apache.calcite.sql.type.SqlTypeName.INT_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.NULL;
import static org.apache.calcite.sql.type.SqlTypeName.NUMERIC_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.REAL;
import static org.apache.calcite.sql.type.SqlTypeName.SMALLINT;
import static org.apache.calcite.sql.type.SqlTypeName.TIME;
import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP;
import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
import static org.apache.calcite.sql.type.SqlTypeName.TINYINT;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public abstract class ExpressionTestBase {

    private static final boolean VERIFY_EVALUATION = true;

    private static final boolean TRACE = true;
    private static final boolean LOG_ON_SUCCESS = false;

    protected static final boolean TEMPORAL_TYPES_ENABLED = false;

    protected static final RelDataType UNKNOWN_TYPE = null;
    protected static final SqlTypeName UNKNOWN_TYPE_NAME = null;

    protected static final Object UNKNOWN_VALUE = new Object() {
        @Override
        public String toString() {
            return "UNKNOWN";
        }
    };

    protected static final Object INVALID_VALUE = new Object() {
        @Override
        public String toString() {
            return "INVALID";
        }
    };
    protected static final BigDecimal INVALID_NUMERIC_VALUE = new BigDecimal(0);
    @SuppressWarnings({"checkstyle:IllegalInstantiation", "UnnecessaryBoxing", "BooleanConstructorCall"})
    protected static final Boolean INVALID_BOOLEAN_VALUE = new Boolean(true);

    public static final HazelcastTypeFactory TYPE_FACTORY = HazelcastTypeFactory.INSTANCE;

    protected static final List<Operand> COLUMNS = new ArrayList<>();
    protected static final List<Operand> LITERALS = new ArrayList<>();
    protected static final List<Operand> PARAMETERS = new ArrayList<>();
    protected static final List<Operand> TYPES = new ArrayList<>();

    protected static final List<Operand> BOOLEAN_COLUMN = new ArrayList<>();

    protected static final SqlOperator IDENTITY = new SqlOperator("", SqlKind.OTHER, 0, 0, null, null, null) {
        @Override
        public SqlSyntax getSyntax() {
            return SqlSyntax.PREFIX;
        }

        @Override
        public SqlOperandCountRange getOperandCountRange() {
            return SqlOperandCountRanges.of(1);
        }

        @Override
        public String toString() {
            return "IDENTITY";
        }
    };

    private static final SqlTypeName[] TYPE_NAMES;

    static {
        if (TEMPORAL_TYPES_ENABLED) {
            TYPE_NAMES = new SqlTypeName[]{VARCHAR, BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL, REAL, DOUBLE,
                    INTERVAL_YEAR_MONTH, INTERVAL_DAY_SECOND, TIME, DATE, TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE, ANY, NULL};
        } else {
            TYPE_NAMES =
                    new SqlTypeName[]{VARCHAR, BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL, REAL, DOUBLE, ANY, NULL};
        }
    }

    private static final Map<String, QueryDataType> FIELDS;
    private static final Map<String, Integer> FIELD_TO_INDEX;
    private static final PlanNodeSchema SCHEMA;

    static {
        Map<String, QueryDataType> fields = new LinkedHashMap<>();
        for (SqlTypeName type : TYPE_NAMES) {
            if (type == NULL) {
                // null is synthetic internal type
                continue;
            }

            fields.put(type.getName().toLowerCase() + "1", SqlToQueryType.map(type));
            fields.put(type.getName().toLowerCase() + "2", SqlToQueryType.map(type));
        }

        FIELDS = Collections.unmodifiableMap(fields);
        SCHEMA = new PlanNodeSchema(new ArrayList<>(FIELDS.values()));

        FIELD_TO_INDEX = new HashMap<>();
        int i = 0;
        for (String field : FIELDS.keySet()) {
            FIELD_TO_INDEX.put(field, i++);
        }
    }

    static {
        // Literals.

        LITERALS.add(numericLiteral(0));
        LITERALS.add(numericLiteral(1));
        LITERALS.add(numericLiteral(-1));
        LITERALS.add(numericLiteral(10));
        LITERALS.add(numericLiteral(-10));

        long byteMax = Byte.MAX_VALUE;
        long byteMin = Byte.MIN_VALUE;
        LITERALS.add(numericLiteral(byteMax));
        LITERALS.add(numericLiteral(byteMax - 1));
        LITERALS.add(numericLiteral(byteMax + 1));
        LITERALS.add(numericLiteral(byteMax - 2));
        LITERALS.add(numericLiteral(byteMax + 2));
        LITERALS.add(numericLiteral(byteMin));
        LITERALS.add(numericLiteral(byteMin - 1));
        LITERALS.add(numericLiteral(byteMin + 1));
        LITERALS.add(numericLiteral(byteMin - 2));
        LITERALS.add(numericLiteral(byteMin + 2));

        long shortMax = Short.MAX_VALUE;
        long shortMin = Short.MIN_VALUE;
        LITERALS.add(numericLiteral(shortMax));
        LITERALS.add(numericLiteral(shortMax - 1));
        LITERALS.add(numericLiteral(shortMax + 1));
        LITERALS.add(numericLiteral(shortMax - 2));
        LITERALS.add(numericLiteral(shortMax + 2));
        LITERALS.add(numericLiteral(shortMin));
        LITERALS.add(numericLiteral(shortMin - 1));
        LITERALS.add(numericLiteral(shortMin + 1));
        LITERALS.add(numericLiteral(shortMin - 2));
        LITERALS.add(numericLiteral(shortMin + 2));

        long intMax = Integer.MAX_VALUE;
        long intMin = Integer.MIN_VALUE;
        LITERALS.add(numericLiteral(intMax));
        LITERALS.add(numericLiteral(intMax - 1));
        LITERALS.add(numericLiteral(intMax + 1));
        LITERALS.add(numericLiteral(intMax - 2));
        LITERALS.add(numericLiteral(intMax + 2));
        LITERALS.add(numericLiteral(intMin));
        LITERALS.add(numericLiteral(intMin - 1));
        LITERALS.add(numericLiteral(intMin + 1));
        LITERALS.add(numericLiteral(intMin - 2));
        LITERALS.add(numericLiteral(intMin + 2));

        BigInteger longMax = BigInteger.valueOf(Long.MAX_VALUE);
        BigInteger longMin = BigInteger.valueOf(Long.MIN_VALUE);
        LITERALS.add(numericLiteral(longMax));
        LITERALS.add(numericLiteral(longMax.subtract(BigInteger.valueOf(1))));
        LITERALS.add(numericLiteral(longMax.add(BigInteger.valueOf(1))));
        LITERALS.add(numericLiteral(longMax.subtract(BigInteger.valueOf(2))));
        LITERALS.add(numericLiteral(longMax.add(BigInteger.valueOf(2))));
        LITERALS.add(numericLiteral(longMin));
        LITERALS.add(numericLiteral(longMin.subtract(BigInteger.valueOf(1))));
        LITERALS.add(numericLiteral(longMin.add(BigInteger.valueOf(1))));
        LITERALS.add(numericLiteral(longMin.subtract(BigInteger.valueOf(2))));
        LITERALS.add(numericLiteral(longMin.add(BigInteger.valueOf(2))));

        LITERALS.add(numericLiteral("0.0"));
        LITERALS.add(numericLiteral("1.0"));
        LITERALS.add(numericLiteral("10.01"));
        LITERALS.add(numericLiteral("-10.01"));
        LITERALS.add(numericLiteral("9223372036854775808.01"));

        LITERALS.add(numericLiteral("1" + TestStringUtils.repeat("0", HazelcastTypeSystem.MAX_DECIMAL_PRECISION)));
        LITERALS.add(numericLiteral("1" + TestStringUtils.repeat("0", HazelcastTypeSystem.MAX_DECIMAL_PRECISION) + ".01"));

        LITERALS.add(new Operand(TYPE_FACTORY.createSqlType(BOOLEAN), false, "FALSE"));
        LITERALS.add(new Operand(TYPE_FACTORY.createSqlType(BOOLEAN), true, "TRUE"));

        // produce string representations of the literals added above
        int size = LITERALS.size();
        for (int i = 0; i < size; ++i) {
            LITERALS.add(stringLiteral(LITERALS.get(i).text));
        }

        LITERALS.add(new Operand(TYPE_FACTORY.createSqlType(NULL), null, "NULL"));
        LITERALS.add(stringLiteral("abc"));

        if (TEMPORAL_TYPES_ENABLED) {
            LITERALS.add(new Operand(TYPE_FACTORY.createSqlType(INTERVAL_YEAR_MONTH), new SqlYearMonthInterval(12 + 2),
                    "INTERVAL '1-2' YEAR TO MONTH"));
            LITERALS.add(new Operand(TYPE_FACTORY.createSqlType(INTERVAL_DAY_SECOND),
                    new SqlDaySecondInterval(24 * 60 * 60 + 2 * 60 * 60 + 3 * 60 + 4, 0), "INTERVAL '1 2:3:4' DAY TO SECOND"));
            LITERALS.add(
                    new Operand(TYPE_FACTORY.createSqlType(TIME), new TimeString("01:23:45").toCalendar(), "TIME '01:23:45'"));
            LITERALS.add(new Operand(TYPE_FACTORY.createSqlType(DATE), new DateString("1111-02-03").toCalendar(),
                    "DATE '1111-02-03'"));
            LITERALS.add(
                    new Operand(TYPE_FACTORY.createSqlType(TIMESTAMP), new TimestampString("1111-02-03 01:23:45").toCalendar(),
                            "TIMESTAMP '1111-02-03 01:23:45'"));
        }

        // Columns and types as seen in CASTs.

        for (SqlTypeName typeName : TYPE_NAMES) {
            if (typeName == NULL) {
                // null is synthetic internal type
                continue;
            }

            RelDataType type;
            switch (typeName) {
                case INTERVAL_YEAR_MONTH:
                    type = TYPE_FACTORY.createSqlIntervalType(
                            new SqlIntervalQualifier(TimeUnit.YEAR, TimeUnit.MONTH, SqlParserPos.ZERO));
                    break;
                case INTERVAL_DAY_SECOND:
                    type = TYPE_FACTORY.createSqlIntervalType(
                            new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.SECOND, SqlParserPos.ZERO));
                    break;
                default:
                    type = TYPE_FACTORY.createSqlType(typeName);
                    break;
            }

            TYPES.add(new Operand(UNKNOWN_TYPE, type, unparse(type)));

            type = TYPE_FACTORY.createTypeWithNullability(type, true);
            COLUMNS.add(new Operand(type, UNKNOWN_VALUE, typeName.getName().toLowerCase() + "1"));
            COLUMNS.add(new Operand(type, UNKNOWN_VALUE, typeName.getName().toLowerCase() + "2"));

            if (typeName == BOOLEAN) {
                BOOLEAN_COLUMN.add(new Operand(type, UNKNOWN_VALUE, typeName.getName().toLowerCase() + "1"));
            }
        }

        // Parameters: one is enough, every occurrence produces a new parameter.

        PARAMETERS.add(new Operand(UNKNOWN_TYPE, UNKNOWN_VALUE, "?"));
    }

    protected static final List<Operand> ALL = combine(COLUMNS, LITERALS, PARAMETERS);

    private final ExecutorService executor = Executors.newWorkStealingPool(RuntimeAvailableProcessors.get());

    @FunctionalInterface
    protected interface ExpectedTypes {

        RelDataType[] compute(Operand[] operands);

    }

    @FunctionalInterface
    protected interface ExpectedValues {

        Object compute(Operand[] operands, RelDataType[] types, Object[] args);

    }

    @After
    public void after() {
        // ease the pains of thread deallocation
        executor.shutdown();
    }

    @SafeVarargs
    protected final void verify(SqlOperator operator, ExpectedTypes expectedTypes, ExpectedValues expectedValues,
                                List<Operand>... operands) {
        verify(operator, -1, -1, expectedTypes, expectedValues, operands);
    }

    @SafeVarargs
    protected final void verify(SqlOperator operator, ExpectedTypes expectedTypes, ExpectedValues expectedValues, String format,
                                List<Operand>... operands) {
        verify(operator, -1, -1, expectedTypes, expectedValues, format, operands);
    }

    @SafeVarargs
    @SuppressWarnings("SameParameterValue")
    protected final void verify(SqlOperator operator, int invocationId, int evaluationId, ExpectedTypes expectedTypes,
                                ExpectedValues expectedValues, List<Operand>... operands) {
        String format;
        switch (operator.getSyntax()) {
            case PREFIX:
                assert operands.length == 1;
                format = operator.getName() + "(%s)";
                break;

            case BINARY:
                assert operands.length == 2;
                format = "%s " + operator.getName() + " %s";
                break;

            default:
                throw new IllegalArgumentException("unexpected syntax: " + operator.getSyntax());
        }

        verify(operator, invocationId, evaluationId, expectedTypes, expectedValues, format, operands);
    }

    @SafeVarargs
    protected final void verify(SqlOperator operator, int invocationId, int evaluationId, ExpectedTypes expectedTypes,
                                ExpectedValues expectedValues, String format, List<Operand>... operands) {
        if (TRACE) {
            String[] labels = new String[operands.length];
            for (int i = 0; i < operands.length; ++i) {
                labels[i] = "op" + (i + 1);
            }
            System.out.print("verifying '" + format(format, l -> l, labels) + "', ");
        }

        Collection<BiTuple<Future<?>, StringBuilder>> invocations = new ArrayList<>();
        int id = 0;

        int[] indexes = new int[operands.length];
        outer:
        while (true) {
            if (invocationId == -1 || id == invocationId) {
                Operand[] args = new Operand[operands.length];
                for (int i = 0; i < args.length; ++i) {
                    args[i] = operands[i].get(indexes[i]);
                }
                StringBuilder trace = TRACE ? new StringBuilder() : null;

                if (trace != null) {
                    trace.append("id: ").append(id).append('\n');
                }

                Future<?> future =
                        executor.submit(() -> verify(operator, expectedTypes, expectedValues, trace, evaluationId, format, args));
                invocations.add(BiTuple.of(future, trace));
            }

            // Generate next substitution (Cartesian product).

            ++id;
            for (int i = indexes.length - 1; i >= 0; --i) {
                if (++indexes[i] < operands[i].size()) {
                    break;
                } else {
                    if (i == 0) {
                        break outer;
                    }
                    indexes[i] = 0;
                }
            }
        }

        if (TRACE) {
            System.out.println(invocations.size() + " invocation(s) generated");
            System.out.println();
        }

        for (BiTuple<Future<?>, StringBuilder> invocation : invocations) {
            Future<?> future = invocation.element1;
            StringBuilder trace = invocation.element2;

            try {
                future.get();

                if (TRACE && LOG_ON_SUCCESS) {
                    System.out.println(trace);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                if (TRACE) {
                    System.out.println(trace);
                }

                if (e.getCause() instanceof RuntimeException) {
                    throw (RuntimeException) e.getCause();
                } else if (e.getCause() instanceof Error) {
                    throw (Error) e.getCause();
                } else {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @SuppressWarnings("checkstyle:NestedIfDepth")
    protected static RelDataType[] inferTypes(Operand[] operands, boolean assumeNumeric) {
        // Infer return type from columns and sub-expressions.

        RelDataType commonType = null;
        boolean seenParameters = false;
        boolean seenChar = false;

        for (Operand operand : operands) {
            RelDataType operandType = operand.type;
            if (operand.isLiteral()) {
                continue;
            }

            if (operand.isParameter()) {
                seenParameters = true;
            } else {
                commonType = commonType == null ? operandType : withHigherPrecedence(operandType, commonType);
                seenChar |= isChar(operandType);
            }
        }

        // Continue inference on numeric literals.

        for (Operand operand : operands) {
            if (!operand.isNumericLiteral()) {
                continue;
            }

            BigDecimal numeric = (BigDecimal) operand.value;
            RelDataType literalType = narrowestTypeFor(numeric, commonType == null ? null : typeName(commonType));
            commonType = commonType == null ? literalType : withHigherPrecedenceForLiterals(literalType, commonType);
        }

        // Continue inference on non-numeric literals.

        for (Operand operand : operands) {
            RelDataType operandType = operand.type;
            if (!operand.isLiteral() || isNumeric(operandType)) {
                continue;
            }

            if (isChar(operandType) && (commonType != null && isNumeric(commonType) || assumeNumeric)) {
                // Infer proper numeric type for char literals.

                BigDecimal numeric = operand.numericValue();
                assert numeric != null;
                operandType = narrowestTypeFor(numeric, commonType == null ? null : typeName(commonType));
            }

            commonType = commonType == null ? operandType : withHigherPrecedenceForLiterals(operandType, commonType);
        }

        // seen only parameters
        if (commonType == null) {
            assert seenParameters;
            return null;
        }

        // can't infer parameter types if seen only NULLs
        if (typeName(commonType) == NULL && seenParameters) {
            return null;
        }

        // fallback to DOUBLE from CHAR, if numeric types assumed
        if (isChar(commonType) && assumeNumeric) {
            commonType = TYPE_FACTORY.createSqlType(DOUBLE);
        }

        // widen integer common type: then ? ... then 1 -> BIGINT instead of TINYINT
        if ((seenParameters || seenChar) && isInteger(commonType)) {
            commonType = TYPE_FACTORY.createSqlType(BIGINT);
        }

        // Assign final types to everything based on the inferred common type.

        RelDataType[] types = new RelDataType[operands.length + 1];
        boolean nullable = false;
        for (int i = 0; i < operands.length; ++i) {
            Operand operand = operands[i];
            RelDataType operandType = operand.type;

            if (operand.isParameter()) {
                types[i] = TYPE_FACTORY.createTypeWithNullability(commonType, true);
                nullable = true;
            } else if (operand.isLiteral()) {
                if (operand.value == null) {
                    types[i] = TYPE_FACTORY.createTypeWithNullability(commonType, true);
                    nullable = true;
                } else if (isNumeric(operandType) || (isChar(operandType) && isNumeric(commonType))) {
                    // Assign final numeric types to numeric and char literals.

                    BigDecimal numeric = operand.numericValue();
                    assert numeric != null;
                    RelDataType literalType;
                    if (typeName(commonType) == DECIMAL) {
                        // always enforce DECIMAL interpretation if common type is DECIMAL
                        literalType = TYPE_FACTORY.createSqlType(DECIMAL);
                    } else {
                        literalType = narrowestTypeFor(numeric, typeName(commonType));
                        if (assumeNumeric && isFloatingPoint(commonType)) {
                            // directly use floating-point representation in numeric contexts
                            literalType = withHigherPrecedence(literalType, commonType);
                            literalType = TYPE_FACTORY.createTypeWithNullability(literalType, false);
                        }
                    }
                    types[i] = literalType;
                } else if (isChar(operandType) && !isChar(commonType) && typeName(commonType) != ANY) {
                    // If common type is non-numeric, just assign it to char literals.

                    types[i] = TYPE_FACTORY.createTypeWithNullability(commonType, false);
                } else {
                    // All other literal types.

                    types[i] = operandType;
                    nullable |= typeName(operandType) == NULL;
                }
            } else {
                // Columns and sub-expressions.

                RelDataType type;
                if (isNumeric(operandType) && typeName(commonType) == DECIMAL) {
                    // always enforce cast to DECIMAL if common type is DECIMAL
                    type = commonType;
                } else if (isChar(operandType) && typeName(commonType) != ANY) {
                    // cast char to common type
                    type = commonType;
                } else {
                    type = operandType;
                }
                types[i] = TYPE_FACTORY.createTypeWithNullability(type, operandType.isNullable());
                nullable |= operandType.isNullable();
            }
        }

        commonType = TYPE_FACTORY.createTypeWithNullability(commonType, nullable);
        types[types.length - 1] = commonType;

        return types;
    }

    protected List<SqlNode> extractOperands(SqlCall call, int expectedOperandCount) {
        return call.getOperandList();
    }

    private void verify(SqlOperator operator, ExpectedTypes expectedTypes, ExpectedValues expectedValues, StringBuilder trace,
                        int evaluationId, String format, Operand... operands) {
        OptimizerContext optimizerContext = makeContext();
        SqlValidator validator = optimizerContext.getValidator();

        String query = "SELECT ";
        query += format(format, o -> o.text, operands);
        query += " FROM t";

        if (trace != null) {
            trace.append("query:  ").append(query).append('\n');
        }

        try {
            SqlNode sqlNode = optimizerContext.parse(query, false).getNode();
            assert sqlNode instanceof SqlSelect;
            if (trace != null) {
                trace.append("parsed: ").append(sqlNode).append('\n');
            }

            RelDataType rowType = nodeType(sqlNode, validator);
            assert rowType.isStruct();
            assert rowType.getFieldCount() == 1;
            RelDataType rowReturnType = rowType.getFieldList().get(0).getType();

            SqlNodeList selectList = ((SqlSelect) sqlNode).getSelectList();
            assert selectList.size() == 1;
            SqlNode node = selectList.get(0);
            RelDataType[] actual;
            if (node instanceof SqlIdentifier || node instanceof SqlLiteral) {
                assert operator.getOperandCountRange().isValidCount(1) && operands.length == 1;
                RelDataType type = nodeType(node, validator);
                actual = new RelDataType[]{type, type};
            } else {
                assert node instanceof SqlCall;
                SqlCall call = (SqlCall) node;
                assertSame(operator, call.getOperator());

                List<SqlNode> operandList = extractOperands(call, operands.length);
                assert operandList.size() == operands.length;

                actual = new RelDataType[operandList.size() + 1];
                for (int i = 0; i < actual.length - 1; ++i) {
                    actual[i] = nodeType(operandList.get(i), validator);
                }
                actual[actual.length - 1] = nodeType(call, validator);
            }

            RelDataType actualReturnType = actual[actual.length - 1];
            assertSame(actualReturnType, rowReturnType);

            RelDataType[] expected = expectedTypes.compute(operands);
            RelDataType expectedReturnType = expected == null ? null : expected[expected.length - 1];

            if (trace != null) {
                trace.append("actual:   ").append(format(format, RelDataType::getFullTypeString, actual)).append(" -> ").append(
                        actualReturnType.getFullTypeString()).append('\n');
                if (expected == null) {
                    trace.append("expected: invalid");
                } else {
                    trace.append("expected: ").append(format(format, RelDataType::getFullTypeString, expected)).append(
                            " -> ").append(expectedReturnType.getFullTypeString()).append('\n');
                }
            }

            if (expected == null) {
                fail(query + ": " + "expected to be invalid");
            }

            assertArrayEquals(expected, actual);

            if (VERIFY_EVALUATION) {
                RelNode relNode = optimizerContext.convert(sqlNode).getRel();
                Expression<?> expression = convertToExpression(relNode, validator.getParameterRowType(sqlNode));

                verifyEvaluation(expected, operands, expression, expectedValues, evaluationId);
            }
        } catch (QueryException e) {
            RelDataType[] expected = expectedTypes.compute(operands);

            if (trace != null) {
                trace.append(e.getMessage()).append('\n');
            }

            if (expected != null) {
                if (trace != null) {
                    StringWriter stringWriter = new StringWriter();
                    e.printStackTrace(new PrintWriter(stringWriter));
                    trace.append(stringWriter.toString()).append('\n');
                }
                fail(query + ": expected " + Arrays.toString(expected) + ", got invalid");
            }
        }
    }

    private void verifyEvaluation(RelDataType[] types, Operand[] operands, Expression<?> expression,
                                  ExpectedValues expectedValues, int evaluationId) {

        // Preallocate row and parameters.

        Object[] columns = new Object[FIELDS.size()];
        Row row = new Row() {
            @SuppressWarnings("unchecked")
            @Override
            public <T> T get(int index) {
                return (T) columns[index];
            }

            @Override
            public int getColumnCount() {
                return columns.length;
            }
        };

        List<Object> parameters = new ArrayList<>();

        // Calculate possible substitutions of values.

        List<List<Operand>> substitutions = new ArrayList<>();
        int[] parameterIndexes = new int[operands.length];
        int parameterCount = 0;
        for (int i = 0; i < operands.length; i++) {
            Operand operand = operands[i];
            if (operand.isParameter()) {
                parameterIndexes[i] = parameterCount++;
                parameters.add(null);
            } else {
                parameterIndexes[i] = -1;
            }
            substitutions.add(operand.substitutions(types[i]));
        }

        int id = 0;
        int[] indexes = new int[operands.length];
        Object[] args = new Object[operands.length];
        outer:
        while (true) {
            if (evaluationId == -1 || id == evaluationId) {
                // Substitute values to row and parameters.

                for (int i = 0; i < indexes.length; ++i) {
                    Operand substitution = substitutions.get(i).get(indexes[i]);

                    Operand operand = operands[i];
                    if (operand.isColumn()) {
                        columns[FIELD_TO_INDEX.get(operand.text)] = convertLiteralOrType(substitution, operand.type);
                    } else if (operand.isParameter()) {
                        parameters.set(parameterIndexes[i], convertLiteralOrType(substitution, types[i]));
                    }
                }

                // Substitute values for expected value calculation.

                for (int i = 0; i < indexes.length; ++i) {
                    Operand substitution = substitutions.get(i).get(indexes[i]);
                    Operand operand = operands[i];
                    if (operand.isLiteral() || operand.isType()) {
                        args[i] = convertLiteralOrType(substitution, types[i]);
                    } else if (operand.isColumn()) {
                        Object columnValue = columns[FIELD_TO_INDEX.get(operand.text)];
                        if (columnValue == null) {
                            args[i] = null;
                        } else {
                            Converter valueConverter = Converters.getConverter(columnValue.getClass());
                            Converter typeConverter = SqlToQueryType.map(types[i].getSqlTypeName()).getConverter();
                            try {
                                args[i] = typeConverter.convertToSelf(valueConverter, columnValue);
                            } catch (QueryException e) {
                                assert e.getCode() == SqlErrorCode.DATA_EXCEPTION;
                                args[i] = INVALID_VALUE;
                            }
                        }
                    } else {
                        assert operand.isParameter();
                        args[i] = parameters.get(parameterIndexes[i]);
                    }
                }

                // Evaluate and assert.

                Object actual;
                try {
                    actual = expression.eval(row, parameters::get);
                } catch (QueryException e) {
                    assert e.getCode() == SqlErrorCode.DATA_EXCEPTION : "id=" + id + ", error=" + e;
                    actual = INVALID_VALUE;
                }

                Object expected = expectedValues.compute(operands, types, args);

                assertEquals(id + ": " + Arrays.toString(args), expected, actual);
            }

            // Generate next substitution (Cartesian product).

            ++id;
            for (int i = indexes.length - 1; i >= 0; --i) {
                if (++indexes[i] < substitutions.get(i).size()) {
                    break;
                } else {
                    if (i == 0) {
                        break outer;
                    }
                    indexes[i] = 0;
                }
            }
        }
    }

    protected static final class Operand {

        private static final Map<RelDataType, List<Operand>> SUBSTITUTIONS_CACHE = new ConcurrentHashMap<>();

        public final RelDataType type;
        protected final Object value;
        private final String text;

        private Operand(RelDataType type, Object value, String text) {
            this.type = type;
            this.value = value;
            this.text = text;
        }

        public boolean isLiteral() {
            return type != UNKNOWN_TYPE && value != UNKNOWN_VALUE;
        }

        protected boolean isNumericLiteral() {
            return isLiteral() && value instanceof BigDecimal;
        }

        public boolean isParameter() {
            return type == UNKNOWN_TYPE && value == UNKNOWN_VALUE;
        }

        protected boolean isType() {
            return type == UNKNOWN_TYPE && value instanceof RelDataType;
        }

        public boolean isColumn() {
            return type != UNKNOWN_TYPE && value == UNKNOWN_VALUE;
        }

        public SqlTypeName typeName() {
            return ExpressionTestBase.typeName(type);
        }

        public BigDecimal numericValue() {
            if (!isLiteral()) {
                return null;
            }

            if (isChar(type)) {
                try {
                    return StringConverter.INSTANCE.asDecimal(value);
                } catch (QueryException e) {
                    return INVALID_NUMERIC_VALUE;
                }
            } else {
                return value instanceof BigDecimal ? (BigDecimal) value : null;
            }
        }

        public Boolean booleanValue() {
            assert isLiteral();
            assert value != null;

            if (isChar(type)) {
                try {
                    return StringConverter.INSTANCE.asBoolean(value);
                } catch (QueryException e) {
                    assert e.getCode() == SqlErrorCode.DATA_EXCEPTION;
                    return INVALID_BOOLEAN_VALUE;
                }
            } else {
                return value instanceof Boolean ? (Boolean) value : INVALID_BOOLEAN_VALUE;
            }
        }

        protected List<Operand> substitutions(RelDataType as) {
            if (isLiteral() || isType()) {
                return Collections.singletonList(this);
            }

            as = isParameter() ? as : type;
            List<Operand> substitutions = SUBSTITUTIONS_CACHE.get(as);
            if (substitutions != null) {
                return substitutions;
            }

            substitutions = new ArrayList<>();
            for (Operand literal : LITERALS) {
                if (as.getSqlTypeName() == ANY) {
                    substitutions.add(literal);
                    continue;
                }

                if (literal.type.getSqlTypeName() == NULL && as.isNullable()) {
                    substitutions.add(literal);
                    continue;
                }

                if (literal.type.getSqlTypeName().getFamily() != as.getSqlTypeName().getFamily()) {
                    continue;
                }

                if (convertLiteralOrType(literal, as) == INVALID_VALUE) {
                    continue;
                }

                substitutions.add(literal);
            }

            SUBSTITUTIONS_CACHE.put(as, substitutions);
            return substitutions;
        }

    }

    protected static SqlTypeName typeName(RelDataType type) {
        return type == UNKNOWN_TYPE ? UNKNOWN_TYPE_NAME : type.getSqlTypeName();
    }

    protected static boolean isChar(RelDataType type) {
        return CHAR_TYPES.contains(typeName(type));
    }

    protected static boolean isInteger(RelDataType type) {
        return INT_TYPES.contains(typeName(type));
    }

    protected static boolean isFloatingPoint(RelDataType type) {
        return FRACTIONAL_TYPES.contains(typeName(type));
    }

    protected static boolean isNumeric(RelDataType type) {
        return NUMERIC_TYPES.contains(typeName(type));
    }

    protected static boolean isNull(RelDataType type) {
        return typeName(type) == NULL;
    }

    protected static boolean canRepresentLiteral(Operand literal, RelDataType as) {
        assert literal.isLiteral();
        return canRepresentLiteral(literal.value, literal.type, as);
    }

    protected static boolean canRepresentLiteral(Object value, RelDataType type, RelDataType as) {
        return HazelcastTypeSystem.canConvert(value, type, as);
    }

    protected static boolean canCastLiteral(Operand literal, RelDataType from, RelDataType to) {
        assert literal.isLiteral();

        if (!HazelcastTypeSystem.canConvert(literal.value, literal.type, from)) {
            return false;
        }

        if (!HazelcastTypeSystem.canConvert(literal.value, literal.type, to)) {
            return false;
        }

        //noinspection RedundantIfStatement
        if (!HazelcastTypeSystem.canConvert(literal.value, from, to)) {
            return false;
        }

        return true;
    }

    @SafeVarargs
    protected static List<Operand> combine(List<Operand>... operandSets) {
        List<Operand> operands = new ArrayList<>();
        for (List<Operand> operandSet : operandSets) {
            operands.addAll(operandSet);
        }
        return operands;
    }

    protected static Number number(Object value) {
        return (Number) value;
    }

    private static OptimizerContext makeContext() {
        List<TableField> fields = new ArrayList<>();
        for (Map.Entry<String, QueryDataType> entry : FIELDS.entrySet()) {
            fields.add(new TableField(entry.getKey(), entry.getValue(), false));
        }

        PartitionedMapTable table =
                new PartitionedMapTable(SCHEMA_NAME_REPLICATED, "t", fields, new ConstantTableStatistics(100), null, null, null,
                        null, emptyList(), PartitionedMapTable.DISTRIBUTION_FIELD_ORDINAL_NONE);

        HazelcastTable hazelcastTable = new HazelcastTable(table, new MapTableStatistic(100));
        return OptimizerContext.create(null, new HazelcastSchema(singletonMap("t", hazelcastTable)),
                HazelcastSchemaUtils.prepareSearchPaths(null, null), 1);
    }

    private static RelDataType nodeType(SqlNode node, SqlValidator validator) {
        return validator.getValidatedNodeType(node);
    }

    private static Operand numericLiteral(long value) {
        return numericLiteral(Long.toString(value));
    }

    private static Operand numericLiteral(BigInteger value) {
        return numericLiteral(value.toString());
    }

    private static Operand numericLiteral(String text) {
        return new Operand(TYPE_FACTORY.createSqlType(DECIMAL), new BigDecimal(text), text);
    }

    private static Operand stringLiteral(String string) {
        return new Operand(TYPE_FACTORY.createSqlType(VARCHAR), string, "'" + string + "'");
    }

    private static String unparse(RelDataType type) {
        return type.getSqlTypeName() == TIMESTAMP_WITH_LOCAL_TIME_ZONE ? "TIMESTAMP WITH LOCAL TIME ZONE" : type.toString();
    }

    /**
     * Makes a mock binding with a SqlCall corresponding to the given expression
     * and its operand types mocked by the given types. Basically, just allows
     * building an expression without messing with SqlNodes directly while still
     * controlling the produced node types.
     */
    public static SqlCallBinding makeMockBinding(String expression, RelDataType... types) {
        OptimizerContext context = makeContext();
        QueryParseResult parseResult = context.parse("select " + expression + " from t", false);

        SqlSelect select = (SqlSelect) parseResult.getNode();
        SqlValidator validator = context.getValidator();
        SqlValidatorScope scope = validator.getSelectScope(select);

        return new SqlCallBinding(validator, scope, (SqlCall) select.getSelectList().get(0)) {
            @Override
            public RelDataType getOperandType(int ordinal) {
                return types[ordinal] == null ? validator.getUnknownType() : types[ordinal];
            }
        };
    }

    private static Expression<?> convertToExpression(RelNode relNode, RelDataType parameterRowType) {
        assert relNode instanceof Project;
        Project project = (Project) relNode;
        assert project.getProjects().size() == 1;

        RexNode rexNode = project.getProjects().get(0);

        QueryParameterMetadata parameterMetadata = new QueryParameterMetadata(mapRowType(parameterRowType));
        return rexNode.accept(new RexToExpressionVisitor(SCHEMA, parameterMetadata));
    }

    private static Object convertLiteralOrType(Operand operand, RelDataType as) {
        assert operand.isLiteral() || operand.isType();

        if (operand.value == null) {
            return null;
        }

        if (operand.isType()) {
            return SqlToQueryType.map(((RelDataType) operand.value).getSqlTypeName());
        }

        Converter valueConverter = Converters.getConverter(operand.value.getClass());
        Converter asConverter = SqlToQueryType.map(as.getSqlTypeName()).getConverter();

        try {
            return asConverter.convertToSelf(valueConverter, operand.value);
        } catch (QueryException e) {
            assert e.getCode() == SqlErrorCode.DATA_EXCEPTION;
            return INVALID_VALUE;
        }
    }

    @SafeVarargs
    private static <O> String format(String format, Function<O, String> operandTransform, O... operands) {
        Object[] args = new Object[operands.length];
        for (int i = 0; i < operands.length; ++i) {
            args[i] = operandTransform.apply(operands[i]);
        }
        return String.format(format, args);
    }

}
