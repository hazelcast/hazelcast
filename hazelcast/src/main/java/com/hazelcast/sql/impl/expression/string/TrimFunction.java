/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.expression.string;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.BiExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;

public class TrimFunction extends BiExpression<String> implements IdentifiedDataSerializable {

    private static final String SPACE_ONLY = " ";
    private static final CharacterTester SPACE_ONLY_TESTER = new SingleCharacterTester(' ');

    private boolean leading;
    private boolean trailing;

    public TrimFunction() {
        // No-op.
    }

    private TrimFunction(Expression<?> input, Expression<?> characters, boolean leading, boolean trailing) {
        super(input, characters);

        this.leading = leading;
        this.trailing = trailing;
    }

    public static TrimFunction create(Expression<?> input, Expression<?> characters, boolean leading, boolean trailing) {
        // It is common for "characters" to be a constant with space character. Handle it as a special case.
        if (characters instanceof ConstantExpression) {
            ConstantExpression<?> characters0 = (ConstantExpression<?>) characters;

            if (SPACE_ONLY.equals(characters0.getValue())) {
                characters = null;
            }
        }

        return new TrimFunction(input, characters, leading, trailing);
    }

    @Override
    public String eval(Row row, ExpressionEvalContext context) {
        String input = StringFunctionUtils.asVarchar(operand1, row, context);

        if (input == null) {
            return null;
        }

        String characters;

        if (operand2 != null) {
            characters = StringFunctionUtils.asVarchar(operand2, row, context);

            if (characters == null) {
                return null;
            }
        } else {
            characters = SPACE_ONLY;
        }

        return trim(input, characters);
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private String trim(String input, String characters) {
        if (input.isEmpty()) {
            // Trim on the empty string is no-op
            return "";
        }

        assert characters != null;

        if (characters.isEmpty()) {
            // Nothing to trim
            return input;
        }

        CharacterTester tester = SPACE_ONLY.equals(characters) ? SPACE_ONLY_TESTER : characters.length() == 1
            ? new SingleCharacterTester(characters.charAt(0)) : new MultipleCharacterTester(characters.toCharArray());

        int from = 0;
        int to = input.length();

        if (leading) {
            while (from < input.length() && tester.test(input.charAt(from))) {
                from++;
            }
        }

        if (trailing) {
            while (to > from && tester.test(input.charAt(to - 1))) {
                to--;
            }
        }

        if (from == 0 && to == input.length()) {
            // Nothing has been trimmed, return the original input
            return input;
        } else {
            // Do trimming
            return input.substring(from, to);
        }
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.VARCHAR;
    }

    public Expression<?> getCharacters() {
        return operand2;
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.EXPRESSION_TRIM;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeBoolean(leading);
        out.writeBoolean(trailing);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        leading = in.readBoolean();
        trailing = in.readBoolean();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        if (!super.equals(o)) {
            return false;
        }

        TrimFunction that = (TrimFunction) o;

        return leading == that.leading && trailing == that.trailing;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (leading ? 1 : 0);
        result = 31 * result + (trailing ? 1 : 0);
        return result;
    }

    private interface CharacterTester {
        boolean test(char c);
    }

    private static final class SingleCharacterTester implements CharacterTester {

        private final char expected;

        private SingleCharacterTester(char expected) {
            this.expected = expected;
        }

        @Override
        public boolean test(char c) {
            return c == expected;
        }
    }

    private static final class MultipleCharacterTester implements CharacterTester {

        private final char[] expected;

        private MultipleCharacterTester(char[] expected) {
            this.expected = expected;
        }

        @Override
        public boolean test(char c) {
            for (char value : expected) {
                if (c == value) {
                    return true;
                }
            }

            return false;
        }
    }
}

