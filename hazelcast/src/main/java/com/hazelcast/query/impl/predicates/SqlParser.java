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

package com.hazelcast.query.impl.predicates;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.hazelcast.internal.util.SetUtil.createHashSet;
import static com.hazelcast.internal.util.StringUtil.lowerCaseInternal;

class SqlParser {
    private static final String SPLIT_EXPRESSION = " ";
    private static final List<Set<String>> PRECEDENCE;

    static {
        final List<Set<String>> precedence = new ArrayList<>();
        precedence.add(createHashSet("(", ")", "escape"));
        precedence.add(createHashSet("=", ">", ">", "<", ">=", "<=", "==", "!=", "<>", "between", "in",
                "like", "ilike", "regex"));
        precedence.add(createHashSet("not"));
        precedence.add(createHashSet("and"));
        precedence.add(createHashSet("or"));
        PRECEDENCE = Collections.unmodifiableList(precedence);
    }

    private static final List<String> CHAR_OPERATORS
            = Arrays.asList("(", ")", " + ", " - ", "=", "<", ">", " * ", " / ", "!");

    private static final int NO_INDEX = -1;
    private static final String IN_LOWER = " in ";
    private static final String IN_LOWER_P = " in(";
    private static final String IN_UPPER = " IN ";
    private static final String IN_UPPER_P = " IN(";

    SqlParser() {
    }

    public List<String> toPrefix(String in) {
        List<String> tokens = buildTokens(alignINClause(in));

        List<String> output = new ArrayList<>();
        List<String> stack = new ArrayList<>();
        for (String token : tokens) {
            if (isOperand(token)) {
                if (token.equals(")")) {
                    while (openParanthesesFound(stack)) {
                        output.add(stack.remove(stack.size() - 1));
                    }
                    if (stack.size() > 0) {
                        // temporarily fix for issue #189
                        stack.remove(stack.size() - 1);
                    }
                } else {
                    while (openParanthesesFound(stack) && !hasHigherPrecedence(token, stack.get(stack.size() - 1))) {
                        output.add(stack.remove(stack.size() - 1));
                    }
                    stack.add(token);
                }
            } else {
                output.add(token);
            }
        }
        while (stack.size() > 0) {
            output.add(stack.remove(stack.size() - 1));
        }
        return output;
    }

    private List<String> buildTokens(String in) {
        List<String> tokens = split(in);
        if (tokens.contains("between") || tokens.contains("BETWEEN")) {
            boolean found = true;
            boolean dirty = false;
            betweens:
            while (found) {
                for (int i = 0; i < tokens.size(); i++) {
                    if ("between".equalsIgnoreCase(tokens.get(i))) {
                        tokens.set(i, "betweenAnd");
                        tokens.remove(i + 2);
                        dirty = true;
                        continue betweens;
                    }
                }
                found = false;
            }
            if (dirty) {
                for (int i = 0; i < tokens.size(); i++) {
                    if ("betweenAnd".equals(tokens.get(i))) {
                        tokens.set(i, "between");
                    }
                }
            }
        }
        return tokens;
    }

    public List<String> split(String in) {
        final StringBuilder result = new StringBuilder();
        final char[] chars = in.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            final char c = chars[i];
            if (CHAR_OPERATORS.contains(String.valueOf(c))) {
                if (i < chars.length - 2 && CHAR_OPERATORS.contains(String.valueOf(chars[i + 1]))
                        && !("(".equals(String.valueOf(chars[i + 1])) || ")".equals(String.valueOf(chars[i + 1])))) {
                    result.append(" ").append(c).append(chars[i + 1]).append(" ");
                    i++;
                } else {
                    result.append(" ").append(c).append(" ");
                }
            } else {
                result.append(c);
            }
        }
        final String[] tokens = result.toString().split(SPLIT_EXPRESSION);
        final List<String> list = new ArrayList<>();
        for (int i = 0; i < tokens.length; i++) {
            tokens[i] = tokens[i].trim();
            if (!tokens[i].equals("")) {
                list.add(tokens[i]);
            }
        }
        return list;
    }

    boolean hasHigherPrecedence(String operator1, String operator2) {
        return getPrecedenceValue(lowerCaseInternal(operator1)) < getPrecedenceValue(lowerCaseInternal(operator2));
    }

    boolean isOperand(String string) {
        return getPrecedenceValue(lowerCaseInternal(string)) != null;
    }

    private Integer getPrecedenceValue(String operand) {
        Integer precedence = null;
        for (int i = 0; i < PRECEDENCE.size(); i++) {
            if (PRECEDENCE.get(i).contains(operand)) {
                precedence = i;
                break;
            }
        }
        return precedence;
    }

    private boolean openParanthesesFound(List<String> stack) {
        return stack.size() > 0 && !stack.get(stack.size() - 1).equals("(");
    }

    /*
    *
    * Recursively finds in-clauses and reformats them for the parser
    *
    * */
    private String alignINClause(String in) {
        String paramIn = in;
        final int indexLowerIn = paramIn.indexOf(IN_LOWER);
        final int indexLowerInWithParentheses = paramIn.indexOf(IN_LOWER_P);
        final int indexUpperIn = paramIn.indexOf(IN_UPPER);
        final int indexUpperInWithParentheses = paramIn.indexOf(IN_UPPER_P);
        // find first occurrence of in clause.
        final int indexIn = findMinIfNot(indexUpperInWithParentheses,
                findMinIfNot(indexUpperIn,
                        findMinIfNot(indexLowerIn, indexLowerInWithParentheses, NO_INDEX), NO_INDEX), NO_INDEX
        );

        if (indexIn > NO_INDEX && (indexIn == indexLowerInWithParentheses || indexIn == indexUpperInWithParentheses)) {
            // 3 is the size of param in ending with a parentheses.
            // add SPLIT_EXPRESSION
            paramIn = paramIn.substring(0, indexIn + 3) + SPLIT_EXPRESSION + paramIn.substring(indexIn + 3);
        }
        String sql = paramIn;
        if (indexIn != NO_INDEX) {
            final int indexOpen = paramIn.indexOf('(', indexIn);
            final int indexClose = paramIn.indexOf(')', indexOpen);
            String sub = paramIn.substring(indexOpen, indexClose + 1);
            sub = sub.replaceAll(" ", "");
            sql = paramIn.substring(0, indexOpen) + sub
                    + alignINClause(paramIn.substring(indexClose + 1));

        }
        return sql;
    }

    /**
     * finds min choosing a lower bound.
     *
     * @param a      first number
     * @param b      second number
     * @param notMin lower bound
     */
    private int findMinIfNot(int a, int b, int notMin) {
        if (a <= notMin) {
            return b;
        }
        if (b <= notMin) {
            return a;
        }
        return Math.min(a, b);
    }
}

