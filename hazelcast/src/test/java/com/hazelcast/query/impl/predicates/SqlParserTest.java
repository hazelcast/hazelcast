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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SqlParserTest {

    private final SqlParser parser = new SqlParser();

    @Test
    public void parseEmpty() {
        List<String> list = parser.toPrefix("");
        assertTrue(list.isEmpty());
    }

    @Test
    public void parseAEqB() {
        String query = "a = b";
        List<String> list = parser.toPrefix(query);
        assertEquals(Arrays.asList("a", "b", "="), list);
    }

    @Test
    public void parseAeqBandXgrtY() {
        assertTrue(parser.hasHigherPrecedence("=", "AND"));
        assertFalse(parser.hasHigherPrecedence("=", ">"));
        List<String> list = parser.toPrefix("a = b AND x > y");
        assertEquals(Arrays.asList("a", "b", "=", "x", "y", ">", "AND"), list);
    }

    @Test
    public void parseAeqBandOpenBsmlCorDgtEclose() {
        String query = "A = B AND ( B < C OR D > E )";
        List<String> list = parser.toPrefix(query);
        assertEquals(Arrays.asList("A", "B", "=", "B", "C", "<", "D", "E", ">", "OR", "AND"), list);
    }

    @Test
    public void testComplexStatement() {
        String query = "age > 5 AND ( ( ( active = true ) AND ( age = 23 ) ) OR age > 40 ) AND ( salary > 10 ) OR age = 10";
        List<String> list = parser.toPrefix(query);
        assertEquals(Arrays.asList("age", "5", ">", "active", "true", "=", "age", "23", "=", "AND", "age", "40", ">", "OR",
                "AND", "salary", "10", ">", "AND", "age", "10", "=", "OR"), list);
    }

    @Test
    public void testTwoInnerParenthesis() {
        String query = "a and b AND ( ( ( a > c AND b > d ) OR ( x = y ) ) ) OR t > u";
        List<String> list = parser.toPrefix(query);
        assertEquals(Arrays.asList("a", "b", "and", "a", "c", ">", "b", "d", ">", "AND", "x", "y", "=", "OR", "AND", "t", "u",
                ">", "OR"), list);
    }

    @Test
    public void testBetweenAnd() {
        String query = "a and b between 10 and 15";
        List<String> list = parser.toPrefix(query);
        assertEquals(Arrays.asList("a", "b", "10", "15", "between", "and"), list);
    }

    @Test
    public void testBetween() {
        String query = "b between 10 and 15";
        List<String> list = parser.toPrefix(query);
        assertEquals(Arrays.asList("b", "10", "15", "between"), list);
    }

    @Test
    public void testBetweenSimple() {
        String s = "b between 'ali' and 'veli''s'";
        List<String> list = parser.toPrefix(s);
        assertEquals(Arrays.asList("b", "'ali'", "'veli''s'", "between"), list);
    }

    @Test
    public void testIn() {
        String query = "a and b OR c in ( 4, 5, 6 )";
        List<String> list = parser.toPrefix(query);
        assertEquals(Arrays.asList("a", "b", "and", "c", "4,5,6", "in", "OR"), list);
    }

    @Test
    public void testNot() {
        String query = "a and not(b) OR c not in ( 4, 5, 6 )";
        List<String> list = parser.toPrefix(query);
        assertEquals(Arrays.asList("a", "b", "not", "and", "c", "4,5,6", "in", "not", "OR"), list);
    }

    @Test
    public void testNotEqual1() {
        String query = "b != 30";
        List<String> list = parser.toPrefix(query);
        assertEquals(Arrays.asList("b", "30", "!="), list);
    }

    @Test
    public void testNotEqual2() {
        String query = "b <> 30";
        List<String> list = parser.toPrefix(query);
        assertEquals(Arrays.asList("b", "30", "<>"), list);
    }

    @Test
    public void split1() {
        List<String> tokens = parser.split("a and b");
        assertEquals(Arrays.asList("a", "and", "b"), tokens);
    }

    @Test
    public void split2() {
        List<String> tokens = parser.split("(a and b)");
        assertEquals(Arrays.asList("(", "a", "and", "b", ")"), tokens);
    }

    @Test
    public void split3() {
        List<String> tokens = parser.split("((a and b))");
        assertEquals(Arrays.asList("(", "(", "a", "and", "b", ")", ")"), tokens);
    }

    @Test
    public void split4() {
        List<String> tokens = parser.split("a and b AND(((a>c AND b> d) OR (x = y )) ) OR t>u");
        assertEquals(Arrays.asList("a", "and", "b", "AND", "(", "(", "(", "a", ">", "c", "AND", "b", ">", "d", ")", "OR", "(",
                "x", "=", "y", ")", ")", ")", "OR", "t", ">", "u"), tokens);
    }

    @Test
    public void split5() {
        List<String> tokens = parser.split("a and b AND(((a>=c AND b> d) OR (x <> y )) ) OR t>u");
        assertEquals(Arrays.asList("a", "and", "b", "AND", "(", "(", "(", "a", ">=", "c", "AND", "b", ">", "d", ")", "OR", "(",
                "x", "<>", "y", ")", ")", ")", "OR", "t", ">", "u"), tokens);
    }

    @Test
    public void testComplexStatementWithGreaterAndEquals() {
        String s = "age>=5 AND ((( active = true ) AND (age = 23 )) OR age > 40) AND( salary>10 ) OR age=10";
        List<String> list = parser.toPrefix(s);
        assertEquals(Arrays.asList("age", "5", ">=", "active", "true", "=", "age", "23", "=", "AND", "age", "40", ">", "OR",
                "AND", "salary", "10", ">", "AND", "age", "10", "=", "OR"), list);
    }

    @Test(expected = NullPointerException.class)
    public void parserShouldNotAcceptNull() {
        parser.toPrefix(null);
        fail();
    }

    @Test
    public void parserShouldThrowOnInvalidInput() {
        parser.toPrefix(")");
    }

    @Test
    public void shouldNotThrowOnRandomInput() {
        Random random = new SecureRandom();
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            stringBuilder.setLength(0);
            for (int n = 0; n < 1000; n++) {
                stringBuilder.append((char) (random.nextInt() & 0xFFFF));
            }
            parser.toPrefix(stringBuilder.toString());
        }
    }
}
