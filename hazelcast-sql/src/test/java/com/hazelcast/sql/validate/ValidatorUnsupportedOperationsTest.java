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

package com.hazelcast.sql.validate;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.support.ModelGenerator;
import com.hazelcast.sql.support.SqlTestSupport;
import org.hamcrest.BaseMatcher;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.rules.ExpectedException;

public class ValidatorUnsupportedOperationsTest extends SqlTestSupport {
    private static final int PERSON_CNT = 100;
    private static HazelcastInstance member;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() {
        member = Hazelcast.newHazelcastInstance();

        ModelGenerator.generatePerson(member, PERSON_CNT);
    }

    @AfterClass
    public static void afterClass() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testUnion() {
        expectException("UNION is not supported");

        String sql = "SELECT p.name, p.deptTitle FROM person p UNION SELECT p.name, p.deptTitle FROM person p ORDER BY 1";

        executeQuery(member, sql);
    }

    private void expectException(String message) {
        Matcher<?> matcher = CoreMatchers.allOf(
            CoreMatchers.instanceOf(HazelcastSqlException.class),
            ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(message)),
            ErrorCodeMatcher.forCode(SqlErrorCode.PARSING)
        );

        expectedException.expect(matcher);
    }

    private static class ErrorCodeMatcher extends BaseMatcher<Object> {
        private final int expectedCode;

        private ErrorCodeMatcher(int expectedCode) {
            this.expectedCode = expectedCode;
        }

        private static ErrorCodeMatcher forCode(int code) {
            return new ErrorCodeMatcher(code);
        }

        @Override
        public boolean matches(Object item) {
            HazelcastSqlException exception = (HazelcastSqlException) item;

            int code = exception.getCode();

            return code == expectedCode;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("error code " + expectedCode);
        }
    }
}
