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

package com.hazelcast.sql.impl;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class UpdateSqlResultImplTest extends CoreSqlTestSupport {

    @Test
    public void test_updateCountResult() {
        try (AbstractSqlResult result = UpdateSqlResultImpl.createUpdateCountResult(10)) {
            assertEquals(10, result.updateCount());

            assertThatThrownBy(result::getQueryId)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("This result contains only update count");
            assertThatThrownBy(result::getRowMetadata)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("This result contains only update count");
            assertThatThrownBy(result::iterator)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("This result contains only update count");
        }
    }
}
