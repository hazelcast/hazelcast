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

package com.hazelcast.mapstore;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JDBCParametersTest {

    @Test
    public void testShiftParametersForUpdateForPosition0() {

        JDBCParameters jDBCParameters = new JDBCParameters();
        jDBCParameters.setParams(new Object[]{1, 2, 3});

        jDBCParameters.shiftParametersForUpdate();

        Object[] params = jDBCParameters.getParams();

        assertThat(params).isEqualTo(new Object[]{2, 3, 1});
    }

    @Test
    public void testShiftParametersForUpdateForPosition1() {

        JDBCParameters jDBCParameters = new JDBCParameters();
        jDBCParameters.setParams(new Object[]{1, 2, 3});
        jDBCParameters.setIdPos(1);

        jDBCParameters.shiftParametersForUpdate();

        Object[] params = jDBCParameters.getParams();

        assertThat(params).isEqualTo(new Object[]{1, 3, 2});
    }
}
