/*
 * Copyright 2026 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.misc;

import com.hazelcast.config.IndexType;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that GROUP BY ... ORDER BY on indexed column produces globally sorted
 * aggregated results (stable order across multiple runs).
 */
public class IndexOrderFixedTest extends OptimizerTestSupport {

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(1, null);
    }

    @Test
    public void testGlobalOrderAfterAggregation() {
        final String mapName = randomName();

        IMap<Integer, String> map = instance().getMap(mapName);
        map.addIndex(IndexType.SORTED, "this");

        List<String> names = new ArrayList<>(List.of(
                "First.Value", "Fifth.value", "Fourth.value", "Second.Value", "Third.value"
        ));
        int key = 0;
        for (String name : names) {
            for (int i = 1; i <= 200; i++) {
                map.put(key++, name);
            }
        }

        String mapping = String.format(
                "CREATE OR REPLACE MAPPING \"%s\" (" +
                        "\"__key\" INT, " +
                        "\"this\" VARCHAR" +
                        ") TYPE IMap OPTIONS (" +
                        "'keyFormat'='java', " +
                        "'keyJavaClass'='java.lang.Integer', " +
                        "'valueFormat'='java', " +
                        "'valueJavaClass'='java.lang.String'" +
                        ")",
                mapName
        );
        instance().getSql().execute(mapping);

        String sql = "SELECT this AS indexed, SUM(__key) AS totalValue " +
                "FROM \"" + mapName + "\" " +
                "GROUP BY this " +
                "ORDER BY this";

        // Expected: natural ascending String order
        List<String> expectedOrder = new ArrayList<>(names);
        Collections.sort(expectedOrder);

        // Run the query multiple times and assert result order matches expected
        for (int run = 0; run < 5; run++) {
            List<String> order = new ArrayList<>();
            try (SqlResult res = instance().getSql().execute(sql)) {
                for (SqlRow row : res) {
                    order.add(row.getObject(0));
                }
            }
            assertThat(order).isEqualTo(expectedOrder);
        }
    }
}
