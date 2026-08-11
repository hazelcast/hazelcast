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
import java.util.List;
import java.util.Set;

/**
 * Test to observe ordering behavior for GROUP BY ... ORDER BY on an indexed column.
 *
 * - Creates an IMap with a SORTED index on column 'this' (value).
 * - Inserts multiple rows with repeated 'names' so GROUP BY has multiple groups.
 * - Creates a simple SQL mapping for the IMap.
 * - Prints physical plan (EXPLAIN) and runs the aggregation query several times,
 *   printing result order each time so you can see if order changes.
 */
public class IndexOrderFlakyTest extends OptimizerTestSupport {

    @BeforeClass
    public static void beforeClass() throws Exception {
        // initialize small embedded cluster for tests
        initialize(1, null);
    }

    @Test
    public void testFlakyOrder() {
        final String mapName = randomName();

        // Create map and add SORTED index on the value ("this")
        IMap<Integer, String> map = instance().getMap(mapName);
        map.addIndex(IndexType.SORTED, "this");
        System.out.println("Map and SORTED index created: " + mapName);

        // Insert data: a few names repeated many times (so grouping makes sense)
        Set<String> names = Set.of("First.Value", "Second.Value", "Third.value", "Fourth.value", "Fifth.value");
        int key = 0;
        for (String name : names) {
            for (int i = 1; i <= 200; i++) {
                map.put(key++, name);
            }
        }
        System.out.println("Inserted " + key + " entries, names=" + names);

        // Create SQL mapping for this IMap: __key INT, this VARCHAR
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
        System.out.println("Created SQL mapping for map: " + mapName);

        // The aggregation query: group by the 'this' value and order by it
        String sql = "SELECT this AS indexed, SUM(__key) AS totalValue " +
                "FROM \"" + mapName + "\" " +
                "GROUP BY this " +
                "ORDER BY this";

        // Print plan (EXPLAIN) so you can inspect "requiresSort" or SortPhysicalRel presence
        System.out.println("==== PHYSICAL PLAN (EXPLAIN) ====");
        try (SqlResult planRes = instance().getSql().execute("EXPLAIN " + sql)) {
            for (SqlRow r : planRes) {
                // EXPLAIN returns rows of plan text lines; print them
                System.out.println(r.getObject(0).toString());
            }
        }

        // Run the query several times and print the groups in output order
        System.out.println("==== RUNNING QUERY MULTIPLE TIMES TO OBSERVE ORDER ====");
        for (int run = 1; run <= 7; run++) {
            List<String> order = new ArrayList<>();
            System.out.printf("Run %d:%n", run);
            try (SqlResult res = instance().getSql().execute(sql)) {
                for (SqlRow row : res) {
                    // row 0 = indexed, row 1 = totalValue
                    String indexed = row.getObject(0);
                    Object total = row.getObject(1);
                    order.add(indexed + ":" + total);
                }
            }
            System.out.println("Result order: " + order);
        }

        System.out.println("==== DONE ====");
    }
}
