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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.config.DataLinkConfig;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.stream.IntStream;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

/**
 * Separate test class for SHOW DATA LINKS.
 * It is required due to inability to remove of config-created data link.
 * Also, it is important for us to test ability of SHOW DATA LINKS
 * to show both sql-created and config-created data links.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ShowDataLinksSqlTest extends SqlTestSupport {

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(1, null);
    }

    @Test
    public void test_showDataLinks() {

        // when no data links - empty list
        assertRowsOrdered("show data links", emptyList());

        // given
        // create data links via CONFIG
        getNodeEngineImpl(instance()).getDataLinkService().createConfigDataLink(
                new DataLinkConfig("dl")
                        .setType("dummy")
        );

        // create data links via SQL
        List<String> dlNames = IntStream.range(0, 5).mapToObj(i -> "dl" + i).collect(toList());
        for (String dlName : dlNames) {
            instance().getSql().execute("CREATE DATA LINK " + dlName  + " TYPE DUMMY SHARED OPTIONS ('b' = 'c')");
        }

        dlNames.add(0, "dl");

        // when & then
        assertRowsOrdered("show data links", Util.toList(dlNames, Row::new));
    }

}
