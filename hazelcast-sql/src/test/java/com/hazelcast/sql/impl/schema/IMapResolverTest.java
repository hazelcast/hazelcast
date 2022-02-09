/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.sql.impl.schema;

import com.hazelcast.cluster.Member;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.schema.model.Person;
import com.hazelcast.sql.impl.client.SqlClientService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IMapResolverTest extends SqlTestSupport {
    private static final String LE = System.lineSeparator();

    @BeforeClass
    public static void beforeClass() {
        initializeWithClient(1, null, null);
    }

    @Test
    public void smokeTest() throws Exception {
        instance().getMap("m1").put(42, new BigDecimal((43)));

        Member member = instance().getCluster().getLocalMember();
        String mappingDdl1 = ((SqlClientService) client().getSql()).mappingDdl(member, "m1").get();
        assertEquals("CREATE MAPPING \"m1\"" + LE
                        + "TYPE IMap" + LE
                        + "OPTIONS (" + LE
                        + "  'keyFormat' = 'java'," + LE
                        + "  'keyJavaClass' = 'java.lang.Integer'," + LE
                        + "  'valueFormat' = 'java'," + LE
                        + "  'valueJavaClass' = 'java.math.BigDecimal'" + LE
                        + ")",
                mappingDdl1);
        instance().getSql().execute(mappingDdl1); // we check that it doesn't fail

        instance().getMap("m2").put("foo", new Person("person name"));
        String mappingDdl2 = ((SqlClientService) client().getSql()).mappingDdl(member, "m2").get();
        assertEquals("CREATE MAPPING \"m2\"" + LE
                        + "TYPE IMap" + LE
                        + "OPTIONS (" + LE
                        + "  'keyFormat' = 'java'," + LE
                        + "  'keyJavaClass' = 'java.lang.String'," + LE
                        + "  'valueFormat' = 'java'," + LE
                        + "  'valueJavaClass' = 'com.hazelcast.jet.sql.impl.schema.model.Person'" + LE
                        + ")",
                mappingDdl2);
        instance().getSql().execute(mappingDdl2);
    }
}
