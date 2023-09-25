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

package com.hazelcast.sql.impl.schema;

import com.hazelcast.cluster.Member;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.schema.model.Person;
import com.hazelcast.sql.impl.client.SqlClientService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IMapResolverTest extends SqlTestSupport {

    @BeforeClass
    public static void beforeClass() {
        initializeWithClient(1, null, null);
    }

    @Before
    public void setup() {
        warmUpPartitions(instances());
        // ensure that client knows owners of all partitions before sending message.
        // if the partition owner is not known, message would not be sent.
        warmUpPartitions(client());
    }

    @Test
    public void smokeTest() throws Exception {
        Member member = instance().getCluster().getLocalMember();

        instance().getMap("m1").put(42, new BigDecimal((43)));
        String mappingDdl1 = ((SqlClientService) client().getSql()).mappingDdl(member, "m1").get();
        assertThat(mappingDdl1).isEqualToNormalizingNewlines(
                "CREATE OR REPLACE EXTERNAL MAPPING \"hazelcast\".\"public\".\"m1\" EXTERNAL NAME \"m1\"\n"
                        + "TYPE \"IMap\"\n"
                        + "OPTIONS (\n"
                        + "  'keyFormat'='java',\n"
                        + "  'keyJavaClass'='java.lang.Integer',\n"
                        + "  'valueFormat'='java',\n"
                        + "  'valueJavaClass'='java.math.BigDecimal'\n"
                        + ")");
        instance().getSql().execute(mappingDdl1); // we check that it doesn't fail

        instance().getMap("m2").put("foo", new Person("person name"));
        String mappingDdl2 = ((SqlClientService) client().getSql()).mappingDdl(member, "m2").get();
        assertThat(mappingDdl2).isEqualToNormalizingNewlines(
                "CREATE OR REPLACE EXTERNAL MAPPING \"hazelcast\".\"public\".\"m2\" EXTERNAL NAME \"m2\"\n"
                        + "TYPE \"IMap\"\n"
                        + "OPTIONS (\n"
                        + "  'keyFormat'='java',\n"
                        + "  'keyJavaClass'='java.lang.String',\n"
                        + "  'valueFormat'='java',\n"
                        + "  'valueJavaClass'='com.hazelcast.jet.sql.impl.schema.model.Person'\n"
                        + ")");
        instance().getSql().execute(mappingDdl2);
    }

    @Test
    public void testCompactSerialization() throws Exception {
        Member member = instance().getCluster().getLocalMember();

        instance().getMap("m3").put("foo", new CompactClass(1));
        String mappingDdl = ((SqlClientService) client().getSql()).mappingDdl(member, "m3").get();
        assertThat(mappingDdl).isEqualToNormalizingNewlines(
                "CREATE OR REPLACE EXTERNAL MAPPING \"hazelcast\".\"public\".\"m3\" EXTERNAL NAME \"m3\" (\n"
                        + "  \"field\" INTEGER EXTERNAL NAME \"this.field\"\n"
                        + ")\n"
                        + "TYPE \"IMap\"\n"
                        + "OPTIONS (\n"
                        + "  'keyFormat'='java',\n"
                        + "  'keyJavaClass'='java.lang.String',\n"
                        + "  'valueFormat'='compact',\n"
                        + "  'valueCompactTypeName'='com.hazelcast.sql.impl.schema.IMapResolverTest$CompactClass'\n"
                        + ")");
        instance().getSql().execute(mappingDdl);
    }

    private static class CompactClass {
        private Integer field;

        CompactClass(Integer field) {
            this.field = field;
        }
    }
}
