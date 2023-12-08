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

package com.hazelcast.jet.sql.impl.validate.types;

import com.hazelcast.jet.sql.impl.validate.types.HazelcastObjectType.Field;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HazelcastObjectTypeTest {
    private static final HazelcastTypeFactory TYPE_FACTORY = HazelcastTypeFactory.INSTANCE;

    @Test
    public void testDigestEscaping() {
        HazelcastObjectType person = new HazelcastObjectType("P(e:r=s,o)n");
        person.addField(new Field("name", 0, TYPE_FACTORY.createTypeWithNullability(
                TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR), true)));
        person.addField(new Field("(:fri=end,)", 1, person));
        HazelcastObjectType.finalizeFields(List.of(person));

        assertEquals("P\\(e\\:r=s\\,o\\)n("
                + "name:VARCHAR CHARACTER SET \"UTF-16LE\", "
                + "\\(\\:fri=end\\,\\):P\\(e\\:r=s\\,o\\)n"
                + ")", person.getFullTypeString());
    }
}
