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

package com.hazelcast.jet.sql.impl.opt.physical.index;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;

import java.util.Comparator;

/**
 * RelCollation comparator. Compares the collations field by field. First compares the direction of the fields,
 * and if they are equal compares the field indexes.
 * <p>
 * If one collation is a prefix of another one, the one with the bigger size is greater.
 */
final class RelCollationComparator implements Comparator<RelCollation> {
    static final RelCollationComparator INSTANCE = new RelCollationComparator();

    private RelCollationComparator() {
        // no-op
    }

    @Override
    public int compare(RelCollation coll1, RelCollation coll2) {
        // Compare the collations field by field
        int coll1Size = coll1.getFieldCollations().size();
        int coll2Size = coll2.getFieldCollations().size();

        for (int i = 0; i < coll1Size; ++i) {
            if (i >= coll2Size) {
                // The coll1 has more fields and the prefixes are equal
                return 1;
            }

            RelFieldCollation fieldColl1 = coll1.getFieldCollations().get(i);
            RelFieldCollation fieldColl2 = coll2.getFieldCollations().get(i);
            // First, compare directions
            int cmp = fieldColl1.getDirection().compareTo(fieldColl2.getDirection());
            if (cmp == 0) {
                // Directions are the same
                if (fieldColl1.getFieldIndex() == fieldColl2.getFieldIndex()) {
                    // And fieldIndex is the same, try the next field
                    continue;
                } else {
                    return Integer.compare(fieldColl1.getFieldIndex(), fieldColl2.getFieldIndex());
                }
            }
            return cmp;
        }

        // All the fields from coll1 are equal to the fields from coll2, compare the size
        return Integer.compare(coll1Size, coll2Size);
    }
}
