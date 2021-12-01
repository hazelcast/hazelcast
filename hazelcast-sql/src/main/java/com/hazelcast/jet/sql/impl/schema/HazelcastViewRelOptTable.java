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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.view.View;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;

import java.util.HashSet;
import java.util.Set;

public class HazelcastViewRelOptTable extends HazelcastRelOptTable {
    private final View view;

    public HazelcastViewRelOptTable(Prepare.PreparingTable delegate, View view) {
        super(delegate);
        this.view = view;
    }

    @Override
    public final RelNode toRel(RelOptTable.ToRelContext context) {
        return expand(context);
    }

    public RelNode tryExpand(ToRelContext context) {
        RelNode original = context.expandView(
                getRowType(),
                view.query(),
                getQualifiedName().subList(0, 2),
                getQualifiedName()
        ).project();
        return RelOptUtil.createCastRel(original, getRowType(), true);
    }

    private RelNode expand(RelOptTable.ToRelContext context) {
        RelNode rel = tryExpand(context);
        Set<String> set = new HashSet<>();
        return rel.accept(
                new RelShuttleImpl() {
                    @Override
                    public RelNode visit(TableScan scan) {
                        final RelOptTable table = scan.getTable();
                        String name = table.getQualifiedName().get(table.getQualifiedName().size() - 1);
                        if (set.contains(name)) {
                            throw QueryException.error("Infinite recursion during view expanding detected");
                        } else {
                            set.add(name);
                        }
                        if (table instanceof HazelcastViewRelOptTable) {
                            return ((HazelcastViewRelOptTable) table).expand(context);
                        }
                        return super.visit(scan);
                    }
                }
        );
    }
}
