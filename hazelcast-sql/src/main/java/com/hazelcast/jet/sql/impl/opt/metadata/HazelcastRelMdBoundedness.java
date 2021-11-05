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

package com.hazelcast.jet.sql.impl.opt.metadata;

import com.hazelcast.jet.sql.impl.connector.SqlConnectorUtil;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.Util;

import java.lang.reflect.Method;

public final class HazelcastRelMdBoundedness
        implements MetadataHandler<HazelcastRelMdBoundedness.BoundednessMetadata> {

    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(
            BoundednessMetadata.METHOD,
            new HazelcastRelMdBoundedness()
    );

    private HazelcastRelMdBoundedness() {
    }

    @Override
    public MetadataDef<BoundednessMetadata> getDef() {
        return BoundednessMetadata.DEF;
    }

    @SuppressWarnings("unused")
    public Boundedness extractBoundedness(TableScan rel, RelMetadataQuery mq) {
        return SqlConnectorUtil.getJetSqlConnector(rel.getTable().unwrap(HazelcastTable.class).getTarget()).isStream()
                ? Boundedness.UNBOUNDED
                : Boundedness.BOUNDED;
    }

    @SuppressWarnings("unused")
    public Boundedness extractBoundedness(RelSubset subset, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        RelNode rel = Util.first(subset.getBest(), subset.getOriginal());
        return query.extractBoundedness(rel);
    }

    @SuppressWarnings("unused")
    public Boundedness extractBoundedness(RelNode rel, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        for (RelNode input : rel.getInputs()) {
            if (query.extractBoundedness(input) == Boundedness.UNBOUNDED) {
                return Boundedness.UNBOUNDED;
            }
        }
        return Boundedness.BOUNDED;
    }

    public interface BoundednessMetadata extends Metadata {

        Method METHOD = Types.lookupMethod(BoundednessMetadata.class, "extractBoundedness");

        MetadataDef<BoundednessMetadata> DEF = MetadataDef.of(
                BoundednessMetadata.class,
                Handler.class,
                METHOD
        );

        @SuppressWarnings("unused")
        Boundedness extractBoundedness();

        interface Handler extends MetadataHandler<BoundednessMetadata> {

            Boundedness extractBoundedness(RelNode rel, RelMetadataQuery mq);
        }
    }
}
