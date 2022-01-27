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

import com.hazelcast.jet.sql.impl.opt.metadata.HazelcastRelMdBoundedness.BoundednessMetadata;
import com.hazelcast.jet.sql.impl.opt.metadata.HazelcastRelMdWatermarkedFields.WatermarkedFieldsMetadata;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

public final class HazelcastRelMetadataQuery extends RelMetadataQuery {

    private BoundednessMetadata.Handler boundednessHandler;
    private WatermarkedFieldsMetadata.Handler watermarkedFieldsHandler;

    private HazelcastRelMetadataQuery() {
        this.boundednessHandler = initialHandler(BoundednessMetadata.Handler.class);
        this.watermarkedFieldsHandler = initialHandler(WatermarkedFieldsMetadata.Handler.class);
    }

    public static HazelcastRelMetadataQuery reuseOrCreate(RelMetadataQuery mq) {
        if (mq instanceof HazelcastRelMetadataQuery) {
            return (HazelcastRelMetadataQuery) mq;
        } else {
            return new HazelcastRelMetadataQuery();
        }
    }

    public Boundedness extractBoundedness(RelNode rel) {
        for (; ; ) {
            try {
                return boundednessHandler.extractBoundedness(rel, this);
            } catch (JaninoRelMetadataProvider.NoHandler e) {
                boundednessHandler = revise(e.relClass, BoundednessMetadata.DEF);
            }
        }
    }

    public WatermarkedFields extractWatermarkedFields(RelNode rel) {
        for (; ; ) {
            try {
                return watermarkedFieldsHandler.extractWatermarkedFields(rel, this);
            } catch (JaninoRelMetadataProvider.NoHandler e) {
                watermarkedFieldsHandler = revise(e.relClass, WatermarkedFieldsMetadata.DEF);
            }
        }
    }
}
