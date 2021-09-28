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

import com.hazelcast.jet.sql.impl.opt.metadata.HazelcastRelMdWindowProperties.WindowPropertiesMetadata;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

public final class HazelcastRelMetadataQuery extends RelMetadataQuery {

    private WindowPropertiesMetadata.Handler windowPropertiesHandler;

    private HazelcastRelMetadataQuery() {
        this.windowPropertiesHandler = initialHandler(WindowPropertiesMetadata.Handler.class);
    }

    public static HazelcastRelMetadataQuery reuseOrCreate(RelMetadataQuery mq) {
        if (mq instanceof HazelcastRelMetadataQuery) {
            return (HazelcastRelMetadataQuery) mq;
        } else {
            return new HazelcastRelMetadataQuery();
        }
    }

    public WindowProperties extractWindowProperties(RelNode rel) {
        for (; ; ) {
            try {
                return windowPropertiesHandler.extractWindowProperties(rel, this);
            } catch (JaninoRelMetadataProvider.NoHandler e) {
                windowPropertiesHandler = revise(e.relClass, WindowPropertiesMetadata.DEF);
            }
        }
    }
}
