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

import com.hazelcast.jet.sql.impl.opt.SlidingWindow;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.Util;

import java.lang.reflect.Method;

public final class HazelcastRelMdSlidingWindowDetector
        implements MetadataHandler<HazelcastRelMdSlidingWindowDetector.SlidingWindowDetectorMetadata> {

    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(
            SlidingWindowDetectorMetadata.METHOD,
            new HazelcastRelMdSlidingWindowDetector()
    );

    private HazelcastRelMdSlidingWindowDetector() {
    }

    @Override
    public MetadataDef<SlidingWindowDetectorMetadata> getDef() {
        return SlidingWindowDetectorMetadata.DEF;
    }

    @SuppressWarnings("unused")
    public Boolean detectSlidingWindow(RelNode rel, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        for (RelNode input : rel.getInputs()) {
            if (query.detectSlidingWindow(input)) {
                return true;
            }
        }
        return false;
    }
    @SuppressWarnings("unused")
    public Boolean detectSlidingWindow(SlidingWindow rel, RelMetadataQuery mq) {
        return true;
    }

    @SuppressWarnings("unused")
    public Boolean detectSlidingWindow(RelSubset subset, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        RelNode rel = Util.first(subset.getBest(), subset.getOriginal());
        return query.detectSlidingWindow(rel);
    }

    @SuppressWarnings("unused")
    public Boolean detectSlidingWindow(HepRelVertex vertex, RelMetadataQuery mq) {
        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(mq);
        RelNode rel = vertex.getCurrentRel();
        return query.detectSlidingWindow(rel);
    }

    public interface SlidingWindowDetectorMetadata extends Metadata {
        Method METHOD = Types.lookupMethod(SlidingWindowDetectorMetadata.class, "detectSlidingWindow");

        MetadataDef<SlidingWindowDetectorMetadata> DEF = MetadataDef.of(
                SlidingWindowDetectorMetadata.class,
                SlidingWindowDetectorMetadata.Handler.class,
                METHOD
        );

        @SuppressWarnings("unused")
        Boolean detectSlidingWindow();

        interface Handler extends MetadataHandler<SlidingWindowDetectorMetadata> {
            Boolean detectSlidingWindow(RelNode rel, RelMetadataQuery mq);
        }
    }
}
