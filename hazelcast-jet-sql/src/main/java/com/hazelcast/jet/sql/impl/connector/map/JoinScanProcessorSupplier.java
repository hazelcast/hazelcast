/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.processor.TransformBatchedP;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.projection.Projection;
import com.hazelcast.sql.impl.expression.Expression;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.impl.util.Util.extendArray;

@SuppressFBWarnings(
        value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
        justification = "the class is never java-serialized"
)
final class JoinScanProcessorSupplier implements ProcessorSupplier, DataSerializable {

    private JetJoinInfo joinInfo;
    private String mapName;
    private KvRowProjector.Supplier rightRowProjectorSupplier;

    private transient IMap<Object, Object> map;

    @SuppressWarnings("unused")
    private JoinScanProcessorSupplier() {
    }

    JoinScanProcessorSupplier(
            JetJoinInfo joinInfo,
            String mapName,
            KvRowProjector.Supplier rightRowProjectorSupplier
    ) {
        this.joinInfo = joinInfo;
        this.mapName = mapName;
        this.rightRowProjectorSupplier = rightRowProjectorSupplier;
    }

    @Override
    public void init(@Nonnull Context context) {
        map = context.jetInstance().getMap(mapName);
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        List<Processor> processors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Processor processor =
                    new TransformBatchedP<Object[], Object[]>(joinFn(joinInfo, map, rightRowProjectorSupplier)) {
                        @Override
                        public boolean isCooperative() {
                            return false;
                        }
                    };
            processors.add(processor);
        }
        return processors;
    }

    private static FunctionEx<Iterable<Object[]>, Traverser<Object[]>> joinFn(
            JetJoinInfo joinInfo,
            IMap<Object, Object> map,
            KvRowProjector.Supplier rightRowProjectorSupplier
    ) {
        Projection<Entry<Object, Object>, Object[]> projection = QueryUtil.toProjection(rightRowProjectorSupplier);

        return lefts -> {
            List<Object[]> rights = new ArrayList<>();
            // TODO it would be nice if we executed the project() with the predicate that the rightRowProjector
            //  uses, maybe the majority of rows are rejected. In general it's good to do filtering as closely to the
            //  source as possible. However, the predicate has state. Without a state the predicate will have to
            //  create QueryTargets and extractors for each row.

            // current rules pull projects up, hence project() cardinality won't be greater than the source's
            // changing the rules might require revisiting
            for (Object[] right : map.project(projection)) {
                if (right != null) {
                    rights.add(right);
                }
            }

            List<Object[]> rows = new ArrayList<>();
            for (Object[] left : lefts) {
                boolean joined = join(rows, left, rights, joinInfo.condition());
                if (!joined && joinInfo.isLeftOuter()) {
                    rows.add(extendArray(left, rightRowProjectorSupplier.columnCount()));
                }
            }
            return traverseIterable(rows);
        };
    }

    private static boolean join(
            List<Object[]> rows,
            Object[] left,
            List<Object[]> rights,
            Expression<Boolean> condition
    ) {
        boolean matched = false;
        for (Object[] right : rights) {
            Object[] joined = ExpressionUtil.join(left, right, condition);
            if (joined != null) {
                rows.add(joined);
                matched = true;
            }
        }
        return matched;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(joinInfo);
        out.writeObject(mapName);
        out.writeObject(rightRowProjectorSupplier);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        joinInfo = in.readObject();
        mapName = in.readObject();
        rightRowProjectorSupplier = in.readObject();
    }
}
