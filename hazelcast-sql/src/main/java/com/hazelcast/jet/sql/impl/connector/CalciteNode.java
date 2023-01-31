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
package com.hazelcast.jet.sql.impl.connector;

import org.apache.calcite.rex.RexNode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * Wraps Calcite's RexNodes, so that connector can be created outside hazelcast-sql module without problems with
 * shaded Calcite dependency.
 */
public final class CalciteNode {
    private final RexNode node;

    private CalciteNode(RexNode node) {
        this.node = node;
    }

    /**
     * Returns underlying Calcite {@linkplain RexNode}.
     * @return
     */
    @Nullable
    public RexNode unwrap() {
        return node;
    }

    /**
     * Creates a new wrapper around given node.
     */
    public static @Nonnull CalciteNode filter(RexNode node) {
        return new CalciteNode(node);
    }

    public static @Nonnull List<CalciteNode> projection(@Nonnull List<RexNode> nodes) {
        return nodes.stream()
                .map(CalciteNode::new)
                .collect(toList());
    }

}
