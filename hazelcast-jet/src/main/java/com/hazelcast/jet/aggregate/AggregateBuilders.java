/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.aggregate;

import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.BatchStageWithKey;
import com.hazelcast.jet.pipeline.StageWithKeyAndWindow;
import com.hazelcast.jet.pipeline.StageWithWindow;

import javax.annotation.Nonnull;

/**
 * TODO: javadoc
 */
public final class AggregateBuilders {

    private AggregateBuilders() {
    }

    /**
     * TODO: javadoc
     */
    public static <T, R> AggregateBuilder<R> aggregateBuilder(
            @Nonnull BatchStage<T> batchStage,
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp
    ) {
        return new AggregateBuilder<>(batchStage, aggrOp);
    }

    /**
     * TODO: javadoc
     */
    public static <T> AggregateBuilder1<T> aggregateBuilder1(
            @Nonnull BatchStage<T> batchStage
    ) {
        return new AggregateBuilder1<>(batchStage);
    }

    /**
     * TODO: javadoc
     */
    public static <T, K, R> GroupAggregateBuilder<K, R> aggregateBuilder(
            @Nonnull BatchStageWithKey<T, K> batchStageWithKey,
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp
    ) {
        return new GroupAggregateBuilder<>(batchStageWithKey, aggrOp);
    }

    /**
     * TODO: javadoc
     */
    public static <T, K> GroupAggregateBuilder1<T, K> aggregateBuilder1(
            @Nonnull BatchStageWithKey<T, K> batchStageWithKey
    ) {
        return new GroupAggregateBuilder1<>(batchStageWithKey);
    }

    /**
     * TODO: javadoc
     */
    public static <T, R> WindowAggregateBuilder<R> aggregateBuilder(
            @Nonnull StageWithWindow<T> stageWithWindow,
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp
    ) {
        return new WindowAggregateBuilder<>(stageWithWindow.streamStage(), aggrOp, stageWithWindow.windowDefinition());
    }

    /**
     * TODO: javadoc
     */
    public static <T> WindowAggregateBuilder1<T> aggregateBuilder1(
            @Nonnull StageWithWindow<T> stageWithWindow
    ) {
        return new WindowAggregateBuilder1<>(stageWithWindow.streamStage(), stageWithWindow.windowDefinition());
    }

    /**
     * TODO: javadoc
     */
    public static <T, K, R> WindowGroupAggregateBuilder<K, R> aggregateBuilder(
            @Nonnull StageWithKeyAndWindow<T, K> stageWithKeyAndWindow,
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp
    ) {
        return new WindowGroupAggregateBuilder<>(stageWithKeyAndWindow, aggrOp);
    }

    /**
     * TODO: javadoc
     */
    public static <T, K> WindowGroupAggregateBuilder1<T, K> aggregateBuilder1(
            @Nonnull StageWithKeyAndWindow<T, K> stageWithKeyAndWindow
    ) {
        return new WindowGroupAggregateBuilder1<>(stageWithKeyAndWindow);
    }

}
