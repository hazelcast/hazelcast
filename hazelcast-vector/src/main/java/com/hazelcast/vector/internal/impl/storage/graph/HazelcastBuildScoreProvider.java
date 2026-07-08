/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.internal.impl.storage.graph;

import com.hazelcast.vector.internal.impl.storage.UpdatableVectorsSource;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.graph.similarity.ScoreFunction;
import io.github.jbellis.jvector.graph.similarity.SearchScoreProvider;
import io.github.jbellis.jvector.vector.VectorUtil;
import io.github.jbellis.jvector.vector.types.VectorFloat;

import java.util.function.Supplier;

/**
 * Handles the comparison of node distances within the GraphIndexBuilder.
 * Depending on the similarity function, it provides the requested score.
 * The approximateCentroid() method calculates the centroid across all vectors,
 * without assuming that node IDs are sequential.
 */

public class HazelcastBuildScoreProvider implements BuildScoreProvider {

    private final Supplier<RandomAccessVectorValues> vectors;
    private final Supplier<RandomAccessVectorValues> vectorsCopy;
    private final HazelcastVectorSimilarityFunction similarityFunction;

    public HazelcastBuildScoreProvider(
            RandomAccessVectorValues vectorsSource,
            HazelcastVectorSimilarityFunction similarityFunction
    ) {
        this.vectors = vectorsSource.threadLocalSupplier();
        this.vectorsCopy = vectorsSource.threadLocalSupplier();
        this.similarityFunction = similarityFunction;
    }

    @Override
    public boolean isExact() {
        return true;
    }

    @Override
    public VectorFloat<?> approximateCentroid() {
        UpdatableVectorsSource vv = (UpdatableVectorsSource) vectors.get();
        var centroid = vts.createFloatVector(vv.dimension());
        vv.entries().forEach(entry -> {
            var v = entry.getValue();
            VectorUtil.addInPlace(centroid, v);
        });
        VectorUtil.scale(centroid, 1.0f / vv.size());
        return centroid;
    }

    @Override
    public SearchScoreProvider searchProviderFor(VectorFloat<?> vector) {
        var vc = vectorsCopy.get();
        return getSearchScoreProviderExact(vector, vc);
    }

    @Override
    public SearchScoreProvider searchProviderFor(int node1) {
        RandomAccessVectorValues randomAccessVectorValues = vectors.get();
        var v = randomAccessVectorValues.getVector(node1);
        return searchProviderFor(v);
    }

    @Override
    public SearchScoreProvider diversityProviderFor(int node1) {
        RandomAccessVectorValues randomAccessVectorValues = vectors.get();
        var v = randomAccessVectorValues.getVector(node1);
        var vc = vectorsCopy.get();
        return getSearchScoreProviderExact(v, vc);
    }

    private SearchScoreProvider getSearchScoreProviderExact(VectorFloat<?> vector, RandomAccessVectorValues ravv) {
        // Similar to SearchScoreProvider.exact() but using interface instead of enum
        var sf = new ScoreFunction.ExactScoreFunction() {
            @Override
            public float similarityTo(int node2) {
                return similarityFunction.compare(vector, ravv.getVector(node2));
            }
        };
        return new SearchScoreProvider(sf);
    }
}
