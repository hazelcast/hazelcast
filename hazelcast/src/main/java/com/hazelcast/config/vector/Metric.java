/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config.vector;


import com.hazelcast.spi.annotation.Beta;

/**
 * Vector similarity metrics: These are utilized in search algorithms
 * to retrieve the top K most similar vectors to a target vector.
 * This labels describes the methodology employed during indexing and searching
 * of vectors to identify the nearest neighbors.
 *
 *  @since 5.5
 */

@Beta
public enum Metric {

    /** Euclidean distance */
    EUCLIDEAN(0),
    /** Cosine distance */
    COSINE(1),
    /** Dot product */
    DOT(2);

    private final int id;

    Metric(int id) {
        this.id = id;
    }

    /**
     * Gets the ID for the given {@link Metric}.
     *
     * @return the ID
     */
    public int getId() {
        return id;
    }

    /**
     * Returns the Metric as an enum.
     *
     * @return the Metric as an enum
     */
    public static Metric getById(final int id) {
        for (Metric type : values()) {
            if (type.id == id) {
                return type;
            }
        }
        throw new IllegalArgumentException("No metric is found for the given id.");
    }
}
