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

package com.hazelcast.jet.hadoop.impl;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.jet.pipeline.file.FileFormat;
import org.apache.hadoop.mapreduce.Job;

import javax.annotation.Nonnull;
import java.io.Serializable;

/**
 * Hadoop map-reduce job configurer.
 */
public interface JobConfigurer extends Serializable {

    /**
     * Configures the given job with the given file format.
     * <p>
     * This method should set the input format class and any required
     * configuration parameters.
     *
     * @param job    map-reduce job to configure
     * @param format format to configure the job with
     */
    <T> void configure(Job job, FileFormat<T> format);

    /**
     * Projection function from the key-value result of the map-reduce job into
     * the item emitted from the source.
     * <p>
     * The types of key/value are determined by the input format class set by
     * the configure method.
     *
     * @return projection function from key-value result into the item emitted from the
     * source
     */
    BiFunctionEx<?, ?, ?> projectionFn();

    /**
     * Returns a string that identifies the {@link FileFormat} supported by
     * this function provider.
     */
    @Nonnull
    String format();
}
