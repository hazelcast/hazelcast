/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.MetricTagger;

import javax.annotation.CheckReturnValue;

import static com.hazelcast.internal.metrics.MetricsUtil.containsSpecialCharacters;
import static com.hazelcast.internal.metrics.MetricsUtil.escapeMetricNamePart;

/**
 * Immutable implementation of {@link MetricTagger}.
 */
public class MetricTaggerImpl implements MetricTagger {

    private final String keyPrefix;
    private final String metricNamePrefix;
    private final String id;
    private final String metricName;

    MetricTaggerImpl(String metricNamePrefix) {
        this.keyPrefix = "[";
        this.metricNamePrefix = metricNamePrefix;
        this.id = null;
        this.metricName = null;
    }

    private MetricTaggerImpl(MetricTaggerImpl tagger, String keyPrefix) {
        this(tagger, keyPrefix, tagger.id, tagger.metricName);
    }

    private MetricTaggerImpl(MetricTaggerImpl tagger, String keyPrefix, String id, String metricName) {
        this.keyPrefix = keyPrefix;
        this.metricNamePrefix = tagger.metricNamePrefix;
        this.id = id;
        this.metricName = metricName;
    }

    @Override
    @CheckReturnValue
    public MetricTaggerImpl withTag(String tag, String value) {
        assert containsSpecialCharacters(tag) : "tag contains special characters";

        return new MetricTaggerImpl(this,
                getKeyPrefix(tag, value));
    }

    private String getKeyPrefix(String tag, String value) {
        return keyPrefix + (keyPrefix.length() == 1 ? "" : ",") + tag + '=' + escapeMetricNamePart(value);
    }

    @Override
    @CheckReturnValue
    public MetricTaggerImpl withIdTag(String tag, String value) {
        assert containsSpecialCharacters(tag) : "tag contains special characters";

        return new MetricTaggerImpl(this,
                getKeyPrefix(tag, value),
                value,
                this.metricName);
    }

    public MetricTaggerImpl withMetricTag(String metricName) {
        String prefixedMetricName = metricNamePrefix != null ? metricNamePrefix + '.' + metricName : metricName;
        return new MetricTaggerImpl(this,
                getKeyPrefix("metric", prefixedMetricName),
                this.id,
                metricName);
    }

    @Override
    public String metricName() {
        String metricName = this.keyPrefix + ']';
        assert metricName != null && !metricName.equals("[]");
        return metricName;
    }

    @Override
    public String metricId() {
        StringBuilder sb = new StringBuilder();
        if (metricNamePrefix != null) {
            sb.append(metricNamePrefix);
        }

        if (id != null) {
            sb.append('[').append(id).append(']');
        }

        if (metricName != null) {
            if (sb.length() > 0) {
                sb.append('.');
            }
            sb.append(metricName);
        }

        return sb.toString();
    }

}
