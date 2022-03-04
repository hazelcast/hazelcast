/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.metrics.MetricDescriptor;

import java.util.function.Supplier;

/**
 * Default {@link MetricDescriptor} {@link Supplier} creating new instance
 * on every call to {@link #get()}.
 */
public class DefaultMetricDescriptorSupplier implements Supplier<MetricDescriptorImpl> {
    public static final Supplier<MetricDescriptorImpl> DEFAULT_DESCRIPTOR_SUPPLIER = new DefaultMetricDescriptorSupplier();

    @Override
    public MetricDescriptorImpl get() {
        return new MetricDescriptorImpl(this);
    }
}
