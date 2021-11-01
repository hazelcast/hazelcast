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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.jet.core.SlidingWindowPolicy;
import com.hazelcast.jet.sql.impl.aggregate.WindowUtils;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Arrays.stream;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public final class WindowProperties {

    private final Map<Integer, WindowProperty> propertiesByIndex;

    public WindowProperties(WindowProperty... properties) {
        this(stream(properties).collect(toMap(WindowProperty::index, identity())));
    }

    private WindowProperties(Map<Integer, WindowProperty> propertiesByIndex) {
        this.propertiesByIndex = Collections.unmodifiableMap(propertiesByIndex);
    }

    public WindowProperties merge(WindowProperties other) {
        if (other == null || other.propertiesByIndex.isEmpty()) {
            return this;
        }

        Map<Integer, WindowProperty> propertiesByIndex = new HashMap<>(this.propertiesByIndex);
        for (WindowProperty otherProperty : other.propertiesByIndex.values()) {
            assert !propertiesByIndex.containsKey(otherProperty.index());
            propertiesByIndex.put(otherProperty.index(), otherProperty);
        }
        return new WindowProperties(propertiesByIndex);
    }

    public WindowProperties retain(Set<Integer> indices) {
        Map<Integer, WindowProperty> propertiesByIndex = new HashMap<>(this.propertiesByIndex);
        propertiesByIndex.keySet().retainAll(indices);
        return new WindowProperties(propertiesByIndex);
    }

    @Nullable
    public WindowProperty findFirst(List<Integer> indices) {
        for (int index : indices) {
            WindowProperty property = propertiesByIndex.get(index);
            if (property != null) {
                return property;
            }
        }
        return null;
    }

    public Stream<WindowProperty> getProperties() {
        return propertiesByIndex.values().stream();
    }

    public interface WindowProperty {

        int index();

        ToLongFunctionEx<Object[]> orderingFn(ExpressionEvalContext context);

        SlidingWindowPolicy windowPolicy(ExpressionEvalContext context);

        /**
         * Returns a copy of this property, but with index changed.
         */
        WindowProperty withIndex(int index);
    }

    public static class WindowStartProperty implements WindowProperty {

        private final int index;
        private final FunctionEx<ExpressionEvalContext, SlidingWindowPolicy> windowPolicyProvider;

        public WindowStartProperty(int index, FunctionEx<ExpressionEvalContext, SlidingWindowPolicy> windowPolicyProvider) {
            this.index = index;
            this.windowPolicyProvider = windowPolicyProvider;
        }

        @Override
        public int index() {
            return index;
        }

        @Override
        public ToLongFunctionEx<Object[]> orderingFn(ExpressionEvalContext context) {
            int index = this.index;
            return row -> WindowUtils.extractMillis(row[index]);
        }

        @Override
        public SlidingWindowPolicy windowPolicy(ExpressionEvalContext context) {
            return windowPolicyProvider.apply(context);
        }

        @Override
        public WindowStartProperty withIndex(int index) {
            return new WindowStartProperty(index, windowPolicyProvider);
        }
    }

    public static class WindowEndProperty implements WindowProperty {

        private final int index;
        private final FunctionEx<ExpressionEvalContext, SlidingWindowPolicy> windowPolicyProvider;

        public WindowEndProperty(int index, FunctionEx<ExpressionEvalContext, SlidingWindowPolicy> windowPolicyProvider) {
            this.index = index;
            this.windowPolicyProvider = windowPolicyProvider;
        }

        @Override
        public int index() {
            return index;
        }

        @Override
        public ToLongFunctionEx<Object[]> orderingFn(ExpressionEvalContext context) {
            int index = this.index;
            SlidingWindowPolicy windowPolicy = this.windowPolicyProvider.apply(context);
            return row -> {
                long millis = WindowUtils.extractMillis(row[index]);
                return millis - windowPolicy.windowSize();
            };
        }

        @Override
        public SlidingWindowPolicy windowPolicy(ExpressionEvalContext context) {
            return windowPolicyProvider.apply(context);
        }

        @Override
        public WindowEndProperty withIndex(int index) {
            return new WindowEndProperty(index, windowPolicyProvider);
        }
    }
}
