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

package com.hazelcast.jet.function;

import com.hazelcast.jet.impl.util.ExceptionUtil;

import java.io.Serializable;
import java.util.function.DoubleBinaryOperator;

/**
 * {@code Serializable} variant of {@link DoubleBinaryOperator
 * java.util.function.DoubleBinaryOperator} which declares checked exception.
 */
@FunctionalInterface
public interface DistributedDoubleBinaryOperator extends DoubleBinaryOperator, Serializable {

    /**
     * Exception-declaring version of {@link DoubleBinaryOperator#applyAsDouble}.
     */
    double applyAsDoubleEx(double left, double right) throws Exception;

    @Override
    default double applyAsDouble(double left, double right) {
        try {
            return applyAsDoubleEx(left, right);
        } catch (Exception e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }
}
