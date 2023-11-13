/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.function;

import com.hazelcast.security.impl.function.SecuredFunction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.security.Permission;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.util.CollectionUtil.isEmpty;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

class FunctionsImpl {
    static class ComposedSecuredFunction<F extends SecuredFunction, G extends SecuredFunction> implements SecuredFunction {
        protected final F before;
        protected final G after;

        ComposedSecuredFunction(F before, G after) {
            checkNotNull(before, "before");
            checkNotNull(after, "after");
            this.before = before;
            this.after = after;
        }

        @Nullable
        @Override
        public List<Permission> permissions() {
            List<Permission> beforeP = before.permissions();
            List<Permission> afterP = after.permissions();
            return sumPermissions(afterP, beforeP);
        }
    }

    static class ComposedFunctionEx<V, T, R>
            extends ComposedSecuredFunction<FunctionEx<? super V, ? extends T>, FunctionEx<? super T, ? extends R>>
            implements FunctionEx<V, R> {

        public ComposedFunctionEx(@Nonnull FunctionEx<? super V, ? extends T> before,
                                  @Nonnull FunctionEx<? super T, ? extends R> after) {
            super(before, after);
        }

        @Override
        public R applyEx(V v) throws Exception {
            return after.applyEx(before.applyEx(v));
        }
    }

    static class ComposedBiFunctionEx<U, V, T, R>
            extends ComposedSecuredFunction<BiFunctionEx<? super U, ? super V, ? extends T>, FunctionEx<? super T, ? extends R>>
            implements BiFunctionEx<U, V, R> {

        public ComposedBiFunctionEx(@Nonnull BiFunctionEx<? super U, ? super V, ? extends T> before,
                                  @Nonnull FunctionEx<? super T, ? extends R> after) {
            super(before, after);
        }

        @Override
        public R applyEx(U t, V u) throws Exception {
            return after.applyEx(before.applyEx(t, u));
        }
    }

    static class ComposedConsumerEx<T>
            extends ComposedSecuredFunction<ConsumerEx<? super T>, ConsumerEx<? super T>>
            implements ConsumerEx<T> {

        public ComposedConsumerEx(@Nonnull ConsumerEx<? super T> before,
                                    @Nonnull ConsumerEx<? super T> after) {
            super(before, after);
        }

        @Override
        public void acceptEx(T t) throws Exception {
            before.acceptEx(t);
            after.acceptEx(t);
        }
    }

    static class ComposedSupplierEx<T, R>
            extends ComposedSecuredFunction<SupplierEx<? extends T>, FunctionEx<? super T, ? extends R>>
            implements SupplierEx<R> {

        ComposedSupplierEx(SupplierEx<? extends T> wrapped, FunctionEx<? super T, ? extends R> wrapperSupplier) {
            super(wrapped, wrapperSupplier);
        }

        @Override
        public R getEx() throws Exception {
            return after.applyEx(before.getEx());
        }
    }

    private static List<Permission> sumPermissions(@Nullable List<Permission> afterP, @Nullable List<Permission> beforeP) {
        if (isEmpty(afterP)) {
            return beforeP;
        } else if (isEmpty(beforeP)) {
            return afterP;
        } else {
            List<Permission> permissions = new ArrayList<>(afterP.size() + beforeP.size());
            permissions.addAll(beforeP);
            permissions.addAll(afterP);
            return permissions;
        }
    }
}
