/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nullable;
import java.security.Permission;
import java.util.List;

import static com.hazelcast.function.Functions.entryKey;
import static com.hazelcast.function.Functions.entryValue;
import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.function.PredicateEx.alwaysFalse;
import static com.hazelcast.function.PredicateEx.alwaysTrue;
import static com.hazelcast.query.impl.predicates.PredicateTestUtils.entry;
import static com.hazelcast.security.permission.ActionConstants.ACTION_CREATE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@Category({QuickTest.class, ParallelJVMTest.class})
@RunWith(HazelcastParallelClassRunner.class)
public class FunctionsTest extends HazelcastTestSupport {

    @Test
    public void constructor() {
        assertUtilityConstructor(Functions.class);
    }

    @Test
    public void when_wholeItem() {
        Object o = new Object();
        assertSame(o, wholeItem().apply(o));
    }

    @Test
    public void when_entryKey() {
        assertEquals(1, entryKey().apply(entry(1, 2)));
    }

    @Test
    public void when_entryValue() {
        assertEquals(2, entryValue().apply(entry(1, 2)));
    }

    @Test
    public void when_alwaysTrue() {
        assertTrue(alwaysTrue().test(3));
    }

    @Test
    public void when_alwaysFalse() {
        assertFalse(alwaysFalse().test(2));
    }

    @Test
    public void when_noopConsumer() {
        // assert it's non-null and doesn't fail
        ConsumerEx.noop().accept(1);
    }


    //region FunctionEx permission
    @Test
    public void when_composeUnsecuredFunctionExWithUnsecured_then_noPermissions() {
        assertThat(FunctionEx.identity().compose(FunctionEx.identity()).permissions()).isNullOrEmpty();
        assertThat(FunctionEx.identity().andThen(FunctionEx.identity()).permissions()).isNullOrEmpty();
    }

    @Test
    public void when_composeFunctionExWithUnsecured_then_propagatePermissions() {
        var permission = new MapPermission("somemap", ACTION_CREATE);
        var fun = new SecurableFunction(permission);
        assertThat(fun.compose(FunctionEx.identity()).permissions()).containsExactly(permission);
        assertThat(fun.andThen(FunctionEx.identity()).permissions()).containsExactly(permission);
        assertThat(FunctionEx.identity().compose(fun).permissions()).containsExactly(permission);
        assertThat(FunctionEx.identity().andThen(fun).permissions()).containsExactly(permission);
    }

    @Test
    public void when_composeFunctionExWithSecured_then_propagatePermissions() {
        var permission = new MapPermission("somemap", ACTION_CREATE);
        var permission2 = new MapPermission("someothermap", ACTION_CREATE);
        var fun = new SecurableFunction(permission);
        var fun2 = new SecurableFunction(permission2);
        assertThat(fun.compose(fun2).permissions()).containsExactlyInAnyOrder(permission, permission2);
        assertThat(fun.andThen(fun2).permissions()).containsExactly(permission, permission2);
    }
    //endregion

    //region BiFunctionEx permissions
    @Test
    public void when_composeUnsecuredBiFunctionExWithUnsecured_then_noPermissions() {
        BiFunctionEx<Object, Object, Object> fun = (a, b) -> null;
        assertThat(fun.andThen(FunctionEx.identity()).permissions()).isNullOrEmpty();
    }

    @Test
    public void when_composeBiFunctionExWithUnsecured_then_propagatePermissions() {
        var permission = new MapPermission("somemap", ACTION_CREATE);
        var fun = new SecurableBiFunction(permission);
        assertThat(fun.andThen(FunctionEx.identity()).permissions()).containsExactly(permission);
    }

    @Test
    public void when_composeBiFunctionExWithSecured_then_propagatePermissions() {
        var permission = new MapPermission("somemap", ACTION_CREATE);
        var permission2 = new MapPermission("someothermap", ACTION_CREATE);
        var fun = new SecurableBiFunction(permission);
        var fun2 = new SecurableFunction(permission2);
        assertThat(fun.andThen(fun2).permissions()).containsExactly(permission, permission2);
    }

    @Test
    public void when_composeUnsecuredBiFunctionExWithSecured_then_propagatePermissions() {
        BiFunctionEx<Object, Object, Object> fun = (a, b) -> null;
        var permission = new MapPermission("somemap", ACTION_CREATE);
        var fun2 = new SecurableFunction(permission);
        assertThat(fun.andThen(fun2).permissions()).containsExactly(permission);
    }
    //endregion

    //region ConsumerEx permissions
    @Test
    public void when_composeUnsecuredConsumerExWithUnsecured_then_noPermissions() {
        assertThat(ConsumerEx.noop().andThen(ConsumerEx.noop()).permissions()).isNullOrEmpty();
    }

    @Test
    public void when_composeConsumerExWithUnsecured_then_propagatePermissions() {
        var permission = new MapPermission("somemap", ACTION_CREATE);
        var fun = new SecurableConsumer(permission);
        assertThat(fun.andThen(ConsumerEx.noop()).permissions()).containsExactly(permission);
        assertThat(ConsumerEx.noop().andThen(fun).permissions()).containsExactly(permission);
    }

    @Test
    public void when_composeConsumerExWithSecured_then_propagatePermissions() {
        var permission = new MapPermission("somemap", ACTION_CREATE);
        var permission2 = new MapPermission("someothermap", ACTION_CREATE);
        var fun = new SecurableConsumer(permission);
        var fun2 = new SecurableConsumer(permission2);
        assertThat(fun.andThen(fun2).permissions()).containsExactly(permission, permission2);
    }
    //endregion

    //region SupplierEx permissions
    @Test
    public void when_composeUnsecuredSupplierExWithUnsecured_then_noPermissions() {
        SupplierEx<String> fun = () -> "aaa";
        assertThat(fun.andThen(FunctionEx.identity()).permissions()).isNullOrEmpty();
    }

    @Test
    public void when_composeSupplierExWithUnsecured_then_propagatePermissions() {
        var permission = new MapPermission("somemap", ACTION_CREATE);
        var fun = new SecurableSupplier(permission);
        assertThat(fun.andThen(FunctionEx.identity()).permissions()).containsExactly(permission);
    }

    @Test
    public void when_composeUnsecuredSupplierExWithSecured_then_propagatePermissions() {
        SupplierEx<String> fun = () -> "aaa";
        var permission2 = new MapPermission("someothermap", ACTION_CREATE);
        var fun2 = new SecurableFunction(permission2);
        assertThat(fun.andThen(fun2).permissions()).containsExactly(permission2);
    }

    @Test
    public void when_composeSupplierExWithSecured_then_propagatePermissions() {
        var permission = new MapPermission("somemap", ACTION_CREATE);
        var permission2 = new MapPermission("someothermap", ACTION_CREATE);
        var fun = new SecurableSupplier(permission);
        var fun2 = new SecurableFunction(permission2);
        assertThat(fun.andThen(fun2).permissions()).containsExactly(permission, permission2);
    }
    //endregion

    static class SecurableFunction implements FunctionEx<Object, Object> {

        private List<Permission> permissions;

        SecurableFunction(List<Permission> permissions) {
            this.permissions = permissions;
        }

        SecurableFunction(Permission permission) {
            this(List.of(permission));
        }

        @Override
        public Object applyEx(Object t) {
            return null;
        }

        @Nullable
        @Override
        public List<Permission> permissions() {
            return permissions;
        }
    }

    static class SecurableBiFunction implements BiFunctionEx<Object, Object, Object> {

        private List<Permission> permissions;

        SecurableBiFunction(List<Permission> permissions) {
            this.permissions = permissions;
        }

        SecurableBiFunction(Permission permission) {
            this(List.of(permission));
        }

        @Override
        public Object applyEx(Object o, Object o2) {
            return null;
        }

        @Nullable
        @Override
        public List<Permission> permissions() {
            return permissions;
        }
    }

    static class SecurableConsumer implements ConsumerEx<Object> {

        private List<Permission> permissions;

        SecurableConsumer(List<Permission> permissions) {
            this.permissions = permissions;
        }

        SecurableConsumer(Permission permission) {
            this(List.of(permission));
        }

        @Override
        public void acceptEx(Object o) throws Exception {
        }

        @Nullable
        @Override
        public List<Permission> permissions() {
            return permissions;
        }
    }

    static class SecurableSupplier implements SupplierEx<Object> {

        private List<Permission> permissions;

        SecurableSupplier(List<Permission> permissions) {
            this.permissions = permissions;
        }

        SecurableSupplier(Permission permission) {
            this(List.of(permission));
        }

        @Override
        public Object getEx() {
            return null;
        }

        @Nullable
        @Override
        public List<Permission> permissions() {
            return permissions;
        }

    }
}
