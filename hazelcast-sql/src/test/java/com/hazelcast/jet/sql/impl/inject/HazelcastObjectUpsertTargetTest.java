/*
 * Copyright 2026 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.inject;

import com.hazelcast.config.JavaSerializationFilterConfig;
import com.hazelcast.internal.serialization.ReflectionClassNameFilter;
import com.hazelcast.sql.impl.expression.RowValue;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static com.hazelcast.jet.sql.impl.inject.PojoUpsertTargetTest.pojoType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HazelcastObjectUpsertTargetTest {

    @Test
    public void when_noFilter_then_sets() {
        when_filterDoesNotReject_then_sets(null);
    }

    @Test
    public void when_defaultFilterDoesNotReject_then_sets() {
        when_filterDoesNotReject_then_sets(new ReflectionClassNameFilter(new JavaSerializationFilterConfig()));
    }

    private void when_filterDoesNotReject_then_sets(ReflectionClassNameFilter filter) {
        var target = new HazelcastObjectUpsertTarget(filter);
        target.init();
        var injector = target.createInjector(null, pojoType());

        injector.set(nestedUdtRow());

        PojoUpsertTargetTest.Pojo expected = new PojoUpsertTargetTest.Pojo();
        expected.setNestedField(new PojoUpsertTargetTest.NestedPojo("value"));
        assertThat(target.conclude()).isEqualTo(expected);
    }

    @Test
    public void when_emptyFilterRejects_then_setFails() {
        JavaSerializationFilterConfig config = new JavaSerializationFilterConfig();
        config.setDefaultsDisabled(true);
        var filter = new ReflectionClassNameFilter(config);

        when_filterRejects_then_setFails(PojoUpsertTargetTest.Pojo.class, filter);
    }

    @Test
    public void when_nonDefaultFilterRejectsNested_then_setFails() {
        JavaSerializationFilterConfig config = new JavaSerializationFilterConfig();
        config.setDefaultsDisabled(true);
        config.getWhitelist().addClasses(PojoUpsertTargetTest.Pojo.class.getName());
        var filter = new ReflectionClassNameFilter(config);

        when_filterRejects_then_setFails(PojoUpsertTargetTest.NestedPojo.class, filter);
    }

    @Test
    public void when_filterRejectsOuter_then_setFails() {
        when_filterRejects_then_setFails(PojoUpsertTargetTest.Pojo.class, createFilterBlocking(PojoUpsertTargetTest.Pojo.class));
    }

    @Test
    public void when_filterRejectsNested_then_setFails() {
        when_filterRejects_then_setFails(PojoUpsertTargetTest.NestedPojo.class, createFilterBlocking(PojoUpsertTargetTest.NestedPojo.class));
    }

    private void when_filterRejects_then_setFails(Class<?> blockedClass, ReflectionClassNameFilter filter) {
        var target = new HazelcastObjectUpsertTarget(filter);
        target.init();
        var injector = target.createInjector(null, pojoType());

        assertThatThrownBy(() -> injector.set(nestedUdtRow()))
                .isInstanceOf(SecurityException.class)
                .hasMessage("Creation of class %s is not allowed.",  blockedClass.getName());
    }

    private static ReflectionClassNameFilter createFilterBlocking(Class<?> blockedClass) {
        JavaSerializationFilterConfig config = new JavaSerializationFilterConfig();
        config.getBlacklist().addClasses(blockedClass.getName());
        return new ReflectionClassNameFilter(config);
    }

    private RowValue nestedUdtRow() {
        return new RowValue(List.of(new RowValue(List.of("value"))));
    }
}
