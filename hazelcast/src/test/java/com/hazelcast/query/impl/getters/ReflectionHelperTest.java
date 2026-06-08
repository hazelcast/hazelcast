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

package com.hazelcast.query.impl.getters;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.ReflectiveAttributeTestObject;
import com.hazelcast.query.impl.getters.policy.DefaultReflectiveAttributeLookupPolicy;
import com.hazelcast.query.impl.getters.policy.FullAccessReflectiveAttributeLookupPolicy;
import com.hazelcast.query.impl.getters.policy.ReflectiveAttributeLookupException;
import com.hazelcast.query.impl.getters.policy.ReflectiveAttributeLookupPolicy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import tools.jackson.databind.introspect.MemberKey;

import static com.hazelcast.internal.util.RootCauseMatcher.rootCause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReflectionHelperTest {

    @Test
    public void extractValue_whenIntermediateFieldIsInterfaceAndDoesNotContainField_thenThrowException() {
        // createGetter() method is catching everything throwable and wraps it in QueryException
        // I don't think it's the right thing to do, but I don't want to change this behaviour.
        // Hence, I have to check the root cause instead of declaring IllegalArgumentException as expected exception.
        assertThatThrownBy(() -> extractValue("emptyInterface.doesNotExist", true))
                .describedAs("Non-existing field has been ignored")
                .has(rootCause(IllegalArgumentException.class));
    }

    @Test
    public void extractValue_whenIntermediateFieldIsInterfaceAndDoesNotContainField_returnsNull()
            throws Exception {
        assertNull(extractValue("emptyInterface.doesNotExist", false));
    }

    @Test
    public void extractValue_whenAttributeMatchesStaticMethodAndGetter_withRestrictedPolicy_usesGetter() throws Exception {
        assertThat(extractValue(new ReflectiveAttributeTestObject("a"), "name", true,
                DefaultReflectiveAttributeLookupPolicy.INSTANCE)).isEqualTo("a");
    }

    @Test
    public void extractValue_whenAttributeMatchesStaticMethodAndGetter_withInactivePolicy_usesStaticMethod() throws Exception {
        assertThat(extractValue(new ReflectiveAttributeTestObject("b"), "name", true,
                FullAccessReflectiveAttributeLookupPolicy.INSTANCE)).isEqualTo("static");
    }

    @Test
    public void extractValue_whenStaticMethodWithRestrictedPolicy_thenThrowException() {
        assertThatThrownBy(() -> extractValue(new ReflectiveAttributeTestObject("c"), "getStaticValue", true,
                DefaultReflectiveAttributeLookupPolicy.INSTANCE))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("cannot be used for attribute extraction");
    }

    @Test
    public void extractValue_whenStaticFieldWithRestrictedPolicy_thenThrowException() {
        assertThatThrownBy(() -> extractValue(new ReflectiveAttributeTestObject("d"), "staticInt", true,
                DefaultReflectiveAttributeLookupPolicy.INSTANCE))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("cannot be used for attribute extraction");
    }

    @Test
    public void extractValue_whenVoidMethodWithRestrictedPolicy_thenThrowException() {
        assertThatThrownBy(() -> extractValue(new ReflectiveAttributeTestObject("e"), "doVoid", true,
                DefaultReflectiveAttributeLookupPolicy.INSTANCE))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("cannot be used for attribute extraction");
    }

    @Test
    public void extractValue_whenBlockedClassWithRestrictedPolicy_thenThrowException() {
        assertThatThrownBy(() -> extractValue(new ReflectiveAttributeTestObject("f"), "hazelcastInstance.name", true,
                DefaultReflectiveAttributeLookupPolicy.INSTANCE))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("cannot be used for attribute extraction");
    }

    @Test
    public void extractValue_whenBlockedClassProxyWithRestrictedPolicy_thenThrowException() {
        assertThatThrownBy(() -> extractValue(new ReflectiveAttributeTestObject("g"), "hazelcastInstanceProxy.name", true,
                DefaultReflectiveAttributeLookupPolicy.INSTANCE))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("cannot be used for attribute extraction");
    }

    @Test
    public void extractValue_whenBlockedClassIsTerminalFieldWithRestrictedPolicy_thenThrowException() {
        assertRestrictedLookupFailure(new ReflectiveAttributeTestObject("h"), "hazelcastInstance");
    }

    @Test
    public void extractValue_whenBlockedClassIsTerminalMethodWithRestrictedPolicy_thenThrowException() {
        assertRestrictedLookupFailure(new ReflectiveAttributeTestObject("i"), "getHazelcastInstance");
    }

    @Test
    public void extractValue_whenThisOnBlockedRootClassWithRestrictedPolicy_thenThrowException() {
        assertRestrictedLookupFailure(mock(HazelcastInstance.class), "this");
    }

    @Test
    public void extractValue_whenNestedPathStartsWithRestrictedAccessor_thenThrowException() {
        assertRestrictedLookupFailure(new ReflectiveAttributeTestObject("j"), "staticChild.child");
    }

    @Test
    public void extractValue_whenNestedPathContainsRestrictedAccessorInMiddle_thenThrowException() {
        assertRestrictedLookupFailure(new ReflectiveAttributeTestObject("k"), "child.hazelcastInstance.name");
    }

    @Test
    public void extractValue_whenNestedPathEndsWithRestrictedAccessor_thenThrowException() {
        ReflectiveAttributeTestObject child2 = new ReflectiveAttributeTestObject("j-child2");
        ReflectiveAttributeTestObject child1 = new ReflectiveAttributeTestObject("j-child1", child2, null, null, true);
        ReflectiveAttributeTestObject parent = new ReflectiveAttributeTestObject("j-parent", child1, null, null, true);
        assertRestrictedLookupFailure(parent, "child.child.staticChild");
    }

    @Test
    public void extractValue_whenNestedPathHasNoRestrictedAccessor_thenExtractValue() throws Exception {
        ReflectiveAttributeTestObject child2 = new ReflectiveAttributeTestObject("k-child2");
        ReflectiveAttributeTestObject child1 = new ReflectiveAttributeTestObject("k-child1", child2, null, null, true);
        ReflectiveAttributeTestObject parent = new ReflectiveAttributeTestObject("k-parent", child1, null, null, true);
        assertThat(extractValue(parent, "child.child.value", true,
                DefaultReflectiveAttributeLookupPolicy.INSTANCE)).isEqualTo("k-child2");
    }

    @Test
    public void extractValue_whenListElementPathHasNoRestrictedAccessor_thenExtractValue() throws Exception {
        ReflectiveAttributeTestObject object = ReflectiveAttributeTestObject.withList("l",
                new ReflectiveAttributeTestObject("list-value"));
        assertThat(extractValue(object, "objectAsList[0].value", true,
                DefaultReflectiveAttributeLookupPolicy.INSTANCE)).isEqualTo("list-value");
    }

    @Test
    public void extractValue_whenListElementPathHasRestrictedAccessor_thenThrowException() {
        ReflectiveAttributeTestObject object = ReflectiveAttributeTestObject.withList("m",
                new ReflectiveAttributeTestObject("list-value"));
        assertRestrictedLookupFailure(object, "objectAsList[0].staticChild");
    }

    @Test
    public void extractValue_whenListElementIsBlockedClass_thenThrowException() {
        ReflectiveAttributeTestObject object = new ReflectiveAttributeTestObject("n");
        object.setHazelcastInstance(mock(HazelcastInstance.class));
        assertRestrictedLookupFailure(object, "hazelcastInstanceList[0].getName");
    }

    @Test
    public void extractValue_whenArrayElementPathHasNoRestrictedAccessor_thenExtractValue() throws Exception {
        ReflectiveAttributeTestObject object = ReflectiveAttributeTestObject.withArray("o",
                new ReflectiveAttributeTestObject("array-value"));

        assertThat(extractValue(object, "objectAsArray[0].value", true,
                DefaultReflectiveAttributeLookupPolicy.INSTANCE)).isEqualTo("array-value");
    }

    @Test
    public void extractValue_whenArrayElementPathHasRestrictedAccessor_thenThrowException() {
        ReflectiveAttributeTestObject object = ReflectiveAttributeTestObject.withArray("p",
                new ReflectiveAttributeTestObject("array-value"));

        assertRestrictedLookupFailure(object, "objectAsArray[0].staticChild");
    }

    @Test
    public void extractValue_whenArrFetchedAndComponentTypeIsBlockedClass_thenThrowException() {
        ReflectiveAttributeTestObject object = new ReflectiveAttributeTestObject("q");
        assertRestrictedLookupFailure(object, "hazelcastInstanceArray");
    }

    @Test
    public void extractValue_whenMultiDimArrFetchedAndComponentTypeIsBlockedClass_thenThrowException() {
        ReflectiveAttributeTestObject object = new ReflectiveAttributeTestObject("r");
        assertRestrictedLookupFailure(object, "hazelcastInstanceMultiDimArray");
    }

    @Test
    public void extractValue_whenArrElementIsBlockedClass_thenThrowException() {
        ReflectiveAttributeTestObject object = new ReflectiveAttributeTestObject("s");
        assertRestrictedLookupFailure(object, "hazelcastInstanceArray[0].name");
    }

    @Test
    public void extractValue_whenArrElementIsBlockedClass_thenThrowException_WithPolicyHint() {
        ReflectiveAttributeTestObject object = new ReflectiveAttributeTestObject("t");
        assertThatThrownBy(() -> extractValue(object, "hazelcastInstanceArray[0].name", true,
                DefaultReflectiveAttributeLookupPolicy.INSTANCE))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining(ReflectiveAttributeLookupException.POLICY_HINT);
    }

    @Test
    public void extractValue_whenJacksonBlockedClass_thenThrowException_WithPolicyHint() {
        var object = new MemberKey("someKey", null);
        assertThatThrownBy(() -> extractValue(object, "argCount", true,
                DefaultReflectiveAttributeLookupPolicy.INSTANCE))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining(ReflectiveAttributeLookupException.POLICY_HINT);
    }

    @SuppressWarnings("unused")
    private static class OuterObject {
        private EmptyInterface emptyInterface;
    }

    private interface EmptyInterface {
    }

    private static Object extractValue(String attributeName, boolean failOnMissingAttribute) throws Exception {
        return extractValue(new OuterObject(), attributeName, failOnMissingAttribute,
                FullAccessReflectiveAttributeLookupPolicy.INSTANCE);
    }

    private static void assertRestrictedLookupFailure(Object object, String attributeName) {
        assertThatThrownBy(() -> extractValue(object, attributeName, true, DefaultReflectiveAttributeLookupPolicy.INSTANCE))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("cannot be used for attribute extraction");
    }

    private static Object extractValue(Object object, String attributeName, boolean failOnMissingAttribute,
                                       ReflectiveAttributeLookupPolicy lookupPolicy) throws Exception {
        return ReflectionHelper.createGetter(object, attributeName, failOnMissingAttribute, lookupPolicy)
                               .getValue(object);
    }
}
