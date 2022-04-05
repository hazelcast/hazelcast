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

package com.hazelcast.query.impl.getters;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MethodGetterTest {

    private Method limbArrayMethod;
    private Method limbCollectionMethod;
    private Method nailArrayMethod;
    private Method nailCollectionMethod;

    private Body body;

    private Nail redNail;
    private Nail greenNail;
    private Limb leg;

    private Nail whiteNail;
    private Nail blackNail;
    private Limb hand;

    private Limb unnamedLimb;

    @Before
    public void setUp() throws NoSuchMethodException {
        limbArrayMethod = Body.class.getMethod("getLimbArray");
        limbCollectionMethod = Body.class.getMethod("getLimbCollection");
        nailArrayMethod = Limb.class.getMethod("getNailArray");
        nailCollectionMethod = Limb.class.getMethod("getNailCollection");

        redNail = new Nail("red");
        greenNail = new Nail("green");
        leg = new Limb("leg", redNail, greenNail);

        whiteNail = new Nail("white");
        blackNail = new Nail("black");
        hand = new Limb("hand", whiteNail, blackNail);

        unnamedLimb = new Limb(null);
        body = new Body("bodyName", leg, hand, unnamedLimb);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_whenModifierIsNotNullAndMethodReturnTypeIsNotArrayOrCollection_thenThrowIllegalArgumentException()
            throws Exception {
        Method method = Body.class.getMethod("getName");
        new MethodGetter(null, method, "[any]", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_whenModifierIsNegative_thenThrowIllegalArgumentException() throws Exception {
        Method method = Body.class.getMethod("getName");
        new MethodGetter(null, method, "[-1]", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_whenModifierIsStarAndMethodReturnTypeIsCollection_thenThrowIllegalArgumentException() {
        new MethodGetter(null, limbCollectionMethod, "[any]", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_whenModifierIsPositionAndMethodReturnTypeIsCollection_thenThrowIllegalArgumentException() {
        new MethodGetter(null, limbCollectionMethod, "[0]", null);
    }

    @Test
    public void getValue_whenModifierOnArrayIsStar_thenReturnMultiValueResultWithAllItems() throws Exception {
        MethodGetter getter = new MethodGetter(null, limbArrayMethod, "[any]", null);
        MultiResult result = (MultiResult) getter.getValue(body);

        assertContainsInAnyOrder(result, leg, hand, unnamedLimb);
    }

    @Test
    public void getValue_whenParentIsMultiValueAndModifierOnArrayIsStar_thenReturnMultiValueResultWithAllItems()
            throws Exception {
        MethodGetter limbGetter = new MethodGetter(null, limbArrayMethod, "[any]", null);
        MethodGetter nailGetter = new MethodGetter(limbGetter, nailArrayMethod, "[any]", null);

        MultiResult result = (MultiResult) nailGetter.getValue(body);

        assertContainsInAnyOrder(result, whiteNail, blackNail, redNail, greenNail, null);
    }

    @Test
    public void getValue_whenParentIsMultiValueAndModifierOnArrayIsPosition_thenReturnMultiValueResultWithItemsAtPosition()
            throws Exception {
        MethodGetter limbGetter = new MethodGetter(null, limbArrayMethod, "[any]", null);
        MethodGetter nailGetter = new MethodGetter(limbGetter, nailArrayMethod, "[0]", null);

        MultiResult result = (MultiResult) nailGetter.getValue(body);

        assertContainsInAnyOrder(result, redNail, whiteNail, null);
    }

    @Test
    public void getValue_whenParentIsMultiValueAndModifierOnCollectionIsStar_thenReturnMultiValueResultWithAllItems()
            throws Exception {
        MethodGetter limbGetter = new MethodGetter(null, limbArrayMethod, "[any]", null);
        MethodGetter nailGetter = new MethodGetter(limbGetter, nailCollectionMethod, "[any]", Nail.class);

        MultiResult result = (MultiResult) nailGetter.getValue(body);

        assertContainsInAnyOrder(result, whiteNail, blackNail, redNail, greenNail, null);
    }

    @Test
    public void getValue_whenParentIsMultiValueAndModifierOnCollectionIsPosition_thenReturnMultiValueResultWithItemsAtPosition()
            throws Exception {
        MethodGetter limbGetter = new MethodGetter(null, limbArrayMethod, "[any]", null);
        MethodGetter nailGetter = new MethodGetter(limbGetter, nailArrayMethod, "[0]", Nail.class);

        MultiResult result = (MultiResult) nailGetter.getValue(body);

        assertContainsInAnyOrder(result, redNail, whiteNail, null);
    }

    @Test
    public void getValue_whenModifierOnCollectionIsStar_thenReturnMultiValueResultWithAllItems() throws Exception {
        MethodGetter getter = new MethodGetter(null, limbCollectionMethod, "[any]", Limb.class);
        MultiResult result = (MultiResult) getter.getValue(body);

        assertContainsInAnyOrder(result, leg, hand, unnamedLimb);
    }

    @Test
    public void getValue_whenModifierOnArrayIsPositionAndElementAtGivenPositionExist_thenReturnTheItem() throws Exception {
        MethodGetter getter = new MethodGetter(null, limbArrayMethod, "[0]", null);
        Limb result = (Limb) getter.getValue(body);

        assertSame(leg, result);
    }

    @Test
    public void getValue_whenModifierOnCollectionIsPositionAndElementAtGivenPositionExist_thenReturnTheItem() throws Exception {
        MethodGetter getter = new MethodGetter(null, limbCollectionMethod, "[0]", Limb.class);
        Limb result = (Limb) getter.getValue(body);

        assertSame(leg, result);
    }

    @Test
    public void getValue_whenModifierOnArrayIsPositionAndElementAtGivenPositionDoesNotExist_thenReturnNull() throws Exception {
        MethodGetter getter = new MethodGetter(null, limbArrayMethod, "[3]", null);
        Limb result = (Limb) getter.getValue(body);

        assertNull(result);
    }

    @Test
    public void getValue_whenModifierOnCollectionIsPositionAndElementAtGivenPositionDoesNotExist_thenReturnNull()
            throws Exception {
        MethodGetter getter = new MethodGetter(null, limbCollectionMethod, "[3]", Limb.class);
        Limb result = (Limb) getter.getValue(body);

        assertNull(result);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getValue_whenNoModifierOnCollection_thenReturnTheCollection() throws Exception {
        MethodGetter getter = new MethodGetter(null, limbCollectionMethod, null, null);
        Collection<Limb> result = (Collection<Limb>) getter.getValue(body);

        assertSame(body.limbCollection, result);
    }

    @Test
    public void getValue_whenParentIsMultiResultAndNoModifier_thenReturnTheMultiResultContainingCurrentObjects()
            throws Exception {
        MethodGetter limbGetter = new MethodGetter(null, limbArrayMethod, "[any]", null);
        Method getLimbNameMethod = Limb.class.getMethod("getName");
        MethodGetter nailNameGetter = new MethodGetter(limbGetter, getLimbNameMethod, null, null);
        MultiResult result = (MultiResult) nailNameGetter.getValue(body);

        assertContainsInAnyOrder(result, "leg", "hand", null);
    }

    @Test
    public void getValue_whenNoModifierOnArray_thenReturnTheArray() throws Exception {
        MethodGetter getter = new MethodGetter(null, limbArrayMethod, null, null);
        Limb[] result = (Limb[]) getter.getValue(body);

        assertSame(body.limbArray, result);
    }

    @Test
    public void getValue_whenInputIsNull_thenReturnNull() throws Exception {
        MethodGetter getter = new MethodGetter(null, limbArrayMethod, null, null);
        Limb[] result = (Limb[]) getter.getValue(null);

        assertNull(result);
    }

    @Test
    public void getReturnType_whenSetExplicitly_thenReturnIt() {
        MethodGetter getter = new MethodGetter(null, limbCollectionMethod, "[any]", Limb.class);
        Class returnType = getter.getReturnType();

        assertEquals(Limb.class, returnType);
    }

    @Test
    public void getReturnType_whenModifierIsPositionAndMethodReturnTypeeIsArray_thenInferReturnTypeFromTheArray() {
        MethodGetter getter = new MethodGetter(null, limbArrayMethod, "[0]", null);
        Class returnType = getter.getReturnType();

        assertEquals(Limb.class, returnType);
    }

    @Test
    public void getReturnType_whenModifierIsStarAndMethodReturnIsArray_thenInferReturnTypeFromTheArray() {
        MethodGetter getter = new MethodGetter(null, limbArrayMethod, "[any]", null);
        Class returnType = getter.getReturnType();

        assertEquals(Limb.class, returnType);
    }

    @Test
    public void getReturnType_whenNoModifierAndMethodReturnIsArray_thenReturnTheArrayType() {
        MethodGetter getter = new MethodGetter(null, limbArrayMethod, null, null);
        Class returnType = getter.getReturnType();

        assertEquals(Limb[].class, returnType);
    }

    private void assertContainsInAnyOrder(MultiResult multiResult, Object... items) {
        List results = multiResult.getResults();
        if (results.size() != items.length) {
            fail("MultiResult " + multiResult + " has size " + results.size() + ", but expected size is " + items.length);
        }
        for (Object item : items) {
            if (!results.contains(item)) {
                fail("MultiResult " + multiResult + " does not contain expected item " + item);
            }
        }
    }

    @SuppressWarnings("unused")
    static class Body {
        String name;
        Limb[] limbArray;
        Collection<Limb> limbCollection;

        Body(String name, Limb... limbs) {
            this.name = name;
            this.limbCollection = Arrays.asList(limbs);
            this.limbArray = limbs;
        }

        public String getName() {
            return name;
        }

        public Collection<Limb> getLimbCollection() {
            return limbCollection;
        }

        public Limb[] getLimbArray() {
            return limbArray;
        }
    }

    @SuppressWarnings("unused")
    static class Limb {
        String name;
        Nail[] nailArray;
        Collection<Nail> nailCollection;

        Limb(String name, Nail... nails) {
            this.name = name;
            this.nailCollection = Arrays.asList(nails);
            this.nailArray = nails;
        }

        public String getName() {
            return name;
        }

        public Collection<Nail> getNailCollection() {
            return nailCollection;
        }

        public Nail[] getNailArray() {
            return nailArray;
        }
    }

    static final class Nail {
        String colour;

        private Nail(String colour) {
            this.colour = colour;
        }
    }
}
