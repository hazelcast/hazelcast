/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.datamodel;

import com.hazelcast.test.HazelcastParallelClassRunner;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
public class EqualityTest {

    @Test
    public void testEqualsAndHashCode_whenBagsByTag() {
        EqualsVerifier.forClass(BagsByTag.class)
                      .suppress(Warning.STRICT_INHERITANCE)
                      .verify();
    }

    @Test
    public void testEqualsAndHashCode_whenItemsByTag() {
        EqualsVerifier.forClass(ItemsByTag.class)
                      .suppress(Warning.STRICT_INHERITANCE)
                      .verify();
    }

    @Test
    public void testEqualsAndHashCode_whenTag() {
        EqualsVerifier.forClass(Tag.class).verify();
    }

    @Test
    public void testEqualsAndHashCode_whenThreeBags() {
        EqualsVerifier.forClass(ThreeBags.class).verify();
    }

    @Test
    public void testEqualsAndHashCode_whenTimestampedEntry() {
        EqualsVerifier.forClass(TimestampedEntry.class).verify();
    }

    @Test
    public void testEqualsAndHashCode_whenTimestampedItem() {
        EqualsVerifier.forClass(TimestampedItem.class).verify();
    }

    @Test
    public void testEqualsAndHashCode_whenTuple2() {
        EqualsVerifier.forClass(Tuple2.class)
                      .suppress(Warning.NONFINAL_FIELDS)
                      .verify();
    }

    @Test
    public void testEqualsAndHashCode_whenTuple3() {
        EqualsVerifier.forClass(Tuple3.class)
                      .suppress(Warning.NONFINAL_FIELDS)
                      .verify();
    }

    @Test
    public void testEqualsAndHashCode_whenTwoBags() {
        EqualsVerifier.forClass(TwoBags.class).verify();
    }

    @Test
    public void testEqualsAndHashCode_whenWindowResult() {
        EqualsVerifier.forClass(WindowResult.class)
                      .suppress(Warning.STRICT_INHERITANCE)
                      .verify();
    }

}
