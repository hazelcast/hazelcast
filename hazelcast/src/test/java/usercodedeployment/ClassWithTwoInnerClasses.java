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

package usercodedeployment;

import com.hazelcast.map.AbstractEntryProcessor;

import java.io.Serializable;
import java.util.Map;

public class ClassWithTwoInnerClasses implements Serializable {
    public static class StaticNestedIncrementingEntryProcessor extends AbstractEntryProcessor<Integer, Integer> {
        @Override
        public Object process(Map.Entry<Integer, Integer> entry) {
            Integer origValue = entry.getValue();
            Integer newValue = origValue + 1;
            entry.setValue(newValue);
            return newValue;
        }
    }

    public static class StaticNestedDecrementingEntryProcessor extends AbstractEntryProcessor<Integer, Integer> {
        @Override
        public Object process(Map.Entry<Integer, Integer> entry) {
            Integer origValue = entry.getValue();
            Integer newValue = origValue - 1;
            entry.setValue(newValue);
            return newValue;
        }
    }
}
