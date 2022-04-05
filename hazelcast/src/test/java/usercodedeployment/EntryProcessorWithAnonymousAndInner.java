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

package usercodedeployment;

import com.hazelcast.map.EntryProcessor;

import java.util.Map;

public class EntryProcessorWithAnonymousAndInner implements EntryProcessor<Integer, Integer, Integer> {

    private static final long serialVersionUID = 8936595533044945435L;

    @Override
    public Integer process(final Map.Entry<Integer, Integer> entry) {
        Integer origValue = entry.getValue();
        final Integer newValue = origValue + 1;

        final Test test = new Test(entry);
        Runnable runnable = () -> test.set(newValue);
        runnable.run();

        return newValue;
    }

    public class Test {
        private final Map.Entry<Integer, Integer> entry;

        Test(Map.Entry<Integer, Integer> entry) {
            this.entry = entry;
        }

        void set(Integer newValue) {
            entry.setValue(newValue);
        }
    }
}
