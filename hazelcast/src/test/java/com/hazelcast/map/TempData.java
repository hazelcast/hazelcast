/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import java.io.Serializable;
import java.util.Map;

public class TempData implements Serializable {

    private static final long serialVersionUID = 1L;
    private String attr1;
    private String attr2;

    public TempData() {
    }

    public TempData(String attr1, String attr2) {
        this.attr1 = attr1;
        this.attr2 = attr2;
    }

    public String getAttr1() {
        return attr1;
    }

    public void setAttr1(String attr1) {
        this.attr1 = attr1;
    }

    public String getAttr2() {
        return attr2;
    }

    public void setAttr2(String attr2) {
        this.attr2 = attr2;
    }

    @Override
    public String toString() {
        return attr1 + " " + attr2;
    }

    public static class DeleteEntryProcessor extends AbstractEntryProcessor<String, TempData> {

        private static final long serialVersionUID = 1L;

        public Object process(Map.Entry<String, TempData> entry) {
            entry.setValue(null);
            return true;
        }

    }

    public static class LoggingEntryProcessor extends AbstractEntryProcessor<String, TempData> {

        private static final long serialVersionUID = 1L;

        public Object process(Map.Entry<String, TempData> entry) {
            return true;
        }

    }
}