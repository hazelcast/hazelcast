/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.opt;

import java.util.ArrayList;
import java.util.List;

/**
 * Rows that constitute the plan.
 */
public class PlanRows {

    private final List<PlanRow> rows = new ArrayList<>();

    public void add(PlanRow row) {
        rows.add(row);
    }

    public PlanRow getRow(int index) {
        return rows.get(index);
    }

    public int getRowCount() {
        return rows.size();
    }

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder();

        int i = 1;

        for (PlanRow row : rows) {
            res.append(String.format("%02d", i++)).append(": ").append(row).append("\n");
        }

        return res.toString();
    }
}
