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

package com.hazelcast.sql.optimizer;

import com.hazelcast.sql.impl.calcite.opt.cost.Cost;

import java.text.DecimalFormat;

public class PlanRow {

    private static final DecimalFormat FORMAT = new DecimalFormat("#.#");

    private final int level;
    private final String node;
    private final String signature;
    private final Double rowCount;
    private final String cost;

    public PlanRow(int level, String node, String signature) {
        this(level, node, signature, null, (String) null);
    }

    public PlanRow(int level, String node, String signature, Double rowCount) {
        this(level, node, signature, rowCount, (String) null);
    }

    public PlanRow(int level, String node, String signature, Double rowCount, Cost cost) {
        this(level, node, signature, rowCount, cost.toString());
    }

    public PlanRow(int level, String node, String signature, Double rowCount, String cost) {
        this.level = level;
        this.node = node;
        this.signature = signature;
        this.rowCount = rowCount;
        this.cost = cost;
    }

    public int getLevel() {
        return level;
    }

    public String getNode() {
        return node;
    }

    public String getSignature() {
        return signature;
    }

    public Double getRowCount() {
        return rowCount;
    }

    public String getCost() {
        return cost;
    }

    public static PlanRow parse(String input) {
        // Get level
        int level = 0;

        while (input.charAt(level * 2) == ' ') {
            level++;
        }

        // Get node and signature
        int signatureOpen = input.indexOf('(');
        int signatureClose = input.contains(":") ? input.indexOf(":") - 1 : input.lastIndexOf(")");

        String node = input.substring(level * 2, signatureOpen);
        String signature = input.substring(signatureOpen + 1, signatureClose);

        // Get row count
        int rowCountPos = input.indexOf("rowcount = ");

        Double rowCount = rowCountPos != -1
            ? Double.parseDouble(input.substring(rowCountPos + 11, input.indexOf(',', rowCountPos + 11))) : null;

        // Get cost
        int costPos = input.indexOf("Cost");

        String cost = costPos != -1 ? input.substring(costPos, input.indexOf("}", costPos)) : null;

        return new PlanRow(level, node, signature, rowCount, cost);
    }

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder();

        for (int i = 0; i < level; i++) {
            res.append("  ");
        }

        res.append(node).append("(").append(signature).append(")");

        if (rowCount != null) {
            res.append(": rowCount = ").append(FORMAT.format(rowCount));
        }

        if (cost != null) {
            res.append(", cumulative cost = ").append(cost);
        }

        return res.toString();
    }
}
