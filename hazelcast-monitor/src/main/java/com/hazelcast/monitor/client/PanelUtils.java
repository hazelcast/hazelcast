/*
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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
 *
 */
package com.hazelcast.monitor.client;

import com.google.gwt.user.client.ui.FlexTable;

public class PanelUtils {
    public static FlexTable createFormattedFlexTable(){
        FlexTable table = new FlexTable();
        table.addStyleName("table");
        table.addStyleName("mapstats");
        table.getRowFormatter().addStyleName(0, "mapstatsHeader");
        return table;
    }

    public static void formatEvenRows(int row, FlexTable table){
        if (row % 2 == 0) {
                table.getRowFormatter().addStyleName(row, "mapstatsEvenRow");
            }
    }

    public static void removeUnusedRows(int row, FlexTable table) {
        while (table.getRowCount() > row) {
            table.removeRow(row);
        }
    }
}
