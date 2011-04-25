/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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

package com.hazelcast.monitor.server;

import com.hazelcast.monitor.client.event.ChangeEventType;
import com.hazelcast.monitor.client.event.InstanceStatistics;
import com.hazelcast.monitor.client.event.MapStatistics;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.DatasetRenderingOrder;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.StandardXYItemRenderer;
import org.jfree.data.time.Second;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;

import java.awt.*;
import java.util.List;

public class MChartGenerator extends InstanceChartGenerator {

    @Override
    ChangeEventType getChangeEventType() {
        return ChangeEventType.MAP_STATISTICS;
    }

    @Override
    protected void afterPlot(List<? super InstanceStatistics> list, JFreeChart chart, XYPlot plot) {
        NumberAxis sizeAxis = (NumberAxis) plot.getRangeAxis(0);
        Font labelFont = sizeAxis.getLabelFont();
        Paint labelPaint = sizeAxis.getLabelPaint();
        TimeSeries tm = new TimeSeries("memory");
        for (int i = 0; i < list.size(); i++) {
            double memory = 0;
            MapStatistics mapStatistics = (MapStatistics) list.get(i);
            for (MapStatistics.LocalMapStatistics localMapStatistics : mapStatistics.getListOfLocalStats()) {
                memory = memory + localMapStatistics.ownedEntryMemoryCost +
                        localMapStatistics.backupEntryMemoryCost + localMapStatistics.markedAsRemovedMemoryCost;
            }
            double mem = new Double(memory / (double) (1024 * 1024));
            tm.addOrUpdate(new Second(((MapStatistics) list.get(i)).getCreatedDate()), mem);
        }
        NumberAxis memoryAxis = new NumberAxis("memory (MB)");
        memoryAxis.setAutoRange(true);
        memoryAxis.setAutoRangeIncludesZero(false);
        plot.setDataset(1, new TimeSeriesCollection(tm));
        plot.setRangeAxis(1, memoryAxis);
        plot.mapDatasetToRangeAxis(1, 1);
        plot.setRenderer(1, new StandardXYItemRenderer());
        plot.setDatasetRenderingOrder(DatasetRenderingOrder.FORWARD);
        increaseRange(memoryAxis);
        memoryAxis.setLabelFont(labelFont);
        memoryAxis.setLabelPaint(labelPaint);
    }
}
