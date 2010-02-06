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
import com.hazelcast.monitor.client.event.MapStatistics;
import com.hazelcast.monitor.server.event.ChangeEventGenerator;
import com.hazelcast.monitor.server.event.MapStatisticsGenerator;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartFrame;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.DatasetRenderingOrder;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.StandardXYItemRenderer;
import org.jfree.data.Range;
import org.jfree.data.time.Second;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import static com.hazelcast.monitor.server.HazelcastServiceImpl.getSessionObject;

public class ChartGenerator extends HttpServlet {
    protected void doGet(HttpServletRequest req, HttpServletResponse response) throws javax.servlet.ServletException, java.io.IOException {
        String name = req.getParameter("name");
        SessionObject sessionObject = getSessionObject(req.getSession());
        List<MapStatistics> list = null;
        for (ChangeEventGenerator eventGenerator : sessionObject.eventGenerators) {
            if (eventGenerator.getChangeEventType().equals(ChangeEventType.MAP_STATISTICS)) {
                MapStatisticsGenerator msg = (MapStatisticsGenerator) eventGenerator;
                if (!msg.getName().equals(name)) {
                    continue;
                }
                list = msg.getPastMapStatistics();
            }
        }
        if (list == null) {
            return;
        }
        JFreeChart chart = generateChart(list);
        try {
            OutputStream out = response.getOutputStream();
            response.setContentType("image/png");
            ChartUtilities.writeChartAsPNG(out, chart, 800, 300);
        } catch (IOException ignore) {
        }
    }

    public JFreeChart generateChart(List<MapStatistics> list) {
        TimeSeries ts = new TimeSeries("size", Second.class);
        TimeSeries tm = new TimeSeries("memory", Second.class);
        for (int i = 0; i < list.size(); i++) {
            MapStatistics mapStatistics = list.get(i);
            double size = mapStatistics.getSize();
            double memory = 0;
            Collection<MapStatistics.LocalMapStatistics> localMapStatses = mapStatistics.getListOfLocalStats();
            for (MapStatistics.LocalMapStatistics localMapStatistics : localMapStatses) {
                memory = memory + localMapStatistics.ownedEntryMemoryCost +
                        localMapStatistics.backupEntryMemoryCost + localMapStatistics.markedAsRemovedMemoryCost;
            }
            ts.addOrUpdate(new Second(list.get(i).getCreatedDate()), new Double(size/(double)1000));
            tm.addOrUpdate(new Second(list.get(i).getCreatedDate()), new Double(memory/(double)(1024*1024)));
        }
        TimeSeriesCollection timeDataset = new TimeSeriesCollection();
        timeDataset.addSeries(ts);
        NumberAxis axis = new NumberAxis("Memory (MB)");
        axis.setAutoRange(true);
        axis.setAutoRangeIncludesZero(false);
        JFreeChart chart =
                ChartFactory.createTimeSeriesChart(null, "Time", "Size (X1000)", timeDataset, true, true, true);
        XYPlot plot = (XYPlot) chart.getPlot();
        plot.setDataset(1, new TimeSeriesCollection(tm));
        plot.setRangeAxis(1, axis);
        plot.mapDatasetToRangeAxis(1, 1);
        plot.setRenderer(1, new StandardXYItemRenderer());
        plot.setDatasetRenderingOrder(DatasetRenderingOrder.FORWARD);
        increaseRange(axis);
        NumberAxis sizeAxis = (NumberAxis) plot.getRangeAxis(0);
        increaseRange(sizeAxis);
        return chart;
    }

    public static void main(String[] args) {
        TimeSeries ts = new TimeSeries("size", Second.class);
        TimeSeries tm = new TimeSeries("memory", Second.class);
        int[] values = {1, 2, 4, 5, 6, 7, 7, 8, 4, 3};
        for (int i = 0; i < 10; i++) {
            int size = values[i];
            int memory = values[i] + 100;
            ts.addOrUpdate(new Second(new Date(i * 100000)), new Integer(size));
            tm.addOrUpdate(new Second(new Date(i * 100000)), new Integer(memory));
        }
        TimeSeriesCollection timeDataset = new TimeSeriesCollection();
        timeDataset.addSeries(ts);
        NumberAxis axis = new NumberAxis("Memory");
        axis.setAutoRange(true);
        axis.setAutoRangeIncludesZero(false);
        JFreeChart chart =
                ChartFactory.createTimeSeriesChart("Map: ", "Time", "Size", timeDataset, true, true, true);
        XYPlot plot = (XYPlot) chart.getPlot();
        plot.setDataset(1, new TimeSeriesCollection(tm));
        plot.setRangeAxis(1, axis);
        plot.mapDatasetToRangeAxis(1, 1);
        plot.setRenderer(1, new StandardXYItemRenderer());//StandardXYItemRenderer.DISCONTINUOUS_LINES
        plot.setDatasetRenderingOrder(DatasetRenderingOrder.FORWARD);
        ChartFrame frame = new ChartFrame("Title", chart);
        frame.setSize(500, 300);
        frame.setVisible(true);
        increaseRange(axis);
        NumberAxis sizeAxis = (NumberAxis) plot.getRangeAxis(0);
        increaseRange(sizeAxis);
    }

    private static void increaseRange(NumberAxis axis) {
        Range range = axis.getRange();
        double lower = range.getLowerBound();
        double upper = range.getUpperBound();
        double diff = upper-lower;
        axis.setRange(lower-diff*0.5, upper+diff*0.5);
    }
}
