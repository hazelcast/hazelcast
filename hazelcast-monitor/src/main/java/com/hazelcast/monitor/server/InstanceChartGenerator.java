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
import com.hazelcast.monitor.server.event.ChangeEventGenerator;
import com.hazelcast.monitor.server.event.InstanceStatisticsGenerator;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.Range;
import org.jfree.data.time.Second;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static com.hazelcast.monitor.server.HazelcastServiceImpl.getSessionObject;

public abstract class InstanceChartGenerator extends HttpServlet {
    static void increaseRange(NumberAxis axis) {
        Range range = axis.getRange();
        double lower = range.getLowerBound();
        double upper = range.getUpperBound();
        double diff = upper - lower;
        axis.setRange(lower - diff * 0.3, upper + diff * 0.3);
    }

    protected void doGet(HttpServletRequest req, HttpServletResponse response) throws javax.servlet.ServletException, java.io.IOException {
        String name = req.getParameter("name");
        String type = req.getParameter("type");
        SessionObject sessionObject = getSessionObject(req.getSession());
        List<? super InstanceStatistics> list = getPastStatistics(name, sessionObject);
        if (list == null) {
            return;
        }
        JFreeChart chart;
        if ("size".equals(type)) {
            chart = generateSizeChart(list);
        } else {
            chart = generateOperationStatsChart(list);
        }
        try {
            OutputStream out = response.getOutputStream();
            response.setContentType("image/png");
            ChartUtilities.writeChartAsPNG(out, chart, 390, 250);
        } catch (IOException ignore) {
        }
    }

    private List<? super InstanceStatistics> getPastStatistics(String name, SessionObject sessionObject) {
        List<? super InstanceStatistics> list = null;
        for (ChangeEventGenerator eventGenerator : sessionObject.eventGenerators) {
            if (eventGenerator.getChangeEventType().equals(getChangeEventType())) {
                InstanceStatisticsGenerator msg = (InstanceStatisticsGenerator) eventGenerator;
                if (!msg.getName().equals(name)) {
                    continue;
                }
                list = msg.getPastStatistics();
            }
        }
        return list;
    }

    abstract ChangeEventType getChangeEventType();

    public JFreeChart generateOperationStatsChart(List<? super InstanceStatistics> list) {
        TimeSeries ts = new TimeSeries("operations per second");
        for (int i = 0; i < list.size(); i++) {
            InstanceStatistics instanceStatistics = (InstanceStatistics) list.get(i);
            ts.addOrUpdate(new Second(instanceStatistics.getCreatedDate()), (double)instanceStatistics.getTotalOPS() / 1000);
        }
        TimeSeriesCollection timeDataset = new TimeSeriesCollection();
        timeDataset.addSeries(ts);
        JFreeChart chart =
                ChartFactory.createTimeSeriesChart(null, "time", "throughput (x1000)", timeDataset, true, true, true);
        XYPlot plot = (XYPlot) chart.getPlot();
        increaseRange((NumberAxis) plot.getRangeAxis(0));
        return chart;
    }

    public JFreeChart generateSizeChart(List<? super InstanceStatistics> list) {
        TimeSeries ts = new TimeSeries("size");
        for (int i = 0; i < list.size(); i++) {
            InstanceStatistics instanceStatistics = (InstanceStatistics) list.get(i);
            ts.addOrUpdate(new Second(instanceStatistics.getCreatedDate()), new Double(instanceStatistics.getSize() / (double) 1000));
        }
        TimeSeriesCollection timeDataset = new TimeSeriesCollection();
        timeDataset.addSeries(ts);
        JFreeChart chart =
                ChartFactory.createTimeSeriesChart(null, "time", "size (x1000)", timeDataset, true, true, true);
        XYPlot plot = (XYPlot) chart.getPlot();
        NumberAxis sizeAxis = (NumberAxis) plot.getRangeAxis(0);
        increaseRange(sizeAxis);
        afterPlot(list, chart, plot);
        return chart;
    }

    protected abstract void afterPlot(List<? super InstanceStatistics> list, JFreeChart chart, XYPlot plot);
}
