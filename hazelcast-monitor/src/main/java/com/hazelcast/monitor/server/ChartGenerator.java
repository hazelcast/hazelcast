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

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;

public class ChartGenerator extends HttpServlet {
    protected void doGet(HttpServletRequest req, HttpServletResponse response) throws javax.servlet.ServletException, java.io.IOException {
        XYSeries zeroSeries = new XYSeries("");
        zeroSeries.add(0.0, 0.0);
        zeroSeries.add(150.0, 0.0);

        XYSeries series = new XYSeries("Average Weight");

        series.add(20.0, Math.random() * 100);
        series.add(40.0, Math.random() * 100);
        series.add(55.0, Math.random() * 100);
        series.add(70.0, Math.random() * 100);
        XYSeriesCollection xyDataset = new XYSeriesCollection(series);
        xyDataset.addSeries(zeroSeries);

        JFreeChart chart = ChartFactory.createXYLineChart
                ("XYLine Chart using JFreeChart", "Age", "Weight",
                        xyDataset, PlotOrientation.VERTICAL, false, false, false);
        try {
            OutputStream out = response.getOutputStream();
            response.setContentType("image/png");
            ChartUtilities.writeChartAsPNG(out, chart, 200, 200);
        } catch (IOException ignore) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
