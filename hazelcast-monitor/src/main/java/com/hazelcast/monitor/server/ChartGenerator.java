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
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.data.time.Second;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.awt.*;
import java.io.IOException;
import java.io.OutputStream;
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
        TimeSeries ts = new TimeSeries("Map.size()", Second.class);
        int maxSize = Integer.MIN_VALUE;
        int minSize = Integer.MAX_VALUE;
        for (int i = 0; i < list.size(); i++) {
            int size = list.get(i).getSize();
            maxSize = Math.max(size, maxSize);
            minSize = Math.min(size, minSize);
            ts.addOrUpdate(new Second(list.get(i).getCreatedDate()), new Integer(size));
        }
        maxSize = (int) (maxSize*1.1);
        minSize = (int) (minSize*0.8);
        TimeSeriesCollection timeDataset = new TimeSeriesCollection();
        timeDataset.addSeries(ts);
//        if (list.size() > 0) {
//            TimeSeries _ts = new TimeSeries("", Second.class);
//            _ts.addOrUpdate(new Second(list.get(0).getCreatedDate()), minSize);
//            _ts.addOrUpdate(new Second(list.get(0).getCreatedDate()), maxSize);
//            timeDataset.addSeries(_ts);
//        }

        JFreeChart chart =
                ChartFactory.createTimeSeriesChart("Map: " + name, "Time", "Size", timeDataset, false, true, true);
        chart.setBackgroundPaint(Color.WHITE);
        try {
            OutputStream out = response.getOutputStream();
            response.setContentType("image/png");
            ChartUtilities.writeChartAsPNG(out, chart, 1000, 300);
        } catch (IOException ignore) {
        }
    }
}
