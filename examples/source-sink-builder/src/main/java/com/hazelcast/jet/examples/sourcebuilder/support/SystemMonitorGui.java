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

package com.hazelcast.jet.examples.sourcebuilder.support;

import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import javax.swing.*;
import java.awt.*;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.UUID;

import static java.lang.Math.max;
import static javax.swing.WindowConstants.EXIT_ON_CLOSE;

/**
 * Displays a live time graph based on the data it gets from a Hazelcast
 * map listener.
 */
public class SystemMonitorGui {
    private static final int WINDOW_X = 100;
    private static final int WINDOW_Y = 100;
    private static final int WINDOW_WIDTH = 1000;
    private static final int WINDOW_HEIGHT = 650;
    private static final int SCALE_Y = 1024;
    private static final int TIME_RANGE = 30_000;
    private static final int Y_RANGE_MIN = -200;
    private static final int Y_RANGE_UPPER_INITIAL = 200;

    private final IMap<Long, Double> hzMap;
    private UUID entryListenerId;

    public SystemMonitorGui(IMap<Long, Double> hzMap) {
        this.hzMap = hzMap;
        EventQueue.invokeLater(this::startGui);
    }

    private void startGui() {
        XYSeries series = new XYSeries("Rate", false);
        XYPlot plot = createChartFrame(series);
        ValueAxis xAxis = plot.getDomainAxis();
        ValueAxis yAxis = plot.getRangeAxis();
        xAxis.setRange(0, TIME_RANGE);
        yAxis.setRange(Y_RANGE_MIN, Y_RANGE_UPPER_INITIAL);

        long initialTimestamp = System.currentTimeMillis();
        EntryAddedListener<Long, Double> entryAddedListener = event -> {
            long x = event.getKey() - initialTimestamp;
            double y = event.getValue() / SCALE_Y;
            EventQueue.invokeLater(() -> {
                series.add(x, y);
                xAxis.setRange(max(0, x - TIME_RANGE), max(TIME_RANGE, x));
                yAxis.setRange(Y_RANGE_MIN, max(series.getMaxY(), Y_RANGE_UPPER_INITIAL));
            });
            hzMap.remove(event.getKey());
        };
        entryListenerId = hzMap.addEntryListener(entryAddedListener, true);
    }

    private XYPlot createChartFrame(XYSeries series) {
        XYSeriesCollection dataSet = new XYSeriesCollection();
        dataSet.addSeries(series);
        JFreeChart chart = ChartFactory.createXYLineChart(
                "Memory Allocation Rate",
                "Time (ms)", "Allocation Rate (MB/s)",
                dataSet,
                PlotOrientation.VERTICAL,
                true, true, false);
        XYPlot plot = chart.getXYPlot();
        plot.setBackgroundPaint(Color.WHITE);
        plot.setDomainGridlinePaint(Color.DARK_GRAY);
        plot.setRangeGridlinePaint(Color.DARK_GRAY);
        plot.getRenderer().setSeriesPaint(0, Color.BLUE);

        JFrame frame = new JFrame();
        frame.setBackground(Color.WHITE);
        frame.setDefaultCloseOperation(EXIT_ON_CLOSE);
        frame.setTitle("Hazelcast Jet Source Builder Sample");
        frame.setBounds(WINDOW_X, WINDOW_Y, WINDOW_WIDTH, WINDOW_HEIGHT);
        frame.setLayout(new BorderLayout());
        frame.add(new ChartPanel(chart));
        frame.addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent windowEvent) {
                hzMap.removeEntryListener(entryListenerId);
            }
        });
        frame.setVisible(true);
        return plot;
    }
}
