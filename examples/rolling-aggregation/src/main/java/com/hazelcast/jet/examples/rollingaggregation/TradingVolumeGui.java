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

package com.hazelcast.jet.examples.rollingaggregation;

import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryUpdatedListener;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;

import javax.swing.*;
import java.awt.*;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.UUID;

import static java.lang.Math.max;
import static javax.swing.WindowConstants.EXIT_ON_CLOSE;

/**
 * Displays a live bar chart of each stock and its current trading volume
 * on the simulated stock exchange.
 */
public class TradingVolumeGui {
    private static final int WINDOW_X = 100;
    private static final int WINDOW_Y = 100;
    private static final int WINDOW_WIDTH = 1200;
    private static final int WINDOW_HEIGHT = 650;
    private static final int INITIAL_TOP_Y = 5_000_000;

    private final IMap<String, Long> hzMap;
    private UUID entryListenerId;

    public TradingVolumeGui(IMap<String, Long> hzMap) {
        this.hzMap = hzMap;
        EventQueue.invokeLater(this::startGui);
    }

    private void startGui() {
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        CategoryPlot chartFrame = createChartFrame(dataset);
        ValueAxis yAxis = chartFrame.getRangeAxis();

        long[] topY = {INITIAL_TOP_Y};
        EntryUpdatedListener<String, Long> entryUpdatedListener = event -> {
            EventQueue.invokeLater(() -> {
                dataset.addValue(event.getValue(), event.getKey(), "");
                topY[0] = max(topY[0], INITIAL_TOP_Y * (1 + event.getValue() / INITIAL_TOP_Y));
                yAxis.setRange(0, topY[0]);
            });
        };
        entryListenerId = hzMap.addEntryListener(entryUpdatedListener, true);
    }

    private CategoryPlot createChartFrame(CategoryDataset dataset) {
        JFreeChart chart = ChartFactory.createBarChart(
                "Trading Volume", "Stock", "Volume, USD", dataset,
                PlotOrientation.HORIZONTAL, true, true, false);
        CategoryPlot plot = chart.getCategoryPlot();
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
        frame.setVisible(true);
        frame.addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent windowEvent) {
                hzMap.removeEntryListener(entryListenerId);
            }
        });
        return plot;
    }
}
