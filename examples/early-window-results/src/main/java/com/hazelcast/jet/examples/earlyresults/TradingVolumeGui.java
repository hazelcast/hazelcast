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

package com.hazelcast.jet.examples.earlyresults;

import com.hazelcast.collection.IList;
import com.hazelcast.collection.ItemEvent;
import com.hazelcast.collection.ItemListener;
import com.hazelcast.jet.datamodel.WindowResult;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.chart.renderer.category.StandardBarPainter;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;

import javax.swing.*;
import java.awt.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static javax.swing.WindowConstants.EXIT_ON_CLOSE;

/**
 * Displays a live bar chart of each stock and its current trading volume
 * on the simulated stock exchange.
 */
public final class TradingVolumeGui {
    private static final int WINDOW_X = 100;
    private static final int WINDOW_Y = 100;
    private static final int WINDOW_WIDTH = 1200;
    private static final int WINDOW_HEIGHT = 650;
    private static final int Y_RANGE_UPPER_INITIAL = 3000;
    private static final double SCALE_Y = 1_000;

    private final IList<WindowResult<Long>> volumeList;
    private final boolean[] finalResultFlags = new boolean[60];
    private final BarRenderer renderer = new BarRenderer() {
        @Override
        public Paint getItemPaint(int row, int column) {
            return column >= finalResultFlags.length || finalResultFlags[column] ? Color.BLUE : Color.GRAY;
        }
    };

    public TradingVolumeGui(IList<WindowResult<Long>> volumeList) {
        this.volumeList = volumeList;
        EventQueue.invokeLater(this::startGui);
    }

    private void startGui() {
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        CategoryPlot plot = createChartFrame(dataset);
        plot.getRangeAxis().setRange(0, Y_RANGE_UPPER_INITIAL);
        for (long ts = 0L; ts <= 60; ts += 2) {
            dataset.addValue(0L, "", new Long(ts));
        }
        AtomicLong first = new AtomicLong(-1L);
        ItemAddedListener<WindowResult<Long>> itemListener = tse -> EventQueue.invokeLater(() -> {
            if (first.get() < 0) {
                first.set(tse.end());
            }
            long x = TimeUnit.MILLISECONDS.toSeconds(tse.end() - first.get());
            double y = tse.result() / SCALE_Y;
            int col = dataset.getColumnIndex(x);
            if (col >= 0 && col < finalResultFlags.length) {
                if (finalResultFlags[col]) {
                    return;
                }
                finalResultFlags[col] = !tse.isEarly();
            }
            dataset.addValue(y, "", new Long(x));
        });
        volumeList.addItemListener(itemListener, true);
    }

    private CategoryPlot createChartFrame(CategoryDataset dataset) {
        JFreeChart chart = ChartFactory.createBarChart(
                "Trading Volume Over Time", "Time", "Trading Volume, millions USD", dataset,
                PlotOrientation.VERTICAL, false, true, false);
        CategoryPlot plot = chart.getCategoryPlot();
        plot.setBackgroundPaint(Color.WHITE);
        plot.setDomainGridlinePaint(Color.DARK_GRAY);
        plot.getDomainAxis().setCategoryMargin(0.0);
        plot.setRangeGridlinePaint(Color.DARK_GRAY);
        renderer.setBarPainter(new StandardBarPainter());
        renderer.setShadowPaint(Color.WHITE);
        plot.setRenderer(renderer);

        final JFrame frame = new JFrame();
        frame.setBackground(Color.WHITE);
        frame.setDefaultCloseOperation(EXIT_ON_CLOSE);
        frame.setTitle("Hazelcast Jet Early Window Results Sample");
        frame.setBounds(WINDOW_X, WINDOW_Y, WINDOW_WIDTH, WINDOW_HEIGHT);
        frame.setLayout(new BorderLayout());
        frame.add(new ChartPanel(chart));
        frame.setVisible(true);
        return plot;
    }

    interface ItemAddedListener<T> extends ItemListener<T> {
        void item(T item);

        @Override default void itemRemoved(ItemEvent<T> event) {
        }
        @Override default void itemAdded(ItemEvent<T> event) {
            item(event.getItem());
        }
    }
}
