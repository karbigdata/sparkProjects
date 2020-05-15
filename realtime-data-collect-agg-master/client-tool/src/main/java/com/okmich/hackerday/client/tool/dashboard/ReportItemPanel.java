/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.hackerday.client.tool.dashboard;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Font;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.List;
import java.util.Map;
import javax.swing.BorderFactory;
import javax.swing.JPanel;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PiePlot;
import org.jfree.chart.title.LegendTitle;
import org.jfree.chart.title.TextTitle;
import org.jfree.data.general.DefaultPieDataset;
import org.jfree.ui.RectangleEdge;

/**
 *
 * @author m.enudi
 */
public final class ReportItemPanel extends JPanel implements ReportItem {

    private final Handler handler;
    private final String title;
    private DefaultPieDataset pieDataset;

    private JPanel chartPanel;

    /**
     * Creates new form ReportItemPanel
     *
     * @param title
     * @param handler
     */
    public ReportItemPanel(String title, Handler handler) {
        this.handler = handler;
        this.handler.registerUI(this);

        //UI specifics
        this.title = title;
        setMinimumSize(new java.awt.Dimension(400, 300));
        setLayout(new java.awt.BorderLayout());
        setBorder(BorderFactory.createLineBorder(Color.GRAY));
//        containPanel = new JPanel(new BorderLayout(5, 5));
//        buildContainerPanel();
//        add(containPanel, BorderLayout.NORTH);
        add(createChartPanel(), BorderLayout.CENTER);
    }

    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        setMinimumSize(new java.awt.Dimension(400, 300));
        setLayout(new java.awt.BorderLayout());
    }// </editor-fold>//GEN-END:initComponents

    /**
     * Creates a sample dataset.
     *
     * @return A sample dataset.
     */
    private DefaultPieDataset getChartDataset() {
        if (this.pieDataset == null) {
            pieDataset = new DefaultPieDataset();
            List<String> model = this.handler.getSchema();
            model.stream().forEach((key) -> {
                pieDataset.setValue(key, 0d);
            });
        }
        return pieDataset;
    }

    /**
     *
     * @param items
     */
    @Override
    public void reportItemModel(Map<String, Double> items) {
        //two strategies
        items.keySet().stream().forEach((key) -> {
            pieDataset.setValue(key, items.get(key));
        });
    }

    public ClientDashboardFrame getClientDashboardFrame() {
        //crazy code - calling for nullpointer
        return (ClientDashboardFrame) getParent() //JPanel
                .getParent() //JLayeredPanel
                .getParent() //JRootPanel
                .getParent(); //JFrame
    }

    /**
     *
     * @return A panel.
     */
    public JPanel createChartPanel() {
        JFreeChart chart = ChartFactory.createPieChart(
                title, // chart title
                getChartDataset(), // data
                true, // include legend
                true,
                false
        );

        TextTitle textTitle = new TextTitle(title, new Font("SansSerif", Font.BOLD, 16));
        chart.setTitle(textTitle);
        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setLabelFont(new Font("SansSerif", Font.PLAIN, 12));
        plot.setNoDataMessage("No data available");
        plot.setCircular(false);
        plot.setLabelGap(0.02);

        LegendTitle legend = chart.getLegend();
        legend.setPosition(RectangleEdge.BOTTOM);
        this.chartPanel = new ChartPanel(chart);
        this.chartPanel.addMouseListener(new MouseListenerImpl(this));

        return this.chartPanel;
    }

    public void onMouseClick() {
        setBorder(BorderFactory.createLineBorder(Color.RED, 1));
    }

    public void onMouseEnter() {
        setBorder(BorderFactory.createLineBorder(Color.YELLOW));
    }

    public void onMouseExit() {
        setBorder(BorderFactory.createLineBorder(Color.GRAY));
    }

    // Variables declaration - do not modify//GEN-BEGIN:variables
    // End of variables declaration//GEN-END:variables
    private class MouseListenerImpl extends MouseAdapter {

        private final ReportItemPanel source;

        MouseListenerImpl(ReportItemPanel source) {
            this.source = source;
        }

        @Override
        public void mouseClicked(MouseEvent e) {

            getClientDashboardFrame().setPanelContext(source);
            source.onMouseClick();
        }

        @Override
        public void mouseEntered(MouseEvent e) {
            source.onMouseEnter();
        }

        @Override
        public void mouseExited(MouseEvent e) {
            source.onMouseExit();
        }
    }
}