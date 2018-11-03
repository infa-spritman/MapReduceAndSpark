package twitter;


import java.io.*;
import java.util.ArrayList;
import java.util.List;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;
import org.knowm.xchart.style.Styler;
import org.knowm.xchart.style.Styler.LegendPosition;

/**
 * Logarithmic Y-Axis
 *
 * <p>Demonstrates the following:
 *
 * <ul>
 *   <li>Logarithmic Y-Axis
 *   <li>Building a Chart with ChartBuilder
 *   <li>Place legend at Inside-NW position
 */
public class FollwersNumvsUsersGraph implements ExampleChart<XYChart> {

    public static void main(String[] args) {

        ExampleChart<XYChart> exampleChart = new FollwersNumvsUsersGraph();
        XYChart chart = exampleChart.getChart();
        new SwingWrapper<XYChart>(chart).displayChart();
    }

    @Override
    public XYChart getChart() {

        // generates Log data
        final List<Integer> yData = new ArrayList<Integer>();
        final List<Double> xData = new ArrayList<Double>();
//        for (int i = -3; i <= 3; i++) {
//            xData.add(i);
//            yData.add(Math.pow(10, i));
//        }

        try{
            File inputF = new File("/home/kodefear/Desktop/MR-2/Assignment-4/input/Twitter-dataset/graphCsv/graph.csv");
            InputStream inputFS = new FileInputStream(inputF);
            BufferedReader br = new BufferedReader(new InputStreamReader(inputFS));
            br.lines().forEach( line -> {
                final String[] row = line.split(",");

                xData.add(Double.parseDouble(row[0]));
                yData.add(Integer.parseInt(row[1]));


            });
//            inputList = br.lines().skip(1).map(mapToItem).collect(Collectors.toList());
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Create Chart
        XYChart chart =
                new XYChartBuilder()
                        .width(800)
                        .height(600)
                        .title("Distribution of Followers")
                        .xAxisTitle("Followers Count")
                        .yAxisTitle("Number Of Users")
                        .theme(Styler.ChartTheme.XChart)
                        .build();

        // Customize Chart
        chart.getStyler().setChartTitleVisible(true);
        chart.getStyler().setLegendPosition(LegendPosition.InsideNW);
        chart.getStyler().setXAxisLabelRotation(45);
        chart.getStyler().setYAxisLogarithmic(true);
        chart.getStyler().setXAxisMax(2000.0);

        // chart.getStyler().setXAxisLabelRotation(0);

//        chart.getStyler().

        // chart.getStyler().setXAxisLabelAlignment(TextAlignment.Right);
        // chart.getStyler().setXAxisLabelRotation(90);
        // chart.getStyler().setXAxisLabelRotation(0);

        // Series
        chart.addSeries("No. Of Users", xData, yData);

        return chart;
    }
}
