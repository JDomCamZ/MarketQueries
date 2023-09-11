/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package marketqueries;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;
public class MarketAvgSpendReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();
    
    @Override
    public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        double total = 0.0;
        int count = 0;

        while (values.hasNext()) {
            total += values.next().get();
            count++;
        }

        if (count > 0) {
            double averageSpending = total / count;
            result.set(averageSpending);
            output.collect(key, result);
        }
    }
}
