/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package marketqueries;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import java.util.*;

public class MarketGeoMeanReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private final DoubleWritable result = new DoubleWritable();

    @Override
    public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        double sumOfLogs = 0.0;
        int count = 0;
        
        //Aplicar logaritmos a los valores para no hacer crecer el valor hasta al infinito
        while (values.hasNext()) {
            double value = values.next().get();
            sumOfLogs += Math.log(value);
            count++;
        }

        //Aplicar exponencial para hallar la media geometrica
        if (count > 0) {
            double geometricMean = Math.exp(sumOfLogs / count);
            result.set(geometricMean);
            output.collect(key, result);
        }
    }
}
