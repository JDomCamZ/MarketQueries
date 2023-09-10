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
            double product = 1.0;
            int count = 0;

            while (values.hasNext()) {
                product *= values.next().get();
                count++;
            }

            if (count > 0) {
                double geometricMean = Math.pow(product, 1.0 / count);
                result.set(geometricMean);
                output.collect(key, result);
            }
        }
    }
