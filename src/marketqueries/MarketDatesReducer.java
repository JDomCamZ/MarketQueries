/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package marketqueries;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import java.util.*;

public class MarketDatesReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        while (values.hasNext()) {
            Text value = values.next();
            output.collect(key, value);
        }
    }
}
