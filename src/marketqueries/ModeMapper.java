/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package marketqueries;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 *
 * @author Miguel Huamani <miguel.huamani.r@uni.pe>
 */
public class ModeMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String valueString = value.toString();
        String[] SingleCombination = valueString.split(",");

        String city = SingleCombination[2];
        String product = SingleCombination[5];


            // Emitir la combinación como clave y 1 como valor
        output.collect(new Text(city), new Text(product));
    }
}
