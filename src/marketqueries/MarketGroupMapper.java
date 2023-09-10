/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package marketqueries;

/**
 *
 * @author juans
 */
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class MarketGroupMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        String valueString = value.toString();
        String[] SingleCombination = valueString.split(",");

        String city = SingleCombination[2];
        String customerType = SingleCombination[3];
        String gender = SingleCombination[4];

        // Crear una clave que represente la combinación de ciudad, tipo de consumidor y género
        String combinationKey = city + "," + customerType + "," + gender;

            // Emitir la combinación como clave y 1 como valor
        output.collect(new Text(combinationKey), one);
    }
}
