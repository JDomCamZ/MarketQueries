/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package marketqueries;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import java.util.*;
/**
 *
 * @author Miguel Huamani <miguel.huamani.r@uni.pe>
 */
public class MinInGroupReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        // El Reducer no necesita realizar una suma, ya que los conteos ya están calculados en el Mapper
        int total = 0;

        // Sumar los valores (que son 1) para contar la frecuencia
        while (values.hasNext()) {
            total += values.next().get();
            
        }

        // Emitir la combinación y su frecuencia
        output.collect(key, new IntWritable(total));
    }
}