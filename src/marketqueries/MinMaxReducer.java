/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package marketqueries;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import java.util.*;
/**
 *
 * @author Miguel Huamani <miguel.huamani.r@uni.pe>
 */
public class MinMaxReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, Text> {

    public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        // El Reducer no necesita realizar una suma, ya que los conteos ya están calculados en el Mapper
        double min = values.next().get();
        double max =min;
        // Sumar los valores (que son 1) para contar la frecuencia
        while (values.hasNext()) {
            double aux= values.next().get();
            if(aux<min)min=aux;
            if(aux>max)max=aux;
        }
        String minMax = "Minimo:"+min+"   -    Maximo:"+ max;
        // Emitir la combinación y su frecuencia
        output.collect(key, new Text(minMax));
        
    }
}