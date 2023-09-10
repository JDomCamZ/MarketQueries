/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package marketqueries;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class MarketGeoMeanMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
    private final DoubleWritable total = new DoubleWritable();

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        String line = value.toString();
        String[] fields = line.split(",");
        
        try {
           // Obtener el valor de la columna "Total"
            double totalValue = Double.parseDouble(fields[9]);
            total.set(totalValue);
            output.collect(new Text("Total"), total);
        } catch (NumberFormatException e) {
            // Manejar errores de formato de número si es necesario
        }  
    }
}
