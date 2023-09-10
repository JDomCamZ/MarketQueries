/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package marketqueries;

/**
 *
 * @author Miguel Huamani <miguel.huamani.r@uni.pe>
 */
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class MinMaxMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
    private final DoubleWritable total = new DoubleWritable();

    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        String valueString = value.toString();
        String[] SingleCountryData = valueString.split(",");

        // Verificar si hay al menos 10
        if (SingleCountryData.length >= 12) {
            String city = SingleCountryData[2];
            //double total = Double.parseDouble(t) ;
            //output.collect(new Text(city),new DoubleWritable(total));
            // Intentar convertir el campo "Total" a un valor numérico
            try {
                double totalValue = Double.parseDouble(SingleCountryData[9]);
                total.set(totalValue);
                output.collect(new Text(city), total);
            } catch (NumberFormatException e) {
                // Manejar el caso en el que no se pueda convertir a un número (puedes ignorarlo o registrar un error)
                System.err.println("Error en la conversión de " + SingleCountryData[9] + " a double.");
            }
        }
    }
}

