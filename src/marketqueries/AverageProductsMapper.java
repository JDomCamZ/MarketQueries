/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package marketqueries;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
/**
 *
 * @author Miguel Huamani <miguel.huamani.r@uni.pe>
 */
public class AverageProductsMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private final static IntWritable one = new IntWritable(1);
    //de cada producto, conseguir las modas de la ciudad, el tipo de consumidor y el género, además del promedio de compras del producto
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String valueString = value.toString();
        String[] SingleCombination = valueString.split(",");

        String product = SingleCombination[5];
        String city = SingleCombination[2];
        String customerType = SingleCombination[3];
        String gender = SingleCombination[4];
        try {
                double quantity =Double.parseDouble(SingleCombination[7]);
                // Crear una clave que represente la combinación de ciudad, tipo de consumidor y género
                String combinationKey = city + "," + customerType + "," + gender+","+quantity;
                    // Emitir la combinación como clave y 1 como valor
                output.collect(new Text(product), new Text(combinationKey));
            } catch (NumberFormatException e) {
                // Manejar el caso en el que no se pueda convertir a un número (puedes ignorarlo o registrar un error)
                System.err.println("Error en la conversión de " +SingleCombination[7]  + " a double.");
            }
    }
}