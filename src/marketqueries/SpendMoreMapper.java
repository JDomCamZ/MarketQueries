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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

//obtener los datos de las compras (máximo x compras) que gastaron más que una cantidad y
public class SpendMoreMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    String cantidad, costo; 

    @Override
    public void configure(JobConf job) {
        // Recuperar los valores configurados en el método main
        cantidad = job.get("cantidad");
        costo = job.get("costo");
    }
    
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String valueString = value.toString();
        String[] SingleCombination = valueString.split(",");
        String id=SingleCombination[0];
        String product = SingleCombination[5];
        String city = SingleCombination[2];
        String customerType = SingleCombination[3];
        
        
        try {
            int cantidadInt=Integer.parseInt(cantidad);
            double costoDou=Double.parseDouble(costo);
            double total =Double.parseDouble(SingleCombination[9]);
                // Si la fecha está dentro del intervalo, emitir la línea
                //outputKey.set(dateStr);
            String info=id+","+product+","+","+city+","+customerType+","+total;
            if(total>costoDou)output.collect(new Text(" "), new Text(info));
        } catch (NumberFormatException e) {
            System.err.println("Error en la conversión de " + " " + " a double.");
        }
    }
}