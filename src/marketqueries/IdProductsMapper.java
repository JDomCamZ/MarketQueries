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

//obtener los datos de las compras (máximo x compras) que gastaron más que una cantidad y
public class IdProductsMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    String cantidad, idSub; 

    @Override
    public void configure(JobConf job) {
        // Recuperar los valores configurados en el método main
        cantidad = job.get("cantidad");
        idSub = job.get("id");
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
            String costoDou=idSub;
            double total =Double.parseDouble(SingleCombination[9]);
            String info=id+","+product+","+city+","+customerType+","+total;
            
            if(id.contains(idSub))output.collect(new Text(" "), new Text(info));
        } catch (NumberFormatException e) {
            System.err.println("Error en la conversión de " + " " + " a double.");
        }
    }
}