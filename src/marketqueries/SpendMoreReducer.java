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
/**
 *
 * @author Miguel Huamani <miguel.huamani.r@uni.pe>
 */
public class SpendMoreReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
     String cantidad, costo; 

    @Override
    public void configure(JobConf job) {
        // Recuperar los valores configurados en el método main
        cantidad = job.get("cantidad");
        costo = job.get("costo");
    }
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        int cantidadInt;
        double costoDou;
        int cont=0;
        
         try {
            cantidadInt=Integer.parseInt(cantidad);
            costoDou=Double.parseDouble(costo);
            
            while (values.hasNext()&&cont<cantidadInt) {
               String[] datos = values.next().toString().split(","); 
               String info=datos[1]+","+datos[2]+","+datos[3]+","+datos[4];
               String id=datos[0];
               output.collect(new Text(id), new Text(info));
               cont+=1;
            }
            
            
        } catch (NumberFormatException e) {
            System.err.println("Error en la conversión de " + " " + " a double.");
        }
    }
}