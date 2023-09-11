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
import org.apache.hadoop.io.DoubleWritable;
import java.util.HashMap;
import java.util.Map;
/**
 *
 * @author Miguel Huamani <miguel.huamani.r@uni.pe>
 */
public class AverageProductsReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        // El Reducer no necesita realizar una suma, ya que los conteos ya están calculados en el Mapper
        double sum = 0;
        int count=0;
        //city + "," + customerType + "," + gender+","+quantity;

        double quantityD;
        String results;
        
        Map<String, Integer> mapCity = new HashMap<>();
        Map<String, Integer> mapType = new HashMap<>();
        Map<String, Integer> mapGenders = new HashMap<>();
        // Contar la frecuencia de cada valor de "Product Line" para la ciudad actual
         while (values.hasNext()) {
             
            String[] datos = values.next().toString().split(","); 
            
            mapCity.put(datos[0], mapCity.getOrDefault(datos[0], 0) + 1);
            mapGenders.put(datos[2],mapGenders.getOrDefault(datos[2],0)+1);
            mapType.put(datos[1],mapType.getOrDefault(datos[1],0)+1); 
            
            quantityD= Double.parseDouble(datos[3]);
            count+=1;
            sum+=quantityD;
        }
        double average=(sum/count);
        results=Mode(mapCity)+" "+Mode(mapType)+" "+Mode(mapGenders)+" "+average;
        // Emitir la combinación y su frecuencia
        output.collect(key, new Text(results));
    }
    String Mode(Map <String,Integer> modeMap){
        String moda = null;
        int maxCount = 0;

        for (Map.Entry<String, Integer> entry : modeMap.entrySet()) {
            if (entry.getValue() > maxCount) {
                moda = entry.getKey();
                maxCount = entry.getValue();
            }
        }
        return moda;
    }
    /*
    @Override
    public void configure(JobConf job) {
        // Recuperar los valores configurados en e
        
    }*/
}
