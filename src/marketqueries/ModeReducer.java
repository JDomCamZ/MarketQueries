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
import java.util.HashMap;
import java.util.Map;
/**
 *
 * @author Miguel Huamani <miguel.huamani.r@uni.pe>
 */
public class ModeReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
       Map<String, Integer> countMap = new HashMap<>();

        // Contar la frecuencia de cada valor de "Product Line" para la ciudad actual
         while (values.hasNext()) {
             
            String productLine =  values.next().toString();
            countMap.put(productLine, countMap.getOrDefault(productLine, 0) + 1);
        }

        // Encontrar el valor con la frecuencia máxima (moda)
        String modaProductLine = null;
        int maxCount = 0;

        for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
            if (entry.getValue() > maxCount) {
                modaProductLine = entry.getKey();
                maxCount = entry.getValue();
            }
        }
        // Emitir la combinación y su frecuencia
        output.collect(key, new Text(modaProductLine));
    }
}
