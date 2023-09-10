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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
/**
 *
 * @author Miguel Huamani <miguel.huamani.r@uni.pe>
 */
public class SubStringMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    String subcadena; 

     @Override
    public void configure(JobConf job) {
        // Recuperar los valores configurados en el método main
        subcadena = job.get("subcadena");
    }
    
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        String valueString = value.toString();
        String[] SingleCombination = valueString.split(",");
        
        String product = SingleCombination[5];
        
        String city = SingleCombination[2];
        String customerType = SingleCombination[3];
        String gender = SingleCombination[4];
        
        if(product.contains(subcadena)){
            String combinationKey ="SubCadena '"+subcadena+"' en "+product + ":, "+ city + ", " + customerType + ", " + gender;
            output.collect(new Text(combinationKey), one);
        }
       
    }
}