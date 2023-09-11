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

/*

case "average":
                 Average(args[0], args[1]);
                 break;


 static void Average(String input, String output){
        JobClient my_client = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf = new JobConf(MarketQueries.class);

        // Set a name of the Job
        job_conf.setJobName("AverageProducts");

        // Specify data type of output key and value
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(Text.class);

        // Specify names of Mapper and Reducer Class
        job_conf.setMapperClass(marketqueries.AverageProductsMapper.class);
        job_conf.setReducerClass(marketqueries.AverageProductsReducer.class);

        // Specify formats of the data type of Input and output
        job_conf.setInputFormat(TextInputFormat.class);
        job_conf.setOutputFormat(TextOutputFormat.class);

        // Set input and output directories using command line arguments,
        //arg[0] = name of input directory on HDFS, and arg[1] =  name of output directory to be created to store the output file.

        FileInputFormat.setInputPaths(job_conf, new Path(input));
        FileOutputFormat.setOutputPath(job_conf, new Path(output));

        my_client.setConf(job_conf);
        try {
            // Run the job
            JobClient.runJob(job_conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }




*/