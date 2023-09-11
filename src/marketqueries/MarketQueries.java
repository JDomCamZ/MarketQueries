/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package marketqueries;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 *
 * @author juans
 */
public class MarketQueries {

    public static void main(String[] args) {
        String consulta = args[2];
         switch (consulta) {
            case "group":
                grouping(args[0], args[1]);
                break;
            case "minmax":
                // Lógica para la segunda consulta
                MinMax(args[0], args[1]);
                break;
            case "dates":
                String fechaInit = args[3];
                String fechaFin = args[4];
                dates(args[0], args[1], fechaInit, fechaFin);
                break;
            case "geomean":
                geomean(args[0], args[1]);
                break;
            case "sub":
                String subcadena = args[3];
                SubString(args[0], args[1], subcadena);
                break;
            case "mode":
                Mode(args[0], args[1]);
                break;
            case "totproduct":
                String product = args[3];
                String payMeth = args[4];
                String rating = args[5];
                fechaInit = args[6];
                fechaFin = args[7];
                totproduct(args[0], args[1], product, payMeth, rating, fechaInit, fechaFin);
                break;
            case "cityspend":
                String ciudad = args[3];
                fechaInit = args[4];
                fechaFin = args[5];
                cityavgspend(args[0], args[1], ciudad, fechaInit, fechaFin);
                break;
            default:
                System.err.println("Consulta no válida: " + consulta);
                System.exit(1);
        }
    }
    
    static void grouping(String input, String output) {
        JobClient my_client = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf = new JobConf(MarketQueries.class);

        // Set a name of the Job
        job_conf.setJobName("MarketGroup");

        // Specify data type of output key and value
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(IntWritable.class);

        // Specify names of Mapper and Reducer Class
        job_conf.setMapperClass(marketqueries.MarketGroupMapper.class);
        job_conf.setReducerClass(marketqueries.MarketGroupReducer.class);

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
    static void MinMax(String input, String output){
        JobClient my_client = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf = new JobConf(MarketQueries.class);

        // Set a name of the Job
        job_conf.setJobName("MinMax");

        // Specify data type of output key and value
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(DoubleWritable.class);

        // Specify names of Mapper and Reducer Class
        job_conf.setMapperClass(marketqueries.MinMaxMapper.class);
        job_conf.setReducerClass(marketqueries.MinMaxReducer.class);

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
    static void dates(String input, String output, String fechaInicio, String fechaFinal) {
        JobClient my_client = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf = new JobConf(MarketQueries.class);

        // Set a name of the Job
        job_conf.setJobName("MarketDates");
        job_conf.set("fechaInicio", fechaInicio);
        job_conf.set("fechaFin", fechaFinal);

        // Specify data type of output key and value
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(Text.class);

        // Specify names of Mapper and Reducer Class
        job_conf.setMapperClass(marketqueries.MarketDatesMapper.class);
        job_conf.setReducerClass(marketqueries.MarketDatesReducer.class);

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
    static void geomean(String input, String output) {
        JobClient my_client = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf = new JobConf(MarketQueries.class);

        // Set a name of the Job
        job_conf.setJobName("MarketDates");

        // Specify data type of output key and value
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(DoubleWritable.class);

        // Specify names of Mapper and Reducer Class
        job_conf.setMapperClass(marketqueries.MarketGeoMeanMapper.class);
        job_conf.setReducerClass(marketqueries.MarketGeoMeanReducer.class);

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

    static void SubString(String input, String output, String sub) {
        JobClient my_client = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf = new JobConf(MarketQueries.class);

        // Set a name of the Job
        job_conf.setJobName("SubString");
        job_conf.set("subcadena", sub);

        // Specify data type of output key and value
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(IntWritable.class);

        // Specify names of Mapper and Reducer Class
        job_conf.setMapperClass(marketqueries.SubStringMapper.class);
        job_conf.setReducerClass(marketqueries.SubStringReducer.class);

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
    static void Mode(String input, String output){
        JobClient my_client = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf = new JobConf(MarketQueries.class);

        // Set a name of the Job
        job_conf.setJobName("Mode");

        // Specify data type of output key and value
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(Text.class);

        // Specify names of Mapper and Reducer Class
        job_conf.setMapperClass(marketqueries.ModeMapper.class);
        job_conf.setReducerClass(marketqueries.ModeReducer.class);

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
    static void totproduct(String input, String output, String prod, String pay, String rating, String dateInt, String dateFin) {
        JobClient my_client = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf = new JobConf(MarketQueries.class);

        // Set a name of the Job
        job_conf.setJobName("MarketProductTotal");
        job_conf.set("producto", prod);
        job_conf.set("payment", pay);
        job_conf.set("fechaInicio", dateInt);
        job_conf.set("fechaFin", dateFin);
        job_conf.set("rating", rating);

        // Specify data type of output key and value
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(DoubleWritable.class);

        // Specify names of Mapper and Reducer Class
        job_conf.setMapperClass(marketqueries.MarketTotProductMapper.class);
        job_conf.setReducerClass(marketqueries.MarketTotProductReducer.class);

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
    
    static void cityavgspend(String input, String output, String city, String dateInt, String dateFin) {
        JobClient my_client = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf = new JobConf(MarketQueries.class);

        // Set a name of the Job
        job_conf.setJobName("MarketCitySpend");
        job_conf.set("ciudad", city);
        job_conf.set("fechaInicio", dateInt);
        job_conf.set("fechaFin", dateFin);

        // Specify data type of output key and value
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(DoubleWritable.class);

        // Specify names of Mapper and Reducer Class
        job_conf.setMapperClass(marketqueries.MarketAvgSpendMapper.class);
        job_conf.setReducerClass(marketqueries.MarketAvgSpendReducer.class);

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
}
