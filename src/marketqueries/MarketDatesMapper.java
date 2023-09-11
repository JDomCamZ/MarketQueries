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

public class MarketDatesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private final Text outputKey = new Text();
    private final Text outputValue = new Text();
    String fechaInicio, fechaFin; 

    @Override
    public void configure(JobConf job) {
        // Recuperar los valores configurados en el método main
        fechaInicio = job.get("fechaInicio");
        fechaFin = job.get("fechaFin");
    }
    
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String line = value.toString();
        String[] fields = line.split(",");

        String dateStr = fields[10];

        SimpleDateFormat dateFormat = new SimpleDateFormat("M/d/yyyy");
        try {
            Date date = dateFormat.parse(dateStr);
            Date startDate = dateFormat.parse(fechaInicio);
            Date endDate = dateFormat.parse(fechaFin);

            if (date.after(startDate) && date.before(endDate)) {
                // Si la fecha está dentro del intervalo, emitir la línea
                outputKey.set(dateStr);
                outputValue.set(fields[0] + ", " + fields[5] + ", " + fields[13]);
                output.collect(outputKey, outputValue);
            }
        } catch (ParseException e) {
            
        }
    }
}
