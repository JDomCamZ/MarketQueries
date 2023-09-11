/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package marketqueries;


import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
import java.text.SimpleDateFormat;

public class MarketAvgSpendMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
    private final DoubleWritable total = new DoubleWritable();
    String inputCity, inputStartDate, inputEndDate;
        
    @Override
    public void configure(JobConf job) {
        // Recuperar los valores configurados en el método main
        inputCity = job.get("ciudad");
        inputStartDate = job.get("fechaInicio");
        inputEndDate = job.get("fechaFin");
    }
    
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        String line = value.toString();
        String[] fields = line.split(",");
        
            try {
                String city = fields[2];
                String dateStr = fields[10];
                double totalValue = Double.parseDouble(fields[9]);
                String gender = fields[4];
                
                SimpleDateFormat dateFormat = new SimpleDateFormat("M/d/yyyy"); 
                
                Date date = dateFormat.parse(dateStr);
                Date startDate = dateFormat.parse(inputStartDate);
                Date endDate = dateFormat.parse(inputEndDate);  
                
                if (city.equals(inputCity) &&
                    date.after(startDate) && date.before(endDate)) {
                    total.set(totalValue);
                    output.collect(new Text(gender), total);
                }
            } catch (NumberFormatException e) {
            
            } catch (ParseException e2f) {
            
            }
        }
    }

