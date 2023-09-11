/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package marketqueries;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MarketTotProductMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
    private final DoubleWritable total = new DoubleWritable();
    String fechaInit, fechaFin, producto, pago, rate;
    
    @Override
    public void configure(JobConf job) {
        // Recuperar los valores configurados en el método main
        fechaInit = job.get("fechaInicio");
        fechaFin = job.get("fechaFin");
        producto = job.get("producto");
        pago = job.get("payment");
        rate = job.get("rating");
    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        String line = value.toString();
        String[] fields = line.split(",");
        
        String dateStr = fields[10];
        String productLine = fields[5];
        String paymentMethod = fields[12];
        
        SimpleDateFormat dateFormat = new SimpleDateFormat("M/d/yyyy");     
        try {
            Date date = dateFormat.parse(dateStr);
            Date startDate = dateFormat.parse(fechaInit);
            Date endDate = dateFormat.parse(fechaFin);  
            double rating = Double.parseDouble(rate);
            
            double totalValue = Double.parseDouble(fields[9]);
            double ratingValue = Double.parseDouble(fields[16]);

            if (date.after(startDate) && date.before(endDate) &&
                productLine.equals(producto) &&
                paymentMethod.equals(pago) &&
                ratingValue > rating) {
                total.set(totalValue);
                output.collect(new Text("Ventas"), total);
            }

        } catch (NumberFormatException e) {
        // Manejar errores de formato de número si es necesario
        } catch (ParseException e2f) {
            // Manejar errores de formato de fecha si es necesario
        }
    }
}
