/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package testehadoopmysql;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

/**
 *
 * @author jjunior
 */
public class Reduce extends Reducer<Text, IntWritable, DBOutputWritable, NullWritable> {

    protected void reduce(Text key, Iterable<IntWritable> values, Context ctx) {
        int sum = 0;

        for (IntWritable value : values) {
            sum += value.get();
        }

        try {
            ctx.write(new DBOutputWritable(key.toString(), sum), NullWritable.get());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
