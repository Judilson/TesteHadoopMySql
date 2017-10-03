/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package testehadoopmysql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
/**
 *
 * @author jjunior
 */
public class TesteHadoopMySql {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        DBConfiguration.configureDB(conf,
                "com.mysql.jdbc.Driver", // driver class
                "jdbc:mysql://10.10.40.179:3306/giex", // db url
                "giex", // user name
                "7xt3Dw87"); //password

        /*DBConfiguration.configureDB(conf,
                "com.mysql.jdbc.Driver", // driver class
                "jdbc:mysql://172.16.59.9:3306/temp", // db url
                "root", // user name
                "root"); //password*/
        
        Job job = new Job(conf);
        job.setJarByClass(TesteHadoopMySql.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(DBOutputWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setInputFormatClass(DBInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);

        DBInputFormat.setInput(
                job,
                DBInputWritable.class,
                "studentinfo", //input table name
                null,
                null,
                new String[]{"id", "name"} // table columns
        );

        DBOutputFormat.setOutput(
                job,
                "output", // output table name
                new String[]{"name", "count"} //table columns
        );

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
