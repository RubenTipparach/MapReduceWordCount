
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Santipab
 */
public class ClassAverage {
    
    private static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        
        private Double val = 0.0;
        
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] itr =  value.toString().split("\n");
            
                //word.set(itr.nextToken());
                //context.write(word, one);
            
        }    
    }
    
    public static class DoubleCalcReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                Reducer.Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    
    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
    	Job job = new Job(conf, "Class Average");
    	
    	job.setJarByClass(ClassAverage.class);
    	job.setMapperClass(TokenizerMapper.class);
    	job.setCombinerClass(DoubleCalcReducer.class);
    	job.setReducerClass(DoubleCalcReducer.class);
    	
    	job.setOutputKeyClass(Text.class);
    	
    }
}


