
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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
    
	/*
	 * Mapper class - handles mapping data to key-value pairs.
	 * Here we map the key to the class number, and the value to the calculated weighted sum.
	 */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, ClassWritable>
    {
    	
        public void map(Object key, Text value, Mapper.Context context
        ) throws IOException, InterruptedException {
            
        	String[] itr =  value.toString().split("\n");
                   
        	// setting the key value pair of numbers and integers
            for(String s : itr)
            {
            	// System.out.println("PARSING: " + s);
            	String[] numbers = s.split("\t");
            	
            	int classNum = Integer.parseInt(numbers[0]);

            	// we should perform the merge here
            	DoubleWritable d1 = new DoubleWritable(
            			0.1 * Double.parseDouble(numbers[1]) +
            			0.2 * Double.parseDouble(numbers[2]) +
            			0.2 * Double.parseDouble(numbers[3]) +
            			0.3 * Double.parseDouble(numbers[4]) +
            			0.2 * Double.parseDouble(numbers[5]));
            	
                context.write(new Text("Class: " + classNum), new ClassWritable(d1, d1, d1));
            }
        }    
    }
    
    /*
     * Calculation stuff.
     */
    public static class DoubleCalcReducer
            extends Reducer< Text, ClassWritable,  Text, ClassWritable> {

        private ClassWritable result = new ClassWritable();

        public void reduce(Text key, Iterable<ClassWritable> values,
        		Reducer< Text, ClassWritable,  Text, ClassWritable>.Context context
        ) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            
            for (ClassWritable val : values) {
                sum += val.getTotal().get();
                count += val.getCount().get();
                
            }
            
            result.setTotal(sum);
            result.SetCount(count);
            result.setAvg(sum/count);
            context.write(key, result);
        }
    }
    
    public static class ClassWritable implements WritableComparable<ClassWritable>
    {
    	private IntWritable count;
    	private DoubleWritable total;
    	private DoubleWritable min;
    	private DoubleWritable max;
    	private DoubleWritable avg;
    	
    	
       	public ClassWritable()
    	{
    		this.count = new IntWritable(0);
    		this.total = new DoubleWritable();
    		this.min =  new DoubleWritable();
    		this.max =  new DoubleWritable();
    		
    		avg = new DoubleWritable();
    	}
    	
    	
    	public ClassWritable(DoubleWritable total, DoubleWritable min, DoubleWritable max)
    	{
    		this.count = new IntWritable(1);
    		this.total = total;
    		this.min = min;
    		this.max = max;
    		
    		avg = new DoubleWritable();
    	}
    	
		@Override
		public void readFields(DataInput in) throws IOException {
			count.readFields(in);
			total.readFields(in);
			min.readFields(in);
			max.readFields(in);
			avg.readFields(in);
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			count.write(out);
			total.write(out);
			min.write(out);
			max.write(out);
			avg.write(out);
		}
		
	
		// Get methods.
		public DoubleWritable getMin()
		{
			return min;
		}
		
		public DoubleWritable geMax()
		{
			return max;
		}
		
		public DoubleWritable getTotal()
		{
			return total;
		}
		
		public IntWritable getCount()
		{
			return count;
		}
		
		// Set methods.
		public void SetMin(double value)
		{
			min.set(value);
		}
		
		public void setMax(double value)
		{
			max.set(value);
		}
		
		public void setTotal(double value)
		{
			total.set(value);
		}
		
		public void SetCount(int value)
		{
			count.set(value);
		}
		
		public void setAvg(double value)
		{
			avg.set(value);
		}
		
		@Override
		public int compareTo(ClassWritable value) {
			// TODO: Idk if I need to do this.
			return 0;
		}
		
		@Override
		public String toString()
		{
			return "Average: " + avg.get() 
			+ " Total: " + total.get()
			+ " Count: " + count.get() 
			+ " min: " + min.get()
			+ " max: " + max.get();
			
		}
    }
    
    /*
     * Method for setting up stuff.
     */
    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
    	Job job = new Job(conf, "Class Average");
    	
    	job.setJarByClass(ClassAverage.class);
    	job.setMapperClass(TokenizerMapper.class);
    	job.setCombinerClass(DoubleCalcReducer.class);
    	job.setReducerClass(DoubleCalcReducer.class);
    	
    	//job.setInputFormatClass(TextInputFormat.class);
    	job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ClassWritable.class);

    	//job.setOutputFormatClass(TextInputFormat.class);
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(ClassWritable.class);
    	
    	String dataFile = args[0];
    	String outputPath = args[1];
    	Integer numNodes = Integer.parseInt(args[2]);
    	
    	FileSystem fs = FileSystem.get(conf);
    	long dataLength = fs.getContentSummary(new Path(dataFile)).getLength();
    	
    	FileInputFormat.setMaxInputSplitSize(job, (long) (dataLength/numNodes));
    	
    	FileInputFormat.addInputPath(job, new Path(dataFile));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

    	//job.setNumReduceTasks(numNodes/2);
    	
    	System.exit(job.waitForCompletion(true) ? 0 : 1);
    	
    }
}


