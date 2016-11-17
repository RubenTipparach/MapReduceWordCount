
import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStreamWriter;
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
    	/*
    	 * (non-Javadoc)
    	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
    	 */
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
     * Calculation stuff. The reducer merges data together via the iterable thing.
     */
    public static class DoubleCalcReducer
            extends Reducer< Text, ClassWritable,  Text, ClassWritable> {

        private ClassWritable result = new ClassWritable();

        /*
         * (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        public void reduce(Text key, Iterable<ClassWritable> values,
        		Reducer< Text, ClassWritable,  Text, ClassWritable>.Context context
        ) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            
            double min = -1;
            double max = -1;
            		
            for (ClassWritable val : values) {
                sum += val.getTotal().get();
                count += val.getCount().get();
                
                // Get new min.
                if(min != -1)
                {
                	double min2 = val.getMin().get();
                	if(min2 < min)
                	{
                		min = min2;
                	}
                }
                else
                {
                	min = val.getMin().get();
                }
                
                // Get new max.
                if(max != -1)
                {
                	double max2 = val.getMax().get();
                	if(max2 > max)
                	{
                		max = max2;
                	}
                }
                else
                {
                	max = val.getMax().get();
                }
            }
            
            result.setTotal(sum);
            result.SetCount(count);
            result.setAvg(sum/count);
            result.SetMin(min);
            result.setMax(max);
            
            context.write(key, result);
        }
    }
    
    /*
     * This is a serializable class for storing statistical data for each "Class".
     */
    public static class ClassWritable implements WritableComparable<ClassWritable>
    {
    	private IntWritable count;
    	private DoubleWritable total;
    	private DoubleWritable min;
    	private DoubleWritable max;
    	private DoubleWritable avg;
    	
    	/*
    	 * Default constructor thingy. Because I get lazy sometimes.
    	 */
       	public ClassWritable()
    	{
    		this.count = new IntWritable(0);
    		this.total = new DoubleWritable();
    		this.min =  new DoubleWritable();
    		this.max =  new DoubleWritable();
    		
    		avg = new DoubleWritable();
    	}
    	
    	/*
    	 * Initial constructor, for reading in data.
    	 */
    	public ClassWritable(DoubleWritable total, DoubleWritable min, DoubleWritable max)
    	{
    		this.count = new IntWritable(1);
    		this.total = total;
    		this.min = min;
    		this.max = max;
    		
    		avg = new DoubleWritable();
    	}
    	
    	/*
    	 * For sereailizable purposes.
    	 * (non-Javadoc)
    	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
    	 */
		@Override
		public void readFields(DataInput in) throws IOException {
			count.readFields(in);
			total.readFields(in);
			min.readFields(in);
			max.readFields(in);
			avg.readFields(in);
		}
		
		/*
		 * For sereailizable purposes.
		 * (non-Javadoc)
		 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
		 */
		@Override
		public void write(DataOutput out) throws IOException {
			count.write(out);
			total.write(out);
			min.write(out);
			max.write(out);
			avg.write(out);
		}
		
	
		/*
		 *  Get methods.
		 */
		public DoubleWritable getMin()
		{
			return min;
		}
		
		public DoubleWritable getMax()
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
		
		/*
		 *  Set methods.
		 */
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
		
		/*
		 * Comparable method. Not really needed right now.
		 * (non-Javadoc)
		 * @see java.lang.Comparable#compareTo(java.lang.Object)
		 */
		@Override
		public int compareTo(ClassWritable value) {
			// TODO: Idk if I need to do this.
			return 0;
		}
		
		/*
		 * Overwritten toString to format the data in a fancy string.
		 * (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString()
		{
			return "Average: " + avg.get() 
			//+ " Total: " + total.get()
			//+ " Count: " + count.get() 
			+ " min: " + min.get()
			+ " max: " + max.get();
			
		}
    }
    
    /*
     * Method for setting up stuff.
     */
    public static void main(String[] args) throws Exception {
    	
    	long timeStart = System.currentTimeMillis();
    	
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
    	String logPath = args[2];
    	Integer numNodes = Integer.parseInt(args[3]);
    	
    	FileSystem fs = FileSystem.get(conf);
    	
    	fs.delete(new Path(outputPath), true);
    	long dataLength = fs.getContentSummary(new Path(dataFile)).getLength();
    	
    	FileInputFormat.setMaxInputSplitSize(job, (long) (dataLength/numNodes));
    	job.setNumReduceTasks(numNodes/2);
    	
    	FileInputFormat.addInputPath(job, new Path(dataFile));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
    

        boolean finished = job.waitForCompletion(true);
        
        long timeStop = System.currentTimeMillis();
        
        // using my custom file thing.
        try
        {
            Path pt = new Path(logPath + "/results.txt");
            
            fs.delete(new Path(logPath), true);
            
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));

            // TO append data to a file, use fs.append(Path f)
            long delta = timeStop - timeStart;
            String line = "Program took: " + delta + " milliseconds. ";
            System.out.println(line);
        	
            br.write(line);
            br.close();
		}
        catch(Exception e)
        {
			System.out.println("File not found");
		}        
        
    	System.exit(finished ? 0 : 1);
    	
    }
}


