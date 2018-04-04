
import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CommonFriends {
	  
	static FileSystem fs;
	static Path pt;
	
	static String bigLog = "";
    /*
     * Method for setting up stuff.
     */
    public static void main(String[] args) throws Exception {
    	
    	long timeStart = System.currentTimeMillis();
    	
    	Configuration conf = new Configuration();
    	Job job = new Job(conf, "Common Friends");
    	
    	job.setJarByClass(CommonFriends.class);
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
    	
    	fs = FileSystem.get(conf);
    	
    	//fs.delete(new Path(outputPath), true);
		 pt = new Path(logPath + "/results.txt");
		 //fs.create(pt,true);
    	 
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
            pt = new Path(logPath + "/results.txt");
            
            //fs.delete(new Path(logPath), true);
            
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));

            // TO append data to a file, use fs.append(Path f)
            long delta = timeStop - timeStart;
            String line = "Program took: " + delta + " milliseconds. ";
            System.out.println(line);
            System.out.println(bigLog);
            br.write(line);
           //.write(bigLog);
            
            br.close();
		}
        catch(Exception e)
        {
			System.out.println("File not found");
		}        
        
    	System.exit(finished ? 0 : 1);
    	
    }
    
    // This log doesn't work all that well anyways...
    public static void WriteLog(String val) {
    	bigLog += val + "\n"; 
    }

}
