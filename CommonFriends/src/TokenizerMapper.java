import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/*
 * Mapper class - handles mapping data to key-value pairs.
 * The friends of each person will be mapped to each friend-person pair.
 * These pairs will be order by Id, and merged in the reduction phase.
 */
public class TokenizerMapper extends Mapper<Object, Text, Text, ClassWritable>
{
	/*
	 * Mapping function.
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
    public void map(Object key, Text value, Mapper.Context context
    ) throws IOException, InterruptedException {
        
    	String[] itr =  value.toString().split("\n");
               
    	// setting the key value pair of numbers and integers
        for(String s : itr)
        {
        	if(s != "" && s != " ")
        	{
        		String[] numbers = s.split(" ");
        	
        		// this might be 0        	
        		String friendstring = "";
        		try {
		        	int profileNum = Integer.parseInt(numbers[0]);
		        	
		        	for(int i = 1; i < numbers.length; i++)
		        	{
		        		String fs = " ";
		        		if(i == 1) {
		        			fs = "";
		        		}
		        		
		        		friendstring += fs + numbers[i];
		        	}
		        	
		        	Text friends = new Text(friendstring);
		        	
		        	// breaks down the friends list into smallest pairs.
		        	for(int i = 1; i < numbers.length; i++)
		        	{
		        		// only write pair that have a value.
		        		if(numbers[i] != "" && numbers[i] != " ")
		        		{
			        		int friend = Integer.parseInt(numbers[i]);
			        		
			        		String mapkey= "";
			        		
			        		//System.out.println("profile: " + profileNum + "friend: " + friends.toString()tring());
			        		if(friend < profileNum) {
			        			mapkey = "Pairs: " + friend   + " " + profileNum;
			        		}
			        		else
			        		{
			        			mapkey = "Pairs: " + profileNum + " " + friend;
			        		}
			        		
			        		context.write(new Text(mapkey), new ClassWritable(friends));
		        		}
		        	}
        		}
        		catch(Exception e)
        		{
        			// throws whatever lol
        		}
        	}
        }
    }    
}