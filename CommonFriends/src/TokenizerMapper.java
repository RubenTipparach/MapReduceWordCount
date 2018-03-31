import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/*
 * Mapper class - handles mapping data to key-value pairs.
 * Here we map the key to the class number, and the value to the calculated weighted sum.
 */
public class TokenizerMapper extends Mapper<Object, Text, Text, ClassWritable>
{
	 static Log logMap = LogFactory.getLog(TokenizerMapper.class);
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
        	String[] numbers = s.split(" ");
        	
        	// this might be 0
        	
        	String friendstring = "";
        	int profileNum = Integer.parseInt(numbers[0]);

        	for(int i = 1; i < numbers.length; i++)
        	{
        		String fs = " ";
        		if(i == 1) {
        			fs = "";
        		}
        		
        		friendstring += fs + Integer.parseInt(numbers[i]);
        	}
        	
        	Text friends = new Text(friendstring);
        	
        	// breaks down the friends list into smallest pairs.
        	for(int i = 1; i < numbers.length; i++)
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
}