import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Calculation stuff. The reducer merges data together via the iterable thing.
 */
public class DoubleCalcReducer
        extends Reducer< Text, ClassWritable,  Text, ClassWritable> {

    private ClassWritable result = new ClassWritable();

    /*
     * Reducing function.
     * (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
     */
    public void reduce(Text key, Iterable<ClassWritable> values,
    		Reducer< Text, ClassWritable,  Text, ClassWritable>.Context context
    ) throws IOException, InterruptedException {
    	    	
    	Map<String, Integer> compareMap = new HashMap<String, Integer> ();
    	
    	int mergeCount = 0;
    	
    	// Start parsing strings and mapping them, fancy data structure would've been faster but I'm lazy
        for (ClassWritable val : values) {
        	String[] sArray  = val.getFriends().toString().split(" ");
        	mergeCount += 1;
        	
        	for(String sa: sArray)
        	{
        		if(compareMap.containsKey(sa))
        		{
        			Integer inc = compareMap.get(sa) + 1;
        			compareMap.put(sa, inc);
        		}
        		else {
        			compareMap.put(sa, 1);
        		}
        	}
        }
        
       // Merge numbers together when they have common members
        String finalString = "";
        
        int cn = 1;
        Iterator it = compareMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();            
            
            // System.out.println(pair.getKey() + " = " + pair.getValue());
            if(mergeCount == 1 || (Integer)pair.getValue() > 1)
            {
            	String fs = " ";
	        	String cs = (String)pair.getKey();
	        	
        		if(cn == 1) {
        			fs = ""; cn += 1;
        		}
        		
            	finalString += fs + cs;
            }

        }        
      
        //System.out.println(finalString);
        result = new ClassWritable(new Text(finalString));
        context.write(key, result);
    }
}