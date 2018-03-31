package poop;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class main {

	public static void main(String[] args) {
		
		String[] values =  {"1 2 3 4 5", "7 2 3 4 5"};
		
		
		// TODO Auto-generated method stub
    	Map<String, Integer> compareMap = new HashMap<String, Integer> ();
    	
        for (String val : values) {
        	String[] sArray  = val.toString().split(" ");
        	
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
        	//System.out.println(val.toString());
        	//result = val;
        }
        
       //CommonFriends.WriteLog("----------------");
        String finalString = "";
        
        int cn = 1;
        
        Iterator it = compareMap.entrySet().iterator();
        
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            
            System.out.println(pair.getKey() + " = " + pair.getValue());
            
            if((Integer)pair.getValue() > 1)
            {
            	String fs = " ";
	        	String cs = (String)pair.getKey();
	        	
        		if(cn == 1) {
        			fs = ""; cn += 1;
        		}
        		
            	finalString += fs + cs;
            }

            it.remove();
        }
        
        
        //
        System.out.println(finalString);
	}

}
