import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/*
	     * This is a serializable class for storing statistical data for each "Class".
	     */
public class ClassWritable implements WritableComparable<ClassWritable>
{

	private Text friends;

	/*
	 * Default constructor thingy. Because I get lazy sometimes.
	 */
   	public ClassWritable()
	{
		this.friends = new Text();
	}
	
	/*
	 * Initial constructor, for reading in data.
	 */
	public ClassWritable(Text friends)
	{
		this.friends = friends;
//    		this.total = total;
//    		this.min = min;
//    		this.max = max;
//    		
//    		avg = new DoubleWritable();
	}
	
	/*
	 * For sereailizable purposes.
	 * (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		friends.readFields(in);
		//count.readFields(in);
		//total.readFields(in);
//			min.readFields(in);
//			max.readFields(in);
//			avg.readFields(in);
	}
	
	/*
	 * For sereailizable purposes.
	 * (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		friends.write(out);
		//count.write(out);
		//total.write(out);
//			min.write(out);
//			max.write(out);
//			avg.write(out);
	}
	

	/*
	 *  Get methods.
	 */
	public Text getFriends()
	{
		return this.friends;
	}

	/*
	 *  Set methods.
	 */
	
	public void setFriends(Text val)
	{
		this.friends = val;
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
		return "friends: " 
			+ this.friends.toString();
	//
	//					"Average: " + avg.get() 
	//+ " Total: " + total.get()
	//+ " Count: " + count.get() 
	//			+ " min: " + min.get()
	//			+ " max: " + max.get();
		
	}
}