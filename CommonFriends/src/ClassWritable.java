import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/*
 * A lot of this class was repurposed from an older project I did in Hadoop
 * The "Class" and "DoubleCalc" are just legacy class names for older programs.
 * I figured this works fine for what I needed to get done, so I didn't refactor the whole thing.
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
	}
	
	/*
	 * For sereailizable purposes. I went and manually serialized everything in the reduce phase.
	 * (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		friends.readFields(in);
	}
	
	/*
	 * For sereailizable purposes.
	 * (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		friends.write(out);
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
	}
}