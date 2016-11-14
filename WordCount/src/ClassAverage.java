
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

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
    
    private static class TokenizerMapper extends Mapper<Object, Long, Long, IntWritable>
    {
        
    }
    
}
