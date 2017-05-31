package com.hadoop.complextype;


//import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;


public class CalcMarks extends UDF{

	//private List<Integer> myList = new ArrayList<Integer>();
	
	public IntWritable evaluate(List<Integer> value){
		
		 int sum = 0;
		    for (int i = 0; i < value.size(); i++) {
		      if (value.get(i) != null) {
		        sum += value.get(i);
		      }
		    }
		    
		    return new IntWritable(sum);		    
		    
	}
	
	
	
	
	
}	

