package com.hadoop.remove;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class removeChar extends UDF{
	
		
		
		private IntWritable colValueinteger = new IntWritable();
		private Text colValue = new Text();
		
		
		public IntWritable evaluate(IntWritable value){
			if(value==null){
				
				return null;
			}
			
			int result = Integer.parseInt(value.toString())*2;  
			colValueinteger.set(result);
			return colValueinteger;
		}
		
		public Text evaluate(Text str, String symbol){
			if(str==null){
				return str;			
			}
			
			String result=StringUtils.strip(str.toString(), symbol);
			colValue.set(result);
			return colValue;			
		}
		
		

	

}
