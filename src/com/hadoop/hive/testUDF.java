package com.hadoop.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class testUDF extends UDF{
	Text colValue= new Text();
	
	public Text evaluate(Text str) {
		if(str==null){
			return str;					
		}
		
		colValue.set(org.apache.commons.lang.StringUtils.strip(str.toString()));
		
		return colValue;
	}

	
}
