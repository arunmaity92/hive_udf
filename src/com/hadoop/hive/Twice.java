package com.hadoop.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.google.common.base.Strings;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;

public class Twice extends UDF{

/*	
 * This section is for working with HADOOP DATATYPES.
 * 
 * If we will use Intwritable datatype which is hadoop wrapper datatype for primitive then it will import 
 * hadoop.io.IntWritable package.
 * 
 * 
 * 
 * private IntWritable colValue = new IntWritable();	
	public IntWritable evaluate(IntWritable value){
		if(value==null){			
			return null;
		}
		
		int result = Integer.parseInt(value.toString())*2;  
		colValue.set(result);
	return colValue;
	}
	
	*/
	
	/*
	 * 
	 * This section is working with JAVA DATATYPES.
	 * 
	 * Here Integer wrapper is used instead of int because null cannot be checked for int types. 
	 * If want to check then we have to use Integer wrapper class.
	 * 
	 */
	  public Integer evaluate(Integer price){
		
		// have to convert the int to Integer because checking of null for int is only possible 
		//with wrapper class Integer.
		
		if(price == null){
			return null;			
		}
		
		int result = price*2;		
		return result;
	  }
	
	  /*
	   * 
	   * Here we are accepting a String record and sending a Integer record. From hive price is coming which is 
	   * a sring datatype in hive DDl. After doing calculations if sended in Integer form.
	   *  
	   * If your hive have table having a string column, then also this integer return value will fit in that 
	   * string column
	   * 
	   * describe table:
	   * name                	string              	                    
	   * price               	string  
	   * insert overwrite table output_fruit_int select name,myamount3(price) from fruit_int;
	   *   
	   */
	  
		/*public Integer evaluate(String price){
			if(price == null){
				return null;			
			}
			int result = Integer.parseInt(price)*2;
			return result;
		}*/
		
		
		/*
		   * 
		   * 
		   * Here we are accepting a Text record and sending a Text record. From hive price is coming which is 
		   * a sring datatype in hive DDl. After doing calculation sended in Text.
		   * 
		   * Text is wrapper of String datatype in hadoop.
		   * 
		   *  
		   */
		
		/*Text colValue = new Text();
		public Text evaluate(Text price){
			if(Strings.isNullOrEmpty(price)){
				return null;			
			}
			
			int result = Integer.parseInt(price.toString())*2;
			//Integer.toString(result);
			//Text final_result=Integer.toString(result);
			String final_result = Integer.toString(result); 
			colValue.set(final_result);
			return colValue;
		}
	  */
	  
	   /* 
	   * Here we are accepting a String record and sending a String record. From hive price is coming which is 
		 a sring datatype in hive DDl. After doing calculations if sended in String it works fine.	 
	   * 
	   *  
	   * 
	   **/ 
	  
	  //|| price=="NULL" || price=="null"
	  
	  /*
	   * You should leverage StringUtils.isEmpty(str), which checks for empty strings 
	   * and handles null gracefully.
	   * 
	   * System.out.println(StringUtils.isEmpty("")); // true
	   * System.out.println(StringUtils.isEmpty(null)); // true
	   * 
	   * Google Guava also provides a similar, probably easier-to-read method: 
	   * It will check for both null and empty strings using this single function.
	   * 
	   * Strings.isNullOrEmpty(str)
	   * 
	   * since guava jar is already in our external jars folder which we have copied it
	   * from the hive lib folder. These jars were already present.
	   *"/usr/lib/hive-0.13.1-bin/lib"
	   * 
	   */
	  
	  
	public String evaluate(String price){
			if(Strings.isNullOrEmpty(price)){
				return null;			
			}
			
			int result = Integer.parseInt(price)*2;
			//Integer.toString(result);
			//Text final_result=Integer.toString(result);
			String final_result = Integer.toString(result); 
			return final_result;
		
	  }
}	
	

	
	
	







