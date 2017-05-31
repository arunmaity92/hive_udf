package com.hadoop.hive;


import java.util.ArrayList;
import org.apache.hadoop.hive.ql.exec.UDF;
//import org.apache.hadoop.io.Text;


public class Upper extends UDF{
	
	/*	
	 * This section is for working with HADOOP DATATYPES.
	 * 
	 * If we will use Text datatype which is hadoop wrapper datatype which is similar to String in Java 
	 * then it will import 
	 * hadoop.io.Text package.
	 * 
	 * 
	 */
	
	/*private Text colValue = new Text();	
	public Text evaluate(Text str){
		if(str==null){			
			return null;
		}
		
		String Result=str.toString().toUpperCase();
		//Text Result=str.toString().toUpperCase();
		colValue.set(Result);
		return colValue;		
	}
	*/
	
	/*
	 * Above code is having only one argument.for two argument we can write another function.
	 * But what is we want an variable argument and whatever number of argument it will get. we will 
	 * convert it into uppercase and return as an array or we can concat all those variables and return it.
	 * 
	 * Note here the below code is working fine but since the ArrayList<String> uppercase = new ArrayList<String>();
	 * is declared globally so it gives output like. 
	 * 
	 * Note ::::
	 * First of all I was thinking how the udf works is for each row in hive it will call the jar file and 
	 * do the working. 
	 * But if it would have been the way of working then for each row in hive this jar would have been
	 * called for each row and in that case this global variable would not have stored the previous 
	 * record result. But from the output you can understand that it is keeping the previous result output
	 * of each row.
	 * 
	 * the conclusion which is inferred is the jar is called once only. And for each row the 
	 * evaluate function will be called. By the output you can clearly see this thing.
	 * 
	 * hive> select name,city,upperlist(name,city) from fruit_int;
	 * 
	 * 	mango	chennai		["MANGO","CHENNAI"]
		$banana	hyd			["MANGO","CHENNAI","$BANANA","HYD"]
		mango	delhi		["MANGO","CHENNAI","$BANANA","HYD","MANGO","DELHI"]
		orange	nagpur		["MANGO","CHENNAI","$BANANA","HYD","MANGO","DELHI","ORANGE","NAGPUR"]
				banglr		["MANGO","CHENNAI","$BANANA","HYD","MANGO","DELHI","ORANGE","NAGPUR","","BANGLR"]
		apple	srinagar	["MANGO","CHENNAI","$BANANA","HYD","MANGO","DELHI","ORANGE","NAGPUR","","BANGLR","APPLE","SRINAGAR"]
		chiku	haryana		["MANGO","CHENNAI","$BANANA","HYD","MANGO","DELHI","ORANGE","NAGPUR","","BANGLR","APPLE","SRINAGAR","CHIKU","HARYANA"]		
	
	*	
	*
	*
	*
	*/
	
	
	
	ArrayList<String> uppercase = new ArrayList<String>();
	
	public ArrayList<String> evaluate(String... columns){
		
		for(String values : columns){
			String result = values.toUpperCase();
			uppercase.add(result);
		}
		
		return uppercase;
	}
	
	
	
	
	/*
	 * The above worked but the output is not expected because of the reason explained above as 
	 * per my understanding.
	 * 
	 * NOTE:: 
	 * So by keeping in mind the way hive and udf works if we will declare the 
	 * ArrayList<String> uppercase = new ArrayList<String>(); variable inside evaluate function rather 
	 * than global declaration then we know that for each record this evaluate function will be called.
	 * 
	 * so the variable uppecase will not store the previous record result.
	 * see the below output and it will be clear.
	 * 
	 * hive> select upperlist1(name,city) from fruit_int;
	 * 
	 * ["MANGO","CHENNAI"]
		["$BANANA","HYD"]
		["MANGO","DELHI"]
		["ORANGE","NAGPUR"]
		["","BANGLR"]
		["APPLE","SRINAGAR"]
		["CHIKU","HARYANA"]
	 * 
	 * 
	 * So we did write the same code but putting the uppercase variable inside the evaluate function.
	 * and it works perfectly as expected.
	 * 
	 */

	
	
	/*public ArrayList<String> evaluate(String... columns){
		
		ArrayList<String> uppercase = new ArrayList<String>();
		
		for(String values : columns){
			String result = values.toUpperCase();
			uppercase.add(result);
		}
		
		return uppercase;
	}
	*/
	
	
	/*
	 * The code for variable argument is working fine in above code snippets.
	 * we are trying to implement the variable argument some other way here in below code.
	 * 
	 * lets see whether it works or not.
	 * 
	 * 
	 */

	
	
	
	/*
	 * The below piece of for loop(both type normal and extended) is working fine: output as expected.
	 * 
	 * hive> elect upperlist3(name,city) from fruit_int;
	 * 	
	 * ["MANGO","CHENNAI"]
		["$BANANA","HYD"]
		["MANGO","DELHI"]
		["ORANGE","NAGPUR"]
		["","BANGLR"]
		["APPLE","SRINAGAR"]
		["CHIKU","HARYANA"]

	 * 
	 */
	
	
	/*public ArrayList<String> evaluate(String[] columns){
		
		ArrayList<String> uppercase = new ArrayList<String>();
		
			
		for( int i=0;i<columns.length;i++){
			String result = columns[i].toUpperCase();
			uppercase.add(result);
		}*/
		
		
		/*for(String values : columns){
			String result = values.toUpperCase();
			uppercase.add(result);			
		}*/
		
	/*			
		return uppercase;
	}*/
	
	
	
	
	}
