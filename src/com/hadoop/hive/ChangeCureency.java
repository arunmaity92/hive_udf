package com.hadoop.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

public class ChangeCureency extends UDF{

	public double evaluate(double amount, String currency){
		
		double result=0;
		switch(currency){ 
		case "usdollar" : 
			result=amount*0.01;
			break;
			
		case "euro" :
			result=amount*0.014;
			break;
			
		}
		
		return result;
	}
	
}
