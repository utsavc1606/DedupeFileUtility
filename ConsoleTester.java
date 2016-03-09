package com.amex.acxiom;

import java.io.IOException;

public class ConsoleTester {


    public static void main(String[] args) throws IOException {
    	String fileLocation = args[0]; //Location where the output file is wanted. 
    	String gbCol = args[1]; //Group By Column
    	String gbOrder = args[2]; //Group By ascending or descending
    	String sbCol = args[3]; //Sort by column
    	String sbOrder = args[4]; //Sort by ascending or descending
    	System.out.println("INSERT OVERWRITE LOCAL DIRECTORY '"+fileLocation+"' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' select * from empdata sort by "+gbCol+" "+gbOrder+", "+sbCol+" "+sbOrder+";");

        ProcessBuilder hiveProcessBuilder = new ProcessBuilder("hive", "-e", "INSERT OVERWRITE LOCAL DIRECTORY '"+fileLocation+"' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' select * from empdata sort by "+gbCol+" "+gbOrder+", "+sbCol+" "+sbOrder+";");
//        String path = processEnv.get("PATH");
        Process hiveProcess = hiveProcessBuilder.start();

        OutputRedirector outRedirect = new OutputRedirector(hiveProcess.getInputStream(), "HIVE_OUTPUT");
        OutputRedirector outToConsole = new OutputRedirector(
                hiveProcess.getErrorStream(), "HIVE_LOG");

        outRedirect.start();
        outToConsole.start();    
    }
	

}
