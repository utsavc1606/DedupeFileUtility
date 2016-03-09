package com.amex.acxiom;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

public class ConsoleTester {


    public static void main(String[] args) throws IOException, InterruptedException {
    	if (args.length < 7){
    		System.out.println("There should be 7 arguments. Please check again. ");
    	}
    	else{
    		String fileLocation = args[0]; //Location where the output file is wanted. 
        	String gbCol = args[1]; //Group By Column
        	String gbOrder = args[2]; //Group By ascending or descending
        	String sbCol = args[3]; //Sort by column
        	String sbOrder = args[4]; //Sort by ascending or descending
        	String headerFile = args[5]; //Location of header file 
        	String tableName = args[6]; //Name the table in which data will be stored and processed
        	String inputFileLocation = args[7]; //input file location
//        	System.out.println("INSERT OVERWRITE LOCAL DIRECTORY '"+fileLocation+"' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' select * from empdata sort by "+gbCol+" "+gbOrder+", "+sbCol+" "+sbOrder+";");
        	
        	String line;
//    		String headerFile = "C:\\Users\\uchatt\\Hadoop_Content\\datasets\\DFU\\prs_na_npa_open_header.dat";

        	try {
                // FileReader reads text files in the default encoding.
                FileReader fileReader = 
                    new FileReader(headerFile);

                // Always wrap FileReader in BufferedReader.
                BufferedReader bufferedReader = 
                    new BufferedReader(fileReader);

                while((line = bufferedReader.readLine()) != null) {
                    String[] headers = line.split("\t");
                    String fields = "";
                    for (String elements : headers){
                    	fields = fields + elements+" STRING,";
                    }
                    if (fields != null && fields.length() > 0 && fields.charAt(fields.length()-1)==',') {
                        fields = fields.substring(0, fields.length()-1);
                      }
                    String createCommand = "CREATE EXTERNAL TABLE "+tableName+"("+fields+")ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n' STORED AS TEXTFILE LOCATION '"+inputFileLocation+"';";
                    System.out.println(createCommand);
                    ProcessBuilder hiveProcessBuilder = new ProcessBuilder("hive", "-e", createCommand);
//                  String path = processEnv.get("PATH");
                    Process hiveProcess1 = hiveProcessBuilder.start();
                    OutputRedirector outRedirect = new OutputRedirector(hiveProcess1.getInputStream(), "HIVE_OUTPUT");
                    OutputRedirector outToConsole = new OutputRedirector(hiveProcess1.getErrorStream(), "HIVE_LOG");

                    outRedirect.start();
                    outToConsole.start();
                    hiveProcess1.waitFor();
                    hiveProcess1.destroy();
                }   
                
                

                // Always close files.
                bufferedReader.close();         
            }
            catch(FileNotFoundException ex) {
                System.out.println(
                    "Unable to open file '" + 
                    headerFile + "'");                
            }
            catch(IOException ex) {
                System.out.println(
                    "Error reading file '" 
                    + headerFile + "'");                  
                // Or we could just do this: 
                // ex.printStackTrace();
            }
        	
        	System.out.println("Table creation was successful. Moving on to sorting now.");
        	
        	try {
        		File dir = new File(fileLocation);
				FileUtils.deleteDirectory(dir );
				System.out.println("Existing output directory will be replaced.");
			} catch (Exception e) {
				System.out.println(e);
			}
        	
        	

            ProcessBuilder hiveProcessBuilder = new ProcessBuilder("hive", "-e", "INSERT OVERWRITE LOCAL DIRECTORY '"+fileLocation+"' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' select * from "+tableName+" sort by "+gbCol+" "+gbOrder+" , "+sbCol+" "+sbOrder+";");
//            String path = processEnv.get("PATH");
            Process hiveProcess2 = hiveProcessBuilder.start();

            OutputRedirector outRedirect = new OutputRedirector(hiveProcess2.getInputStream(), "HIVE_OUTPUT");
            OutputRedirector outToConsole = new OutputRedirector(
                    hiveProcess2.getErrorStream(), "HIVE_LOG");

            outRedirect.start();
            outToConsole.start();
            hiveProcess2.waitFor();
    	}
    	    
    }
	

}
