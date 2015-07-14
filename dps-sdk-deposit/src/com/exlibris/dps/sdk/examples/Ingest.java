/* INGEST TEST UTIL
 * Usage ingest.bat <directory_path>
 * <directory_path> that include the ingest.properties
 *
 */
//Original source: https://github.com/ExLibrisGroup/Rosetta.dps-sdk-projects/blob/master/4.2/dps-sdk-deposit/src/com/exlibris/dps/sdk/examples/Ingest.java 

package com.exlibris.dps.sdk.examples;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Calendar;
import java.util.Hashtable;
import java.util.Properties;

import javax.xml.namespace.QName;

import com.exlibris.core.infra.common.util.IOUtil;
import com.exlibris.core.sdk.utils.FileUtil;
import com.exlibris.digitool.deposit.service.xmlbeans.DepData;
import com.exlibris.digitool.deposit.service.xmlbeans.DepositDataDocument;
import com.exlibris.digitool.deposit.service.xmlbeans.DepositDataDocument.DepositData;
import com.exlibris.dps.DepositWebServices;
import com.exlibris.dps.DepositWebServices_Service;
import com.exlibris.dps.ProducerWebServices;
import com.exlibris.dps.ProducerWebServices_Service;
import com.exlibris.dps.sdk.pds.PdsClient;

public class Ingest {

	private static final String DEFAULT_INGEST_PROPERTIES_FILE_PATH =
		"..//dps-sdk-deposit//conf//ingest.properties";
	private static final String SETTINGS_FILE_PATH =
		File.separator+"content"+File.separator+"settings"+File.separator+"settings.properties";

	//replace these below with values we got from Michelle:
	private static final String PROPERTY_SOURCE_DIR_ON_SERVER = "data//depositExamples"; //"/home/dagedv/development/ROSETTA/Rosetta_deposit/dps-sdk-deposit/data/depositExamples"; // ?????
	private static final String D_PROPERTY_WSDL_URL = "http://rosetta.develop.lza.tib.eu/dpsws/deposit/DepositWebServices?wsdl";
	private static final String P_PROPERTY_WSDL_URL = "http://rosetta.develop.lza.tib.eu/dpsws/deposit/ProducerWebServices?wsdl";
	private static final String PROPERTY_PDS_URL = "https://rosetta.develop.lza.tib.eu/pds";
	private static final String PROPERTY_MATERIAL_FLOW_ID = "36411632";
	private static final String PROPERTY_DEPOSIT_SET_ID = "1";
	private static final String PROPERTY_USER_NAME = "user_name"; 	// secret
	private static final String PROPERTY_USER_PASSWORD = "user_password";		// secret
	private static final String PROPERTY_USER_INSTITUTION = "TIB";
	private static final String PROPERTY_USER_PRODUCER_ID = "36384081";


	
	private static Hashtable<String, BufferedOutputStream> m_fosHash = new Hashtable<String, BufferedOutputStream>();
	private static String fileName;
    /**
     * Example of a bulk deposit program using the API of DepositWebServices
     * @param args location of ingest.properties file.
     */
	
	// ==> Need input parameter that contains (a) the settings file and (b) the directory where the upload is sent to on Rosetta
	
	public static void main(String[] args) {
		
		
		System.out.println("Working Directory = " + System.getProperty("user.dir"));
		
		Properties ingestSettings = new Properties();
		String ingestPropertiesFilePath;
		if ((args != null) && (args.length > 0) &&
			(args[0] != null) && (args[0].trim().length() > 0)){
			ingestPropertiesFilePath = args[0];
		}
		else {
			ingestPropertiesFilePath = DEFAULT_INGEST_PROPERTIES_FILE_PATH;
		}
		try {
			ingestSettings.load(new FileInputStream(ingestPropertiesFilePath));
		} catch (Exception e) {
			reportException(e);
			return;
		}

		String sourceDir =  PROPERTY_SOURCE_DIR_ON_SERVER; //ingestSettings.getProperty(PROPERTY_SOURCE_DIR_ON_SERVER);
		File root = new File(sourceDir);
		File[] inFiles = root.listFiles();
		int index = sourceDir.lastIndexOf("/");
		if(!(index>0)){
			index= sourceDir.lastIndexOf("\\");
		}
		
		fileName = sourceDir.substring(0,index)+ File.separator + "log"+ File.separator+ "ingest.log";
		IOUtil.deleteFile(fileName);
		FileUtil.createFile(sourceDir.substring(0,index) + File.separator + "log");
		System.out.print("Log File name:" + fileName);
		
		
		String dwsdlUrl = D_PROPERTY_WSDL_URL; //ingestSettings.getProperty(D_PROPERTY_WSDL_URL);
		String pwsdlUrl = P_PROPERTY_WSDL_URL; //ingestSettings.getProperty(P_PROPERTY_WSDL_URL);

		DepositWebServices dpws = null;
		ProducerWebServices pws = null;

		String pdsHandle = null;
		String pdsUrl = null;
		String retval = null;

		try {
			dpws = new DepositWebServices_Service(new URL(dwsdlUrl),new QName("http://dps.exlibris.com/", "DepositWebServices")).getDepositWebServicesPort();
		} catch (Exception e) {
			reportException(e);
			return;
		}

		try {
			pws = new ProducerWebServices_Service(new URL(pwsdlUrl),new QName("http://dps.exlibris.com/", "ProducerWebServices")).getProducerWebServicesPort();
		} catch (Exception e) {
			reportException(e);
			return;
		}

		PdsClient pds = PdsClient.getInstance();
		try{
			pdsUrl = PROPERTY_PDS_URL; //ingestSettings.getProperty(PROPERTY_PDS_URL);
		} catch(Exception e){
			reportException(e);
			return;
		}
		pds.init(pdsUrl,false);



		// For each directory submit it for deposit.
		for(int i=0;i<inFiles.length;i++) {
			File currInFile = inFiles[i];
			Properties settings = new Properties();
			if(currInFile.isFile()){
				continue;
			}
			System.out.println("file="+ currInFile.getAbsoluteFile()+ SETTINGS_FILE_PATH);
		    try {
				settings.load(new FileInputStream(currInFile.getAbsoluteFile()+ SETTINGS_FILE_PATH));
			} catch (Exception e) {
				reportException(e);
				continue;
			}
		    
		    String materialFlowId = PROPERTY_MATERIAL_FLOW_ID; //settings.getProperty(PROPERTY_MATERIAL_FLOW_ID);
		    String depositSetId = PROPERTY_DEPOSIT_SET_ID; //settings.getProperty(PROPERTY_DEPOSIT_SET_ID);
		    String userName = settings.getProperty(PROPERTY_USER_NAME);
		    String userPassword = settings.getProperty(PROPERTY_USER_PASSWORD);
		    String userInstitution = PROPERTY_USER_INSTITUTION; //settings.getProperty(PROPERTY_USER_INSTITUTION);
		    String producerId = PROPERTY_USER_PRODUCER_ID; //settings.getProperty(PROPERTY_USER_PRODUCER_ID);

			try {
				// Login with the corrected user, according to the setting.properties
				pdsHandle = pds.login(userInstitution, userName, userPassword);
			} catch (Exception e) {
				reportException(e);
				continue;
			}

			//check that producer is affiliated with producerAgent
			boolean isValid = false;
			try{
				String producerAgentId = pws.getInternalUserIdByExternalId(userName);
				String xmlReply = pws.getProducersOfProducerAgent(producerAgentId);
				DepositDataDocument depositReply = DepositDataDocument.Factory.parse(xmlReply);
				DepositData depositData = depositReply.getDepositData();
				DepData[] depdata = depositData.getDepDataArray();
				for(int j=0;j<depdata.length;j++){
					if(producerId.equals(depdata[j].getId())){
						isValid = true;
					}
				}
			}catch(Exception e) {
				reportException(e);
				continue;
			}

			if(!isValid){
				System.err.println("producerId is invalid");
				continue;
			}

			//check that materialFlow is affiliated with producerAgent
			isValid = false;
			try{
				String xmlReply = pws.getMaterialFlowsOfProducer(producerId);
				DepositDataDocument depositReply = DepositDataDocument.Factory.parse(xmlReply);
				DepositData depositData = depositReply.getDepositData();
				DepData[] depdata = depositData.getDepDataArray();
				for(int j=0;j<depdata.length;j++){
					if(materialFlowId.equals(depdata[j].getId())){
						isValid = true;
					}
				}
			}catch(Exception e) {
				reportException(e);
				continue;
			}

			if(!isValid){
				System.err.println("materialFlowId is invalid");
				continue;
			}
			Calendar calendar1 = Calendar.getInstance();
			writeMsg("Started Processing Deposit For : " + currInFile.getName());
			try{
				// Submit the activity to deposit server, retval is XML that holds the response.
				retval = dpws.submitDepositActivity(pdsHandle,materialFlowId,currInFile.getName(), producerId, depositSetId);
				writeMsg("Sip Result: " + retval);
			} catch(Exception e){
				writeMsg("Failed depositing " + currInFile.getName() + "!");
				System.err.println("Failed depositing " + currInFile.getName() + "!");
				reportException(e);
				continue;
			}
			Calendar calendar2 = Calendar.getInstance();
			long processTime  =calendar2.getTimeInMillis() - calendar1.getTimeInMillis();
			writeMsg("Finished Processing Deposit For : " + currInFile.getName() + ", processing took " + processTime + " milliseconds");

		}
		writeMsgInternal("Ingest finished",true,false);
	}

	public static void writeMsg(String msg) {
	    writeMsgInternal(msg, false, true);
	  }

	public static void writeMsgInternal(String msg, boolean closeFile, boolean writeToConsole) {
		BufferedOutputStream fos = null;
		try{
			fos = m_fosHash.get(fileName);
		}catch(Exception e){

		}

	    if (fos == null) {
	      try {
	        fos = new BufferedOutputStream(new FileOutputStream(fileName, true));
	      } catch (Exception e) {
	        System.out.println("Fail to init log file:" + fileName);
	      }
	    }

	    try {
	    	StringBuffer fullMsg = new StringBuffer(Calendar.getInstance().getTime().toString()).append(
	          " : ").append(msg).append("\n");

	      IOUtil.copy(fullMsg.toString(), fos);
	      if (writeToConsole) {
	        System.out.print(fullMsg);
	      }

	      //Saving to hash;
	      if (!closeFile) {
	        m_fosHash.put(fileName, fos);
	      } else {
	        fos.close();
	      }

	    } catch (IOException e1) {
	      System.out.println("Fail writing message to log:" + msg);
	    }
	}

	private static void reportException(Exception exception) {
		if(fileName != null){
			writeMsg("ERROR! " + exception.getClass().getName() + " :: " + exception.getMessage());
		}
		System.err.println("ERROR! " + exception.getClass().getName() + " :: " + exception.getMessage());
		exception.printStackTrace();
	}
}
