package com.tmobile.space.loader.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import javax.annotation.PostConstruct;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;

/**
 * Created by Syam on 1/25/16.
 */
@Service("masterDataLoadService")

public class MasterDataLoaderService extends AbstractSpaceLoaderService {
	protected PreparedStatement storeDataInsertStmt;
	protected PreparedStatement postalDataInsertStmt;
    private Workbook workbook;

    static final int STOREDATA_STOREID_COL         = 0;
    static final int STOREDATA_STORENAME_COL       = 1;
    static final int STOREDATA_STOREADDR_COL       = 2;
    static final int STOREDATA_STORECITY_COL       = 3;
    static final int STOREDATA_STOREZIP_COL        = 4;
    static final int STOREDATA_STORESTATE_COL      = 5;
    static final int STOREDATA_STORECOUNTY_COL     = 6;
    static final int STOREDATA_STORECNTRY_COL      = 7;
    static final int STOREDATA_STORENAME2_COL      = 15;
    static final int STOREDATA_STORETYPE_COL       = 19;
    static final int STOREDATA_STORETYPE2_COL      = 20;
    
    static final int POSTALDATA_ZIP_COL        = 0;
    static final int POSTALDATA_COUNTY_COL     = 13;
    static final int POSTALDATA_NAME_COL       = 2;    
    static final int POSTALDATA_STATE_COL      = 11;    
    static final int POSTALDATA_CNTRY_COL      = 7;
    
    @PostConstruct
    void init() {
        try {
            storeDataInsertStmt = cqlTemplate.getSession().prepare(
                    "INSERT INTO STOREDATA (" +
                            "storeid, " +
                            "storename, " +
                            "streetaddress, " +
                            "city, " +
                            "zipcode, " +
                            "state, " +
                            "county, " +
                            "country, " +
                            "storename2, " +
                            "storetype, " +
                            "storetype2) " +                            
                            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
            
            postalDataInsertStmt = cqlTemplate.getSession().prepare(
                    "INSERT INTO POSTALDATA (" +
                            "zipcode, " +
                            "county, " +
                            "name, " +
                            "state, " +
                            "country) " +                            
                            "VALUES(?, ?, ?, ?, ?);");

                       
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
	 public String loadStoreData(MultipartFile file) {
	    	try{    	                         
	    	       
	    	    workbook = WorkbookFactory.create(file.getInputStream());
	    	}
	    	catch(Exception e)
	    	{
	    		System.out.println(e.toString());
	    	}
	    	BatchStatement batchStatement = new BatchStatement();
	        Sheet sheet = workbook.getSheet("Sheet1");
	        int startRow = 1;
	        Row row = sheet.getRow(startRow);
	        while(row != null ) {
	        	String storeId = getStringCellValue(row,STOREDATA_STOREID_COL);
	        	String storeName = getStringCellValue(row,STOREDATA_STORENAME_COL);
	        	String streetAddress = getStringCellValue(row,STOREDATA_STOREADDR_COL);
	        	String city = getStringCellValue(row,STOREDATA_STORECITY_COL);
	        	String zipCode = getStringCellValue(row,STOREDATA_STOREZIP_COL);
	        	String state = getStringCellValue(row,STOREDATA_STORESTATE_COL);
	        	String county = getStringCellValue(row,STOREDATA_STORECOUNTY_COL);
	        	String country = getStringCellValue(row,STOREDATA_STORECNTRY_COL);
	        	String storeName2 = getStringCellValue(row,STOREDATA_STORENAME2_COL);
	        	String storeType = getStringCellValue(row,STOREDATA_STORETYPE_COL);
	        	String storeType2 = getStringCellValue(row,STOREDATA_STORETYPE2_COL);        	
	        	batchStatement.add(storeDataInsertStmt.bind(storeId,storeName,streetAddress,city,zipCode,state,county,country,storeName2,storeType,storeType2));
	            row = sheet.getRow(++startRow); 
	          }
	        if(batchStatement != null && batchStatement.size() > 0) {
	    		 cqlTemplate.getSession().execute(batchStatement);
	                batchStatement.clear();
	            }

	        return "loaded store data";
	    }
	 
	 
	 public String loadPostalData(File file) {
		    int stmtCount = 0;
	    	BufferedReader br = null;
	    	BatchStatement batchStatement = new BatchStatement();
	    	try{	    	   	
	    		
	    	 br = new BufferedReader(new FileReader(file));
	    	
	    	String line = "";
	    	String cvsSplitBy = ",";    	
	    	
	    	while ((line = br.readLine()) != null)
	    	 {	        
			   String[] postalData = line.split(cvsSplitBy);
			   String zip = postalData[POSTALDATA_ZIP_COL];
	    	   String postalName = postalData[POSTALDATA_NAME_COL];
	    	   String state = postalData[POSTALDATA_STATE_COL];
	    	   String county = postalData[POSTALDATA_COUNTY_COL];
	    	   String country = "US";
	    	   
	    	   batchStatement.add(postalDataInsertStmt.bind(zip,county,postalName,state,country));
	    	   stmtCount++;
	    	   if( stmtCount % 100 == 0) {
	    		   cqlTemplate.getSession().execute(batchStatement);
                   batchStatement.clear();
               }
	    	   
	    	  }
	    	 if(batchStatement != null && batchStatement.size() > 0) {
	    		   cqlTemplate.getSession().execute(batchStatement);
	               batchStatement.clear();
	            }
	    	
	      	}
	    	catch(Exception e)
	    	{
	    		System.out.println(e.toString());
	    	}	    	
	    	
	        return "loaded Postal data";
	    }


}
