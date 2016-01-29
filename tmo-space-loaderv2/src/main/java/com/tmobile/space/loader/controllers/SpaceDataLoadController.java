package com.tmobile.space.loader.controllers;

import com.tmobile.space.loader.service.MasterDataLoaderService;
import com.tmobile.space.loader.service.ORDLoader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;

/**
 * Created by basu on 1/19/16.
 */
@RestController
@RequestMapping("/space/dataloader/load")

public class SpaceDataLoadController {

    @Autowired
    private ORDLoader ordLoader;
	
	@Autowired
    private MasterDataLoaderService masterDataLoader;

    @RequestMapping(value="/upload", method=RequestMethod.POST)
    public @ResponseBody
    String handleFileUpload(@RequestParam("loadType") String loadType,
                            @RequestParam("file") MultipartFile file){
        System.out.println(loadType);
        String returnValue =  "You successfully uploaded !";
        if (!file.isEmpty()) {
            try {
                byte[] bytes = file.getBytes();
                InputStream inputStream = new ByteArrayInputStream(bytes);
                switch (loadType) {
                    case "Channel":
                        ordLoader.loadCDC(inputStream);
                    break;
                    case "OfferData":
                        ordLoader.loadOfferData(inputStream);
                        break;
                    case "Compatibility":
                        ordLoader.loadCompatibilityMatrix(inputStream);
                        break;
						case "StoreData":
                    	masterDataLoader.loadStoreData(file);
                        break;
                    case "PostalData":
                    
                    	File tmpFile = new File(System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + 
                    			file.getOriginalFilename());
                    	try{
                    	    file.transferTo(tmpFile);    	
                    
                    	}catch(Exception e) 
                    	{
                    		e.toString();
                    	}
                    	masterDataLoader.loadPostalData(tmpFile);
                        break;
                }

                return returnValue;
            } catch (Exception e) {
                return "You failed to upload  => " + e.getMessage();
            }
        } else {
            return "You failed to upload  because the file was empty.";
        }
    }

}
