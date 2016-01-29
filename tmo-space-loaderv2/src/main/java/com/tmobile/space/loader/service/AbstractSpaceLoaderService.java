package com.tmobile.space.loader.service;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cassandra.core.CqlOperations;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by basu on 12/27/15.
 */
public abstract class AbstractSpaceLoaderService {
    @Autowired
    protected CqlOperations cqlTemplate;


    protected String getStringCellValue(Row row, int cellNum) {
        String returnValue = null;
        if(row != null) {
            Cell cell = row.getCell(cellNum);
            if(cell != null) {
                cell.setCellType(Cell.CELL_TYPE_STRING);
                if( cell.getStringCellValue() != null && cell.getStringCellValue().trim().length() > 0) {
                    returnValue = cell.getStringCellValue().trim();
                }
            }
        }
        return returnValue;
    }

    protected List<String> getMultiLineInputAsList(String input)  {
        List<String> channelList = null;
        if(input != null) {
            try {
                BufferedReader bufferedReader = new BufferedReader(new StringReader(input));
                String line = bufferedReader.readLine();
                while (line != null) {
                    //String[] channels = line.replace("[", "").replace("]", "").split("\\.");
                    if (channelList == null) {
                        channelList = new ArrayList<>();
                    }
                    line = line.replace("[","").replace("]","").trim();
                    if(line.length() > 0) {
                        String[] values = line.split(",");
                        for(String value : values) {
                            channelList.add(value.trim());
                        }
                    }
                    line = bufferedReader.readLine();
                }
                bufferedReader.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        return channelList;
    }

}
