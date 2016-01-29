package com.tmobile.space.loader.service;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.t_mobile.services.EpcExtract;
import com.t_mobile.services.ProductOffering;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.CellRangeAddress;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cassandra.core.CqlOperations;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.namespace.QName;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.*;

/**
 * Created by basu on 1/19/16.
 */
@Service("ORDLoaderService")
@Configuration
@PropertySource("classpath:ORDLoader.properties")
public class ORDLoader extends AbstractSpaceLoaderService {
    @Value("${cdc_sheetName}")
    private String cdc_sheetName;
    @Value("${compatibility_sheetNames}")
    private String compatibilityMatrixSheetNames;
    private JAXBContext jaxbContext;
    @Autowired
    CqlOperations cqlOperations;

    PreparedStatement cdcInsertStmt;
    PreparedStatement offer2CodeMappingInsertStmt;
    PreparedStatement offerDataInsertStmt;
    PreparedStatement compatibilityMatrixInsertStmt;

    @PostConstruct
    private void init() {
        try {
            jaxbContext = JAXBContext.newInstance("com.t_mobile.services");
            cdcInsertStmt = cqlOperations.getSession().prepare(
                            "INSERT INTO CDC (" +
                            "channel_segment," +
                            "code," +
                            "source) VALUES (?,?,?);");

            offer2CodeMappingInsertStmt = cqlOperations.getSession().prepare(
                    "INSERT INTO OFFER2CODEMAPPING (" +
                            "decision_code," +
                            "included_offer," +
                            "source) VALUES (?,?,?);");

            offerDataInsertStmt = cqlOperations.getSession().prepare(
                    "INSERT INTO OFFERDATA (" +
                            "offerid," +
                            "productoffering) VALUES (?,?);");

            compatibilityMatrixInsertStmt = cqlOperations.getSession().prepare(
                    "INSERT INTO COMPATIBILITYMATRIX (" +
                            "product_offer," +
                            "related_offer," +
                            "relationship_type," +
                            "source," +
                            "code) VALUES (?,?,?,?,?);");

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public String loadCDC(InputStream inputStream) {
        System.out.println(cdc_sheetName);
        try {
            Workbook workbook = WorkbookFactory.create(inputStream);
            int channelDefinitionRow = 14;
            int subChannelDefinitionRow = 15;
            int offerStartRow = 16;
            int channelDefinitionStartCol = 4;
            Sheet s = workbook.getSheet(cdc_sheetName);
            List<CellRangeAddress> mergedRegions = s.getMergedRegions();
            Row channelDefRow = s.getRow(channelDefinitionRow);
            Map<Integer,CellRangeAddress> cellRangeAddressMap = new HashMap<>();
            for(CellRangeAddress cellRangeAddress : mergedRegions) {
                if(cellRangeAddress.getFirstRow() == channelDefinitionRow ) {
                    cellRangeAddressMap.put(cellRangeAddress.getFirstColumn(),cellRangeAddress);
                    System.out.println("Cell Range start col = "+cellRangeAddress.getFirstColumn());
                }
            }
            String channel = null;
            List<String> channels = new ArrayList<>();
            Map<String,List<String>> cdcMap = new LinkedHashMap<>();

            while((channel = getStringCellValue(channelDefRow,channelDefinitionStartCol++)) != null) {
                channels.add(channel);
                CellRangeAddress cellRangeAddress = cellRangeAddressMap.get(channelDefinitionStartCol - 1 );
                Row subChannelDefintionRow = s.getRow(subChannelDefinitionRow);
                int startCol = channelDefinitionStartCol - 1;
                int endCol = startCol;



                if(cellRangeAddress != null) {
                    channelDefinitionStartCol = cellRangeAddress.getLastColumn() + 1;
                    endCol = cellRangeAddress.getLastColumn();
                }

                for(int i = startCol;i<=endCol;i++) {
                    String subChannel = getStringCellValue(subChannelDefintionRow,i).replace("\n","").trim();
                    int startRow = offerStartRow;
                    Row offerRow = s.getRow(startRow++);
                    String offerId = getStringCellValue(offerRow,2);
                    while (offerId != null) {
                        String categories = getStringCellValue(offerRow,i);
                        if(categories != null) {
                            List<String> catergoryList = getMultiLineInputAsList(categories);
                            for (String categroy : catergoryList) {
                                String cdc = "[" + channel + "][" + subChannel + "][" + categroy + "]";
                                List<String> includedOffers = cdcMap.get(cdc);
                                if (includedOffers == null) {
                                    includedOffers = new ArrayList<>();
                                    cdcMap.put(cdc, includedOffers);
                                }
                                if (includedOffers.indexOf(offerId) == -1) {
                                    includedOffers.add(offerId);
                                }
                            }
                        }
                        offerRow = s.getRow(startRow++);
                        offerId = getStringCellValue(offerRow,2);
                    }
                }
            }
            int i = 0;
            BatchStatement batchStatement = new BatchStatement();
            for(Map.Entry<String,List<String>> cdcEntry : cdcMap.entrySet()) {
                String code = "CDC"+String.format("%05d",++i);
                batchStatement.add(cdcInsertStmt.bind(cdcEntry.getKey(),code,"ORD"));
                batchStatement.add(offer2CodeMappingInsertStmt.bind(code,String.join(",",cdcEntry.getValue()),"ORD"));
                if(i % 5 == 0) {
                    cqlOperations.execute(batchStatement);
                    batchStatement.clear();
                }
            }
            if(batchStatement.getStatements().size() > 0) {
                cqlOperations.execute(batchStatement);
            }
        } catch (Exception ex) {
            ex.getMessage();
            ex.printStackTrace();
        }
        return "Loaded CDC from ATT-SG-066 Channel sheet";
    }

    public String loadOfferData(InputStream inputStream) {
        int  i = 0;
        try {
            Marshaller marshaller = jaxbContext.createMarshaller();
            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
            EpcExtract epcExtract  = (EpcExtract)unmarshaller.unmarshal(inputStream);
            for(ProductOffering productOffering : epcExtract.getProductOfferings().getProductOffering()) {
                StringWriter stringWriter = new StringWriter();
                marshaller.marshal(new JAXBElement<>(new QName("ProductOffering"),ProductOffering.class,productOffering),stringWriter);
                String productOfferingPaylod = stringWriter.toString();
                cqlOperations.getSession().execute(offerDataInsertStmt.bind(productOffering.getProductOfferingId(),productOfferingPaylod));
                i++;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return "Loaded "+i + " product offerings";
    }

    public String loadCompatibilityMatrix(InputStream inputStream) {
        try {
            Workbook workbook = WorkbookFactory.create(inputStream);
            String[] sheets = compatibilityMatrixSheetNames.split(",");
            int b = 0;
            BatchStatement batchStatement = new BatchStatement();

            for(String sheetDetail : sheets) {
                String[] details = sheetDetail.split(":");
                Sheet sheet = workbook.getSheet(details[0]);
                int offerCodeLeftStartRow =  Integer.parseInt(details[1]);
                int offerCodeLeftStartCol = Integer.parseInt(details[2]);
                int offerCodeTopStartRow = Integer.parseInt(details[3]);
                int offerCodeTopStartCol = Integer.parseInt(details[4]);

                Row offerCodeTopRow = sheet.getRow(offerCodeTopStartRow);
                int i = 0;
                String topRowOfferId = getStringCellValue(offerCodeTopRow,offerCodeTopStartCol+i);
                List<String> topRowOfferIdList = new ArrayList<>();

                while(topRowOfferId != null) {
                    topRowOfferIdList.add(topRowOfferId.trim());
                    topRowOfferId = getStringCellValue(offerCodeTopRow,offerCodeTopStartCol + (++i));
                }
                i = 0;
                Row leftOfferRow = sheet.getRow(offerCodeLeftStartRow);
                String leftOfferId = getStringCellValue(leftOfferRow,offerCodeLeftStartCol);
                Map<String,List<String>> inclusionMap = new HashMap<>();
                while(leftOfferId != null) {
                    leftOfferId = leftOfferId.trim();
                    List<String> inclusionList = new ArrayList<>();
                    for(int j = 0; j<topRowOfferIdList.size();j++) {
                        String compatible = getStringCellValue(leftOfferRow,offerCodeTopStartCol + j);
                        /*if(compatible != null && compatible.toUpperCase().equals("ALLOWED")) {
                            inclusionList.add(topRowOfferIdList.get(j));
                        }*/

                        if(compatible != null) {
                            batchStatement.add(compatibilityMatrixInsertStmt.bind(leftOfferId,topRowOfferIdList.get(j),compatible.trim(),"ORD","COMPAT"+String.format("%06d",++b)));
                        }
                        if( b % 10 == 0) {
                            cqlOperations.getSession().execute(batchStatement);
                            batchStatement.clear();
                        }
                    }
                    inclusionMap.put(leftOfferId,inclusionList);
                    leftOfferRow = sheet.getRow(offerCodeLeftStartRow + (++ i));
                    leftOfferId = getStringCellValue(leftOfferRow,offerCodeLeftStartCol);
                }
                //System.out.println(inclusionMap);
            }
            if(batchStatement.size() > 0) {
                cqlOperations.getSession().execute(batchStatement);
                batchStatement.clear();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }


}
