package global;


//Java imports
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.*;



//Apache POI - HSSF imports
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;




public class ExcelParser {
	
	HSSFSheet m_sheet;
	int m_iNbRows;
	int m_iCurrentRow = 0;
 private static final String JAVA_TOSTRING =
"EEE MMM dd HH:mm:ss zzz yyyy";
	
	public ExcelParser(HSSFSheet sheet)
	{
		m_sheet = sheet;
		m_iNbRows = sheet.getPhysicalNumberOfRows();
	}
	
	
	/* Returns the contents of an Excel row in the 
form of a String array.
	 * @see com.ibm.ccd.common.parsing.Parser#splitLine()
	 */
	public String[] splitLine() throws Exception {
		if (m_iCurrentRow == m_iNbRows)
			return null;
		
		HSSFRow row = m_sheet.getRow(m_iCurrentRow);
		if(row == null)
		{
			return null;
		}
		else
		{
			int cellIndex = 0; 
			int noOfCells =	row.getPhysicalNumberOfCells();
			String[] values = new String[noOfCells];        
			short firstCellNum = row.getFirstCellNum();
			short lastCellNum = row.getLastCellNum();
			
			if (firstCellNum >=0 && lastCellNum >=0) 
			{
				for(short iCurrent = firstCellNum; iCurrent <lastCellNum; iCurrent++) 
			{
					HSSFCell cell = (HSSFCell)row.getCell(iCurrent);
					if(cell == null)
					{
						values[iCurrent] = "";
						cellIndex++;        		
						continue;
					}
					else
					{
						switch(cell.getCellType())
						{        					
						case HSSFCell.CELL_TYPE_NUMERIC:
						double value = cell.getNumericCellValue();
						if(HSSFDateUtil.isCellDateFormatted(cell)) 
								
						{
							if(HSSFDateUtil.isValidExcelDate(value))
							{
								Date date = HSSFDateUtil.getJavaDate(value);
								SimpleDateFormat dateFormat = new SimpleDateFormat(JAVA_TOSTRING);	
								values[iCurrent] = dateFormat.format(date);								
							}
							else
							{
								throw new Exception("Invalid Date value found at row number " +
										row.getRowNum()+" and column number "+cell.getCellNum());	
							}
						}
						else
						{
							values[iCurrent] = value + "";
						}
						break;
						
						case HSSFCell.CELL_TYPE_STRING:
							values[iCurrent] = cell.getStringCellValue();
						break;
						
						case HSSFCell.CELL_TYPE_BLANK:
							values[iCurrent] = null;	
						break;
						
						default:
							values[iCurrent] = null;	
						}        	
					}        	            	
				}        
			}
			m_iCurrentRow++;
			return values;	            
		}
		
	}}