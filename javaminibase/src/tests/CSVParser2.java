//package tests;
//
//import global.AttrType;
//import heap.Heapfile;
//import heap.InvalidTupleSizeException;
//import heap.InvalidTypeException;
//import heap.Tuple;
//import iterator.FileScan;
//import iterator.FileScanException;
//import iterator.FldSpec;
//import iterator.InvalidRelation;
//import iterator.RelSpec;
//import iterator.Iterator;
//import iterator.TupleUtilsException;
//
//import java.io.BufferedReader;
//import java.io.File;
//import java.io.FileNotFoundException;
//import java.io.FileReader;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.util.ArrayList;
//
//import au.com.bytecode.opencsv.CSVReader;
//import chainexception.ChainException;
//
//class data1Class{
//	
//}
//public class CSVParser2 {
//
//	private AttrType[] _attrs;
//	private short[] _str_sizes;
//	private String _heapFileName;
//	private boolean metadata_set = false;
//	
//	public CSVParser(String csvFilePath) throws IOException {
//		metadata_set = setupTupleMetaData(csvFilePath);
//	}
//
//	public AttrType[] get_attrs() {
//		return _attrs;
//	}
//
//	public short[] get_str_sizes() {
//		return _str_sizes;
//	}
//	
//	public String getHeapFileName() {
//		return _heapFileName;
//	}
//
//	public Heapfile ConvertToHeapfile(String csvFilePath) throws IOException,
//			ChainException, InvalidTupleSizeException {
//		if (!metadata_set) {
//			metadata_set = setupTupleMetaData(csvFilePath);
//		}
//		// filename = full path location
//		Heapfile hf = new Heapfile(_heapFileName);
//		
//		// re-initalize csvReader
//		CSVReader csvReader = new CSVReader(new FileReader(csvFilePath));
//		csvReader.readNext(); // throw the first line
//		
//		// for each csv line
//		while (true) {
//			String[] csv_line = csvReader.readNext();
//			if (csv_line == null) break;
//
//			Tuple curr_tuple = new Tuple();
//			// / debug here
//			curr_tuple.setHdr((short) _attrs.length, _attrs, _str_sizes);
//
//			// for each cell data in csv_line
//			for (int cell_no = 0; cell_no < csv_line.length; cell_no++) {
//				String cell_data = csv_line[cell_no];
//				
//				if (isInteger(cell_data)) {
//					if(Integer.parseInt(cell_data)==1)
//					{
//						curr_tuple.setFloFld(cell_no + 1,
//								1.0f);
//						curr_tuple.setScore(1.0f); 
//					}
//					else if
//					(Integer.parseInt(cell_data)==0)
//					{
//						curr_tuple.setFloFld(cell_no + 1,
//								0.0f);
//						curr_tuple.setScore(0.0f); 
//					}
//					else
//					{
//						
//					
//					System.out.println("----------------------printing test data with data is"+Integer.parseInt(cell_data));
//					curr_tuple.setIntFld(cell_no + 1,
//							Integer.parseInt(cell_data));}
//				} else if (isFloat(cell_data)) {
//				      if(cell_data.equals("1"))
//				      {
//				    	  curr_tuple.setFloFld(cell_no + 1,
//									1.0f);
//							curr_tuple.setScore(1.0f); 
//				      }
//				      else
//				      {
//					curr_tuple.setFloFld(cell_no + 1,
//							Float.parseFloat(cell_data));
//					curr_tuple.setScore(Float.parseFloat(cell_data));}
//				} else { // string type
//					curr_tuple.setStrFld(cell_no + 1, cell_data);
//				}
//			}
//			hf.insertRecord(curr_tuple.getTupleByteArray());
//		}
//		csvReader.close();
//
//		return hf;
//	}
//
//	
//	
//	public Iterator ConvertToIterator(String csvFilePath) throws IOException, ChainException {
//		ConvertToHeapfile(csvFilePath); 
//		
//		FldSpec[] Sprojection = new FldSpec[_attrs.length];
//		
//		for (int i = 0; i < Sprojection.length; i++) {
//			Sprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
//		}
//		
//		Iterator fscan = new FileScan(_heapFileName, _attrs, get_str_sizes(),
//				(short) _attrs.length, _attrs.length, Sprojection, null);
//
//		return fscan;
//	}
//	
//	public boolean setupTupleMetaData(String csvFilePath) throws IOException {
//		_heapFileName = (new File(csvFilePath)).getName()+".hf";
//		// _filename is set
//		
//		
//		CSVReader csvReader = new CSVReader(new FileReader(csvFilePath));
//		csvReader.readNext(); // remove header line
//		String[] csv_line = csvReader.readNext(); // read first line
//		this._attrs = new AttrType[csv_line.length];
//
//		ArrayList<Integer> str_col_indexes = new ArrayList<Integer>();
//		short[] str_col_max_sizes = new short[csv_line.length-1];
//		
//		// set up attr types of tuple
//		//for (int cell_no = 0; cell_no < csv_line.length; cell_no++) 
//			for (int cell_no = 0; cell_no < csv_line.length; cell_no++){
//			String cell_data = csv_line[cell_no];
//			//System.out.println("cell data : "+cell_data);
//			if (isInteger(cell_data)) {
//				_attrs[cell_no] = new AttrType(AttrType.attrInteger);
//			} else if (isFloat(cell_data)) {
//				_attrs[cell_no] = new AttrType(AttrType.attrReal);
//			} else {
//				_attrs[cell_no] = new AttrType(AttrType.attrString);
//				str_col_indexes.add(cell_no);
//				str_col_max_sizes[cell_no] = (short) cell_data.length();
//			}
//		}
//		csvReader.close();
//		// _attrs is now set
//
//
//		csvReader = new CSVReader(new FileReader(csvFilePath));
//		csvReader.readNext(); // remove header line
//		// if string column, find out the max string size
//		// build str_col_max_sizes
//		int count1=0;
//		while ((csv_line = csvReader.readNext()) != null) {
//			for(int i=0;i<csv_line.length;i++)
//			{
//				count1++;
//				System.out.println(csv_line[i]);
//				if(csv_line[i]=="1.4E-45")
//				{
//					csv_line[i]="1.0";
//				}
//				
//				
//			}
//			System.out.println("total count is : "+count1);
//			for (int str_col_index : str_col_indexes) {
//				// check if current cell has a longer string size than curr max
//				if (csv_line[str_col_index].length() > str_col_max_sizes[str_col_index]) {
//					str_col_max_sizes[str_col_index] = (short) csv_line[str_col_index]
//							.length();
//				}
//			}
//		}
//		csvReader.close();
//
//		
//		_str_sizes = new short[str_col_indexes.size()];
//		int count = 0;
//		// build _str_sizes
//		for (int str_col_index : str_col_indexes) {
//			_str_sizes[count++] = str_col_max_sizes[str_col_index];
//		}
//		// _str_sizes now set
//		
//		
//		return true;
//	}
//
//	private static boolean isInteger(String s) {
//		try {
//			Integer.parseInt(s);
//		} catch (NumberFormatException e) {
//			return false;
//		}
//
//		return true;
//	}
//
//	private static boolean isFloat(String s) {
//		try {
//			Float.parseFloat(s);
//			
//		} catch (NumberFormatException e) {
//			return false;
//		}
//		return true;
//
//	}
//}
