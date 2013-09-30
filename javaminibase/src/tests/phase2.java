package tests;

import global.AttrOperator;
import global.AttrType;
import global.ExcelParser;
import global.GlobalConst;
import global.IndexType;
import global.RID;
import global.TupleOrder;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import index.IndexScan;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.Iterator;
import iterator.RelSpec;
import iterator.TopRankJoin;
import iterator.TopSortMerge;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;

import btree.BTreeFile;
import btree.IntegerKey;

import btree.StringKey;

public class phase2 implements GlobalConst{

	private static boolean OK = true;
	private static boolean FAIL = false;

	public static void TopNestedLoopJointest(){

		Scanner scanner = new Scanner(System.in);
		boolean status = true;
		try{
			System.out.println("Enter name for Table1");
			String tableName1 = scanner.nextLine();
			System.out.println("Enter file location");
			String fileLoc1 = scanner.nextLine();

			//SOC
			HSSFWorkbook workBook = null; 
			File file  = new File(fileLoc1);
			InputStream excelDocumentStream = null;
			try 
			{
				excelDocumentStream = new FileInputStream(file);
				POIFSFileSystem fsPOI = new POIFSFileSystem(new BufferedInputStream(excelDocumentStream));
				workBook = new HSSFWorkbook(fsPOI);         
				ExcelParser parser = new ExcelParser(workBook.getSheetAt(0));
				String [] res;

				
				Heapfile topFile = new Heapfile(tableName1+".in");
				

				Tuple t = new Tuple();

				int numOfColms = scanner.nextInt();
				System.out.println("noOfColms ="+numOfColms);
				AttrType [] Stypes = new AttrType[numOfColms];
				int col=0;
				int numOfStringCol =1;
				while(col<numOfColms){
					System.out.println("Enter the datatype of column["+(col+1)+"]0-> String 1-> Integer 2->Real");
					int attrType = scanner.nextInt();
					switch(attrType){
					case 0: Stypes[col] = new AttrType(AttrType.attrString);
					numOfStringCol++;
					break;

					case 1: Stypes[col] = new AttrType(AttrType.attrInteger);
					break;

					case 2: Stypes[col] = new AttrType(AttrType.attrReal);
					break;

					default: System.out.println("Invalid option");
					System.exit(0);
					}
					col++;
				}

				short [] Ssizes = new short [numOfStringCol];
				Ssizes[0] = 30; 

				try {
					t.setHdr((short) numOfColms,Stypes, Ssizes);
				}
				catch (Exception e) {
					System.err.println("*** error in Tuple.setHdr() ***");
					status = false;
					e.printStackTrace();
				}

				RID rid = null;
				while ((res = parser.splitLine()) != null)
				{
					for (int i = 0; i < res.length; i++)
					{
						System.out.println("Data Found [" + res[i] + "]");
						AttrType attr= Stypes[i];
						int type = attr.attrType;
						switch(type){
						case AttrType.attrString: t.setStrFld(i+1, res[i]);
						break;
						case AttrType.attrInteger: t.setIntFld(i+1, Integer.parseInt(res[i]));
						break;
						case AttrType.attrReal: t.setFloFld(i+1, Float.parseFloat(res[i]));
						break;
						}

						rid = topFile.insertRecord(t.getTupleByteArray());
					}

				}
				excelDocumentStream.close();

			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			//EOC
			scanner.nextLine();
			System.out.println("Enter name for Table2");
			String tableName2 = scanner.nextLine();

			System.out.println("Enter file location");
			String fileLoc2 = scanner.nextLine();

			//SOC
			HSSFWorkbook workBook1 = null; 
			File file1  = new File(fileLoc2);
			InputStream excelDocumentStream1 = null;
			try 
			{
				excelDocumentStream1 = new FileInputStream(file1);
				POIFSFileSystem fsPOI = new POIFSFileSystem(new BufferedInputStream(excelDocumentStream1));
				workBook1 = new HSSFWorkbook(fsPOI);         
				ExcelParser parser = new ExcelParser(workBook1.getSheetAt(0));
				String [] res;

			
				Heapfile topFile = new Heapfile(tableName2+".in");

				Tuple t = new Tuple();

				int numOfColms = scanner.nextInt();
				System.out.println("noOfColms ="+numOfColms);
				AttrType [] Stypes = new AttrType[numOfColms];
				int col=0;
				int numOfStringCol =1;
				while(col<numOfColms){
					System.out.println("Enter the datatype of column["+(col+1)+"] 0-> String 1-> Integer 2->Real");
					int attrType = scanner.nextInt();
					switch(attrType){
					case 0: Stypes[col] = new AttrType(AttrType.attrString);
					numOfStringCol++;
					break;

					case 1: Stypes[col] = new AttrType(AttrType.attrInteger);
					break;

					case 2: Stypes[col] = new AttrType(AttrType.attrReal);
					break;

					default: System.out.println("Invalid option");
					System.exit(0);
					}
					col++;
				}

				short [] Ssizes = new short [numOfStringCol];
				Ssizes[0] = 30; 

				try {
					t.setHdr((short) numOfColms,Stypes, Ssizes);
				}
				catch (Exception e) {
					System.err.println("*** error in Tuple.setHdr() ***");
					status = false;
					e.printStackTrace();
				}

				RID rid = null;
				while ((res = parser.splitLine()) != null)
				{
					for (int i = 0; i < res.length; i++)
					{
						System.out.println("Token Found [" + res[i] + "]");
						AttrType attr= Stypes[i];
						int type = attr.attrType;
						switch(type){
						case AttrType.attrString: t.setStrFld(i+1, res[i]);
						break;
						case AttrType.attrInteger: t.setIntFld(i+1, Integer.parseInt(res[i]));
						break;
						case AttrType.attrReal: t.setFloFld(i+1, Float.parseFloat(res[i]));
						break;
						}

						rid = topFile.insertRecord(t.getTupleByteArray());
					}

				}
				excelDocumentStream.close();

			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			//EOC


			System.out.println("Enter total memory");
			long memory = scanner.nextLong();

			System.out.println("Enter the order - 0-ascending 1-descending");
			int order = scanner.nextInt();

			System.out.println("Enter the number of top tuples in the resulting relation");
			int topK = scanner.nextInt();

			scanner.nextLine();
			System.out.println("Enter the columns to be projected in comma separated form");
			String projectionList = scanner.nextLine();

			System.out.println("Enter the conditions list");
			String conditionsList = scanner.nextLine();
		}catch(Exception e){

		}finally{
			scanner.close();
		}

	}

	/**
	 * TopSortMerge
	 */

	public static void TopSortMergetest(){

		Scanner scanner = new Scanner(System.in);
		boolean status = true;
		try{
			//System.out.println("Enter number of tables to be joined");
			int numOfTables = 2;
			AttrType [][] attrTypeList = new AttrType[numOfTables][];
			int [] numOfColsList = new int[numOfTables];
			short [][] stringSizesList = new short[numOfTables][];
			int [] joinedColList = new int[numOfTables];
			int [] joinedColDataTypeList = new int[numOfTables]; 
			Iterator [] iteratorList = new Iterator[numOfTables];
			
			String [] tableNameList = new String[numOfTables];

			FldSpec[] projList = new FldSpec[20];

			FileScan [] amList = new FileScan[numOfTables];
		

			int projlistIndex = 0;  //Gives number of output attribute columns in the projection
			for(int k=0;k<numOfTables;k++)
			{
				boolean flag = true;
				System.out.println("Enter name for Table"+(k+1));
				String tableName = scanner.nextLine();
				tableNameList[k] = tableName;
				System.out.println("Enter file location");
				String fileLoc1 = scanner.nextLine();

				System.out.println("Enter the column number in the table to be joined");
				int joinColNumber = scanner.nextInt();

				joinedColList[k] = joinColNumber; 
				//SOC
				HSSFWorkbook workBook = null; 
				File file  = new File(fileLoc1);
				InputStream excelDocumentStream = null;
				try 
				{
					excelDocumentStream = new FileInputStream(file);
					POIFSFileSystem fsPOI = new POIFSFileSystem(new BufferedInputStream(excelDocumentStream));
					workBook = new HSSFWorkbook(fsPOI);         
					ExcelParser parser = new ExcelParser(workBook.getSheetAt(0));
					String [] res;

					Heapfile topFile = new Heapfile(tableName+".in");

					Tuple t = new Tuple();

					System.out.println("Enter the number of Columns in the table");
					int numOfColms = scanner.nextInt();

					System.out.println("noOfColms ="+numOfColms);
					numOfColsList[k] = numOfColms;

					AttrType [] Stypes = new AttrType[numOfColms];
					int col=0;
					int numOfStringCol =1;
					while(col<numOfColms){
						System.out.println("Enter the datatype of column["+(col+1)+"]\n 0-> String\n 1-> Integer\n 2->Real");
						int attrType = scanner.nextInt();
						switch(attrType){
						case 0: Stypes[col] = new AttrType(AttrType.attrString);
						numOfStringCol++;
						break;

						case 1: Stypes[col] = new AttrType(AttrType.attrInteger);
						break;

						case 2: Stypes[col] = new AttrType(AttrType.attrReal);
						break;

						default: System.out.println("Invalid option");
						System.exit(0);
						}

						col++;
					}

					attrTypeList[k] = Stypes;

					short [] Ssizes = new short [numOfStringCol];
					Ssizes[0] = 30; 

					stringSizesList[k] = Ssizes;

					switch(Stypes[joinColNumber-1].attrType){

					case 0: joinedColDataTypeList[k] = Ssizes[0];
					break;

					case 1: joinedColDataTypeList[k] = 4;
					break;

					case 2: joinedColDataTypeList[k] = 4;
					break;

					default: System.out.println("Invalid Attribute Data type");
					System.exit(0);
					}

					System.out.println("Data type of joined column ="+joinedColDataTypeList[k]);


					try {
						t.setHdr((short) numOfColms,Stypes, Ssizes);
					}
					catch (Exception e) {
						System.err.println("*** error in Tuple.setHdr() ***");
						status = false;
						e.printStackTrace();
					}

					RID rid = null;
					while ((res = parser.splitLine()) != null)
					{
						int i;
						for (i = 0; i < res.length; i++)
						{
							System.out.println("Token Found [" + res[i] + "]");
							AttrType attr= Stypes[i];
							int type = attr.attrType;
							System.out.println("type = "+type);
							switch(type){
							case AttrType.attrString: t.setStrFld(i+1, res[i]);
							break;
							case AttrType.attrInteger: t.setIntFld(i+1, Integer.parseInt(res[i]));
							break;
							case AttrType.attrReal: t.setFloFld(i+1, Float.parseFloat(res[i]));
							break;
							}
							System.out.println("i =" + i);
							System.out.println("res.length =" + res.length);
							if(i==res.length-1){
								System.out.println("Score =" + res[i]);
								t.setScore(Float.parseFloat(res[i]));
							}

						}
						rid = topFile.insertRecord(t.getTupleByteArray());

					}
					excelDocumentStream.close();

					//Create attribute/column list for each table
					FldSpec [] Sprojection = new FldSpec[numOfColms];
					for(int q=0;q<Sprojection.length;q++){

						Sprojection[q] = new FldSpec(new RelSpec(RelSpec.outer), q+1);
					}  

					FileScan am = null;

					try {
						am  = new FileScan(tableName+".in", Stypes, Ssizes, 
								(short)numOfColsList[k], (short)numOfColsList[k],
								Sprojection, null);

						Tuple t1;
						while((t1 =am.get_next())!=null){
							t1.print(attrTypeList[k]);
						}

						amList[k] = am;
					}
					catch (Exception e) {
						status = false;
						System.err.println (""+e);
					}

					if (status != true) {
						//bail out
						System.err.println ("*** Error setting up scan ");
						Runtime.getRuntime().exit(1);
					}


					RelSpec rel = null;
					//int projColTableSpecificOffset= 1;
					String moreColFlag;
					FldSpec[] projListSpecificToTable = new FldSpec[20];
					int projListSpecificToTableIndex=0;

					while(flag){                                        
						System.out.println("Enter the column number to be projected from this table");
						int colNum = scanner.nextInt();
						scanner.nextLine();
						//Set relation as outer for 1st table and as inner for 2nd table
						if(k==0){
							rel = new RelSpec(RelSpec.outer);
						}
						else{
							rel = new RelSpec(RelSpec.innerRel);
						}
						projList[projlistIndex] = new FldSpec(new RelSpec(RelSpec.outer), colNum);
						System.out.println("projlistIndex="+projlistIndex); 

						System.out.println("Are there more columns to be projected from this table (Y/N)?");
						moreColFlag = scanner.nextLine();

						if(moreColFlag.equalsIgnoreCase("Y"))
							flag=true;
						else if(moreColFlag.equalsIgnoreCase("N"))
							flag=false;
						projlistIndex++;
					}                           

				}
				catch(Exception e)
				{
					e.printStackTrace();
				}


				//EOC
				//scanner.nextLine();

			}
			System.out.println("projlistIndex ="+projlistIndex);
			System.out.println("Enter the number of conditions in the where clause ");

			int numCondExpr = scanner.nextInt();
			scanner.nextLine();
			String [] conditionString = new String[numCondExpr];
			for(int a=0; a<numCondExpr ; a++)
			{
				System.out.println("Enter the "+(a+1)+" condition in the following format\n" + 
						"<**TableNumber**>.<**ColumnNumber**> <**operator**> <**TableNumber**>.<**ColumnNumber/Constant**> ");
				conditionString[a] = scanner.nextLine();

			}
			for(int a=0; a<numCondExpr ; a++){

				System.out.println(conditionString[a]);
				if(a!=numCondExpr-1){
					System.out.println("AND");
				}
			}

			for(int i=0;i<attrTypeList.length;i++){
				AttrType[] attr = attrTypeList[i];
				for(int j=0;j<attr.length;j++){
					System.out.println("attr type ="+attr[j].attrType);
				}
			}

			CondExpr [] condExprList = new CondExpr[numCondExpr+1];
			System.out.println("expr.length ="+condExprList.length);

			for(int a=0;a<condExprList.length-1;a++){

				System.out.println("condString =");
				condExprList[a] = new CondExpr();
				String condString = conditionString[a];
				condExprList[a].next  = null;
				System.out.println("condString ="+condString);
				System.out.println("a ="+a);
				int beginIndex = 0;
				int endIndex = condString.indexOf(".", 0);
				System.out.println("endIndex ="+endIndex);
				//char tableNum=condString.charAt(index-1);
				String tableNumString = condString.substring(beginIndex, endIndex);
				System.out.println("tableNumString ="+tableNumString);
				int tableNum = Integer.parseInt(tableNumString);
				System.out.println("tableNum ="+tableNum);
				//String tableName = tableNameList[tableNum-1];
				int endIndex1 = condString.indexOf(" ",endIndex);
				System.out.println("endIndex1 ="+endIndex1);
				String colNumString = condString.substring(endIndex+1, endIndex1);
				System.out.println("colNumString ="+colNumString);
				int colNum = Integer.parseInt(colNumString);
				System.out.println("colNum ="+colNum);

				condExprList[a].type1 = new AttrType(AttrType.attrSymbol);

				condExprList[a].operand1.symbol = new FldSpec (new RelSpec(tableNum),colNum);

				int endIndex2 = condString.indexOf(" ",endIndex1+1);
				System.out.println("endIndex2 ="+endIndex2);
				String operator = condString.substring(endIndex1+1, endIndex2);
				System.out.println("operator = "+operator);
				switch(operator){
				case "=": condExprList[a].op = new AttrOperator(AttrOperator.aopEQ);
				break;

				case "<": condExprList[a].op = new AttrOperator(AttrOperator.aopLT);
				break;

				case ">": condExprList[a].op = new AttrOperator(AttrOperator.aopGT);
				break;

				case "!=": condExprList[a].op = new AttrOperator(AttrOperator.aopNE);
				break;

				case "<=": condExprList[a].op = new AttrOperator(AttrOperator.aopLE);
				break;

				case ">=": condExprList[a].op = new AttrOperator(AttrOperator.aopGE);
				break;
				}

				System.out.println("expr[a].op = "+condExprList[a].op);

				int endIndex3 = condString.indexOf(".",endIndex2);
				System.out.println("endIndex3 = "+endIndex3);
				if(endIndex3==-1){

					String constantString = condString.substring(endIndex2+1);
					System.out.println("constantString = "+constantString);
					constantString = constantString.trim();
					System.out.println("constantString after trim() = "+constantString);

					System.out.println("Enter the data type of the constant in the condition\n0-> String\n 1-> Integer\n 2->Real");
					int opt  = scanner.nextInt();

					switch(opt){
					case 0: condExprList[a].type2 = new AttrType(AttrType.attrString);
					condExprList[a].operand2.string = constantString;
					break;

					case 1: condExprList[a].type2 = new AttrType(AttrType.attrInteger);
					condExprList[a].operand2.integer = Integer.parseInt(constantString);
					break;

					case 2: condExprList[a].type2 = new AttrType(AttrType.attrReal);
					condExprList[a].operand2.real = Float.parseFloat(constantString);
					break;
					default: System.out.println("Invalid option");
					System.exit(0);
					}

					System.out.println("expr[a].type2 = "+condExprList[a].type2);                           

				}
				else{
					String tableNumString1 = condString.substring(endIndex2+1, endIndex3);
					System.out.println("tableNumString1 = "+tableNumString1);
					int tableNum1 = Integer.parseInt(tableNumString1);
					System.out.println("tableNum1 = "+tableNum1);
					//String tableName = tableNameList[tableNum-1];

					//int endIndex4 = condString.indexOf(" ",endIndex3);
					String colNumString1 = condString.substring(endIndex3+1, condString.length());
					int colNum1 = Integer.parseInt(colNumString1);
					System.out.println("colNum1 = "+colNum1);
					//attr = attrTypeList[tableNum1-1][colNum1-1];
					//expr[a].type2 = new AttrType(attr.attrType);
					condExprList[a].type2 = new AttrType(AttrType.attrSymbol);
					System.out.println("expr[a].type2 = "+condExprList[a].type2);
					condExprList[a].operand2.symbol = new FldSpec (new RelSpec(tableNum1),colNum1);
					System.out.println("expr[a].operand2.symbol = "+condExprList[a].operand2.symbol);
				}
			}

			condExprList[condExprList.length-1] = null;

			System.out.println("Enter total memory");
			int memory = scanner.nextInt();

			System.out.println("Enter the number of top K tuples required in the resulting relation");
			int topK = scanner.nextInt();

			scanner.nextLine();

			TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
			TopSortMerge sm = null;
			try {
				sm = new TopSortMerge(attrTypeList[0], numOfColsList[0], stringSizesList[0],
						attrTypeList[1], numOfColsList[0], stringSizesList[1],
						joinedColList[0], 4, 
						joinedColList[1], 4, 
						memory,
						amList[0], amList[1], 
						false, false, ascending,
						condExprList, projList, projlistIndex, topK);
			}
			catch (Exception e) {
				System.err.println("*** join error in SortMerge constructor ***"); 
				status = false;
				System.err.println (""+e);
				e.printStackTrace();
			}
			if (status != true) {
				//bail out
				System.err.println ("*** Error constructing SortMerge");
				Runtime.getRuntime().exit(1);
			}

			QueryCheck qcheck1 = new QueryCheck(1);
			Tuple t = null;

			try {
				while ((t = sm.get_next()) != null) {
		

					qcheck1.Check(t);
				}
				sm.get_topRanked();
			}
			catch (Exception e) {
				System.err.println (""+e);
				e.printStackTrace();
				status = FAIL;
			}
			if (status != OK) {
				//bail out
				System.err.println ("*** Error in get next tuple ");
				Runtime.getRuntime().exit(1);
			}

			qcheck1.report(1);
			try {
				sm.close();
			}
			catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}
			System.out.println ("\n"); 
			if (status != OK) {
				//bail out
				System.err.println ("*** Error in closing ");
				Runtime.getRuntime().exit(1);
			}


		}catch(Exception e){

			e.printStackTrace();

		}finally{
			scanner.close();
		}
		System.out.println("Exit processTopSortMerge()\n");

	}

	


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		JoinsDriver jjoin = new JoinsDriver();

		System.out.println("DBI Phase II");

		System.out.println("Enter the operation to be performed\n 1 -> TopNestedLoopJoin \n 2 -> TopSortMerge\n 3 -> TopRankJoin\n 0 -> Exit ");
		Scanner scanner = new Scanner(System.in);

		try{

			int option = scanner.nextInt();

			switch(option){

			case 1:  TopNestedLoopJointest();
			break;

			case 2: TopSortMergetest();
			break;

	
			}



		}catch(Exception e){

		}finally{
			scanner.close();
		}

	}



}
