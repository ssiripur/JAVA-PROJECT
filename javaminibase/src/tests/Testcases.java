package tests;


import global.AttrOperator;
import global.AttrType;
import global.ExcelParser;
import global.GlobalConst;
import global.IndexType;
import global.RID;
import global.SystemDefs;
import global.TupleOrder;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import index.IndexScan;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.Iterator;
import iterator.NestedLoopJoinTop;
import iterator.RelSpec;
import iterator.Sort;
//import iterator.TopJoinsRank;
import iterator.TopRankJoin;
import iterator.TopSortMerge;
import iterator.TopSortMergeExcl;
import iterator.TopjoinsRank;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Scanner;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;

import btree.BTreeFile;
import btree.IntegerKey;
//import btree.RealKey;
import btree.StringKey;

public class Testcases implements GlobalConst{

        private static boolean OK = true;
        private static boolean FAIL = false;
        SystemDefs sysdef;
        public Testcases() {
            
            //build Sailor, Boats, Reserves table
                    
            String dbpath = System.getProperty("user.name")+".minibase.jointestdb"; 
            String logpath = System.getProperty("user.name")+".joinlog";

            String remove_cmd = "/bin/rm -rf ";
            String remove_logcmd = remove_cmd + logpath;
            String remove_dbcmd = remove_cmd + dbpath;
            String remove_joincmd = remove_cmd + dbpath;

            try {
              Runtime.getRuntime().exec(remove_logcmd);
              Runtime.getRuntime().exec(remove_dbcmd);
              Runtime.getRuntime().exec(remove_joincmd);
            }
            catch (IOException e) {
              System.err.println (""+e);
            }

           
            /*
            ExtendedSystemDefs extSysDef = 
              new ExtendedSystemDefs( "/tmp/minibase.jointestdb", "/tmp/joinlog",
                                      1000,500,200,"Clock");
            */

            sysdef = new SystemDefs( dbpath, 1000, NUMBUF, "Clock" );
            
        }

        public static void processTopNestedLoopJoin(){
                
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
                        AttrType [] indexAttrType = new AttrType[numOfTables];
                        String [] indexNameList = new String[numOfTables];
                        String [] tableNameList = new String[numOfTables];
                        //ArrayList colNameList = new ArrayList();
                        //scanner.nextLine();
                      
                        FldSpec[] projList = new FldSpec[20];
                        
                        //Stores the attribute type of the output tuple
                        AttrType [] JJtype = new AttrType[20];  
                    FileScan [] amList = new FileScan[numOfTables];
                    //projlist[1] = new FldSpec(rel, 2);
                        
                        int projlistIndex = 0;  //Gives number of output attribute columns in the projection
                        // Build Index first
                    IndexType b_index = new IndexType (IndexType.B_Index);
                    Iterator am = null;
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
                                        int numOfColms = scanner.nextInt();/*parser.getNumberOfColms()*/;
                                        
                                        //System.out.println("noOfColms ="+numOfColms);
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
                                        
                                        //Saves the attribute type of the column to be joined which indicates the index type
                                        if(col==joinColNumber-1)
                                                indexAttrType[k] = Stypes[col];
                                        col++;
                                        }
                                        
                                        attrTypeList[k] = Stypes;
                                        
                                        short [] Ssizes = new short [numOfStringCol];
                                        for(int e=0;e<numOfStringCol;e++)
                                                Ssizes[e] = 30; 
                                        
                                        stringSizesList[k] = Ssizes;
                                        
                                        //Set length for the sort field i.e the field to be joined
                                        
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
                                        
                                        //System.out.println("Data type of joined column ="+joinedColDataTypeList[k]);
                                        
                                        
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
                                                for (i = 0; i < res.length-1; i++)
                                                {
                                                        //System.out.println("Token Found [" + res[i] + "]");
                                                        AttrType attr= Stypes[i];
                                                        int type = attr.attrType;
                                                        //System.out.println("type = "+type);
                                                        switch(type){
                                                                case AttrType.attrString: t.setStrFld(i+1, res[i]);
                                                                        break;
                                                                case AttrType.attrInteger: t.setIntFld(i+1,(int)(Float.parseFloat(res[i])));
                                                                        
                                                                        break;
                                                                case AttrType.attrReal: t.setFloFld(i+1, Float.parseFloat(res[i]));
                                                                        break;
                                                        }
                                                                                                                                                                        
                                                }
                                                
                                                if(i==res.length-1){
                                                        //System.out.println("Score =" + res[i]);
                                                        t.setScore(Float.parseFloat(res[i]));
                                                }
                                                //t.print(Stypes);
                                                rid = topFile.insertRecord(t.getTupleByteArray());
                                
                                        }
                                        excelDocumentStream.close();
                                        
                                        // create an scan on the heapfile
                                    Scan scan = null;
                                    
                                    try {
                                      scan = new Scan(topFile);
                                    }
                                    catch (Exception e) {
                                      status = false;
                                      e.printStackTrace();
                                      Runtime.getRuntime().exit(1);
                                    }

                                    // create the index file only for 1st relation
                                    if(k==0){
                                            BTreeFile btf = null;
                                            try {
                                              btf = new BTreeFile(tableName+"_BTreeIndex", indexAttrType[k].attrType, GlobalConst.INDEX_REC_LEN, 1/*delete*/); 
                                              indexNameList[k] = tableName+"_BTreeIndex";
                                            }
                                            catch (Exception e) {
                                              status = false;
                                              e.printStackTrace();
                                              Runtime.getRuntime().exit(1);
                                            }
        
                                            System.out.println("BTreeIndex created successfully.\n"); 
                                            
                                            RID rid1 = new RID();
                                            //String key = null;
                                            Tuple temp = null;
                                            
                                            try {
                                              temp = scan.getNext(rid1);
                                              //temp.print(Stypes);
                                            }
                                            catch (Exception e) {
                                              status = false;
                                              e.printStackTrace();
                                            }
                                            while ( temp != null) {
                                              t.tupleCopy(temp);
                                              
                                              try {
                                                  //System.out.println("indexAttrType[k].attrType ="+indexAttrType[k].attrType);
                                                  //System.out.println("t.getIntFld ="+t.getIntFld(joinColNumber));
                                                  switch(indexAttrType[k].attrType){
                                                                case AttrType.attrString: String keyString = t.getStrFld(joinColNumber);
                                                                                                                  btf.insert(new StringKey(keyString), rid1); 
                                                                                                                        break;
                                                                case AttrType.attrInteger: int keyInt = t.getIntFld(joinColNumber);
                                                                                                                        btf.insert(new IntegerKey(keyInt), rid1); 
                                                                                                                        break;
                                                               // case AttrType.attrReal: float keyFloat = t.getFloFld(joinColNumber);
                                                                                                                //btf.insert(new RealKey(keyFloat), rid1); 
                                                                                                                       // break;
                                                        }
                                                                                                                                        
                                              }
                                              catch (Exception e) {
                                                  status = false;
                                                  e.printStackTrace();
                                              }
                                             try {
                                                 temp = scan.getNext(rid1);
                                              }
                                              catch (Exception e) {
                                                  status = false;
                                                  e.printStackTrace();
                                              }
                                            }
                                            
                                            // close the file scan
                                            scan.closescan();
                                            
                                            System.out.println("BTreeIndex file created successfully.\n"); 
                                            
                                            
                                                //amList[k] = am;
                                            
                                            //Create attribute/column list for 1st table
                                                FldSpec [] Sprojection = new FldSpec[numOfColms];
                                                for(int q=0;q<Sprojection.length;q++){
                                                
                                                        Sprojection[q] = new FldSpec(new RelSpec(RelSpec.outer), q+1);
                                                }  
                                            
                                                try {
                                                        am = new IndexScan(b_index, tableNameList[0]+".in",indexNameList[0], attrTypeList[0],
                                                                        stringSizesList[0], (short)numOfColsList[0], (short)numOfColsList[0], Sprojection, null, joinedColList[0], false);
                                                        
                                                        /*Tuple t1;
                                                        while((t1=am.get_next())!=null){
                                                                t1.print(attrTypeList[0]);
                                                        }*/
                                                }

                                                catch (Exception e) {
                                                        System.err.println("*** Error creating scan for Index scan");
                                                        System.err.println("" + e);
                                                        Runtime.getRuntime().exit(1);
                                                }

                                    }
                                        
                                                                                    
                                    RelSpec rel = null;
                                    //int projColTableSpecificOffset= 1;
                                    String moreColFlag;
                                    //FldSpec[] projListSpecificToTable = new FldSpec[20];
                                                            
                                                             
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
                                                projList[projlistIndex] = new FldSpec(rel, colNum);
                                                //System.out.println("projlistIndex="+projlistIndex); 
                                                
                                                JJtype[projlistIndex] = Stypes[colNum-1];
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
                //System.out.println("projlistIndex ="+projlistIndex);
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
                
                /*for(int i=0;i<attrTypeList.length;i++){
                        AttrType[] attr = attrTypeList[i];
                        for(int j=0;j<attr.length;j++){
                                System.out.println("attr type ="+attr[j].attrType);
                        }
                }*/
                
                CondExpr [] condExprList = new CondExpr[numCondExpr+1];
                //System.out.println("expr.length ="+condExprList.length);
                
                for(int a=0;a<condExprList.length-1;a++){
                        
                                //System.out.println("condString =");
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
                                //System.out.println("tableNum ="+tableNum);
                                //String tableName = tableNameList[tableNum-1];
                                int endIndex1 = condString.indexOf(" ",endIndex);
                                System.out.println("endIndex1 ="+endIndex1);
                                String colNumString = condString.substring(endIndex+1, endIndex1);
                                System.out.println("colNumString ="+colNumString);
                                int colNum = Integer.parseInt(colNumString);
                                //System.out.println("colNum ="+colNum);
                                
                                condExprList[a].type1 = new AttrType(AttrType.attrSymbol);
                                
                                //Set relation as outer for 1st table and as inner for 2nd table
                                RelSpec rel1 = null;
                                if(tableNum==0){
                                        rel1 = new RelSpec(RelSpec.outer);
                                }
                                else{
                                        rel1 = new RelSpec(RelSpec.innerRel);
                                }
                                                                
                                condExprList[a].operand1.symbol = new FldSpec (rel1,colNum);
                                                                
                                int endIndex2 = condString.indexOf(" ",endIndex1+1);
                                //System.out.println("endIndex2 ="+endIndex2);
                                String operator = condString.substring(endIndex1+1, endIndex2);
                                //System.out.println("operator = "+operator);
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
                                
                                //System.out.println("expr[a].op = "+condExprList[a].op);
                                        
                                int endIndex3 = condString.indexOf(".",endIndex2);
                                //System.out.println("endIndex3 = "+endIndex3);
                                if(endIndex3==-1){
                                        
                                        String constantString = condString.substring(endIndex2+1);
                                        //System.out.println("constantString = "+constantString);
                                        constantString = constantString.trim();
                                        //System.out.println("constantString after trim() = "+constantString);
                                        
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
                                        
                                        //System.out.println("expr[a].type2 = "+condExprList[a].type2);                         
                                        
                                }
                                else{
                                        String tableNumString1 = condString.substring(endIndex2+1, endIndex3);
                                        //System.out.println("tableNumString1 = "+tableNumString1);
                                        int tableNum1 = Integer.parseInt(tableNumString1);
                                        //System.out.println("tableNum1 = "+tableNum1);
                                        //String tableName = tableNameList[tableNum-1];
                                        
                                        //int endIndex4 = condString.indexOf(" ",endIndex3);
                                        String colNumString1 = condString.substring(endIndex3+1, condString.length());
                                        int colNum1 = Integer.parseInt(colNumString1);
                                        //System.out.println("colNum1 = "+colNum1);
                                        //attr = attrTypeList[tableNum1-1][colNum1-1];
                                        //expr[a].type2 = new AttrType(attr.attrType);
                                        condExprList[a].type2 = new AttrType(AttrType.attrSymbol);
                                        //System.out.println("expr[a].type2 = "+condExprList[a].type2);
                                        
                                        RelSpec rel2 = null;
                                        if(tableNum1==0){
                                                rel2 = new RelSpec(RelSpec.outer);
                                        }
                                        else{
                                                rel2 = new RelSpec(RelSpec.innerRel);
                                        }
                                                                        
                                        condExprList[a].operand2.symbol = new FldSpec (rel2,colNum1);
                                        //System.out.println("expr[a].operand2.symbol = "+condExprList[a].operand2.symbol);
                                }
                }
                
                condExprList[condExprList.length-1] = null;
                
                System.out.println("Enter total memory(<=10)");
                int memory = scanner.nextInt();
                
                System.out.println("Enter the number of top K tuples required in the resulting relation");
                int topK = scanner.nextInt();
                
                scanner.nextLine();
                
                //***********TopNestedLoop Start**********************//
            
                System.out.println("+++ TOP NESTED LOOP JOIN BEGINS");

                NestedLoopJoinTop nlj = null;
               
                
                try {
                        nlj = new NestedLoopJoinTop(attrTypeList[0], numOfColsList[0], stringSizesList[0], attrTypeList[1], numOfColsList[1],
                                        stringSizesList[1], memory, am, tableNameList[1]+".in", condExprList, null, projList, projlistIndex, topK);
                } catch (Exception e) {
                        System.err.println("*** Error preparing for nested_loop_join");
                        System.err.println("" + e);
                        e.printStackTrace();
                        Runtime.getRuntime().exit(1);																																																																																																																																																																																																																																																																																																																										
                }
                

        		
                System.out.println("+++ TOP NESTED LOOP JOIN ENDS");
                //***********TopNestedLoop END**********************//

                
//                
//                TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
//                Sort sort_names = null;
//                try {
//                        int jjSize = 4;
//                        switch(JJtype[0].attrType){
//                        
//                                case AttrType.attrString: jjSize=30;
//                                        break;
//                                case AttrType.attrInteger: jjSize=4;
//                                        break;
//                                case AttrType.attrReal: jjSize=4;
//                                        break;
//                        }
//                        sort_names = new Sort(JJtype, (short) projlistIndex, stringSizesList[0],(iterator.Iterator) nlj, 1, ascending, jjSize, memory);
//                        
//                } catch (Exception e) {
//                        System.err.println("*** Error preparing for nested_loop_join");
//                        System.err.println("" + e);
//                        Runtime.getRuntime().exit(1);
//                        
//                }
//                
//            if (status != true) {
//                        //bail out
//                        System.err.println ("*** Error nested_loop_join");
//                        Runtime.getRuntime().exit(1);
//                }
//            
//            
//            try {
//                        
//                        Tuple t = null;
//                        while ((t = sort_names.get_next()) != null) {
//                                //t.print(JJtype);
//                                //System.out.println("SCORE IS " + t.getScore());
//                                // qcheck2.Check(t);
//                        }
//                 nlj.get_topK();
//                } catch (Exception e) {
//                        e.printStackTrace();
//                        Runtime.getRuntime().exit(1);
//                }
//
//                // qcheck2.report(2);
//
//                System.out.println("\n");
//                try {
//                        sort_names.close();
//                        System.out.println("\n");
//                } catch (Exception e) {
//                        status = FAIL;
//                        e.printStackTrace();
                
//                }
                
                Tuple p =null;
        		try{
        		    
        			while((p=nlj.get_TopRanked())!=null)
        			{
        		p.print(JJtype);
        		System.out.println(p.getScore());
        			}
        		}catch(Exception e){
                    
                    e.printStackTrace();
        		
        		}
                if (status != OK) {
                        // bail out

                        Runtime.getRuntime().exit(1);
                }

                                     
                
                        
                }finally{
                        scanner.close();
                }
                System.out.println("Exit processTopNestedLoopsJoins()\n");

                
        }
        
        /**
         * TopSortMerge
         */
        
        public static long processTopSortMerge(){
                
                
                long totalTime = 0;
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
                        //AttrType [] indexAttrType = new AttrType[numOfTables];
                        //String [] indexNameList = new String[numOfTables];
                        String [] tableNameList = new String[numOfTables];
                        //ArrayList colNameList = new ArrayList();
                        //scanner.nextLine();
                        
                        FldSpec[] projList = new FldSpec[20];
                        ArrayList<FldSpec> list = new ArrayList<FldSpec>();
                    
                    FileScan [] amList = new FileScan[numOfTables];
                    //projlist[1] = new FldSpec(rel, 2);
                        
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
                                        int numOfColms = scanner.nextInt();/*parser.getNumberOfColms()*/;
                                        
                                        //System.out.println("noOfColms ="+numOfColms);
                                        numOfColsList[k] = numOfColms;
                                        
                                        AttrType [] Stypes = new AttrType[numOfColms];
                                        int col=0;
                                        int numOfStringCol =0;
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
                                        for(int e=0;e<numOfStringCol;e++)
                                        Ssizes[e] = 32; 
                                        
                                        stringSizesList[k] = Ssizes;
                                        
                                        //Set length for the sort field i.e the field to be joined
                                        
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
                                        
                                        //System.out.println("Data type of joined column ="+joinedColDataTypeList[k]);
                                        
                                        
                                        try {
                                                t.setHdr((short)(numOfColms),Stypes, Ssizes);
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
                                                for (i = 0; i < res.length-1; i++)
                                                {
                                                        //System.out.println("Token Found [" + res[i] + "]");
                                                        AttrType attr= Stypes[i];
                                                        int type = attr.attrType;
                                                        //System.out.println("type = "+type);
                                                        switch(type){
                                                                case AttrType.attrString: t.setStrFld(i+1, res[i]);
                                                                //System.out.println("t.getStrFld(i+1)="+t.getStrFld(i+1));
                                                                        break;
                                                                case AttrType.attrInteger: t.setIntFld(i+1, (int)Float.parseFloat(res[i]));
                                                                        //System.out.println("t.getIntFld(i+1)="+t.getIntFld(i+1));
                                                                        break;
                                                                case AttrType.attrReal: t.setFloFld(i+1, Float.parseFloat(res[i]));
                                                                        break;
                                                        }
                                                        //System.out.println("i =" + i);
                                                        //System.out.println("res.length =" + res.length);
                                                        //System.out.println("t.data = "+t.data);
                                                                                                                                                                                
                                                }
                                                if(i==res.length-1){
                                                    //System.out.println("Score =" + res[i]);
                                                    t.setScore(Float.parseFloat(res[i]));
                                                    t.setFloFld(i+1, Float.parseFloat(res[i]));
                                                }
//                                                if(i==res.length-1){
//                                                        //System.out.println("Score =" + res[i]);
//                                                        t.setScore(Float.parseFloat(res[i]));
//                                                }
                                                //System.out.println("t.data = "+t.data);
                                                //t.print(Stypes);
                                                rid = topFile.insertRecord(t.getTupleByteArray());
                                                //t.print(Stypes);
                                
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
                                      
                                      /*Tuple t1;
                                                while((t1 =am.get_next())!=null){
                                                        t1.print(attrTypeList[k]);
                                                }*/
                                                
                                      amList[k] = am;
                                                                      
                                                
                                    }
                                    catch (Exception e) {
                                      status = false;
                                      System.err.println (""+e);
                                    }

                                    if (status != true) {
                                      //bail out
                                      System.err.println ("*** Error setting up scan for sailors");
                                      Runtime.getRuntime().exit(1);
                                    }

                                            
                                    RelSpec rel = null;
                                    //int projColTableSpecificOffset= 1;
                                    String moreColFlag;
                                    //FldSpec[] projListSpecificToTable = new FldSpec[20];
                                    //int projListSpecificToTableIndex=0;
                                                               
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
                                                projList[projlistIndex] = new FldSpec(rel, colNum);
                                   
                                                                             
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
                //System.out.println("projlistIndex ="+projlistIndex);
                System.out.println("Enter the number of conditions in the where clause ");
                                
                int numCondExpr = scanner.nextInt();
                scanner.nextLine();
                String [] conditionString = new String[numCondExpr];
                for(int a=0; a<numCondExpr ; a++)
                {
                        System.out.println("Enter the "+(a+1)+" condition in the following format\n" + 
                                        "<**TableNumber**>.<**ColumnNumber**> <**operator**> <**TableNumber**>.<**ColumnNumber**>/<**Constant**> ");
                        conditionString[a] = scanner.nextLine();
                                
                }
                for(int a=0; a<numCondExpr ; a++){
                                
                        System.out.println(conditionString[a]);
                        if(a!=numCondExpr-1){
                                System.out.println("AND");
                        }
                }
                
                /*for(int i=0;i<attrTypeList.length;i++){
                        AttrType[] attr = attrTypeList[i];
                        for(int j=0;j<attr.length;j++){
                                System.out.println("attr type ="+attr[j].attrType);
                        }
                }*/
                
                CondExpr [] condExprList = new CondExpr[numCondExpr+1];
                //System.out.println("expr.length ="+condExprList.length);
                
                for(int a=0;a<condExprList.length-1;a++){
                        
                                //System.out.println("condString =");
                                condExprList[a] = new CondExpr();
                                String condString = conditionString[a];
                                condExprList[a].next  = null;
                                //System.out.println("condString ="+condString);
                                //System.out.println("a ="+a);
                                int beginIndex = 0;
                                int endIndex = condString.indexOf(".", 0);
                                //System.out.println("endIndex ="+endIndex);
                                //char tableNum=condString.charAt(index-1);
                                String tableNumString = condString.substring(beginIndex, endIndex);
                                //System.out.println("tableNumString ="+tableNumString);
                                int tableNum = Integer.parseInt(tableNumString);
                                //System.out.println("tableNum ="+tableNum);
                                //String tableName = tableNameList[tableNum-1];
                                int endIndex1 = condString.indexOf(" ",endIndex);
                                //System.out.println("endIndex1 ="+endIndex1);
                                String colNumString = condString.substring(endIndex+1, endIndex1);
                                //System.out.println("colNumString ="+colNumString);
                                int colNum = Integer.parseInt(colNumString);
                                //System.out.println("colNum ="+colNum);
                                
                                condExprList[a].type1 = new AttrType(AttrType.attrSymbol);
                                
                                //Set relation as outer for 1st table and as inner for 2nd table
                                RelSpec rel1 = null;
                                if(tableNum==0){
                                        rel1 = new RelSpec(RelSpec.outer);
                                }
                                else{
                                        rel1 = new RelSpec(RelSpec.innerRel);
                                }
                                                                
                                condExprList[a].operand1.symbol = new FldSpec (rel1,colNum);
                                                                
                                int endIndex2 = condString.indexOf(" ",endIndex1+1);
                                //System.out.println("endIndex2 ="+endIndex2);
                                String operator = condString.substring(endIndex1+1, endIndex2);
                                //System.out.println("operator = "+operator);
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
                                
                                //System.out.println("expr[a].op = "+condExprList[a].op);
                                        
                                int endIndex3 = condString.indexOf(".",endIndex2);
                                //System.out.println("endIndex3 = "+endIndex3);
                                if(endIndex3==-1){
                                        
                                        String constantString = condString.substring(endIndex2+1);
                                        //System.out.println("constantString = "+constantString);
                                        constantString = constantString.trim();
                                        //System.out.println("constantString after trim() = "+constantString);
                                        
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
                                        
                                        //System.out.println("expr[a].type2 = "+condExprList[a].type2);                         
                                        
                                }
                                else{
                                        String tableNumString1 = condString.substring(endIndex2+1, endIndex3);
                                        //System.out.println("tableNumString1 = "+tableNumString1);
                                        int tableNum1 = Integer.parseInt(tableNumString1);
                                        //System.out.println("tableNum1 = "+tableNum1);
                                        //String tableName = tableNameList[tableNum-1];
                                        
                                        //int endIndex4 = condString.indexOf(" ",endIndex3);
                                        String colNumString1 = condString.substring(endIndex3+1, condString.length());
                                        int colNum1 = Integer.parseInt(colNumString1);
                                        //System.out.println("colNum1 = "+colNum1);
                                        //attr = attrTypeList[tableNum1-1][colNum1-1];
                                        //expr[a].type2 = new AttrType(attr.attrType);
                                        condExprList[a].type2 = new AttrType(AttrType.attrSymbol);
                                        //System.out.println("expr[a].type2 = "+condExprList[a].type2);
                                        
                                        RelSpec rel2 = null;
                                        if(tableNum1==0){
                                                rel2 = new RelSpec(RelSpec.outer);
                                        }
                                        else{
                                                rel2 = new RelSpec(RelSpec.innerRel);
                                        }
                                                                        
                                        condExprList[a].operand2.symbol = new FldSpec (rel2,colNum1);
                                        //System.out.println("expr[a].operand2.symbol = "+condExprList[a].operand2.symbol);
                                }
                }
                
                condExprList[condExprList.length-1] = null;
                
                System.out.println("Enter total memory");
                int memory = scanner.nextInt();
                
                System.out.println("Enter the number of top K tuples required in the resulting relation");
                int topK = scanner.nextInt();
                
                scanner.nextLine();
                Tuple t=new Tuple();
                
                TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
            TopSortMergeExcl sm = null;
            /*for(int h=0;h<projlistIndex;h++){
                System.out.println("projList[h].offset="+projList[h].offset);
                System.out.println("projList[h].relation.key="+projList[h].relation.key);
                
            }*/
            try {
//            	
//            System.out.println(attrTypeList[0]);
//            System.out.println(numOfColsList[0]);
//            System.out.println(numOfColsList[1]);
////    		t=amList[0].get_next();
//            	while(amList[0]!=null)
//            	{
//            		
//            		t.print(attrTypeList[0]);
//            		t=amList[0].get_next();
//            		
//            	}
                       long start = System.currentTimeMillis();
                      sm = new TopSortMergeExcl(attrTypeList[0], numOfColsList[0], stringSizesList[0],
                                  attrTypeList[1], numOfColsList[1], stringSizesList[1],
                                         joinedColList[0], 4, 
                                         joinedColList[1], 4, 
                                         memory,
                                         amList[0], amList[1], 
                                         false, false, ascending,
                                         condExprList, projList, projlistIndex, topK);
                        long finish = System.currentTimeMillis();
                        totalTime = finish - start;
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
                t = null;
                 
                try {
                  while ((t = sm.get_TopRanked()) != null) {
                    //t.print(jtype);
                    
                    qcheck1.Check(t);
                  }
                //  sm.get_topK();
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
                return totalTime;
        }

        /**
         * TopRankJoin
         */
        public static void processTopRankJoin(){
                
                Scanner scanner = new Scanner(System.in);
                boolean status = true;
                try{
                        System.out.println("Enter number of tables to be joined");
                        int numOfTables = scanner.nextInt();
                        AttrType [][] attrTypeList = new AttrType[numOfTables][];
                        int [] numOfColsList = new int[numOfTables];
                        short [][] stringSizesList = new short[numOfTables][];
                        int [] joinedColList = new int[numOfTables]; 
                        Iterator [] iteratorList = new Iterator[numOfTables];
                        AttrType [] indexAttrType = new AttrType[numOfTables];
                        String [] indexNameList = new String[numOfTables];
                        IndexType[] b_index = new IndexType[numOfTables];
                    String[] fileNames = new String[numOfTables];
                        String [] tableNameList = new String[numOfTables];
                        ArrayList<FldSpec> list = new ArrayList<FldSpec>();
                        FldSpec[] newProjList = null;
                        //ArrayList colNameList = new ArrayList();
                        scanner.nextLine();
                        
                        FldSpec[] projList = new FldSpec[20];
                        FldSpec[][] projTableSpecificList = new FldSpec[numOfTables][20];
                    RelSpec rel = null;
                    //projlist[1] = new FldSpec(rel, 2);
                        
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
                                
                                joinedColList[k] = joinColNumber-1; 
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
                                        int numOfColms = scanner.nextInt();/*parser.getNumberOfColms()*/;
                                        
                                        //System.out.println("noOfColms ="+numOfColms);
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
                                                
                                        //Saves the attribute type of the column to be joined which indicates the index type
                                        if(col==joinColNumber-1)
                                                indexAttrType[k] = Stypes[col];
                                        
                                        col++;
                                        }
                                        
                                        attrTypeList[k] = Stypes;
                                        
                                        short [] Ssizes = new short [numOfStringCol];
                                        for(int e=0;e<numOfStringCol;e++)
                                                Ssizes[e] = 30;                                         
                                        stringSizesList[k] = Ssizes;
                                        
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
                                                for (i = 0; i < res.length-1; i++)
                                                {
                                                        //System.out.println("Token Found [" + res[i] + "]");
                                                        AttrType attr= Stypes[i];
                                                        int type = attr.attrType;
                                                        //System.out.println("type = "+type);
                                                        switch(type){
                                                                case AttrType.attrString: t.setStrFld(i+1, res[i]);
                                                                        break;
                                                                case AttrType.attrInteger: t.setIntFld(i+1, (int)(Float.parseFloat(res[i])));
                                                                        break;
                                                                case AttrType.attrReal: t.setFloFld(i+1, Float.parseFloat(res[i]));
                                                                        break;
                                                        }
                                                
                                           
                                                        
                                                }
                                                
                                                if(i==res.length-1){
                                                        //System.out.println("Score =" + res[i]);
                                                        t.setScore(Float.parseFloat(res[i]));
                                                        t.setFloFld(i+1, Float.parseFloat(res[i]));
                                                }
                                                //t.print(Stypes);
                                                rid = topFile.insertRecord(t.getTupleByteArray());
                                
                                        }
                                        excelDocumentStream.close();
                       
                       
                                        // create an scan on the heapfile
                                    Scan scan = null;
                                    
                                    try {
                                      scan = new Scan(topFile);
                                    }
                                    catch (Exception e) {
                                      status = false;
                                      e.printStackTrace();
                                      Runtime.getRuntime().exit(1);
                                    }

                                    // create the index file
                                    BTreeFile btf = null;
                                    try {
                                      btf = new BTreeFile(tableName+"_BTreeIndex", indexAttrType[k].attrType, GlobalConst.INDEX_REC_LEN, 1/*delete*/); 
                                      indexNameList[k] = tableName+"_BTreeIndex";
                                      b_index[k] = new IndexType(IndexType.B_Index);
                                      fileNames[k] = tableNameList[k]+".in";
                                    }
                                    catch (Exception e) {
                                      status = false;
                                      e.printStackTrace();
                                      Runtime.getRuntime().exit(1);
                                    }

                                    System.out.println("BTreeIndex created successfully.\n"); 
                                    
                                    RID rid1 = new RID();
                                    //String key = null;
                                    Tuple temp = null;
                                    
                                    try {
                                      temp = scan.getNext(rid1);
                                    }
                                    catch (Exception e) {
                                      status = false;
                                      e.printStackTrace();
                                    }
                                    while ( temp != null) {
                                      t.tupleCopy(temp);
                                      
                                      try {
                                          //System.out.println("indexAttrType[k].attrType ="+indexAttrType[k].attrType);
                                          //System.out.println("t.getIntFld ="+t.getIntFld(joinColNumber));
                                          switch(indexAttrType[k].attrType){
                                                        case AttrType.attrString: String keyString = t.getStrFld(joinColNumber);
                                                                                                          btf.insert(new StringKey(keyString), rid1); 
                                                                                                                break;
                                                        case AttrType.attrInteger: int keyInt = t.getIntFld(joinColNumber);
                                                                                                                btf.insert(new IntegerKey(keyInt), rid1); 
                                                                                                                break;
                                                       // case AttrType.attrReal: float keyFloat = t.getFloFld(joinColNumber);
                                                                                                        //btf.insert(new RealKey(keyFloat), rid1); 
                                                                                                              //  break;
                                                }
                                                                                                                                
                                      }
                                      catch (Exception e) {
                                          status = false;
                                          e.printStackTrace();
                                      }
                                     try {
                                         temp = scan.getNext(rid1);
                                      }
                                      catch (Exception e) {
                                          status = false;
                                          e.printStackTrace();
                                      }
                                    }
                                    
                                    // close the file scan
                                    scan.closescan();
                                    
                                    System.out.println("BTreeIndex file created successfully.\n"); 
                                    
                                    rel = new RelSpec(k);
                                    int projColTableSpecificOffset= 0;
                                    String moreColFlag;
                                    int [] colNumList = new int[20];
                                    //int count=1;
                                    while(flag){
                                        
                                        System.out.println("Enter the column number to be projected from this table");
                                                int colNum = scanner.nextInt();
                                                scanner.nextLine();
//                                              /System.out.println("projlistIndex="+projlistIndex); 
                                                FldSpec fld = new FldSpec(rel, colNum);
                                                list.add(fld);
                                                projList[projlistIndex] = new FldSpec(rel, colNum);
                                                colNumList[projColTableSpecificOffset]=colNum;
                                                projColTableSpecificOffset++;
                                                
                                                System.out.println("Are there more columns to be projected from this table (Y/N)?");
                                                moreColFlag = scanner.nextLine();
                                                
                                                if(moreColFlag.equalsIgnoreCase("Y"))
                                                        flag=true;
                                                
                                                else if(moreColFlag.equalsIgnoreCase("N"))
                                                        flag=false;
                                                projlistIndex++;
                                                
                                                
                                    }
                                    newProjList = new FldSpec[list.size()];
                                    for(int i = 0;i<list.size();i++){
                                        newProjList[i] = list.get(i);
                                    }
                                    projlistIndex = newProjList.length;
                                    FldSpec[] pList = new FldSpec[projColTableSpecificOffset];
                                    for(int l=0;l<projColTableSpecificOffset;l++){
                                        pList[l] = new FldSpec(rel, colNumList[l]);
                                         
                                    }
                                    projTableSpecificList[k] = pList;
                                    
                          }
                      catch(Exception e)
                      {
                        e.printStackTrace();
                      }
                 
                                
                                
                        //EOC
                    //scanner.nextLine();
                    
                }
                //System.out.println("projlistIndex ="+projlistIndex);
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
                
                /*for(int i=0;i<attrTypeList.length;i++){
                        AttrType[] attr = attrTypeList[i];
                        System.out.println("i ="+i);
                        for(int j=0;j<attr.length;j++){
                                System.out.println("attr type ="+attr[j].attrType);
                        }
                }*/
                
                CondExpr [] condExprList = new CondExpr[numCondExpr+1];
                //System.out.println("expr.length ="+condExprList.length);
                
                for(int a=0;a<condExprList.length-1;a++){
                        
                                //System.out.println("condString =");
                                condExprList[a] = new CondExpr();
                                String condString = conditionString[a];
                                condExprList[a].next  = null;
                                //System.out.println("condString ="+condString);
                                //System.out.println("a ="+a);
                                int beginIndex = 0;
                                int endIndex = condString.indexOf(".", 0);
                                //System.out.println("endIndex ="+endIndex);
                                //char tableNum=condString.charAt(index-1);
                                String tableNumString = condString.substring(beginIndex, endIndex);
                                //System.out.println("tableNumString ="+tableNumString);
                                int tableNum = Integer.parseInt(tableNumString);
                                //System.out.println("tableNum ="+tableNum);
                                //String tableName = tableNameList[tableNum-1];
                                int endIndex1 = condString.indexOf(" ",endIndex);
                                //System.out.println("endIndex1 ="+endIndex1);
                                String colNumString = condString.substring(endIndex+1, endIndex1);
                                //System.out.println("colNumString ="+colNumString);
                                int colNum = Integer.parseInt(colNumString);
                                //System.out.println("colNum ="+colNum);
                                
                                condExprList[a].type1 = new AttrType(AttrType.attrSymbol);
                                
                                condExprList[a].operand1.symbol = new FldSpec (new RelSpec(tableNum-1),colNum);
                                                                
                                int endIndex2 = condString.indexOf(" ",endIndex1+1);
                                //System.out.println("endIndex2 ="+endIndex2);
                                String operator = condString.substring(endIndex1+1, endIndex2);
                                //System.out.println("operator = "+operator);
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
                                
                                //System.out.println("expr[a].op = "+condExprList[a].op);
                                        
                                int endIndex3 = condString.indexOf(".",endIndex2);
                                //System.out.println("endIndex3 = "+endIndex3);
                                if(endIndex3==-1){
                                        
                                        String constantString = condString.substring(endIndex2+1);
                                        //System.out.println("constantString = "+constantString);
                                        constantString = constantString.trim();
                                        //System.out.println("constantString after trim() = "+constantString);
                                        
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
                                        
                                        //System.out.println("expr[a].type2 = "+condExprList[a].type2);                         
                                        
                                }
                                else{
                                        String tableNumString1 = condString.substring(endIndex2+1, endIndex3);
                                        //System.out.println("tableNumString1 = "+tableNumString1);
                                        int tableNum1 = Integer.parseInt(tableNumString1);
                                        //System.out.println("tableNum1 = "+tableNum1);
                                        //String tableName = tableNameList[tableNum-1];
                                        
                                        //int endIndex4 = condString.indexOf(" ",endIndex3);
                                        String colNumString1 = condString.substring(endIndex3+1, condString.length());
                                        int colNum1 = Integer.parseInt(colNumString1);
                                        //System.out.println("colNum1 = "+colNum1);
                                        //attr = attrTypeList[tableNum1-1][colNum1-1];
                                        //expr[a].type2 = new AttrType(attr.attrType);
                                        condExprList[a].type2 = new AttrType(AttrType.attrSymbol);
                                        //System.out.println("expr[a].type2 = "+condExprList[a].type2);
                                        condExprList[a].operand2.symbol = new FldSpec (new RelSpec(tableNum1-1),colNum1);
                                        //System.out.println("expr[a].operand2.symbol = "+condExprList[a].operand2.symbol);
                                }
                }
                
                //condExprList[condExprList.length-1] = null;
                
                System.out.println("Enter total memory");
                int memory = scanner.nextInt();
                
        /*      System.out.println("Enter the order - 0-ascending 1-descending");
                int order = scanner.nextInt();*/
                
                System.out.println("Enter the number of top K tuples required in the resulting relation");
                int topK = scanner.nextInt();
                
                scanner.nextLine();
                
                System.out.println("Allow duplicates? (Y/N)");
                String dup = scanner.nextLine();
                boolean dupFlag = false;
                
                if(dup.equalsIgnoreCase("Y"))
                                dupFlag = true;
                        
                        for(int j=0;j<numOfTables;j++){
                                
                                //Create attribute/column list for each table
                                FldSpec [] Sprojection = new FldSpec[numOfColsList[j]];
                                for(int q=0;q<Sprojection.length;q++){
                                
                                        Sprojection[q] = new FldSpec(new RelSpec(RelSpec.outer), q+1);
                                }
                                                
                                Iterator am = null;
                                
                            try {
                              am  = new FileScan(tableNameList[j]+".in", attrTypeList[j], stringSizesList[j], 
                                                          (short)numOfColsList[j], (short)numOfColsList[j],
                                                          Sprojection, null);
                              
                              /*Tuple t1;
                                        while((t1 =am.get_next())!=null){
                                                t1.print(attrTypeList[k]);
                                        }*/
                                                                      
                            } 
                            catch (Exception e) {
                                   status = false;
                                   System.err.println (""+e);
                                 }

                                if (status != true) {
                                      //bail out
                                   System.err.println ("*** Error setting up scan for sailors");
                                   Runtime.getRuntime().exit(1);
                                }
                                
                           
Iterator itr = null;
itr = new Sort(attrTypeList[j], (short)attrTypeList[j].length, stringSizesList[j],am, numOfColsList[j], new TupleOrder(TupleOrder.Descending), 4, memory );
                                iteratorList[j] = itr;
                                                                       } 
                        for(int i=0;i<joinedColList.length;i++)
                        {
                        	joinedColList[i]=	joinedColList[i]+1;
                        }
                        short[] count=new short[numOfTables];
                        for(short i:count)
                        {
                        	i=0;
                        }
                        
                        for(int i=0;i<numOfTables;i++)
                        {
                        	for(int j=0;j<attrTypeList[i].length;j++)
                        	{
                        		if(attrTypeList[i][j].attrType==AttrType.attrString)
                        		{
                        		count[i]=count[i]++;
                        	}}
                        }
                       short [][] str=new short[numOfTables][];
                       for(int i=0;i<numOfTables;i++)
                       {
                    	  str[i]=new short[count[i]];
                    	   for(int j=0;j<count[i];i++)
                    	   {
                    		str[i][j]=(short)30;   
                    	   }
                       }
                    	   
                        	TopjoinsRank tp=new TopjoinsRank(numOfTables,attrTypeList, numOfColsList,str,joinedColList,iteratorList,b_index
                        			,indexNameList,fileNames,memory,condExprList,newProjList,projlistIndex,topK);
                        	//TopjoinsRank tp= new TopjoinsRank(2,attrTypes,len_in,strSizes,joinColumnIndex,am,type,indexNames,relNames,10,outFilter,projection,6,2);//relnames after
                        	Tuple t=new Tuple();
                        	
while((t=tp.get_next())!=null)   
{

}
                        	
//                        //TopRankJoin trj = new TopRankJoin(numOfTables, attrTypeList, numOfColsList, stringSizesList, 
                                       // joinedColList, iteratorList, b_index, indexNameList, memory, condExprList, newProjList, projlistIndex, topK, 1 , fileNames);
                                        //                
                }catch(Exception e){
                        
                }finally{
                        scanner.close();
                }
                System.out.println("Exit processTopRankJoin()\n");

                
        }
        
        
        /**
         * @param args
         */
        public static void main(String[] args) {
                // TODO Auto-generated method stub
                
                Testcases jjoin = new Testcases();
                
                //JoinsDriver2 jjoin = new JoinsDriver2();

                System.out.println("DBI Phase II");
                
                System.out.println("Enter the operation to be performed\n 1 -> TopNestedLoopJoin \n 2 -> TopSortMerge\n 3 -> TopRankJoin\n 0 -> Exit ");
                Scanner scanner = new Scanner(System.in);
                
                try{
                
                        int option = scanner.nextInt();
                
                        switch(option){
                        
                                case 1: System.out.println("Before getPageAccessCount()="+SystemDefs.JavabaseBM.getPageAccessCount()); 
                                                processTopNestedLoopJoin();
                                                System.out.println("After getPageAccessCount()="+SystemDefs.JavabaseBM.getPageAccessCount());
                                                //System.out.println("Time taken to process: " + totalTime);
                                        break;
                        
                                case 2: System.out.println("Before getPageAccessCount()="+SystemDefs.JavabaseBM.getPageAccessCount());
                                                long totalTime = processTopSortMerge();
                                                System.out.println("After getPageAccessCount()="+SystemDefs.JavabaseBM.getPageAccessCount());
                                                System.out.println("Time taken to process: " + totalTime);
                                        break;
                        
                                case 3: processTopRankJoin();
                                        break;
                                case 0:
                                        System.out.println("Goodbye");
                                        break;
                        }
                        
                        
                        
                }catch(Exception e){
                        
                }finally{
                        scanner.close();
                }
        
                
        }
        
        

}

