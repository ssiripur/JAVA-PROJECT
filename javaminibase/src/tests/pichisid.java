package tests;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;
import java.io.File;

import btree.BTreeFile;
import btree.DeleteFashion;
import btree.StringKey;
import bufmgr.PageNotReadException;

import chainexception.ChainException;
import diskmgr.PCounter;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import index.IndexException;
import index.IndexScan;
import iterator.*;
import global.AttrOperator;
import global.AttrType;
import tests.CSVParser;
import global.IndexType;
import global.RID;
import global.SystemDefs;
import global.TupleOrder;
import global.GlobalConst;

public class pichisid {

	private static short REC_LEN1 = 32;
	private static short REC_LEN2 = 160;
	private static int SORTPGNUM = 12;

	public static void main(String[] args) {

		// Kill anything that might be hanging around
		String remove_cmd = GlobalConst.remove_cmd;
		String remove_dbcmd = remove_cmd + GlobalConst.dbpath;
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		System.out.println("Welcome to Fuzzy Database Project Phase 2");
		int choice = 0;
		do {
			System.out.println("\n\n Menu ");
			System.out.println("1.TopNestedLoopJoins Test");
			System.out.println("2.TopSortMerge Test");
			System.out.println("3.TopRankJoin Test");
			System.out.println("4.ThresholdTopK Test");
			System.out.println("5.NRATopK Test");
			System.out.println("6.Exit");
			System.out.print("Enter your Option: ");
			try {
				choice = Integer.parseInt(br.readLine());
			} catch (Exception e) {
				e.printStackTrace();
				continue;
			}
			switch (choice) {
			case 1:
				topNestedLoopJoinsTest();
				break;
			case 2:

				topSortMergeTest();
				break;
			case 3:
				topRankJoinTest();
				break;
			case 4:
				thesholdtopkTest();
			
			case 5:
				nraTopk();
				
			case 6:
				System.exit(0);
			default:
				System.out.println("Please enter valid choice");
			}
		} while (choice != 6);

		// Clean up
		try {
			Runtime.getRuntime().exec(remove_dbcmd);
		} catch (IOException e) {
			System.err.println("IO error: " + e);
		}

		System.out.println("Done.");

	}

	private static void nraTopk() 
	{
		
		try {
			SystemDefs sysdef = new SystemDefs(GlobalConst.dbpath, GlobalConst.MINIBASE_DB_SIZE , GlobalConst.MINIBASE_BUFFER_POOL_SIZE,
					"Clock");
			
			BufferedReader br = new BufferedReader(new InputStreamReader(
					System.in));

			System.out.print("Enter number of input relations: ");
			int numTables = Integer.parseInt(br.readLine());
			ArrayList<File> files = promptFileInputs(numTables);
			int[] joincolumns = promptJoinColumns(numTables);

			int amt_mem = promptAmtOfMemSize();
			int k = promptNumOfTopResults();

			ArrayList<CSVParser> csvparsers = new ArrayList<CSVParser>();
			Iterator[] iterators = new Iterator[numTables];
			AttrType[][] attrslist = new AttrType[numTables][];
			int[] attrslistlengths = new int[numTables];
			short[][] stringsizes = new short[numTables][];
			IndexType[] indexes = new IndexType[numTables];
			String[] heapfilenames = new String[numTables];
			String[] indNames = new String[numTables];
			BTreeFile[] indexfiles = new BTreeFile[numTables];
			Heapfile[] heapFiles = new Heapfile[numTables];

			for (int i = 0; i < numTables; i++) {
				csvparsers.add(new CSVParser(files.get(i).getAbsolutePath()));
				heapfilenames[i] = csvparsers.get(i).getHeapFileName();
				heapFiles[i] = csvparsers.get(i).ConvertToHeapfile(
						files.get(i).getAbsolutePath());
				iterators[i] = csvparsers.get(i).ConvertToIterator(
						files.get(i).getAbsolutePath());
				attrslist[i] = new AttrType[csvparsers.get(i).get_attrs().length];
				System.arraycopy(csvparsers.get(i).get_attrs(), 0,
						attrslist[i], 0, csvparsers.get(i).get_attrs().length);
				attrslistlengths[i] = csvparsers.get(i).get_attrs().length;
				stringsizes[i] = new short[csvparsers.get(i).get_str_sizes().length];
				System.arraycopy(csvparsers.get(i).get_str_sizes(), 0,
						stringsizes[i], 0,
						csvparsers.get(i).get_str_sizes().length);
				indexes[i] = new IndexType(IndexType.B_Index); // only btree
																// supported
				indNames[i] = "BTreeIndex" + i;
				indexfiles[i] = new BTreeFile(indNames[i],
						attrslist[i][joincolumns[i]].attrType, REC_LEN1,
						DeleteFashion.NAIVE_DELETE);
				Scan scan = new Scan(heapFiles[i]);
				RID rid = new RID();
				for (Tuple t = scan.getNext(rid); t != null; t = scan
						.getNext(rid)) {
					Tuple tuple = new Tuple();
					tuple.setHdr((short) attrslist[i].length, attrslist[i],
							stringsizes[i]);
					tuple.tupleCopy(t);
					String keyVal = tuple.getStrFld(1);
					indexfiles[i].insert(new StringKey(keyVal), rid);
				}
				scan.closescan();
			}

			FldSpec[] Sprojection = new FldSpec[csvparsers.get(0).get_attrs().length];
			for (int i = 0; i < Sprojection.length; i++) {
				Sprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
			}
			Tuple t=new Tuple();
			int n_out_flds = csvparsers.get(0).get_attrs().length;
			Iterator[] iter=new Iterator[numTables];
			PCounter.initialize();
			for(int i=0;i<numTables;i++)
			{
				iter[i]=new Sort(attrslist[i],(short)attrslistlengths[i],stringsizes[i],iterators[i],attrslistlengths[i],new TupleOrder(TupleOrder.Descending),4,30);
			}
			NraTopK2 np = new NraTopK2(numTables, attrslist,
					attrslistlengths, stringsizes, joincolumns, iter,
					indexes,indNames, heapfilenames,  amt_mem, null,
					Sprojection, n_out_flds, k);
			
			np.sort_heapfile();
			while((t=np.get_next())!=null)
			{
				t.print(attrslist[0]);
			}
			
			PCounter.println();
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}



	private static void topNestedLoopJoinsTest() { // DONE
		try {
			SystemDefs sysdef = new SystemDefs(GlobalConst.dbpath, GlobalConst.MINIBASE_DB_SIZE , GlobalConst.MINIBASE_BUFFER_POOL_SIZE,
					"Clock");
			
			ArrayList<File> files = promptFileInputs(2);
			int amt_mem = promptAmtOfMemSize();
			int k = promptNumOfTopResults();

			CSVParser csv1 = new CSVParser(files.get(0).getAbsolutePath());
			CSVParser csv2 = new CSVParser(files.get(1).getAbsolutePath());

			Iterator outer_rel_itr = csv1.ConvertToIterator(files.get(0)
					.getAbsolutePath());

			Heapfile inner_rel_hf = csv2.ConvertToHeapfile(files.get(1)
					.getAbsolutePath());
			
			Iterator inner_rel_itr=csv1.ConvertToIterator(files.get(0).getAbsolutePath());

			CondExpr[] outFilter = new CondExpr[2];
			outFilter[0] = new CondExpr();
			outFilter[0].next = null;
			outFilter[0].op = new AttrOperator(AttrOperator.aopEQ); // equi-join
																	// on the
																	// first
																	// column of
																	// each
																	// relation
			outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
			outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
			outFilter[0].operand1.symbol = new FldSpec(new RelSpec(
					RelSpec.outer), 1);
			outFilter[0].operand2.symbol = new FldSpec(new RelSpec(
					RelSpec.innerRel), 1);
			outFilter[1] = null;

			FldSpec[] Sprojection = new FldSpec[csv1.get_attrs().length];
			int i=0;
			for ( i= 0; i < Sprojection.length-2; i++) {
				Sprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
			}
			Sprojection[i]=new FldSpec(new RelSpec(RelSpec.innerRel), i + 1);
			Sprojection[i+1]=new FldSpec(new RelSpec(RelSpec.outer), csv1.get_attrs().length);
			//Sprojection[Sprojection.length-1]=new FldSpec(new RelSpec(RelSpec.innerRel),2);
			Tuple t=new Tuple();
			int 	count=0;
			System.out.println("printintig table0");
//			while(count!=20)
//					{
//				t=outer_rel_itr.get_next();
//				t.print(csv1.get_attrs());
//				count++;
//					}
//			System.out.println("printintig table1");
//			while(count!=40)
//			{
//				t=inner_rel_itr.get_next() ;
//		t.print(csv1.get_attrs());
//		count++;
//			}
			
			PCounter.initialize();
			TopNestedLoopJoinEx top_nested = new TopNestedLoopJoinEx(
					csv1.get_attrs(), csv1.get_attrs().length,
					csv1.get_str_sizes(), csv2.get_attrs(),
					csv2.get_attrs().length, csv2.get_str_sizes(), amt_mem,
					outer_rel_itr, csv2.getHeapFileName(), outFilter,null,
					Sprojection, Sprojection.length, k);
//			public TopNestedLoopJoinEx(AttrType[] in1, int len_in1,
//					short[] t1_str_sizes, AttrType[] in2, int len_in2,
//					short[] t2_str_sizes, int amt_of_mem, Iterator am1,
//					String relationName, CondExpr[] outFilter, CondExpr[] rightFilter,
//					FldSpec[] proj_list, int n_out_flds,int num) throws IOException,
//					NestedLoopException, SortException
//			
			
			
			
			
			
			
			t = null;
	         
	         try {
	           while ((t = top_nested.get_TopRanked()) != null) {
	           // t.print(jtype);
	             
	             //qcheck1.Check(t);
	           }
	         
	         }
	         catch (Exception e) {
	           System.err.println (""+e);
	            e.printStackTrace();
	           // status = FAIL;
	         }

			//PrintIterator(top_nested, csv1.get_attrs());
			PCounter.println();
			System.out.println("Records in inner heapfile: "
					+ inner_rel_hf.getRecCnt());

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void topSortMergeTest() {
		try {
			SystemDefs sysdef = new SystemDefs(GlobalConst.dbpath, GlobalConst.MINIBASE_DB_SIZE , GlobalConst.MINIBASE_BUFFER_POOL_SIZE,
					"Clock");
			
			ArrayList<File> files = promptFileInputs(2);
			int amt_mem = promptAmtOfMemSize();
			int k = promptNumOfTopResults();
			Tuple t=new Tuple();
			CSVParser csv1 = new CSVParser(files.get(0).getAbsolutePath());
			CSVParser csv2 = new CSVParser(files.get(1).getAbsolutePath());

			Iterator outer_itr = csv1.ConvertToIterator(files.get(0)
					.getAbsolutePath());

			Iterator inner_itr = csv2.ConvertToIterator(files.get(1)
					.getAbsolutePath());

			CondExpr[] outFilter = new CondExpr[2];
			outFilter[0] = new CondExpr();
			outFilter[0].next = null;
			outFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
			outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
			outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
			outFilter[0].operand1.symbol = new FldSpec(new RelSpec(
					RelSpec.outer), 1);
			outFilter[0].operand2.symbol = new FldSpec(new RelSpec(
					RelSpec.innerRel), 1);
			outFilter[1] = null;

			FldSpec[] Sprojection = new FldSpec[csv1.get_attrs().length];
			for (int i = 0; i < Sprojection.length; i++) {
				Sprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
			}
//			while((t=outer_itr.get_next())!=null)
//				{
//			t.print(csv1.get_attrs());
//				}
//		while((t=inner_itr.get_next())!=null)
//		{
//	t.print(csv1.get_attrs());
//		}

			TopSortMergeExcl top_SM = null;
			PCounter.initialize();
		 top_SM = new TopSortMergeExcl(csv1.get_attrs(),
					csv1.get_attrs().length, csv1.get_str_sizes(),
					csv2.get_attrs(), csv2.get_attrs().length,
					csv2.get_str_sizes(), 1, csv1.get_str_sizes()[0], 1,
					csv2.get_str_sizes()[0], amt_mem, outer_itr, inner_itr,
					false, false, new TupleOrder(TupleOrder.Descending),
					outFilter, Sprojection, Sprojection.length, k);

			;
		 t = null;
         
         try {
           while ((t = top_SM.get_TopRanked()) != null) {
             //t.print(jtype);
             
             //qcheck1.Check(t);
           }
         
         }
         catch (Exception e) {
           System.err.println (""+e);
            e.printStackTrace();
           // status = FAIL;
         }
			PCounter.println();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void topRankJoinTest() {
		try {
			SystemDefs sysdef = new SystemDefs(GlobalConst.dbpath, GlobalConst.MINIBASE_DB_SIZE , GlobalConst.MINIBASE_BUFFER_POOL_SIZE,
					"Clock");
			
			BufferedReader br = new BufferedReader(new InputStreamReader(
					System.in));

			System.out.print("Enter number of input relations: ");
			int numTables = Integer.parseInt(br.readLine());
			ArrayList<File> files = promptFileInputs(numTables);
			int[] joincolumns = promptJoinColumns(numTables);

			int amt_mem = promptAmtOfMemSize();
			int k = promptNumOfTopResults();

			ArrayList<CSVParser> csvparsers = new ArrayList<CSVParser>();
			Iterator[] iterators = new Iterator[numTables];
			AttrType[][] attrslist = new AttrType[numTables][];
			int[] attrslistlengths = new int[numTables];
			short[][] stringsizes = new short[numTables][];
			IndexType[] indexes = new IndexType[numTables];
			String[] heapfilenames = new String[numTables];
			String[] indNames = new String[numTables];
			BTreeFile[] indexfiles = new BTreeFile[numTables];
			Heapfile[] heapFiles = new Heapfile[numTables];

			for (int i = 0; i < numTables; i++) {
				csvparsers.add(new CSVParser(files.get(i).getAbsolutePath()));
				heapfilenames[i] = csvparsers.get(i).getHeapFileName();
				heapFiles[i] = csvparsers.get(i).ConvertToHeapfile(
						files.get(i).getAbsolutePath());
				iterators[i] = csvparsers.get(i).ConvertToIterator(
						files.get(i).getAbsolutePath());
				attrslist[i] = new AttrType[csvparsers.get(i).get_attrs().length];
				System.arraycopy(csvparsers.get(i).get_attrs(), 0,
						attrslist[i], 0, csvparsers.get(i).get_attrs().length);
				attrslistlengths[i] = csvparsers.get(i).get_attrs().length;
				stringsizes[i] = new short[csvparsers.get(i).get_str_sizes().length];
				System.arraycopy(csvparsers.get(i).get_str_sizes(), 0,
						stringsizes[i], 0,
						csvparsers.get(i).get_str_sizes().length);
				indexes[i] = new IndexType(IndexType.B_Index); // only btree
																// supported
				indNames[i] = "BTreeIndex" + i;
				indexfiles[i] = new BTreeFile(indNames[i],
						attrslist[i][joincolumns[i]].attrType, REC_LEN1,
						DeleteFashion.NAIVE_DELETE);
				Scan scan = new Scan(heapFiles[i]);
				RID rid = new RID();
				for (Tuple t = scan.getNext(rid); t != null; t = scan
						.getNext(rid)) {
					Tuple tuple = new Tuple();
					tuple.setHdr((short) attrslist[i].length, attrslist[i],
							stringsizes[i]);
					tuple.tupleCopy(t);
					String keyVal = tuple.getStrFld(1);
					indexfiles[i].insert(new StringKey(keyVal), rid);
				}
				scan.closescan();
			}

			FldSpec[] Sprojection = new FldSpec[csvparsers.get(0).get_attrs().length];
			for (int i = 0; i < Sprojection.length; i++) {
				Sprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
			}
			Tuple t=new Tuple();
			int n_out_flds = csvparsers.get(0).get_attrs().length;
			Iterator[] iter=new Iterator[numTables];
			PCounter.initialize();
			for(int i=0;i<numTables;i++)
			{
				iter[i]=new Sort(attrslist[i],(short)attrslistlengths[i],stringsizes[i],iterators[i],attrslistlengths[i],new TupleOrder(TupleOrder.Descending),4,30);
			}
			TopJoinsRank top_rank = new TopJoinsRank(numTables, attrslist,
					attrslistlengths, stringsizes, joincolumns, iter,
					indexes,indNames, heapfilenames,  amt_mem, null,
					Sprojection, n_out_flds, k);
			while((t=top_rank.get_next())!=null)
			{
	
			}
			
			PCounter.println();
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void thesholdtopkTest() {
		try {
			SystemDefs sysdef = new SystemDefs(GlobalConst.dbpath, GlobalConst.MINIBASE_DB_SIZE , GlobalConst.MINIBASE_BUFFER_POOL_SIZE,
					"Clock");
			
			BufferedReader br = new BufferedReader(new InputStreamReader(
					System.in));

			System.out.print("Enter number of input relations: ");
			int numTables = Integer.parseInt(br.readLine());
			ArrayList<File> files = promptFileInputs(numTables);
			int[] joincolumns = promptJoinColumns(numTables);

			int amt_mem = promptAmtOfMemSize();
			int k = promptNumOfTopResults();

			ArrayList<CSVParser> csvparsers = new ArrayList<CSVParser>();
			Iterator[] iterators = new Iterator[numTables];
			AttrType[][] attrslist = new AttrType[numTables][];
			int[] attrslistlengths = new int[numTables];
			short[][] stringsizes = new short[numTables][];
			IndexType[] indexes = new IndexType[numTables];
			String[] heapfilenames = new String[numTables];
			String[] indNames = new String[numTables];
			BTreeFile[] indexfiles = new BTreeFile[numTables];
			Heapfile[] heapFiles = new Heapfile[numTables];

			for (int i = 0; i < numTables; i++) {
				csvparsers.add(new CSVParser(files.get(i).getAbsolutePath()));
				heapfilenames[i] = csvparsers.get(i).getHeapFileName();
				heapFiles[i] = csvparsers.get(i).ConvertToHeapfile(
						files.get(i).getAbsolutePath());
				iterators[i] = csvparsers.get(i).ConvertToIterator(
						files.get(i).getAbsolutePath());
				attrslist[i] = new AttrType[csvparsers.get(i).get_attrs().length];
				System.arraycopy(csvparsers.get(i).get_attrs(), 0,
						attrslist[i], 0, csvparsers.get(i).get_attrs().length);
				attrslistlengths[i] = csvparsers.get(i).get_attrs().length;
				stringsizes[i] = new short[csvparsers.get(i).get_str_sizes().length];
				System.arraycopy(csvparsers.get(i).get_str_sizes(), 0,
						stringsizes[i], 0,
						csvparsers.get(i).get_str_sizes().length);
				indexes[i] = new IndexType(IndexType.B_Index); // only btree
																// supported
				indNames[i] = "BTreeIndex" + i;
				indexfiles[i] = new BTreeFile(indNames[i],
						attrslist[i][joincolumns[i]].attrType, REC_LEN1,
						DeleteFashion.NAIVE_DELETE);
				Scan scan = new Scan(heapFiles[i]);
				RID rid = new RID();
				for (Tuple t = scan.getNext(rid); t != null; t = scan
						.getNext(rid)) {
					Tuple tuple = new Tuple();
					tuple.setHdr((short) attrslist[i].length, attrslist[i],
							stringsizes[i]);
					tuple.tupleCopy(t);
					String keyVal = tuple.getStrFld(1);
					indexfiles[i].insert(new StringKey(keyVal), rid);
				}
				scan.closescan();
			}

			FldSpec[] Sprojection = new FldSpec[csvparsers.get(0).get_attrs().length];
			for (int i = 0; i < Sprojection.length; i++) {
				Sprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
			}
			Tuple t=new Tuple();
			int n_out_flds = csvparsers.get(0).get_attrs().length;
			Iterator[] iter=new Iterator[numTables];
			PCounter.initialize();
			for(int i=0;i<numTables;i++)
			{
				iter[i]=new Sort(attrslist[i],(short)attrslistlengths[i],stringsizes[i],iterators[i],attrslistlengths[i],new TupleOrder(TupleOrder.Descending),4,30);
			}
			ThresoldTopK tp= new  ThresoldTopK(numTables, attrslist,
					attrslistlengths, stringsizes, joincolumns, iter,
					indexes,indNames, heapfilenames,  amt_mem, null,
					Sprojection, n_out_flds, k);//relnames after
			
	        

            while((t=tp.get_next())!=null)
            {
//            	System.out.println("got tuple");
             //t.print(attr);
             }
            
			
			
			PCounter.println();
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	private static void indexScanTest() {
		try {
			SystemDefs sysdef = new SystemDefs(GlobalConst.dbpath, GlobalConst.MINIBASE_DB_SIZE , GlobalConst.MINIBASE_BUFFER_POOL_SIZE,
					"Clock");
			
			BTreeFile index;
			BufferedReader br = new BufferedReader(new InputStreamReader(
					System.in));
			ArrayList<File> file = promptFileInputs(1);
			CSVParser csv_file = new CSVParser(file.get(0).getAbsolutePath());

			AttrType[] indexkey = csv_file.get_attrs();

			index = new BTreeFile("BTreeIndex", indexkey[0].attrType, REC_LEN1,
					1);
			IndexType idxtype = new IndexType(IndexType.B_Index);

			FldSpec[] Sprojection = new FldSpec[csv_file.get_attrs().length];
			for (int i = 0; i < Sprojection.length; i++) {
				Sprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
			}

			IndexScan is = new IndexScan(idxtype, "Test", "BTreeIndex",
					csv_file.get_attrs(), csv_file.get_str_sizes(),
					csv_file.get_attrs().length, csv_file.get_attrs().length,
					Sprojection, null, 1, true);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	

	

	

	private static void displayCSVTest() { // DONE
		try {
			SystemDefs sysdef = new SystemDefs(GlobalConst.dbpath, GlobalConst.MINIBASE_DB_SIZE , GlobalConst.MINIBASE_BUFFER_POOL_SIZE,
					"Clock");
			ArrayList<File> file = promptFileInputs(1);
			CSVParser csv_file = new CSVParser(file.get(0).getAbsolutePath());
			PrintIterator(
					csv_file.ConvertToIterator(file.get(0).getAbsolutePath()),
					csv_file.get_attrs());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	// output in bytes
	public static int promptNumOfPages() {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		System.out.print("Enter number of pages for DB: ");
		int numOfPages = 0;
		try {
			numOfPages = Integer.parseInt(br.readLine());
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return numOfPages;
	}

	// output in bytes
	public static int promptBufferPoolSize() {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		System.out.print("Enter buffer pool size for DB: ");
		int bufferPoolSize = 0;
		try {
			bufferPoolSize = Integer.parseInt(br.readLine());
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return bufferPoolSize;
	}

	public static int promptAmtOfMemSize() {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		System.out.print("Enter amount of memory (pages): ");
		int amtOfMem = 0;
		try {
			amtOfMem = Integer.parseInt(br.readLine());
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return amtOfMem;
	}

	private static ArrayList<File> promptFileInputs(int num) {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		ArrayList<File> relationNames = new ArrayList<File>();

		for (int i = 0; i < num; i++) {
			System.out.print("Enter Input filepath " + (i + 1) + ": ");
			try {
				String temp = br.readLine();
				relationNames.add(new File(temp));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return relationNames;
	}

	private static String[] promptIndexNames(int num) {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		java.lang.String[] retnames = new java.lang.String[num];

		for (int i = 0; i < num; i++) {
			System.out.print("Enter name of index file " + (i + 1) + ": ");
			try {
				String temp = br.readLine();
				retnames[i] = temp;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return retnames;
	}

	private static int[] promptJoinColumns(int num) {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		int[] returnjccols = new int[num];

		for (int i = 0; i < num; i++) {
			System.out.print("Enter the join column for relation " + (i + 1)
					+ ": ");
			try {
				int temp = Integer.parseInt(br.readLine());
				returnjccols[i] = temp;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return returnjccols;
	}

	private static int promptNumOfTopResults() {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		System.out.print("Enter number of results to be displayed: ");
		int numResults = 0;
		try {
			numResults = Integer.parseInt(br.readLine());
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return numResults;
	}

	public static void PrintIterator(Iterator tuples,
			AttrType[] tuple_attributes) {
		try {
			Tuple next_tuple = new Tuple();
			next_tuple = null;
			while ((next_tuple = tuples.get_next()) != null) {
				next_tuple.print(tuple_attributes);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
}

