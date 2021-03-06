package tests;

import global.AttrOperator;
import global.AttrType;
import global.IndexType;
import global.RID;
import global.SystemDefs;
import global.TupleOrder;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.Tuple;
import index.IndexException;
import index.IndexScan;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FileScan2_delete;
import iterator.FldSpec;
import iterator.Iterator;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.NraTopK;
import iterator.NraTopK2;
import iterator.PredEvalException;
import iterator.RelSpec;
import iterator.Sort;
import iterator.SortException;
import iterator.StreamCombine;
import iterator.ThresoldTopK;
import iterator.TopJoinRank;
import iterator.TopRankJoin;
import iterator.TopRankJoin1;
import iterator.TopRankJoin3;
import iterator.TopSortMergeExcl;
import iterator.TopJoinsRank;

import iterator.TopRanks;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;

import java.io.IOException;
import java.util.ArrayList;

import btree.BTreeFile;
import btree.IntegerKey;
import btree.StringKey;
import bufmgr.PageNotReadException;

public class TopRankJoinTest extends TestDriver {

    ArrayList<Sailor> sailors = new ArrayList<Sailor>();
    ArrayList<Boats> boats = new ArrayList<Boats>();
    ArrayList<Reserves> reserves = new ArrayList<Reserves>();
    Iterator[] am;

    public boolean runAllTests() {
        System.out
                .println("\n" + "Running " + testName() + " tests...." + "\n");

        SystemDefs sysdef = new SystemDefs(dbpath, 1000, 50, "Clock");

        // Kill anything that might be hanging around
        
        String newdbpath;
        String newlogpath;
        String remove_logcmd;
        String remove_dbcmd;
        String remove_cmd = "/bin/rm -rf ";

        newdbpath = dbpath;
        newlogpath = logpath;

        remove_logcmd = remove_cmd + logpath;
        remove_dbcmd = remove_cmd + dbpath;

        // Commands here is very machine dependent. We assume
        // user are on UNIX system here
  
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("" + e);
        }

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        // This step seems redundant for me. But it's in the original
        // C++ code. So I am keeping it as of now, just in case I
        
        // I missed something
        
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("" + e);
        }

        // Run the tests. Return type different from C++
       // testTopRankJoin();

        // Clean up again
    
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("" + e);
        }
        return false;

    }
    public TopRankJoinTest() {

        super("TopRankJoin");
        // Create data.
        sailors.add(new Sailor(52, "Bob Holloway", 9, 53.6, 0.3f));
        sailors.add(new Sailor(54, "Susan Horowitz", 1, 34.2, 0.3f));
        sailors.add(new Sailor(57, "Yannis Ioannidis", 8, 40.2, 0.3f));
        sailors.add(new Sailor(59, "Deborah Joseph", 10, 39.8, 0.3f));
        sailors.add(new Sailor(61, "Landwebber", 8, 56.7, 0.3f));
        sailors.add(new Sailor(63, "James Larus", 9, 30.3, 0.3f));
        sailors.add(new Sailor(64, "Barton Miller", 5, 43.7, 0.3f));
        sailors.add(new Sailor(67, "David Parter", 1, 99.9, 0.3f));
        sailors.add(new Sailor(69, "Raghu Ramakrishnan", 9, 37.1, 0.3f));
        sailors.add(new Sailor(71, "Guri Sohi", 10, 42.1, 0.3f));
        sailors.add(new Sailor(73, "Prasoon Tiwari", 8, 39.2, 0.3f));
        sailors.add(new Sailor(39, "Anne Condon", 3, 30.3, 0.3f));
        sailors.add(new Sailor(47, "Charles Fischer", 6, 46.3, 0.3f));
        sailors.add(new Sailor(49, "James Goodman", 4, 50.3, 0.3f));
        sailors.add(new Sailor(50, "Mark Hill", 5, 35.2, 0.3f));
        sailors.add(new Sailor(75, "Mary Vernon", 7, 43.1, 0.3f));
        sailors.add(new Sailor(79, "David Wood", 3, 39.2, 0.3f));
        sailors.add(new Sailor(84, "Mark Smucker", 9, 25.3, 0.3f));
        sailors.add(new Sailor(87, "Martin Reames", 10, 24.1, 0.3f));
        sailors.add(new Sailor(10, "Mike Carey", 9, 40.3, 0.1f));
        sailors.add(new Sailor(21, "David Dewitt", 10, 47.2, 0.9f));
        sailors.add(new Sailor(29, "Tom Reps", 7, 39.1, 0.3f));
        sailors.add(new Sailor(31, "Jeff Naughton", 5, 35.0, 0.3f));
        sailors.add(new Sailor(35, "Miron Livny", 7, 37.6, 0.3f));
        sailors.add(new Sailor(37, "Marv Solomon", 10, 48.9, 0.3f));

        boats.add(new Boats(1, "Onion", "white", 0.8f));
        boats.add(new Boats(2, "Buckey", "red", 0.6f));
        boats.add(new Boats(8, "Enterprise", "blue", 0.9f));
        boats.add(new Boats(5, "Pegasus", "orange", 0.95f));
        boats.add(new Boats(4, "Voyager", "green", 0.9f));
        boats.add(new Boats(5, "Wisconsin5", "red", 0.8f));
        boats.add(new Boats(6, "Wisconsin6", "red", 0.9f));
        boats.add(new Boats(7, "Wisconsin7", "red", 0.91f));
        boats.add(new Boats(8, "Wisconsin8", "red", 0.91f));
        boats.add(new Boats(9, "Wisconsin9", "red", 0.92f));
        boats.add(new Boats(10, "Wisconsin10", "red", 0.93f));
        boats.add(new Boats(5, "Wisconsin", "red", 0.96f));
        boats.add(new Boats(5, "Wisconsin", "red", 0.95f));

        reserves.add(new Reserves(10, 5, "Pegasus", 0.91f));
        reserves.add(new Reserves(10, 2, "Wisconsin", 0.82f));
        reserves.add(new Reserves(57, 8, "Enterprise", 0.76f));
        reserves.add(new Reserves(10, 2, "Buckey", 0.82f));
        reserves.add(new Reserves(35, 1,  "Onion", 0.71f));
        reserves.add(new Reserves(10, 4, "Voyager", 0.6f));
        reserves.add(new Reserves(21, 8, "Enterprise", 0.3f));
        reserves.add(new Reserves(31, 9, "Enterprise", 0.4f));
        reserves.add(new Reserves(69, 4, "Voyager", 0.7f));
        reserves.add(new Reserves(69, 5, "Enterprise", 0.7f));
        reserves.add(new Reserves(21, 5, "Pegasus", 0.9f));
        reserves.add(new Reserves(57, 8, "Wisconsin8", 0.76f));
        reserves.add(new Reserves(35, 1,  "Wisconsin10", 0.71f));
        reserves.add(new Reserves(10, 2, "Wisconsin7", 0.82f));
        reserves.add(new Reserves(35, 1,  "Wisconsin5", 0.71f));
        reserves.add(new Reserves(57, 8, "Wisconsin6", 0.76f));
        reserves.add(new Reserves(10, 4, "Wisconsin10", 0.6f));
        reserves.add(new Reserves(10, 5, "Wisconsin9", 0.91f));
        reserves.add(new Reserves(10, 2, "Wisconsin9", 0.82f));
        
    }

    public void testTopRankJoin() throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, IOException, Exception {

        int numOfTables = 2;

       

        AttrType[][] attrTypes = new AttrType[2][];
        short[][] strSizes = new short[2][];
        int [] len_in=new int[2];
        len_in[0]=4;
        len_in[1]=4;
        attrTypes[0] = new AttrType[4];
        attrTypes[0][0] = new AttrType(AttrType.attrInteger);
        attrTypes[0][1] = new AttrType(AttrType.attrString);
        attrTypes[0][2] = new AttrType(AttrType.attrString);
        attrTypes[0][3] = new AttrType(AttrType.attrReal);

        strSizes[0] = new short[2];
        strSizes[0][0] = 30;
        strSizes[0][1] = 20;
        attrTypes[1] = new AttrType[4];
        attrTypes[1][0] = new AttrType(AttrType.attrInteger);
        attrTypes[1][1] = new AttrType(AttrType.attrInteger);
        attrTypes[1][2] = new AttrType(AttrType.attrString);
        attrTypes[1][3] = new AttrType(AttrType.attrReal);

        strSizes[1] = new short[1];
        strSizes[1][0] = 15;

        // Join column index.//changes to check values
        int[] joinColumnIndex = new int[2];
        joinColumnIndex[0] =2;
        joinColumnIndex[1] =3;

        FldSpec[] boatsProjection = new FldSpec[4];
        boatsProjection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        boatsProjection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        boatsProjection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
        boatsProjection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

        FldSpec[] reservesProjection = new FldSpec[4];
        reservesProjection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        reservesProjection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        reservesProjection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
        reservesProjection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);
        

        Tuple boatTuple = new Tuple();
        Tuple reserveTuple = new Tuple();
        int size = 0;
        int rsize=0;
        try {
            boatTuple.setHdr((short) 4, attrTypes[0], strSizes[0]);
            size = boatTuple.size();
            
            reserveTuple.setHdr((short) 4, attrTypes[1], strSizes[1]);
            rsize = reserveTuple.size();
        } catch (Exception e) {
            e.printStackTrace();
        }

        Heapfile fBoats = null;
        Heapfile fReserves = null;

  
        try {
            fBoats = new Heapfile("boats.in");
            fReserves = new Heapfile("reserves.in");

        } catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            e.printStackTrace();
        }
        
        String[] relNames= {"boats.in","reserves.in"};
        
        Tuple t = new Tuple(size);
        try {
            t.setHdr((short) 4, attrTypes[0], strSizes[0]);
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        
        for (int i = 0; i < 13; i++) {
            try {
                t.setIntFld(1, ((Boats) boats.get(i)).bid);
                t.setStrFld(2, ((Boats) boats.get(i)).bname);
                t.setStrFld(3, ((Boats) boats.get(i)).color);
                t.setFloFld(4, ((Boats) boats.get(i)).score);
                t.setScore((float) ((Boats) boats.get(i)).score);
                t.print(attrTypes[0]);
                System.out.println("score: "+t.getFloFld(4));

            } catch (Exception e) {
                System.err.println("*** error in Tuple.setStrFld() ***");
                e.printStackTrace();
            }

            RID rid;
            try {
                rid = fBoats.insertRecord(t.returnTupleByteArray());
                Tuple te = new Tuple();
               te.setHdr((short) 4, attrTypes[0], strSizes[0]);
                		te = fBoats.getRecord(rid);
                		te.setHdr((short) 4, attrTypes[0], strSizes[0]);
            } catch (Exception e) {
                System.err.println("*** error in Heapfile.insertRecord() ***");
                e.printStackTrace();
            }
        }
        FldSpec[] proj_list = new FldSpec[attrTypes[0].length];
        for(int i =0;i<attrTypes[0].length;i++){
        	proj_list[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
        }
        FileScan sc = new FileScan("boats.in", attrTypes[0], strSizes[0],
    			(short)attrTypes[0].length, attrTypes[0].length, proj_list,
    			null);
   
        t = new Tuple(rsize);
        try {
            t.setHdr((short) 4, attrTypes[1], strSizes[1]);
           
        } catch (Exception e) {
            e.printStackTrace();
        }
        

        for (int i = 0; i < 19; i++) {
            try {
                t.setIntFld(1, ((Reserves) reserves.get(i)).sid);
                t.setIntFld(2, ((Reserves) reserves.get(i)).bid);
                t.setStrFld(3, ((Reserves) reserves.get(i)).date);
                t.setFloFld(4, ((Reserves) reserves.get(i)).score);
                t.setScore((float) ((Reserves) reserves.get(i)).score);
                t.print(attrTypes[0]);

            } catch (Exception e) {
                System.err.println("*** error in Tuple.setStrFld() ***");
                e.printStackTrace();
            }

            RID rid = new RID();
            try {
                rid = fReserves.insertRecord(t.returnTupleByteArray());
                
            } catch (Exception e) {
                System.err.println("*** error in Heapfile.insertRecord() ***");
                e.printStackTrace();
            }
        }

        // Create iterators for each table. Iterators should be sorted on score.
        FileScan boatsScan = null;
        try {
            boatsScan = new FileScan("boats.in", attrTypes[0], strSizes[0],
                    (short) 4, (short) 4, boatsProjection, null);
        } catch (Exception e) {
            System.err.println("" + e);
        }

        FileScan reservesScan = null;
        try {
            reservesScan = new FileScan("reserves.in", attrTypes[1],
                    strSizes[1], (short) 4, (short) 4, reservesProjection, null);
        } catch (Exception e) {
            System.err.println("" + e);
        }


        BTreeFile btBoats = null;
        BTreeFile btReserves = null;
        try {
            btBoats = new BTreeFile("Btree_boats.bt", AttrType.attrString, 30, 1);
            btReserves = new BTreeFile("Btree_reserves.bt", AttrType.attrString,
                    30, 1);
        } catch (Exception e) {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        
        Tuple tt = new Tuple(size);
        try {
            tt.setHdr((short) 4, attrTypes[0], strSizes[0]);
        } catch (Exception e) {
          
            e.printStackTrace();
        }
      

        RID rid = new RID();
        Scan scan = null;
        String key =new String();
        Tuple temp= null;
        
        try {
            
            scan = new Scan(fBoats);
           temp = scan.getNext(rid);
            while (temp != null) {
                tt.tupleCopy(temp);
                key = tt.getStrFld(2);
                btBoats.insert(new StringKey(key), rid);
            
                temp = scan.getNext(rid);
            }

        } catch (Exception e) {
            // TODO: handle exception
        }
        
        
         tt = new Tuple(rsize);
        try {
            tt.setHdr((short) 4, attrTypes[1], strSizes[1]);
        } catch (Exception e) {
          
            e.printStackTrace();
        }
      
        RID rid1 = new RID();
        try {
            // Create reserves index
            scan = new Scan(fReserves);
            temp = scan.getNext(rid1);
            while (temp != null) {
                tt.tupleCopy(temp);
                key = tt.getStrFld(3);
                btReserves.insert(new StringKey(key), rid1);
                temp = scan.getNext(rid1);
            }

        } catch (Exception e) {
            // TODO: handle exception
        }

        
        
        // Index names array.
        String[] indexNames=new String[2];
        indexNames[0]="Btree_boats.bt";
        indexNames[1]="Btree_reserves.bt";
        
        // Out filter.
        CondExpr[] outFilter = new CondExpr[2];
        outFilter[0] = new CondExpr();
        
        outFilter[0].next = null;
        outFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
        outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
        outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
        outFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
        outFilter[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 3);


        // Projection list.
        FldSpec[] projection=new FldSpec[6];
        projection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        projection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        projection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
        projection[3] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
        projection[4] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
        projection[5] = new FldSpec(new RelSpec(RelSpec.innerRel), 3);
        FldSpec[] projection1=new FldSpec[6];
        projection1[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        projection1[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        projection1[2] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
        projection1[3] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
        projection1[4] = new FldSpec(new RelSpec(RelSpec.innerRel), 3);
        projection1[5] = new FldSpec(new RelSpec(RelSpec.innerRel), 4);//on
        
        IndexType []type=new IndexType[2];
        IndexType b_index = new IndexType(IndexType.B_Index);
        type[0]=b_index;
        type[1]=new IndexType(IndexType.B_Index);
        
        iterator.Iterator sm[] = new iterator.Iterator[2];
        iterator.Iterator am[] = new iterator.Iterator[2];
        
        try {
            sm[0] = new FileScan( "boats.in", attrTypes[0],
                    strSizes[0], (short)4, 4, boatsProjection, null);
            Tuple newTup = sm[0].get_next();
            System.out.println("t1: "+newTup.getIntFld(1));
            System.out.println("t4: "+ newTup.getFloFld(4));
            am[0]=new Sort(attrTypes[0],(short)4,strSizes[0], sm[0], 4, (new TupleOrder(TupleOrder.Descending)) , 4, 10);
           
        }

        catch (Exception e) {
        	e.printStackTrace();
            System.err.println("*** Error creating scan for Index scan");
            System.err.println("" + e);
            Runtime.getRuntime().exit(1);
        }
        
        try {
            sm[1] = new FileScan("reserves.in", attrTypes[1],
                    strSizes[1], (short)4, 4, reservesProjection, null);
            am[1]=new Sort(attrTypes[1],(short)4,strSizes[1], sm[1], 4,new TupleOrder(TupleOrder.Descending) , 4, 10);
            
        }catch (Exception e) {
            System.err.println("*** Error creating scan for Index scan");
            System.err.println("" + e);
            Runtime.getRuntime().exit(1);
        }
        int number =5;
        
       
        Tuple t2=new Tuple();
//        TopSortMergeExcl tsp=new TopSortMergeExcl(attrTypes[0], len_in[0], strSizes[0], attrTypes[1], len_in[1], strSizes[1], joinColumnIndex[0], 30, joinColumnIndex[1], 30, 15, sm[0], sm[1], false, false, new TupleOrder(TupleOrder.Descending), outFilter, projection1, 6,number);
//        try {
//        	System.out.println("---------Top Sort Merge Called :printing top---------- "+number);
//            while((t2=tsp.get_TopRanked())!=null){
//            	System.out.println("got tuple");
//             //t2.print(attr);
//            	}
// System.out.println("top rank join printing over");
//} catch (Exception e) {
//	e.printStackTrace();
//}
      
        
        
        
        
        
      //  FileScan2_delete f=new FileScan2_delete();
       ThresoldTopK tp= new    ThresoldTopK(2,attrTypes,len_in,strSizes,joinColumnIndex,am,type,indexNames,relNames,10,outFilter,projection,6,5);//relnames after
      // tp.sort_heapfile();
        Tuple t1=new Tuple();
        short[] str_sizes={100,100,100};
        AttrType[] attr={new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrString),new AttrType(AttrType.attrString),new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrString)};
            try {
                       while((t1=tp.get_next())!=null){
                       	System.out.println("got tuple");
                        t1.print(attr);}
            System.out.println("top rank join printing over");
		} catch (Exception e) {
			e.printStackTrace();
		}
    }

    /**
     * @param args
     * @throws Exception 
     * @throws IOException 
     * @throws UnknownKeyTypeException 
     * @throws UnknowAttrType 
     * @throws LowMemException 
     * @throws SortException 
     * @throws PredEvalException 
     * @throws TupleUtilsException 
     * @throws PageNotReadException 
     * @throws InvalidTypeException 
     * @throws InvalidTupleSizeException 
     * @throws IndexException 
     * @throws JoinsException 
     */
    public static void main(String[] args) throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, IOException, Exception {
        // TODO Auto-generated method stub
        TopRankJoinTest test = new TopRankJoinTest();
        test.runAllTests();
        test.testTopRankJoin();
        
    }

}
