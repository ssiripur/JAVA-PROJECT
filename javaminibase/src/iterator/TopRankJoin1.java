package iterator;
import heap.*;
import global.*;
import btree.*;
import bufmgr.*;
import diskmgr.*;
import index.*;

import java.lang.*;
import java.util.ArrayList;
import java.util.Map;
import java.io.*;
//import 

public class TopRankJoin1 extends Iterator {
	
	
	
	/*
	int numTables;
	AttrType[][] in;
	int[] len_in;
	short[][] s_sizes;
	int[] join_col_in;
	Iterator[] am;
	IndexType[] index;
	java.lang.String[] indNames;

	// make sure if this should be included
	java.lang.String[] relNames;
	int n_buf_pgs;
	CondExpr[] outFilter;
	FldSpec[] proj_list;
	int n_out_flds;
	int num;

	private Tuple joinTuple;
	private AttrType[] resAttrTypes;
	private short[] str_sizes;

	private Map<String, float[]> scoreMap;
	private Map<String, Float> sortedScoreMap;
	private int[] scoreField;
	private int counter = 0;
	*/
	
	
	
	
	private int numTables1;
	private AttrType[][] in1;
	private int[] len_in;
	private short[][] s_sizes;
	private int[] join_col_in;
	private Iterator[] am1;
	private String[] relnames;
	private IndexType[] index1;
	private java.lang.String[] indNames1;
	private int amt_of_mem1;
	private CondExpr[] outFilter;
	private FldSpec[] proj_list1;
	private int n_out_flds1;
	private int num1;
	short[] s;
	//private IndexType[] index;
	
	private AttrType joinkey_AttrType;
	private Object joinkey_Value;
	private int joinkey_ssize;
	
	private ArrayList<joinKeyObject> joinKey_Objects;
	private ArrayList<Tuple> join_candidates; 
	BTreeFile[] bt;
	CondExpr[][] out;
	//IndexScan is;
	Iterator ir,ir1;
	 AttrType[] at;

	

	public TopRankJoin1 (int numTables,AttrType[][] in,int[] len_in,short[][] s_sizes,int[] join_col_in,
			Iterator[] am,IndexType[] index,java.lang.String[] indNames,String[] relnames,int amt_of_mem,CondExpr[] outFilter,FldSpec[] proj_list,
			int n_out_flds,int num) throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, IOException, Exception
	
	  {
	
		// Testing.
		for (int i = 0; i < numTables; i++) {
			Tuple tuple=new Tuple();
			
			try {
				tuple.setHdr((short) len_in[i], in[i], s_sizes[i]);
				
				Tuple tempTuple=null;
				while((tempTuple=am[i].get_next())!=null){
					tuple.tupleCopy(tempTuple);
					tuple.print(in[i]);
				}
			} catch (Exception e) {
				// TODO: handle exception
			}
			
		}
		
		
		
		
		
		
		
		//copy
	//	this.index=index;
		this.relnames=relnames;
		numTables1=numTables;
		 in1=in;
		 this.len_in=len_in;
		 this.s_sizes= s_sizes;
		 this.join_col_in=join_col_in;
		am1=am;
		index1=index;
		indNames1= indNames;
		this.outFilter= outFilter;
		proj_list1= proj_list;
		n_out_flds1= n_out_flds;
		num1=num;
		 amt_of_mem1=amt_of_mem;
	    out = new CondExpr[numTables][];
	// out[0]=new CondExpr[numTables];
		
	    for(int i=0;i<numTables;i++)
		{
	    out[i]=new CondExpr[2];
	    out[i][0]=null;
	    out[i][1]=null;
	   // out[i]=new CondExpr[1];
	 // out[i][1]=null;
		}
		
	  bt=new BTreeFile[numTables];
//	 for(int i=0;i<numTables;i++)
//	 {
//	 System.out.println(relnames[i]);
//	 }
		Tuple tempTuple=new Tuple();
	    joinKey_Objects = new ArrayList<joinKeyObject>();
		int tIndex = 0;
		int count_Reached = 0;
        int joinSize=4;
			while (count_Reached<num&& (tempTuple=am[tIndex].get_next())!=null)
			{
			  //tempTuple = am[tIndex].get_next();
			//  System.out.println("temp tuple:"+ tempTuple);
			  joinkey_AttrType = in[tIndex][join_col_in[tIndex]];
			
			 if (joinkey_AttrType.attrType == AttrType.attrInteger) 
			 	{					
				  joinkey_Value = tempTuple.getIntFld(join_col_in[tIndex]);
				  
	            } 
			 else if (joinkey_AttrType.attrType == AttrType.attrReal)
			 	{
					joinkey_Value = tempTuple.getFloFld(join_col_in[tIndex]);
			 	}
			  else if (joinkey_AttrType.attrType == AttrType.attrString) 
			 	{
				joinkey_Value = tempTuple.getStrFld(join_col_in[tIndex]);
				
				int i= 0;
				for (i = 0; i < in[tIndex].length; i++) {
					if (in[tIndex][i].attrType == AttrType.attrString&&i==join_col_in[tIndex]) {
			             break;
						}
                  }
				joinkey_ssize = s_sizes[tIndex][0];
				joinSize=joinkey_ssize ;
			 }
			
			int keyIndex = isKey_Present(joinkey_Value,joinKey_Objects);
			joinKeyObject new_joinKeyObject;
			if (keyIndex == -1) 
				{
				  new_joinKeyObject = new joinKeyObject(numTables,joinkey_Value,joinkey_AttrType, joinSize );
	              TupleUtils.setup_op_tuple(new_joinKeyObject.tuple,new AttrType[n_out_flds], in, s_sizes, proj_list,n_out_flds);
				  new_joinKeyObject.setScore(tIndex,tempTuple.getScore());
			      joinKey_Objects.add(new_joinKeyObject);
			      //System.out.println();
				} else 
					{
						new_joinKeyObject = joinKey_Objects.get(keyIndex);
						new_joinKeyObject.setScore(tIndex,tempTuple.getScore());
					}

		    if (new_joinKeyObject.isFull())
			count_Reached++;

		    tIndex = ++tIndex % numTables;
		}
			
			// sorted access end
			
			for(int i=0;i<numTables;i++)
			{
				bt[i]=new BTreeFile(relnames[i],joinkey_AttrType.attrType,100,1);
			}
			//ArrayList<joinKeyObject> unfilled=new ArrayList<joinKeyObject>();
			for(joinKeyObject jko :joinKey_Objects)
			{
				if(jko.isFull())
				{
				
				}
				else
					{
				for(int i=0;i<numTables1;i++)
				{
			       if(jko.score[i]==-1)
			       {
			    	   this.fillscore(jko,i);
			       }
			       }
				if(jko.isFull())
				{
					count_Reached++;
				}
				}
			}
	  }		
		
//	public Tuple get_next()
//	{
//		Tuple t=new Tuple(); 
//	return t;	
//	}
//	
//		
	

	private int isKey_Present(Object key, ArrayList<joinKeyObject> joinKeyObjects)
	{

		for (int i=0; i <joinKeyObjects.size(); i++) {
			if (joinKeyObjects.get(i).keyval==key)
				return i;
		}
		
		return -1;
	}
	
	public Tuple get_next()
		    throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception
		    {
		      Tuple t =new Tuple();
		      Tuple fina=new Tuple();
		      Tuple tuple[]=new Tuple[numTables1];
		      t.setHdr((short)2, at, s);
		     
		      t=ir1.get_next();
		     // System.out.println(t);
		      
		      if(t==null){
		    	  return null;
		      }
		     
		      Object o=null;
		      
		      if(at[0].attrType==AttrType.attrInteger)
		      {
		    	o=t.getIntFld(0);  
		      }
		      if(at[0].attrType==AttrType.attrString)
		      {
		    	o=t.getStrFld(0);  
		      }
		      
		   //  FldSpec[] proj_list=new FldSpec[];
		      for(int i=0;i<numTables1;i++)
		      {
		    	  FldSpec[] proj_list=new FldSpec[len_in[i]];
		    	  for(int j=0;j<len_in[i];j++)
		    	  {
		    	  proj_list[j]=new FldSpec (new RelSpec(RelSpec.outer),j);
		    	  }
		    	  out[i][0].next=null;
		  		out[i][0].op=new AttrOperator(AttrOperator.aopEQ);
		  		out[i][0].type1=new AttrType(AttrType.attrSymbol);
		  		out[i][0].type1=new AttrType(AttrType.attrSymbol);
		  		out[i][0].operand1.symbol=new FldSpec((new RelSpec(RelSpec.outer)),join_col_in[i]);
		  		if(at[0].attrType==AttrType.attrInteger)
		  		{
		  		out[i][0].operand2.integer=(Integer)o;
		  		}
		  		if(at[0].attrType==AttrType.attrString)
		  		{
		  		out[i][0].operand2.string=(String)o;
		  		}
		    	  
		    	 IndexScan is=new IndexScan(index1[i],relnames[i],
		                indNames1[i],in1[i],s_sizes[i],len_in[i],1,proj_list,out[i],join_col_in[i],false); 
		    	 tuple[i]=is.get_next();
		    	 tuple[i].setHdr((short)len_in[i], in1[i], s_sizes[i]);
		    	 
		      }
		      Projection.Join(tuple, in1, fina,proj_list1,n_out_flds1);
		      
				return fina;
		
		    }
	public void fillscore(joinKeyObject k,int i) throws IndexException, InvalidTypeException, InvalidTupleSizeException, UnknownIndexTypeException, IOException, UnknownKeyTypeException, FieldNumberOutOfBoundException
	{
	
	
		FldSpec[] proj_list=new FldSpec[]{new FldSpec (new RelSpec(RelSpec.outer),join_col_in[i])};
		//AttrType[] keyattr1=new AttrType[]{k.joinKeyType};
		
		//BTreeFile bt=new BTreeFile(indNames1[i],k.joinKeyType.attrType,k.key_size,1);
		//BTFileScan bs=bt.new_scan((KeyClass)k.keyval,(KeyClass)k.keyval);
	//	IndexScan i;
		out[i][0] = new CondExpr();
		out[i][0].next=null;
		out[i][0].op=new AttrOperator(AttrOperator.aopEQ);
		out[i][0].type1=new AttrType(AttrType.attrSymbol);
		out[i][0].type1=new AttrType(AttrType.attrSymbol);
		out[i][0].operand1.symbol=new FldSpec((new RelSpec(RelSpec.outer)),join_col_in[i]);
		if(k.joinKeyType.attrType==AttrType.attrInteger)
		{
		out[i][0].operand2.integer=(Integer)k.keyval;
		}
		if(k.joinKeyType.attrType==AttrType.attrString)
		{
		out[i][0].operand2.string=(String)k.keyval;
		}
		
		
IndexScan is=new IndexScan(index1[i],relnames[i],indNames1[i],in1[i],s_sizes[i],len_in[i],1,proj_list,null,1,!true);
	
               // Heapfile f=new Heapfile(indNames1[i]);//
                Tuple tuple=new Tuple();
                tuple.setHdr((short)len_in[i],in1[i], s_sizes[i]);
                tuple=is.get_next();
                
				k.score[i]=tuple.getScore();
		}
		
		
		
	
	
	
	public void sort()throws Exception
			{
		 Heapfile f = new Heapfile("temper.in");
			 int 	ts_size =0;
	
			 Tuple[] tup=new Tuple[numTables1];
			 //Tuple f=new Tuple();
			 
			 Tuple tupl=new Tuple();
			 
			 at=new AttrType[2];
			 at[0]=joinkey_AttrType;
			 at[1]=new AttrType(AttrType.attrReal);
			 s=new short[1];
			//  s=null;
			 if(joinkey_AttrType.attrType==AttrType.attrString)
			 {
				 s=new short[1];
				 s[0]=(short)joinkey_ssize;
				
			 }
		
				 
			 for(int i=0;i<joinKey_Objects.size();i++)
			 {
				 joinKeyObject j=joinKey_Objects.get(i);
				// Object o=j.keyval;
				 //short[] str_sizes;
				 short[] str_sizes={(short) j.key_size};
				 tupl.setHdr((short)2, at,str_sizes);
				 if(joinkey_AttrType.attrType==AttrType.attrString)
					{
						tupl.setStrFld(1,(String)j.keyval);
						s[0]=(short)j.key_size;
					}
            						 
				 if(joinkey_AttrType.attrType==AttrType.attrReal)
					{
					 tupl.setFloFld(1,(Float)j.keyval);
					}
            	 tupl.setFloFld(2, j.avgscore);
            	 try
            	 {
            		 tupl.print(at);
            		 
            		 System.out.print("new");
            		 
            	 RID rid=f.insertRecord(tupl.returnTupleByteArray());
           
            	 }catch(Exception e){}
			 }
            	 

			 FldSpec[] fld=new FldSpec[2];
			 fld[0]=new FldSpec(new RelSpec(RelSpec.outer),0);
			 fld[0]=new FldSpec(new RelSpec(RelSpec.outer),1);
            	 ir = new FileScan("temper.in", at,s,(short)2,(short)1,fld, null);
            	// System.out.println(ir.get_next());
            	 ir1= new Sort(at, (short)2, s
 	    				, ir,
 	    				(short)2,
 	    				  new TupleOrder(TupleOrder.Descending), 1,amt_of_mem1);	
 	    
     			
   			
			 }






	@Override
	public void close() throws IOException, JoinsException, SortException,
			IndexException {
		// TODO Auto-generated method stub
		
	}
			 
			 
		 }