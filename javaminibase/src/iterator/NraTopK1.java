/**
 * 
 */
package iterator;

import global.AttrOperator;
import global.AttrType;
import global.IndexType;
import global.RID;
import global.TupleOrder;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.SpaceNotAvailableException;
import heap.Tuple;
import index.IndexException;
import index.IndexScan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

import btree.BTreeFile;
import btree.KeyClass;
import btree.StringKey;
import bufmgr.PageNotReadException;

/**
 * @author beck
 *
 */
public class NraTopK1 extends Iterator {
	
	/* (non-Javadoc)
	 * @see iterator.Iterator#get_next()
	 */
	
	int numTables;
	int Strlen;
	AttrType[][] in;
	int[] len_in;
	short[][] s_sizes;
	int[] join_col_in;
	Iterator[] am;
	IndexType[] index;
	java.lang.String[] indNames;
	public Iterator it,st,sortiter;
	java.lang.String[] relNames;
	int n_buf_pgs;
	CondExpr[] outFilter;
	FldSpec[] proj_list;
	int n_out_flds;
	int num,brk;
	 private Heapfile f;
	 private Heapfile f1;
	 Sort sortiter1;
	 private AttrType[] resAttrTypes,s;
	private short[] str,resstr;
	private Map<String,ArrayList<float[]>> scored;
	private int counter = 0;
	FldSpec[] proj1;
	private AttrType Key_Type; 
	AttrType[] strTypes;
	FldSpec[][] project2;
	 AttrType[] sumAttr;
	 short[] heapstr;
	 float thresold_current=numTables;
	 float[] current;
	 TreeMap<Float,Integer> tm=new TreeMap<Float,Integer>();
	 BTreeFile btscores=null;
	 int treecount=0;

	/**
	 * @param numTables
	 * @param in
	 * @param len_in
	 * @param s_sizes
	 * @param join_col_in
	 * @param am
	 * @param index
	 * @param indNames
	 * @param relNames
	 * @param amt_of_mem
	 * @param outFilter
	 * @param proj_list
	 * @param n_out_flds
	 * @param num
	 * @throws UnknowAttrType
	 * @throws LowMemException
	 * @throws JoinsException
	 * @throws Exception
	 */
	public NraTopK1(int numTables, AttrType[][] in, int[] len_in,
			short[][] s_sizes, int[] join_col_in, Iterator[] am,
			IndexType[] index, String[] indNames, String[] relNames,
			int amt_of_mem, CondExpr[] outFilter, FldSpec[] proj_list,
			int n_out_flds, int num) throws UnknowAttrType, LowMemException, JoinsException, Exception {
		this.numTables = numTables;
		
		HashSet<String> checkcontain=new HashSet<String>();
		project2=new FldSpec[numTables][];
		  RID rid=new RID();
		
        for(int i=0;i<numTables;i++)
        {
      	  project2[i]=new FldSpec[len_in[i]];
      	  for(int j=0;j<len_in[i];j++)
      	  {
      		  project2[i][j]=new FldSpec(new RelSpec(RelSpec.outer),j+1);
      	  }
        }
		 proj1=new FldSpec[numTables+2];
		 for(int i=0;i<numTables+2;i++ ){
			 proj1[i]=new FldSpec(new RelSpec(RelSpec.outer),i+1);
		 }
          System.out.println("-----------------------------------this is NRATopK -----------------");
          f1 = new Heapfile("scored.in");
          int sumlen=0;
         // AttrType[] sumAttr=new AttrType[];
          for(int k:len_in)
	        {
	        	sumlen+=k;
	        }
          sumlen+=2;
          sumlen+=numTables;
		   sumAttr=new AttrType[sumlen];
		   int atcount=1;
		   sumAttr[0]=new AttrType(AttrType.attrString);
		   for(AttrType[] k:in)
		   {
			   for(AttrType p:k)
			   {
				  
				   sumAttr[atcount]=p;
				   atcount++;
			   }
		   }
		   sumAttr[atcount]=new AttrType(AttrType.attrReal);
		   for(int k=0;k<numTables;k++)
		   {
			   atcount++;
			   sumAttr[atcount]=new AttrType(AttrType.attrReal);
		   }
		   atcount=1;
		   short strlen=0;
		   for(short[] l:s_sizes)
		   {
			   strlen+=l.length;
		   }
		    heapstr=new short[strlen+1];
		    heapstr[0]=50;
		   System.out.println("length of strings in arris");
		   for(short[] l:s_sizes)
		   {
			  for(short p:l)
			  {
				  
				  heapstr[atcount]=p;
				  atcount++;
				  System.out.println(p);
			  }
		   }
          
		 Key_Type=in[0][join_col_in[0]-1];
		   current =new float[numTables];
			for (int l = 0; l < numTables; l++) 
			{
				current[l] = 1;
			}
			
			
		this.in = new AttrType[numTables][];
		
		for (int i = 0; i < numTables; i++) {
			
			AttrType[] _in = in[i];
			
			this.in[i] = new AttrType[_in.length];
			
			System.arraycopy(_in, 0, this.in[i], 0, _in.length);
			
		}
		
		 f1=new Heapfile("scored.in");
		 String IndexNames="Scored.bt";
		 String Relation="scored.in";
         strTypes=new AttrType[numTables+2];
		 strTypes[0]=new AttrType(AttrType.attrString);
		 
		 try{
         btscores = new  BTreeFile("scored.bt", AttrType.attrString, 50, 1);
             }
		 catch(Exception e)
		 {
			 e.printStackTrace();
		 }
		 
        	
//        
//		for(int i=1;i<numTables+2;i++)
//		{
//			strTypes[i]=new AttrType(AttrType.attrReal);
//		}
		 
		str=new short[1];
		str[0]=100;
		this.len_in = len_in;
		this.s_sizes = s_sizes;
		this.join_col_in = join_col_in;
        this.am = am;
		this.index = new IndexType[index.length];
		System.arraycopy(index, 0, this.index, 0, index.length);
        this.indNames = new String[indNames.length];
		System.arraycopy(indNames, 0, this.indNames, 0, indNames.length);
		this.relNames = new String[relNames.length];
		System.arraycopy(relNames, 0, this.relNames, 0, relNames.length);
        this.n_buf_pgs = amt_of_mem;
        this.outFilter = outFilter;
		this.proj_list = proj_list;
		this.n_out_flds = n_out_flds;
		this.num = num;
		this.brk=num;
	  // scored=new HashMap<String,ArrayList< float[]>>();
	   Tuple t=new Tuple();
	   int count=0;
	   boolean flagcheck=true;
	   FldSpec[] projection=null;
	   for(float i=0;i<num;i++)
	   {
	   tm.put((float) 0,0);
	   }
	   thresold_current=numTables;
	   while(thresold_current>tm.firstKey())
	   {
		   Tuple temp;
			int col;
			String key = null;
			 RID rid1;
			for (int i = 0; i < numTables; i++) {
				try {
					temp = am[i].get_next();
					col = join_col_in[i];
		             temp.print(in[i]);	
		             current[i]=temp.getScore();
		            // TreeMap<String ,Float[]> nra=new TreeMap<String,Float[]>();
					
					if (in[i][col - 1].attrType == AttrType.attrInteger) {
						key = String.valueOf(temp.getIntFld(col));
					} else if (in[i][col - 1].attrType == AttrType.attrString) {
						key = temp.getStrFld(col);
					} else if (in[i][col - 1].attrType == AttrType.attrReal) {
						key = String.valueOf(temp.getFloFld(col));
					}
					if(checkcontain.contains(key))
					{
						System.out.println("key received by NRA_key_By_access is:" + key);
						
						 projection = new FldSpec[sumlen];
						CondExpr[] expr = new CondExpr[2];
						expr[0] = new CondExpr();
						expr[0].op = new AttrOperator(AttrOperator.aopEQ);
						expr[0].type1 = new AttrType(AttrType.attrSymbol);
						expr[0].type2 = new AttrType(sumAttr[0].attrType);
						expr[0].next = null;
						expr[1]  = null;// while((tup=sortiter1.get_next())!=null)// while((tup=sortiter1.get_next())!=null)
						expr[0].operand2.string = key;
						expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),
								1);
						projection=new FldSpec[sumlen];
						for(int j=0;j<sumlen;j++)
						{
							
									projection[j] = new FldSpec(new RelSpec(RelSpec.outer),
											j+1);
						}
						  IndexType b_index = new IndexType(IndexType.B_Index);
						IndexScan iscan = null;
						try
						{
							iscan = new IndexScan(b_index, "scored.in","scored.bt" ,
									sumAttr, heapstr, sumlen,sumlen,projection, expr,
										1, false);//
						}
						catch(Exception e)
						{
							e.printStackTrace();
							
						}
						
						Tuple tf=new Tuple();
						tf.setHdr((short)sumlen, sumAttr, heapstr);
						
						Tuple t2;
						if((t2=iscan.get_next())!=null)
						{
							System.out.println();
							tf.tupleCopy(t2);
							int indexofsc=0;
							int indexcheck=0;
						for(int j=0;j<i;j++)
						{
							indexofsc+=len_in[j];
						}
							t2.print(sumAttr);
						indexofsc++;
						indexcheck=sumlen-(numTables-(i+1));
						float score=tf.getFloFld(indexcheck);//may not work check for the other values
							if(score==-1)
							{
//								indexofsc-=len_in[i];
								for(int j=indexofsc+1;j<indexofsc+len_in[i]+1;j++)
								{
									if(sumAttr[j-1].attrType==AttrType.attrInteger)
									{
									tf.setIntFld(j, temp.getIntFld(j-indexofsc));
									}
									if(sumAttr[j-1].attrType==AttrType.attrString)
									{
										tf.setStrFld(j, temp.getStrFld(j-indexofsc));
									}
									if(sumAttr[j-1].attrType==AttrType.attrReal)
									{
										tf.setFloFld(j, temp.getFloFld(j-indexofsc));
									}
								}
								tf.setFloFld(indexcheck,temp.getScore());
								float tupscore=0.0f;
								tupscore=tf.getFloFld(sumlen-numTables);
								
								RID r;
								r=iscan.getrid();
								
								//RID r2=new RID(r.pageNo,r.slotNo);
								
                                System.out.println("priorvufsudcydf yu"+r.slotNo);		
                                System.out.println("page of :db v jhdihv"+r.pageNo);
								//f1.updateRecord(r, tf);
								System.out.println(f1.deleteRecord(r));
								tf.print(sumAttr);

								RID r1=f1.insertRecord(tf.getTupleByteArray());
								//btscores.Delete(new StringKey(tf.getStrFld(1)), r1);
                              btscores.insert(new StringKey(tf.getStrFld(1)), r1)	;							
							}
							
							else
							{
								//duplicate record condition****************
	
								Tuple t_dup=new Tuple();
								t_dup.setHdr((short)sumlen, sumAttr, heapstr);
								t_dup.tupleCopy(tf);
								float lastscore=0.0f;
								int indexst=0;
								for(int j=0;j<i;j++)
								{
									indexst+=len_in[j];
								}
								indexst++;
							
								for(int j=indexst+1, k=1;k<len_in[i]+1;j++,k++)//j<indexst+len_in[i]
								{
									if(sumAttr[j-1].attrType==AttrType.attrInteger)
									{
									t_dup.setIntFld(j, temp.getIntFld(k));//	tf.setIntFld(j, temp.getIntFld(j-indexst));
									}
									else if(sumAttr[j-1].attrType==AttrType.attrString)
									{
										t_dup.setStrFld(j, temp.getStrFld(k));//	tf.setStrFld(j, temp.getStrFld(j-indexst));
									}
									else if(sumAttr[j-1].attrType==AttrType.attrReal)
									{
										t_dup.setFloFld(j, temp.getFloFld(k));//tf.setFloFld(j, temp.getFloFld(j-indexst));
									}
								}
								lastscore=t_dup.getFloFld(indexcheck);
								t_dup.setFloFld(indexcheck,temp.getScore());
//								if(tm.containsKey(temp.getScore()+tf.getFloFld(sumlen-numTables)))
//								{//tf.setFloFld(sumlen, temp.getScore()+tf.getFloFld(sumlen));
//									tm.put(temp.getScore()+tf.getFloFld(sumlen-numTables)-lastscore, tm.get(temp.getScore()+tf.getFloFld(sumlen-numTables)-lastscore)+1);
//								}
//								else
//								{
//									tm.put(temp.getScore()+tf.getFloFld(sumlen-numTables)-lastscore, 1);
//								}
							t_dup.setFloFld(sumlen-numTables, temp.getScore()+tf.getFloFld(sumlen-numTables)-lastscore);
							f1.insertRecord(tf.getTupleByteArray());	}
							
//							//						
//						
						}
					}
					else
					{
						checkcontain.add(key);
						Tuple tf1=new Tuple();
						tf1.setHdr((short)sumlen, sumAttr, heapstr);
						tf1.setStrFld(1, key);
						for(int  j=1;j<sumlen;j++)
						{
							if(sumAttr[j].attrType==AttrType.attrInteger)
							{
							tf1.setIntFld(j+1, -1);
							}
							if(sumAttr[j].attrType==AttrType.attrString)
							{
								tf1.setStrFld(j+1, "val");
							}
							if(sumAttr[j].attrType==AttrType.attrReal)
							{
								tf1.setFloFld(j+1, -1.0f);
							}
							
						}
		                  tf1.print(sumAttr);
						int indexst=0;
						for(int j=0;j<i;j++)
						{
							indexst+=len_in[j];
						}
						indexst++;
					
						for(int j=indexst+1, k=1;k<len_in[i]+1;j++,k++)//j<indexst+len_in[i]
						{
							if(sumAttr[j-1].attrType==AttrType.attrInteger)
							{
							tf1.setIntFld(j, temp.getIntFld(k));//	tf.setIntFld(j, temp.getIntFld(j-indexst));
							}
							else if(sumAttr[j-1].attrType==AttrType.attrString)
							{
								tf1.setStrFld(j, temp.getStrFld(k));//	tf.setStrFld(j, temp.getStrFld(j-indexst));
							}
							else if(sumAttr[j-1].attrType==AttrType.attrReal)
							{
								tf1.setFloFld(j, temp.getFloFld(k));//tf.setFloFld(j, temp.getFloFld(j-indexst));
							}
						}
						tf1.setFloFld(sumlen-(numTables-(i+1)), temp.getScore());
						tf1.setFloFld(sumlen-(numTables), temp.getScore());
						
						System.out.println("this is not the accurate information");
					tf1.print(sumAttr);
					try{
					rid1=f1.insertRecord(tf1.returnTupleByteArray());
					System.out.println(key);
					 btscores.insert(new StringKey(tf1.getStrFld(1)), rid1);
					 System.out.println("page no:"+rid1.pageNo);
					 System.out.println(rid1.slotNo);
					Tuple te=new Tuple();
				    te.setHdr((short)sumlen, sumAttr, heapstr);
				    te=f1.getRecord(rid1);
				    te.setHdr((short)sumlen, sumAttr, heapstr);
				    }
					 catch(Exception e)
					    {
						e.printStackTrace();
					    }
					}
				    
					}				
					catch(Exception e)
			    {
				e.printStackTrace();
			    
				}
				
	//		boolean fla
	  // if(fla)
	//   FileScan F1=new FileScan("scored.in",sumAttr,heapstr,(short)sumlen,sumlen,projection,null);
//	   Tuple t1=new Tuple();
//	   t1.setHdr((short)sumlen, sumAttr, heapstr);
//	   int count5=0;
//	   System.out.println("printini");
//	   while((t1=F1.get_next())!=null)
//	   {
//		   count5++;
//		   t1.print(sumAttr);
//	   }
//	   if(count5==1)
//	   {
//		 System.out.println("only one tuple in the heapfile");  
//	   }
//	   else
//	   {
			}
	   FileScan F2=new FileScan("scored.in",sumAttr,heapstr,(short)sumlen,sumlen,projection,null);
	   
	   Sort sortiter=new Sort(sumAttr,(short)sumlen,heapstr,F2,sumlen-numTables,new TupleOrder(TupleOrder.Descending),4,10);
	   Tuple t12=new Tuple();
		t12.setHdr((short)sumlen,sumAttr, heapstr);
		while((t12=sortiter.get_next())!=null)
		{
			t12.print(sumAttr);
		}
	   // Sort srtr=new Sort(sumAttr,(short)sumlen,heapstr,F2,sumlen,new TupleOrder(TupleOrder.Descending),4,10);
//	   /*
//	    * public Sort(AttrType[] in,
//            short len_in,
//            short[] str_sizes,
//            Iterator am,
//            int sort_fld,
//            TupleOrder sort_order,
//            int sort_fld_len,
//            int n_pages)*/
//	  int count4=0;
//	  Tuple tp=new Tuple();
//	  Tuple tes=new Tuple();
//	  
//	  tp.setHdr((short)sumlen, sumAttr, heapstr);
//	  tes.setHdr((short)sumlen, sumAttr, heapstr);
//			thresold_current=0;
//	  for(float i1:current)
//	  {
//		  thresold_current+=i1;  
//	  }
//	  boolean flag=true;
//	  while(count4!=num||((tp=sortiter.get_next())!=null)||flag)
//	  {
//		  
//		  System.out.println("vtrxdctbfvnygbhunj");
//		  tes.tupleCopy(tp);
//		  //tp.print(sumAttr);
//		  if(tp.getFloFld(sumlen-numTables)<thresold_current)
//		  {
//			  flag=false;
//		  }
//		  else
//		  {
//			  count4++;
//		  }
//					  
//	  }
//	  count=count4;
	  
	   
	   /*
            sm[1] = new FileScan("reserves.in", attrTypes[1],
                    strSizes[1], (short)4, 4, reservesProjection, null);
            am[1]=new Sort(attrTypes[1],(short)4,strSizes[1], sm[1], 4,new TupleOrder(TupleOrder.Descending) , 4, 10);
	    * */
	   
	   
					
	   
			}
	   
				
				
				
	   
	   System.out.println("stopping condition reached");}
			
	   
//	FileScan F2=new FileScan("scored.in",sumAttr,heapstr,(short)sumlen,sumlen,projection,null);
//	Heapfile h1=new Heapfile("scored1.in");
//	Tuple t1=new Tuple();
//	t1.setHdr((short)sumlen,sumAttr, heapstr);
//	while((t1=F2.get_next())!=null)
//	{
//		
//	
//		h1.insertRecord(t1.getTupleByteArray());
//	}
//	FileScan F12=new FileScan("scored.in",sumAttr,heapstr,(short)sumlen,sumlen,projection,null);
//	Sort sortiter=new Sort(sumAttr,(short)sumlen,heapstr,F12,sumlen-numTables,new TupleOrder(TupleOrder.Descending),4,10);
//	Tuple t12=new Tuple();
//	t12.setHdr((short)sumlen,sumAttr, heapstr);
//	while((t12=sortiter.get_next())!=null)
//	{
//		t12.print(sumAttr);
//	}
	
	 //old place:stopping condition
					
//					float score = temp.getScore();
//					System.out.println(score);
//					float s_value[];
//						
//					
//				//	ArrayList<float[]> al;
//					if (scored.containsKey(key)){
//	                   = scored.get(key);
//	                System.out.println("key is present");
//	                  for(float[] a:al)
//	                  { System.out.println("printing float data");
//	                	  if(a[i]==-1)
//	                	  {
//	                		  System.out.print("score is:"+score+"in index"+i);
//	                		  a[i]=score;
//	                		  a[numTables+1]++;
//	                		  System.out.println("printing data");
//	                		  System.out.println("");
//	                		  System.out.println("");
//	                		  System.out.println("");
//	                		  System.out.println("");
//	                		  
//	                		  for(float k:a)
//	                    	  {
//	                    		System.out.println(k);  
//	                    	  }
//	                	  }
//	                	
//	                	//  System.out.println("");
//	                	  else
//	                	  {
//	                		  float[] s_value1=new float[numTables+2];
//	                		  System.arraycopy(a, 0, s_value1,0, a.length);
//	                		  s_value1[i]=score;
//	                		  al.add(s_value1);
//	                		  
//	                	  }
//	                	 
//	                  }
		   
		   
		   
		   
		   
		   
		   
		   
		   
				
	   
//	   
//		for(Object o: this.scored.keySet())
//		{  
//	        float sco=0.0f;
//			Tuple tup=new Tuple();
//            tup.setHdr((short) (numTables+2), strTypes, str);
//			ArrayList<float[]> k = this.scored.get(o);
//			float[] value;
//			for(int j=0;j<k.size();j++)
//			{
//				value=k.get(j);
//				sco=0.0f;
//		    if(value[value.length-2]==numTables)
//			{
//	
//		    	tup.setStrFld(1, (String)o);
//		    	
//			for(int l=0;l< numTables;l++)
//			{
//				
//				sco += value[l];
//				tup.setFloFld(l+2,value[l]);
//				
//			}
//			
//			tup.setFloFld(numTables+2, sco);
//			tup.print(strTypes);
//			f1.insertRecord(tup.getTupleByteArray());
//			
//			}
//		    
//	      }
//		
//     Iterator   it1=new FileScan("scored.in",strTypes,str,(short) (numTables+2),(numTables+2),proj1,null);
//    
//       
//         sortiter1=new Sort(strTypes,(short) (numTables+2),str,it1,(numTables+2),new TupleOrder(TupleOrder.Descending),4,15);
//   	}
	
	
	

//
//	private void sequential_access() {
//		Tuple temp;
//		int col;
//		String key = null;
//
//		for (int i = 0; i < numTables; i++) {
//			try {
//				temp = am[i].get_next();
//				col = join_col_in[i];
//	             temp.print(in[i]);			
//	             TreeMap<String ,Float[]> nra=new TreeMap<String,Float[]>();
//				
//				if (in[i][col - 1].attrType == AttrType.attrInteger) {
//					key = String.valueOf(temp.getIntFld(col));
//				} else if (in[i][col - 1].attrType == AttrType.attrString) {
//					key = temp.getStrFld(col);
//				} else if (in[i][col - 1].attrType == AttrType.attrReal) {
//					key = String.valueOf(temp.getFloFld(col));
//				}
//				if(nra.containsKey(key))
//				{
//					
//				}
//				float score = temp.getScore();
//				System.out.println(score);
//				float s_value[];
//					
//				
//			//	ArrayList<float[]> al;
//				if (scored.containsKey(key)){
//                   = scored.get(key);
//                System.out.println("key is present");
//                  for(float[] a:al)
//                  { System.out.println("printing float data");
//                	  if(a[i]==-1)
//                	  {
//                		  System.out.print("score is:"+score+"in index"+i);
//                		  a[i]=score;
//                		  a[numTables+1]++;
//                		  System.out.println("printing data");
//                		  System.out.println("");
//                		  System.out.println("");
//                		  System.out.println("");
//                		  System.out.println("");
//                		  
//                		  for(float k:a)
//                    	  {
//                    		System.out.println(k);  
//                    	  }
//                	  }
//                	
//                	//  System.out.println("");
//                	  else
//                	  {
//                		  float[] s_value1=new float[numTables+2];
//                		  System.arraycopy(a, 0, s_value1,0, a.length);
//                		  s_value1[i]=score;
//                		  al.add(s_value1);
//                		  
//                	  }
//                	 
//                  }
//                  
//                  }
//				
//				
//
//				else {
//					 al=new ArrayList<float[]>();
//					
//					s_value = new float[numTables + 2];
//					for (int f = 0; f < numTables; f++) {
//						s_value[f] = -1;
//					}
//					s_value[i]=score;
//					System.out.println("the float array");
//					for(float l:s_value)
//					{
//						System.out.println(l);
//					}
//					al.add(s_value);
//					
//
//				}
//				scored.put(key,al);

//			if(s_value[i]==-1)
//			{
//				s_value[i] =score;
//				s_value[numTables] +=1;
//			}
			
//				for(int k=0;k<numTables;k++)
//				{
//					
//				}
//				
//             scored.put(key, s_value);

//			if (s_value[numTables] >= numTables)
//					counter = counter + 1;
//				System.out.println("value of the counter is"+counter);
//
//				if (counter >= num)
//					break;

				
				
				
				
			
			
		

	


	public Tuple get_next() throws SortException, UnknowAttrType, LowMemException, JoinsException, IOException, Exception {
		String key=new String();
		Tuple tempTuple=new Tuple();
		Tuple TupleTuple1=new Tuple();
		
		if(((TupleTuple1=sortiter1.get_next())!=null) &&(brk!=0))
		{
		brk--;
		key=(String)(TupleTuple1.getStrFld(1));
		Tuple[] tupleArray = new Tuple[this.numTables];
			for (int i = 0; i < tupleArray.length; i++) {
				tupleArray[i]=getTupleByScore(i,TupleTuple1.getFloFld(i+1), key, Key_Type.attrType);  
				}

		try {
			resAttrTypes=new AttrType[n_out_flds];
			for(int i=0;i<n_out_flds;i++)
			{
				resAttrTypes[i]=in[proj_list[i].relation.key][proj_list[i].offset-1];
				
			}

			resstr=TupleUtils.setup_op_tuple(tempTuple,resAttrTypes, in, len_in, s_sizes, proj_list,n_out_flds);
				
				Projection1.Join(tupleArray,in,tempTuple, this.proj_list, this.n_out_flds);//this.sortedScoreMap.get(key)
				if(tempTuple==null)
				{
					System.out.println("no data in the tuple");
				}
				else
				{
				
				tempTuple.print(resAttrTypes);}
			System.out.println(tempTuple.getScore());
			
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}
		
		}
		else
		{
			tempTuple=null;
			System.out.println("print the waste data");
		}
		return tempTuple;
	}
	
	private Tuple getTupleByScore(int tableIndex,float score, String key, int attrType) {
		Tuple tuple = null;
		IndexScan iscan = null;
		FldSpec[] projection = new FldSpec[len_in[tableIndex]];
		CondExpr[] expr = new CondExpr[2];
		expr[0] = new CondExpr();
		expr[0].op = new AttrOperator(AttrOperator.aopEQ);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].type2 = new AttrType(attrType);
		expr[0].next = null;
		expr[1] = null;
//        boolean flag1=true;
		int ikey;
		float rkey;

		if (attrType == AttrType.attrInteger) {
			ikey = Integer.parseInt(key);
			expr[0].operand2.integer = ikey;
		}

		else if (attrType == AttrType.attrReal) {
			rkey = Float.parseFloat(key);
			expr[0].operand2.real = rkey;

		} else {
			expr[0].operand2.string = key;
		}

		for (int i = 0; i < len_in[tableIndex]; i++) {
			projection[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);			
		}

		// fix the field on which to be indexed
		expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),
				join_col_in[tableIndex]);

		try {
			iscan = new IndexScan(index[tableIndex], relNames[tableIndex], indNames[tableIndex], in[tableIndex],
					s_sizes[tableIndex], len_in[tableIndex], len_in[tableIndex], projection, expr, join_col_in[tableIndex],
					false);
		} catch (Exception e) {

			e.printStackTrace();
		}
		try {
			tuple=iscan.get_next();
			while(tuple.getScore()!=score){
			tuple = iscan.get_next();
			}
			System.out.println("my score is:"+tuple.getScore());
		} catch (Exception e) {
			e.printStackTrace();
		}
	return tuple;
	}
	


	/* (non-Javadoc)
	 * @see iterator.Iterator#close()
	 */
	@Override
	public void close() throws IOException, JoinsException, SortException,
			IndexException {
		// TODO Auto-generated method stub

	}

}