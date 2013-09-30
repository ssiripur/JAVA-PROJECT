package iterator;
import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;

import java.lang.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.io.*;

import btree.*;

public class TopRanks extends Iterator {
private int numTables1;
private AttrType[][] in;
private int[] len_in;
private short[][] s_sizes;
private int[] join_col_in;
private Iterator[] am1;
private IndexType[] index1;
private java.lang.String[] indNames1;
private TupleOrder order1;
private int amt_of_mem1;
private CondExpr[] outFilter;
private FldSpec[] proj_list1;
private int n_out_flds1;
private int num1;
private short[] t_size,t1_str_sizes,t2_str_sizes, ssize;
BTreeFile[] bt;
public Tuple Jtuple,t,tempTuple,t1;
BTreeFile btf;
public AttrType[] Jtypes;
BTFileScan btfs;
BTFileScan[] bts;
KeyDataEntry kde;
KeyClass lo;
LeafData de;
Object o;
RID r;
Iterator iter,iter_heap;
AttrType[] s = null;
Heapfile f;
Integer p=(int)Math.random();
String k="AAA"+p.toString();
Tuple out[];
//private Scan scan;
public TopRanks (int numTables,
		AttrType[][] in,
		int[] len_in,
		short[][] s_sizes,
		int[] join_col_in,
		Iterator[] am,
		IndexType[] index,
		java.lang.String[] indNames,
		int amt_of_mem,
		CondExpr[] outFilter,
		FldSpec[] proj_list,
		int n_out_flds,
		int num) throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, IOException, Exception
		{
	
		numTables1=numTables;
		proj_list1=proj_list;
		num1=num;
	int tIndex=0;
	Tuple temp=null;
	float sc;
	AttrType join_keyAttribute;
		int joinkey_ssize;
		
		
		
		btf=new BTreeFile(k,in[tIndex][join_col_in[tIndex]].attrType,100,1);
		btfs=btf.new_scan(lo,lo);
		f=new Heapfile(k);
		for(tIndex=0;tIndex<numTables;tIndex++)
			{
				bt[tIndex]=new BTreeFile(indNames[tIndex],in[tIndex][join_col_in[tIndex]].attrType,100,1);
			}

		int countReached=0;
		while(countReached<num)//Sorted Access
		{
		tempTuple = am[tIndex].get_next();
		join_keyAttribute=in[tIndex][join_col_in[tIndex]];
		for(int i=1;i<=numTables;i++)
		{	
		s[i]=new AttrType (AttrType.attrReal);
		}
		btfs=btf.new_scan(lo, lo);
		if((kde=btfs.get_next())==null)
		{
		if(join_keyAttribute.attrType==AttrType.attrInteger)
			{
				s[0]=new AttrType(join_keyAttribute.attrType);
				ssize[0]=0;
				t.setHdr((short)(numTables+1),s,ssize);
				t.setIntFld(0, tempTuple.getIntFld(join_col_in[tIndex]));
				lo=new IntegerKey(tempTuple.getIntFld(join_col_in[tIndex]));
					for(int i=1;i<numTables;i++)
						{
							t.setFloFld(1, 0.0f);
						}		
					o=tempTuple.getIntFld(join_col_in[tIndex]);
			}
		else if(join_keyAttribute.attrType==AttrType.attrString)
		{
			s[0]=new AttrType(join_keyAttribute.attrType);
			for(int i=0;i<in[tIndex].length;i++)
				{
					if(in[tIndex][i].attrType == AttrType.attrString &&i==join_col_in[tIndex])
					{ 
						break;
					}
					joinkey_ssize=s_sizes[tIndex][i];
					ssize[0]=(short) joinkey_ssize;
				}
			t.setHdr((short)(numTables+1),s,ssize);
			t.setStrFld(0,tempTuple.getStrFld(join_col_in[tIndex]));
			lo=new StringKey(tempTuple.getStrFld(join_col_in[tIndex]));
	for(int i=1;i<numTables;i++)
	{
		t.setFloFld(1, 0.0f);
	}
//o=tempTuple.getStrFld(join_col_in[tIndex]);
		}
		t.setFloFld(tIndex+1,tempTuple.getScore());
		f.insertRecord(t.returnTupleByteArray());
		}
	else{
		int i;
		float sum=0;
		de=(LeafData)(kde.data);
		r=de.getData();
		temp=f.getRecord(r);
		temp.setHdr((short)(numTables+1),s,ssize);
		temp.setFloFld(tIndex+1, tempTuple.getScore());
		for(i=1;i< numTables;i++)
		{
			sc=temp.getFloFld(i);
			sum=sum+sc;
			if(sc==0)
			{
				break;
			}
		}
		if(i==(numTables-1))
		{
			countReached++;
			sum=sum/numTables;
			temp.setScore(sum);
			
		}
		f.insertRecord(temp.returnTupleByteArray());
	}
	}
		
		// Random Access
		
	btfs=btf.new_scan(null,null);//will return the values of all the data in the file
	out=new Tuple[numTables];
	while((kde=btfs.get_next())!=null)
	{
	de=(LeafData)(kde.data);
	r=de.getData();
	temp=f.getRecord(r);
	temp.setHdr((short)(numTables+1),s,ssize);
	float sum=0;
	for(int i=1;i<numTables;i++)
	{

		if((temp.getFloFld(i))==0)
		{
				bts[i]=bt[i].new_scan(new StringKey(temp.getStrFld(0)), new StringKey(temp.getStrFld(0)));
				kde=bts[i].get_next();
				de=(LeafData)(kde.data);
				r=de.getData();
				tempTuple=f.getRecord(r);
				tempTuple.setHdr((short)len_in[i],in[i],s_sizes[i]);
				temp.setFloFld(i,(tempTuple.getScore()));
				sum=sum+tempTuple.getScore();
		}
	}
	sum=sum/numTables;
	temp.setScore(sum);
	f.insertRecord(temp.getTupleByteArray());

	}

	FldSpec[] perm =new FldSpec[numTables];
	for(int i=0;i<=numTables;i++)
	{
		perm[i]=new FldSpec(new RelSpec(RelSpec.outer),i);

	}

	
	iter_heap = new FileScan(k,s,ssize,(short)(numTables+1),(short)4,perm,null);

	
	 iter=iter_heap;
	 order1=new TupleOrder(1);
		iter= new Sort(s, (short)(numTables+1), ssize
				, iter_heap,numTables,order1, numTables+1, amt_of_mem,true);
		
	
	  	
		 TupleUtils.setup_op_tuple(Jtuple,Jtypes, in, s_sizes, proj_list, n_out_flds)	;
		 
	    }
		 

	

public Tuple get_TopRanked() throws JoinsException, IndexException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception
{
	  if(num1>0)
	     {
	 
		  t1 = iter.get_next();
		 t1.setHdr((short)(numTables1+1),s,ssize);
		 String s=t1.getStrFld(0);
		 
		 for(int i=0;i<numTables1;i++)
		 {
			 bts[i]=bt[i].new_scan(new StringKey(s), new StringKey(s));
			 kde=bts[i].get_next();
				de=(LeafData)(kde.data);
				r=de.getData();
				tempTuple=f.getRecord(r);
				tempTuple.setHdr((short)len_in[i],in[i],s_sizes[i]);
				out[i]=tempTuple;
				Projection.Join(out, in, Jtuple, proj_list1); 
		 }
		 num1--;
		 return Jtuple;
	     }
	return null;
}
@Override
public Tuple get_next() throws IOException, JoinsException, IndexException,
InvalidTupleSizeException, InvalidTypeException,
PageNotReadException, TupleUtilsException, PredEvalException,
SortException, LowMemException, UnknowAttrType,
UnknownKeyTypeException, Exception {
// TODO Auto-generated method stub
	return null;
	}
@Override
public void close() throws IOException, JoinsException, SortException,
IndexException {
// TODO Auto-generated method stub
	}
}