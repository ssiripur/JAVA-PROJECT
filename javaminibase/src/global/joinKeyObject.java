package global;

import java.util.ArrayList;

import heap.Tuple;

public class joinKeyObject {
	
	public  float score[];
	public  int tableNum;
	public  Tuple tuple = new Tuple();
	public AttrType joinKeyType;
	public Object keyval=new Object();
	public int numoftables;
	public float avgscore;
	public ArrayList<Tuple> joinTuples;
	public int key_size;
	
	
	public joinKeyObject(int not,Object keyval, AttrType joinType,int ksize)
	{
	
	   numoftables=not;
	   joinKeyType=joinType;
	   this.keyval=keyval;
	   score=new float[not];
		for(int i=0;i<not;i++)
		{
		score[i]=-1;
		}
		key_size=ksize;
		
	}
	public String toString()
	{
		String s=new String("new jOIn KEy object");
		s=s+"data of the key is"+keyval.toString()+"score values"+score.toString();
	return s;
	}
	
	 
	// public joinKeyObject(int numTables, Object joinkey_Value,
		//	AttrType joinkey_AttrType, int joinkey_ssize) {
		// TODO Auto-generated constructor stub
	//}
	public void add_joinTuples(int tablenum,Tuple jT)
	{
		joinTuples.add(tablenum,jT);
	}

	public void setScore(int tablenum,float f)
	 {
	 score[tablenum]=f;
	 if(this.isFull())
	 {
		 for(int i=0;i<numoftables;i++)
		 {
			 avgscore=avgscore+score[i];
		 }
		 avgscore=avgscore/numoftables;
		 
	 }
	 }
	
	 public float getScore(int tablenum,int score1)
	 {
	if(score[tablenum]==-1)
	{
	return -1;
	}
	  return  score[tablenum];
	 }
	public boolean isFull()
	{
		int i=0;
		for(i=0;i<numoftables;i++)
		{
		if(score[i]==-1)
		{
			return false;
		}
		}
		    return true;
		
		}
	
}
