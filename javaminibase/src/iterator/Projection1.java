package iterator;

import heap.*;
import global.*;

import java.io.*;
/**
 * Jtuple has the appropriate types.
 * Jtuple already has its setHdr called to setup its vital stas.
 */

public class Projection1
{
  /**
   *Tuple t1 and Tuple t2 will be joined, and the result 
   *will be stored in Tuple Jtuple,before calling this method.
   *we know that this two tuple can join in the common field
   *@param t1 The Tuple will be joined with t2
   *@param type1[] The array used to store the each attribute type
   *@param t2 The Tuple will be joined with t1
   *@param type2[] The array used to store the each attribute type
   *@param Jtuple the returned Tuple
   *@param perm_mat[] shows what input fields go where in the output tuple
   *@param nOutFlds number of outer relation field 
   *@exception UnknowAttrType attrbute type does't match
   *@exception FieldNumberOutOfBoundException field number exceeds limit
   *@exception IOException some I/O fault 
   */
  public static void Join( Tuple  t1, AttrType type1[],
                           Tuple  t2, AttrType type2[],
        	           Tuple Jtuple, FldSpec  perm_mat[], 
                           int nOutFlds
			   )
    throws UnknowAttrType,
	   FieldNumberOutOfBoundException,
	   IOException
    {
      
      
      for (int i = 0; i < nOutFlds; i++)
	{
	  switch (perm_mat[i].relation.key)
	    {
	    case RelSpec.outer:        // Field of outer (t1)
	      switch (type1[perm_mat[i].offset-1].attrType)
		{
		case AttrType.attrInteger:
		  Jtuple.setIntFld(i+1, t1.getIntFld(perm_mat[i].offset));
		  break;
		case AttrType.attrReal:
		  Jtuple.setFloFld(i+1, t1.getFloFld(perm_mat[i].offset));
		  break;
		case AttrType.attrString:
		  Jtuple.setStrFld(i+1, t1.getStrFld(perm_mat[i].offset));
		  break;
		default:
		  
		  throw new UnknowAttrType("Don't know how to handle attrSymbol, attrNull");
		  
		}
	      break;
	      
	    case RelSpec.innerRel:        // Field of inner (t2)
	      switch (type2[perm_mat[i].offset-1].attrType)
		{
		case AttrType.attrInteger:
		  Jtuple.setIntFld(i+1, t2.getIntFld(perm_mat[i].offset));
		  break;
		case AttrType.attrReal:
		  Jtuple.setFloFld(i+1, t2.getFloFld(perm_mat[i].offset));
		  break;
		case AttrType.attrString:
		  Jtuple.setStrFld(i+1, t2.getStrFld(perm_mat[i].offset));
		  break;
		default:
		  
		  throw new UnknowAttrType("Don't know how to handle attrSymbol, attrNull");  
		  
		}
	      break;
	    }
//	  AttReal jmean=new AttReal();
      float value=(t1.getScore()+t2.getScore())/2;
      Jtuple.setScore(value);
	 
	}
      return;
    }
  /*
   * For n tables
   */
  
  public static void Join(Tuple[] in_tuples, AttrType[][] in_attrs,
			Tuple out_tuple, FldSpec[] proj_list,int noutflds) throws Exception {
		for (int i = 0; i <proj_list.length ; i++) {
			int tupleIndex = proj_list[i].relation.key;
			switch (in_attrs[tupleIndex][proj_list[i].offset - 1].attrType) {
			case AttrType.attrInteger:
				out_tuple.setIntFld(i+1 , in_tuples[tupleIndex].getIntFld(proj_list[i].offset));
				break;
			case AttrType.attrReal:
				out_tuple.setFloFld(i+1  , in_tuples[tupleIndex].getFloFld(proj_list[i].offset));
				break;
			case AttrType.attrString:
				out_tuple.setStrFld(i+1  , in_tuples[tupleIndex].getStrFld(proj_list[i].offset));
				break;
			default:
				throw new UnknowAttrType(
						"Don't know how to handle attrSymbol, attrNull");
			}
			//out_tuple.setScore(scoreatr);
		}
		
		// set avg score for output tuple
		float total_score = 0.0f;
		for (Tuple tuple : in_tuples) {
			total_score += tuple.getScore();
		}
		out_tuple.setScore(total_score/in_tuples.length);
		
		return;
	}

  
  
  
  /**
   *Tuple t1 will be projected  
   *the result will be stored in Tuple Jtuple
   *@param t1 The Tuple will be projected
   *@param type1[] The array used to store the each attribute type
   *@param Jtuple the returned Tuple
   *@param perm_mat[] shows what input fields go where in the output tuple
   *@param nOutFlds number of outer relation field 
   *@exception UnknowAttrType attrbute type doesn't match
   *@exception WrongPermat wrong FldSpec argument
   *@exception FieldNumberOutOfBoundException field number exceeds limit
   *@exception IOException some I/O fault 
   */
  
  public static void Project(Tuple  t1, AttrType type1[], 
                             Tuple Jtuple, FldSpec  perm_mat[], 
                             int nOutFlds
			     )
    throws UnknowAttrType,
	   WrongPermat,
	   FieldNumberOutOfBoundException,
	   IOException
    {
      
      
      for (int i = 0; i < nOutFlds; i++)
	{
	  switch (perm_mat[i].relation.key)
	    {
	    case RelSpec.outer:      // Field of outer (t1)
	      switch (type1[perm_mat[i].offset-1].attrType)
		{
		case AttrType.attrInteger:
		  Jtuple.setIntFld(i+1, t1.getIntFld(perm_mat[i].offset));
		  break;
		case AttrType.attrReal:
		  Jtuple.setFloFld(i+1, t1.getFloFld(perm_mat[i].offset));
		  break;
		case AttrType.attrString:
		  Jtuple.setStrFld(i+1, t1.getStrFld(perm_mat[i].offset));
		  break;
		default:
		  
		  throw new UnknowAttrType("Don't know how to handle attrSymbol, attrNull"); 
	
		}
	      break;
	      
	    default:
	      
	      throw new WrongPermat("something is wrong in perm_mat");
	     
	    }
	}
      return;
    }
  
}


