package global;

import java.io.File;

public interface GlobalConst {

  public static final int MINIBASE_MAXARRSIZE = 50;
  public static final int NUMBUF = 100;
  public static final int MAX_NAME = 50;//need to chnage


  /** Size of page. */
  public static final int MINIBASE_PAGESIZE = 5120;           // in bytes

  /** Size of each frame. */
  public static final int MINIBASE_BUFFER_POOL_SIZE = 5120;   // in Frames

  public static final int MAX_SPACE = 10240;   // in Frames
  
  /**
   * in Pages => the DBMS Manager tells the DB how much disk 
   * space is available for the database.
   */
  public static final int MINIBASE_DB_SIZE = 100000;           
  public static final int MINIBASE_MAX_TRANSACTIONS = 100;
  public static final int MINIBASE_DEFAULT_SHAREDMEM_SIZE = 1000;
  
  /**
   * also the name of a relation
   */
  public static final int MAXFILENAME  = 15;          
  public static final int MAXINDEXNAME = 40;
  public static final int MAXATTRNAME  = 15;    

  public static final int INVALID_PAGE = -1;
public static final int INDEX_REC_LEN = 32;


public static final String dbpath = (new File(System.getProperty("java.io.tmpdir"), "minibase.db")).getPath();
public static final String logpath = (new File(System.getProperty("java.io.tmpdir"), "minibase.db.log")).getPath();
public static final String remove_cmd = "del /F ";
}
