package com.wesley.bloblib;

public final class Constants {
	/* non custom configurable options */
	public static final String	DEFAULT_CHARSET = "UTF-8";
	public static final String	PATH_DELIMITER = "/";
	public static final String	VIRTUAL_DIRECTORY_NODE_NAME = ".$$$";
	public static final String  BLOBFS_TEMP_FILE_PREFIX = "bfs";
	public static final String	GET_UID_ON_UNIX_CMD = "id -u";
	public static final String	GET_GID_ON_UNIX_CMD = "id -g";
	public static final String  BLOB_META_DATA_LEASE_ID_KEY = "leaseID";
	public static final String 	BLOB_META_DATA_COMMITED_BLOBKS_KEY = "commitedBlocks";
	public static final String 	BLOB_META_DATA_ISlINK_KEY = "isLink";
	public static final String  BLOB_META_DATE_FORMAT = "E, dd-MMM-yy HH:mm:ss.SSS";
	public static final String  BLOB_META_DATE_UID_KEY = "uid";
	public static final String  BLOB_META_DATE_GID_KEY = "gid";
	public static final long  	DEFAULT_MAXWRITE_BYTES = 128 * 1024;//default 128K
	
	public static final int 	BLOB_BUFFERED_INS_DOWNLOAD_SIZE = 4 * 1024 * 1024; //default is 4MB:  4 * 1024 * 1024 = 4194304
	public static final int 	BLOB_BUFFERED_OUTS_BUFFER_SIZE = 4 * 1024 * 1024; //default is 4MB:  4 * 1024 * 1024 = 4194304
	public static final int 	BLOB_BUFFERED_OUTS_BLOCKBLOB_CHUNK_SIZE = 4 * 1024 * 1024; //default is 4MB:  4 * 1024 * 1024 = 4194304
	public static final int 	BLOB_BUFFERED_OUTS_APPENDBLOB_CHUNK_SIZE = 4 * 1024 * 1024; //default is 4MB:  4 * 1024 * 1024 = 4194304
	public static final int 	BLOB_BUFFERED_OUTS_PAGEBLOB_CHUNK_SIZE = 4 * 1024 * 1024; //default is 4MB:  4 * 1024 * 1024 = 4194304

	public static final int 	BFS_FILES_CACHE_INIT_CAPACITY = 1024; //1024
	public static final int 	BFS_FILES_CACHE_MAX_CAPACITY = 65535; //opened file
	public static final int 	BFS_FILES_CACHE_EXPIRE_TIME = 7 * 24 * 60 * 60 * 1000; //7 days
	
	public static final int 	BFS_Messages_CACHE_INIT_CAPACITY = 1024; //1024
	public static final int 	BFS_Messages_CACHE_MAX_CAPACITY = 65535; //opened file
	public static final int 	BFS_Messages_CACHE_EXPIRE_TIME = 60 * 60 * 1000; //1 hour
	
	public static final int		DEFAULT_BLOB_OPRATION_RETRY_TIMES = 3;
	public static final int		DEFAULT_THREAD_SLEEP_MILLS = 100;
	public static final int		DEFAULT_BFC_THREAD_SLEEP_MILLS = 2 * 100; // 200 mill seconds
	public static final int		DEFAULT_OFM_THREAD_SLEEP_MILLS = 10 * 1000; // 20 seconds
	public static final int 	CONCURRENT_REQUEST_COUNT = 4;
	public static final int 	SINGLE_BLOB_PUT_THRESHOLD = 16 * 1024 * 1024; //16MB
	public static final int 	STREAM_WRITE_SIZE = 4 * 1024 * 1024; //4MB	
	public static final int 	COMMAND_EXECUTION_BUFFER_SIZE = 4 * 1024;
	public static final int 	DEFAULT_BFS_CACHE_CAPACITY = 1 * 1024;
	public static final int 	DEFAULT_BFS_CACHE_EXPIRE_TIME = 5 * 60 * 1000; //5 minutes
	public static final int 	BLOB_BUFFER_INS_CACHE_INIT_CAPACITY = 4; //8
	public static final int 	BLOB_BUFFER_INS_MAX_CAPACITY = 8; //8
	public static final int 	BLOB_BUFFER_INS_EXPIRE_TIME = 5 * 60 * 1000; //5 minutes
	public static final int 	OPENED_FILE_MANAGER_INIT_CAPACITY = 1024; //1024
	public static final int 	OPENED_FILE_MANAGER_MAX_CAPACITY = 65535; //opened file
	public static final int 	OPENED_FILE_MANAGER_EXPIRE_TIME = 7 * 24 * 60 * 60 * 1000; //7 days

	/* set the retry policy */
	public static final int 	UPLOAD_BACKOFF_INTERVAL = 1 * 1000; // 1s
	public static final int 	UPLOAD_RETRY_ATTEMPTS = 15;
	
	public static final int 	APPENDBLOB_SPLIT_CHUNK_SIZE = 4 * 1000 * 1000; // default is 4MB
	public static final int 	UPLOAD_MULTIPARTS_SPLIT_CHUNK_SIZE = 4 * 1024 * 1024; // default is 4MB
	public static final int 	DOWNLOAD_MULTIPARTS_SPLIT_CHUNK_SIZE = 4 * 1024 * 1024; // default is 4MB
	public static final int 	BLOB_INS_DOWNLOAD_SIZE = 4 * 1024 * 1024; // default is 4MB
	public static final int 	BLOB_INS_CENTRAL_BUFFER_SIZE = 4 * 1024 * 1024; // default is 4MB
	public static final int		BLOB_INPUTSTREAM_MAXIMUM_READ_SIZE = 4 * 1024 * 1024; // default is 4MB
	public static final int 	BLOB_BLOCK_NUMBER_LIMIT = 50000; // default is 50000
	public static final long 	APPENDBLOB_SIZE_LIMIT = 195L * 1024L * 1024L * 1024L; //default is 195GB
	public static final int 	APPENDBLOB_BLOCK_SIZE_LIMIT = 4 * 1024 * 1024; //default is 4MB
	public static final long 	BLOCKBLOB_SIZE_LIMIT = 5L * 1024L * 1024L * 1024L * 1024L; //default is 5TB
	public static final int 	BLOCKBLOB_BLOCK_SIZE_LIMIT = 4 * 1024 * 1024; //default is 4MB
	public static final long 	PAGEBLOB_SIZE_LIMIT = 1L * 1024L * 1024L * 1024L * 1024L; //default is 1TB
	public static final int		PAGEBLOB_MINIMUM_SIZE = 512;	
	public static final int		BLOB_LOCKED_SECONDS = 60;  /* leaseTimeInSeconds should be between 15 and 60 */



}
