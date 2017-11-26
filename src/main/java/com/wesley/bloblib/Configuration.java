package com.wesley.bloblib;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.pmw.tinylog.Configurator;
import org.pmw.tinylog.Level;
import org.pmw.tinylog.writers.FileWriter;

public final class Configuration {
	
	static IniReader iniReader;
	final static String confFile = "blobfs.conf";
	//public static String	DEFAULT_TIMEZONE =  null;
	public static String STORAGE_CONNECTION_STRING = null;
	public static String  DEFAULT_BLOB_PREFIX = null;
	public static String  DEFAULT_MOUNT_POINT = null;
	public static String WIN_MOUNT_POINT = null;
	public static int 	DEFAULT_BFS_UID = 0;
	public static int 	DEFAULT_BFS_GID = 0;
	public static boolean  BFS_DEBUG_ENABLED = false;
	public static boolean  BFS_CLUSTER_ENABLED = false;
	public static String  DEFAULT_BFS_LOG_LEVEL = null;
	public static boolean  BFS_CACHE_ENABLED = false;
	public static int  BFS_CACHE_TTL_MS = 0;
	public static String 	QUEUE_NAME = null;
	public static boolean AUTO_CHANGE_BLOCK_BLOB_TO_APPEND_BLOB = false;
	
	static {
		iniReader = IniReader.getInstance(confFile);
		readConfiguration();
		initLogger();
	}
	
	/**
	 * initialize the logger
	 */
	public static void initLogger(){
		String logFile = "blobfs-" + new SimpleDateFormat("yyyy-MM-dd").format(new Date()) + ".log";
		Configurator.defaultConfig() 
        .writer(new FileWriter(logFile, false, true)) 
        .formatPattern("{date} {level}: {class}.{method}() {message}")
        .level(getLogLevel()) 
        .activate(); 
	}
	
	
	/**
	 * initialize the log level
	 * @return
	 */
	public static Level getLogLevel (){
		switch (DEFAULT_BFS_LOG_LEVEL) {
		case "TRACE":
			return Level.TRACE;
		case "DEBUG":
			return Level.DEBUG;
		case "WARNING":
			return Level.WARNING;
		case "ERROR":
			return Level.ERROR;
		default:
			return Level.INFO;
		}
	}

	/* custom configurable options */
	public static void readConfiguration(){
		STORAGE_CONNECTION_STRING = iniReader.getProperty("Storage_Connection_String");
		//DEFAULT_TIMEZONE = iniReader.getProperty("timezone");
		DEFAULT_BLOB_PREFIX = iniReader.getProperty("blob_prefix");
		DEFAULT_MOUNT_POINT = iniReader.getProperty("mount_point");
	    WIN_MOUNT_POINT = iniReader.getProperty("win_mount_point");
	    DEFAULT_BFS_UID = Integer.parseInt(iniReader.getProperty("uid"));
	    DEFAULT_BFS_GID = Integer.parseInt(iniReader.getProperty("gid"));
	    BFS_DEBUG_ENABLED = Boolean.parseBoolean(iniReader.getProperty("debug_enabled"));
	    BFS_CLUSTER_ENABLED = Boolean.parseBoolean(iniReader.getProperty("cluster_enabled"));
	    DEFAULT_BFS_LOG_LEVEL = iniReader.getProperty("log_level");
	    BFS_CACHE_ENABLED = Boolean.parseBoolean(iniReader.getProperty("cache_enabled"));
	    BFS_CACHE_TTL_MS = Integer.parseInt(iniReader.getProperty("cache_TTL"))*1000;
	    QUEUE_NAME = iniReader.getProperty("queue_name");
	    AUTO_CHANGE_BLOCK_BLOB_TO_APPEND_BLOB = ("true".equals(iniReader.getProperty("auto_change_block_blob_to_append_blob").toLowerCase())) ? true : false;
	}

}
