package com.wesley.bloblib;

import java.util.ArrayList;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.pmw.tinylog.Logger;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonObject;

/**
 * @author weswu
 * cache the BlobProperties
 *
 */
public class CachedFilesInMemManager extends BfsCacheBase {
	private static CachedFilesInMemManager instance;
	private static ExecutorService cacheAutoCleanupES;
	private static boolean clusterEnabled = Configuration.BFS_CLUSTER_ENABLED;
	private static MsgsToBeSentCache msgsToBeSentCache = MsgsToBeSentCache.getInstance();
	
	private CachedFilesInMemManager(){
		cacheStore = new ConcurrentHashMap<String, CachedObject>(Constants.BFS_FILES_CACHE_INIT_CAPACITY, 0.9f, 1);
		capacity = Constants.BFS_FILES_CACHE_MAX_CAPACITY;
		expireTime = Configuration.BFS_CACHE_TTL_MS;
		/* start the lease auto cleanup service */
		startCacheAutoCleanupService();
	}
	
	public static CachedFilesInMemManager getInstance(){
        if(instance == null){
            synchronized (CachedFilesInMemManager.class) {
                if(instance == null){
                    instance = new CachedFilesInMemManager();
                }
            }
        }
        return instance;
    }

	private final void startCacheAutoCleanupService(){
		/* start the lease auto cleanup service */
		if (clusterEnabled){
			/* Make it a daemon */
			cacheAutoCleanupES = Executors.newSingleThreadExecutor(new ThreadFactory(){
			    public Thread newThread(Runnable r) {
			        Thread t = new Thread(r);
			        t.setDaemon(true);
			        return t;
			    }        
			});
			cacheAutoCleanupES.submit(new MsgAutoPorcessor());
		}
	}
	
	@Override
	public void finalize() {
		cacheAutoCleanupES.shutdown();
	}
	
	/**
	 * use the full path as the final key :/container/blob
	 * @param containerName
	 * @param blobName
	 * @return
	 */
	public String getTheFormattedKey(String containerName, String blobName){
		if (null == blobName || "" == blobName.trim()){
			return "/" + containerName;
		}
		return "/" + containerName + "/" + blobName;
	}
	
	@Override
	public boolean put(String cacheKey, Object cached) throws BfsException {
		// TODO Auto-generated method stub
		if (null == cacheKey || "" == cacheKey){return true;}
		if (has(cacheKey)){delete(cacheKey);}
		return super.put(cacheKey, cached);
	}
	@Override
	public Object get(String cacheKey) {
		// TODO Auto-generated method stub
		return super.get(cacheKey);
	}
	
	@Override
	public boolean has(String cacheKey) {
		// TODO Auto-generated method stub
		return super.has(cacheKey);
	}
	
	@Override
	public void delete(String cacheKey) {
		// TODO Auto-generated method stub
		if (null == cacheKey || "" == cacheKey){return;}
		if (Configuration.BFS_CLUSTER_ENABLED)
		{
			try {
				@SuppressWarnings("static-access")
				String finalMsg = MessageService.getInstance().buildNewMsgToBeSent(cacheKey);
				msgsToBeSentCache.put(UUID.randomUUID().toString().substring(0, 10), finalMsg);
				
			} catch (BfsException ex) {
				// TODO Auto-generated catch block
				Logger.error(ex.getMessage());
			}
		}
		super.delete(cacheKey);
	}
	
	public void deleteFileInCache(String path){
		CachedFilesInMemManager bfsFilesCache = CachedFilesInMemManager.getInstance();
		BfsPath msgPath = new BfsPath(path);
		BfsPathType msgPathType = msgPath.getBfsPathProperties().getBfsPathType();
		if ("ROOT".equals(msgPathType.toString())){
			bfsFilesCache.clear();
		} else if ("CONTAINER".equals(msgPathType.toString()) || "SUBDIR".equals(msgPathType.toString())){
			for (Entry<String, CachedObject> entry : bfsFilesCache.cacheStore.entrySet()) {
				if (entry.getKey().startsWith(path)) {
					bfsFilesCache.delete(entry.getKey());
				}
			}
		} else if ("BLOB".equals(msgPathType.toString()) || "LINK".equals(msgPathType.toString())){
			if (bfsFilesCache.has(path)) {
    			bfsFilesCache.delete(path);
        	}
		} else {}  
	}
	
	private class MsgAutoPorcessor implements Runnable{
		@SuppressWarnings("static-access")
		@Override
		public void run() {
			try {				
				/* start the auto cleanup process */
				while (true){
					//CachedFilesInMemManager bfsFilesCache = CachedFilesInMemManager.getInstance();
					/* Retrieve the msgs from the service bus topic */
					ArrayList<String> msgs = new ArrayList<>();
					msgs = MessageService.getInstance().receiveMessages();
					for (String msg: msgs){	
						JsonObject pObject = Json.parse(msg).asObject();
						// delete the cached file in DB if it exists
						String fileName = pObject.get("file").asString();
						// the file name should be the full path 
						deleteFileInCache(fileName);						
			    		// processed the message , add in self host id, and then re-send the message to the shared queue
    					msg = MessageService.getInstance().buildProcessedMsgToBeSent(msg);
    					MessageService.getInstance().sendMessage(msg);
					}					
					Thread.sleep(Constants.DEFAULT_BFC_THREAD_SLEEP_MILLS);
				}			
			} catch (Exception ex) {
				Logger.error(ex.getMessage());
			}
		}
	
	}

}
