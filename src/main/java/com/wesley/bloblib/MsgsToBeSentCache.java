package com.wesley.bloblib;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.pmw.tinylog.Logger;

public class MsgsToBeSentCache extends BfsCacheBase {
	private static MsgsToBeSentCache instance;
	private static ExecutorService msgAutoSendES;
	private static boolean clusterEnabled = Configuration.BFS_CLUSTER_ENABLED;
	
	private MsgsToBeSentCache(){
		cacheStore = new ConcurrentHashMap<String, CachedObject>(Constants.BFS_Messages_CACHE_INIT_CAPACITY, 0.9f, 1);
		capacity = Constants.BFS_Messages_CACHE_MAX_CAPACITY;
		expireTime = Configuration.BFS_CACHE_TTL_MS;
		startAutoSendService();
	}
	
	public final void startAutoSendService(){
		/* start the lease auto cleanup service */
		if (clusterEnabled){
			/* Make it a daemon */
			msgAutoSendES = Executors.newSingleThreadExecutor(new ThreadFactory(){
			    public Thread newThread(Runnable r) {
			        Thread t = new Thread(r);
			        t.setDaemon(true);
			        return t;
			    }        
			});
			msgAutoSendES.submit(new msgsAutoSender());
		}
	}
	
	@Override
	protected boolean put(String cacheKey, Object cached) throws BfsException {
		// TODO Auto-generated method stub
		if (clusterEnabled){
			return super.put(cacheKey, cached);
		}
		return true;
	}
	
	@Override
	protected Object get(String cacheKey) {
		// TODO Auto-generated method stub
		if (clusterEnabled){
			return super.get(cacheKey);
		}
		return null;
	}
	
	@Override
	public void finalize() {
		if (clusterEnabled){
			msgAutoSendES.shutdown();
		}
	}
	
	public static MsgsToBeSentCache getInstance(){
        if(instance == null){
            synchronized (MsgsToBeSentCache.class) {
                if(instance == null){
                    instance = new MsgsToBeSentCache();
                }
            }
        }
        return instance;
    }
	
	private class msgsAutoSender implements Runnable{
		@SuppressWarnings("static-access")
		@Override
		public void run() {
			try {
				/* start the auto send process */
				while (true){
					MsgsToBeSentCache bfsMessageCache = MsgsToBeSentCache.getInstance();
					/* Retrieve the msgs from the cache */
					for (Entry<String, CachedObject> entry : bfsMessageCache.cacheStore.entrySet()) {
	    				if (null != bfsMessageCache.get(entry.getKey())){
	    					/* send the message to the shared queue */
	    					String msg = (String)bfsMessageCache.get(entry.getKey());
	    					/* change string file name in to json */
	    					boolean sRes = MessageService.getInstance().sendMessage(msg);
	    					if (sRes){bfsMessageCache.delete(entry.getKey());}
	    				}						
	    			}				
					Thread.sleep(Constants.DEFAULT_BFC_THREAD_SLEEP_MILLS);
				}			
			} catch (Exception ex) {
				Logger.error(ex.getMessage());
			}
		}
	
	}

}
