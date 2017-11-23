package com.wesley.bloblib;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import org.pmw.tinylog.Logger;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.queue.CloudQueueClient;

/* Factory to create multiple blob clients */ 
public final class BlobClientService {
	
	private static BlobClientService instance = new BlobClientService();
	private static CloudStorageAccount storageAccount;
	private static CloudBlobClient blobClient;
	private static CloudQueueClient queueClient;
	
	private BlobClientService(){
		try {
        	/* Retrieve storage account from connection-string */
			storageAccount = CloudStorageAccount.parse(Configuration.STORAGE_CONNECTION_STRING);
        }
        catch (IllegalArgumentException|URISyntaxException ex) {
        	Logger.error(ex.getMessage());
        	ex.printStackTrace();
        }
        catch (InvalidKeyException ex) {
        	Logger.error(ex.getMessage());
        	ex.printStackTrace();
        }
	};
	
	@SuppressWarnings("static-access")
	public static CloudBlobClient getBlobClient() throws Exception{
		return instance.createBlobClient();			
	}
	
	@SuppressWarnings("static-access")
	public static CloudQueueClient getQueueClient() throws Exception{
		return instance.createQueueClient();			
	}
	
	/**
	 * create the blob client
	 * @return
	 * @throws Exception
	 */
	private static CloudBlobClient createBlobClient () throws Exception{
		blobClient = null;
		try {
	        /* Create the blob client */
			blobClient = storageAccount.createCloudBlobClient();
		} catch (Exception ex) {
			Logger.error(ex.getMessage());
		}		
        return blobClient;
		
	}
	/**
	 * create the queue client
	 * @return
	 * @throws Exception
	 */
	private static CloudQueueClient createQueueClient () throws Exception{
		blobClient = null;
		try {
	        /* Create the blob client */
			queueClient = storageAccount.createCloudQueueClient();
		} catch (Exception ex) {
			Logger.error(ex.getMessage());
		}		
        return queueClient;
		
	}

}
