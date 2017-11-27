package com.wesley.bloblib;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.DatatypeConverter;

import org.pmw.tinylog.Logger;

import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.blob.BlockEntry;
import com.microsoft.azure.storage.blob.CloudAppendBlob;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.wesley.bloblib.threads.ThreadPuddle;
import com.wesley.bloblib.threads.ThreadPuddleFactory;
import com.wesley.bloblib.utils.BfsUtility;

/**
 * parallel upload the blob data with multiple threads
 * this applies only to block blobs
 * @author weswu
 *
 */
public class ParallelUploader {
	/* instance of the object */
	private static ParallelUploader instance = null;
	/* the number of threads */
	private int defaultNumOfThreads = 8;
	/* the minimum chunk size */
	private int minChunkSize = 512 * 1024; // 512K
	/* the uploader threads pool */
	private static ThreadPuddle uploaderThreadsPool;
	/* the factory of thread puddle class */
	private static ThreadPuddleFactory threadPuddleFactory;
	
	/**
	 * the task class of read/write operations
	 * @author weswu
	 *
	 */
	private class UploadTask{
		int taskID;
		String chunkID;
		int offset;
		long length;
		public UploadTask(int taskID, String chunkID, int offset, long length) {
			this.taskID = taskID;
			this.chunkID = chunkID;
			this.offset = offset;
			this.length = length;
		}
		
	}
	
	/**
	 * constructor
	 */
	private ParallelUploader(){
		initTheUploaderThreadsPool(this.defaultNumOfThreads);
	}
	
	/**
	 * get the singleton instance
	 * @return
	 */
	public synchronized static ParallelUploader getInstance () {
	    if(instance == null){
            synchronized (ParallelUploader.class) {
                if(instance == null){
                    instance = new ParallelUploader();
               }
            }
        }
	    return instance;
	}
	
	/**
	 * initialize the threads pool
	 */
	private final void initTheUploaderThreadsPool(int numOfthreads){
		threadPuddleFactory = new ThreadPuddleFactory();
		threadPuddleFactory.setThreads(numOfthreads);
		threadPuddleFactory.setFifo(true);
		uploaderThreadsPool = threadPuddleFactory.build();
	}
	
	
	/**
	 * @param length
	 * @return number of chunks
	 */
	private int getFinalNumOfChunks(long length){
		int tmpBlockCount = (int)((float)length / (float)minChunkSize) + 1;
		/* the final number of the chunks */
		int numOfChunks = Math.min(defaultNumOfThreads, tmpBlockCount);
		return numOfChunks;
	}

	
	/**
	 * generate the parallel tasks
	 */
	private final List<UploadTask> splitJobIntoTheParallelTasks(int blobOffset, long length, int chunkNumber){
		List<UploadTask> uploadTasksList = new ArrayList<UploadTask>();
		int taskSequenceID = 0;
		int tmpOffset = blobOffset;
		/* get the number of chunks */
		int numOfChunks = getFinalNumOfChunks(length);
		/* the final size of the chunk */
		long numBtsInEachChunk = (long)(float)(length/numOfChunks);
		long bytesLeft = length;
		while( bytesLeft > 0 ) {
			/* how much to read (only last chunk may be smaller) */
            int bytesToWrite = 0;
            if ( bytesLeft >= (long)numBtsInEachChunk ) {
              bytesToWrite = (int)numBtsInEachChunk;
            } else {
              bytesToWrite = (int)bytesLeft;
            }
            chunkNumber ++;
        	/* save chunk id in array (must be base64) */
            String chunkId = DatatypeConverter.printBase64Binary(String.format("BlockId%07d", chunkNumber).getBytes(StandardCharsets.UTF_8));
            /* generate a new task , and put it into tasks list */ 
            UploadTask ulTask = new UploadTask(taskSequenceID, chunkId, tmpOffset, bytesToWrite);
            uploadTasksList.add(ulTask);
            
            /* increment the task sequence */ 
            tmpOffset += bytesToWrite;
            /* increment the task sequence */  
            taskSequenceID ++;
            /* increment/decrement counters */         
            bytesLeft -= bytesToWrite;
		}
		return uploadTasksList;
	}
	
	public final int[] uploadBlobWithParallelThreads(final byte[] dlJobCentralBuffer , final CloudBlob blob, final int blobOffset, final long length, final List<BlockEntry> blockList, 
			final AccessCondition accCondtion, final String leaseID, int chunkNumber) throws Exception{
		/* completed task's ID so far */
		final int completedTaskID[] = new int[1];
		completedTaskID[0] = -1;
		int tmpchunkNumber = chunkNumber;
		final int failedTasks[] = new int[1];
		/* resutl, 0: totalBtsuploaded, 1: chunkNumber */
		final int uploadRes[] = new int[2];
		/* get the upload tasks */
		List<UploadTask> uploadTasksList = splitJobIntoTheParallelTasks(blobOffset, length, chunkNumber);
		/* no tasks, return immediately */
		if (uploadTasksList.size() == 0){return uploadRes;}
		/* update the sequence number of the chunks */
		uploadRes[1] = tmpchunkNumber + uploadTasksList.size() ;
		/* start the parallel reading */
		for (final UploadTask uploadTask : uploadTasksList){
			uploaderThreadsPool.run(new Runnable() 
			{
				@Override
				public void run() 
				{
					RetryObj rObj = new RetryObj(3);
					while (rObj.shouldRetry()) {
						try 
						{
							ByteArrayInputStream bInput = new ByteArrayInputStream(dlJobCentralBuffer, uploadTask.offset, (int) uploadTask.length);
							synchronized(blockList){
								 BlockEntry chunk = new BlockEntry(uploadTask.chunkID);
					             blockList.add(chunk); 								
							}
							((CloudBlockBlob) blob).uploadBlock(uploadTask.chunkID, bInput, (long)uploadTask.length, accCondtion, null, null);
							bInput.close();
							
							/* wait for the pre-task to be completed */
							while(completedTaskID[0] != uploadTask.taskID -1){
								Thread.sleep((Constants.DEFAULT_BFC_THREAD_SLEEP_MILLS/5));										
							}
							/* set leaseID in the meta data */
							/* when upload huge file, the request will out of the range */
//							BlobReqParams cbParams = new BlobReqParams();
//							cbParams.setBlobInstance(blob);
//							cbParams.setLeaseID(leaseID);
//							BlobService.setBlobMetadata(cbParams,Constants.BLOB_META_DATA_COMMITED_BLOBKS_KEY, BfsUtility.blockIds(blockList));
							/* update the total number of bytes uploaded so far */
							uploadRes[0] += uploadTask.length;
							/* update the offset of the completed task */
							completedTaskID[0] = uploadTask.taskID;
							/* success */
							return;

						} catch (Exception ex) {
							Logger.error(ex.getMessage());
							/* check if retry is needed */
							try {
								rObj.errorOccured();
							} catch (BfsException bfsex) {
								failedTasks[0] ++;
								Logger.error(bfsex.getMessage());
							}
						}
					}
					
				}
			});
		}
		
		/* wait for all jobs have been done */
		while(true){
			/* if there are any error, we should abandon this read operation */
			if (failedTasks[0] > 0){
				throw new BfsException("write Failed:" + blob.getName());
			}
			/* completedTaskID start form 0 */
			if (uploadTasksList.size() == completedTaskID[0] + 1 && uploadRes[0] == length){
				break;
			}
			Thread.sleep((Constants.DEFAULT_BFC_THREAD_SLEEP_MILLS/5));
		}
		/* return the result */
		return uploadRes;

	}
	
	public void destroy(){
		uploaderThreadsPool.finish();
	}
	
}
