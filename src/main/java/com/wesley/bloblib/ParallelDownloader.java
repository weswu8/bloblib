package com.wesley.bloblib;

import java.util.ArrayList;
import java.util.List;

import org.pmw.tinylog.Logger;

import com.microsoft.azure.storage.blob.CloudBlob;
import com.wesley.bloblib.threads.ThreadPuddle;
import com.wesley.bloblib.threads.ThreadPuddleFactory;

/**
 * parallel reader, download the blob data with multiple threads
 * @author weswu
 *
 */
public class ParallelDownloader {
	
	/* the blob reference */
	private CloudBlob blob;
	/* the number of threads */
	private int defaultNumOfThreads = 8;
	/* the minimum chunk size */
	private int minChunkSize = 512 * 1024; // 512K
	/* the downloader threads pool */
	private ThreadPuddle downloaderThreadsPool;
	/* the factory of thread puddle class */
	private ThreadPuddleFactory threadPuddleFactory;
	/* final count of chunks */
	private int numOfChunks;
	/* completed task's ID so far */
	private int completedTaskID = -1;
	/* the start offset of the blob to be downloaded from */
	private int blobOffset;
	/* the length of bytes needed to be downloaded, 
	 * the length should be within a pre-defined range */
	private long length;
	/* the tasks list */
	private List<DownloadTask> downloadTasksList = new ArrayList<DownloadTask>();
	
	/**
	 * the task class of read/write operations
	 * @author weswu
	 *
	 */
	private class DownloadTask{
		int taskID;
		int offset;
		long length;
		public DownloadTask(int taskID, int offset, long length) {
			this.taskID = taskID;
			this.offset = offset;
			this.length = length;
		}
	}
	
	
	/**
	 * initialize the threads pool
	 */
	private final void initTheDownLoaderThreadsPool(int numOfthreads){
		threadPuddleFactory = new ThreadPuddleFactory();
		threadPuddleFactory.setThreads(numOfthreads);
		threadPuddleFactory.setTaskLimit(numOfthreads);
		threadPuddleFactory.setFifo(true);
		downloaderThreadsPool = threadPuddleFactory.build();
	}
	
	public ParallelDownloader(CloudBlob blob, int blobOffset, long length){
		this.blob = blob;
		this.blobOffset = blobOffset;
		this.length = length;
		getFinalNumOfChunks();
		initTheDownLoaderThreadsPool(numOfChunks);
	}
	
	private void getFinalNumOfChunks(){
		int tmpBlockCount = (int)((float)length / (float)minChunkSize) + 1;
		/* the final number of the chunks */
		numOfChunks = Math.min(defaultNumOfThreads, tmpBlockCount);
		return;
	}
	
	/**
	 * generate the parallel tasks
	 */
	private final void splitJobIntoTheParallelTasks(){
		int taskSequenceID = 0;
		int tmpOffset = this.blobOffset;
		/* the final size of the chunk */
		long numBtsInEachChunk = (long)(float)(length/numOfChunks);
		long bytesLeft = length;
		while( bytesLeft > 0 ) {
			/* how much to read (only last chunk may be smaller) */
            int bytesToRead = 0;
            if ( bytesLeft >= (long)numBtsInEachChunk ) {
              bytesToRead = (int)numBtsInEachChunk;
            } else {
              bytesToRead = (int)bytesLeft;
            }
            /* generate a new task , and put it into tasks list */ 
            DownloadTask dlTask = new DownloadTask(taskSequenceID, tmpOffset, bytesToRead);
            downloadTasksList.add(dlTask);
            
            /* increment the task sequence */ 
            tmpOffset += bytesToRead;
            /* increment the task sequence */  
            taskSequenceID ++;
            /* increment/decrement counters */         
            bytesLeft -= bytesToRead;
		}
		return;
	}
	
	public final byte[] downloadBlobWithParallelThreads() throws Exception{
		final byte[] dlJobCentralBuffer = new byte[(int)length];
		final int failedTasks[] = new int[1];
		final int totalBtsDownloaded[] = new int[1];
		/* get the download tasks */
		splitJobIntoTheParallelTasks();
		/* no tasks, return immediately */
		if (downloadTasksList.size() == 0){return dlJobCentralBuffer;}
		/* start the parallel reading */
		for (final DownloadTask downloadTask : downloadTasksList){
			downloaderThreadsPool.run(new Runnable() 
			{
				@Override
				public void run() 
				{
					int bytesDownloaded = 0;
					byte[] threadLocalBuffer = new byte[(int) downloadTask.length];
					RetryObj rObj = new RetryObj(3);
					while (rObj.shouldRetry()) {
						try 
						{
							bytesDownloaded = blob.downloadRangeToByteArray(downloadTask.offset, downloadTask.length, threadLocalBuffer, 0); 
							/* check the downloaded result, break the loop if success*/
							if (bytesDownloaded == downloadTask.length) {
								/* wait for the pre-task to be completed */
								while(completedTaskID != downloadTask.taskID -1){
									Thread.sleep((Constants.DEFAULT_BFC_THREAD_SLEEP_MILLS/5));										
								}
								/* get the offset in the central buffer */
								int offsetInCentralBuffer = downloadTask.offset - blobOffset;
								/* copy the content to central buffer */
								synchronized(dlJobCentralBuffer){
									System.arraycopy(threadLocalBuffer, 0, dlJobCentralBuffer, offsetInCentralBuffer, bytesDownloaded);
									/* update the total bytes downloaded */
									totalBtsDownloaded[0] += bytesDownloaded;
									/* update the offset of the completed task */
									completedTaskID = downloadTask.taskID;
								}


								/* success */
								return;
								
							}
						} catch (Exception ex) {
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
				downloaderThreadsPool.die();
				throw new BfsException("read Failed:" + blob.getName());
			}
			/* completedTaskID start form 0 */
			if (downloadTasksList.size() == completedTaskID + 1 && totalBtsDownloaded[0] == length){
				break;
			}
			Thread.sleep((Constants.DEFAULT_BFC_THREAD_SLEEP_MILLS/5));
		}
		/* return the result */
		return dlJobCentralBuffer;

	}
	
	public void destroy(){
		downloaderThreadsPool.finish();
	}
	
	
}
