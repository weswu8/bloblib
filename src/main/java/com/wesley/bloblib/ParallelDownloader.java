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
	
	/* instance of the object */
	private static ParallelDownloader instance = null;
	/* the number of threads */
	private int defaultNumOfThreads = 8;
	/* the minimum chunk size */
	private int minChunkSize = 512 * 1024; // 512K
	/* the downloader threads pool */
	private static ThreadPuddle downloaderThreadsPool;
	/* the factory of thread puddle class */
	private static ThreadPuddleFactory threadPuddleFactory;
	
	
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
	 * constructor
	 */
	private ParallelDownloader(){
		initTheDownLoaderThreadsPool(this.defaultNumOfThreads);
	}
	
	/**
	 * get the singleton  instance
	 * @return
	 */
	public synchronized static ParallelDownloader getInstance () {
	    if(instance == null){
            synchronized (ParallelDownloader.class) {
                if(instance == null){
                    instance = new ParallelDownloader();
               }
            }
        }
	    return instance;
	}
	
	/**
	 * initialize the threads pool
	 */
	private final void initTheDownLoaderThreadsPool(int numOfthreads){
		threadPuddleFactory = new ThreadPuddleFactory();
		threadPuddleFactory.setThreads(numOfthreads);
		threadPuddleFactory.setFifo(true);
		downloaderThreadsPool = threadPuddleFactory.build();
	}
	
	
	private int getFinalNumOfChunks(long length){
		int tmpBlockCount = (int)((float)length / (float)minChunkSize) + 1;
		/* the final number of the chunks */
		int numOfChunks = Math.min(defaultNumOfThreads, tmpBlockCount);
		return numOfChunks;
	}
	
	/**
	 * generate the parallel tasks
	 */
	private final List<DownloadTask> splitJobIntoTheParallelTasks(int blobOffset, long length){
		List<DownloadTask> downloadTasksList = new ArrayList<DownloadTask>();
		int taskSequenceID = 0;
		int tmpOffset = blobOffset;
		/* get the number of chunks */
		int numOfChunks = getFinalNumOfChunks(length);
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
		return downloadTasksList;
	}
	
	public final byte[] downloadBlobWithParallelThreads(final CloudBlob blob, final int blobOffset, final long length) 
			throws Exception{
		final byte[] dlJobCentralBuffer = new byte[(int)length];
		/* completed task's ID so far */
		final int completedTaskID[] = new int[1];
		completedTaskID[0] = -1;
		final int failedTasks[] = new int[1];
		final int totalBtsDownloaded[] = new int[1];
		/* generate the download tasks */
		List<DownloadTask> downloadTasksList = splitJobIntoTheParallelTasks(blobOffset,length);
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
								while(completedTaskID[0] != downloadTask.taskID -1){
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
									completedTaskID[0] = downloadTask.taskID;
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
				//downloaderThreadsPool.die();
				throw new BfsException("read Failed:" + blob.getName());
			}
			/* completedTaskID start form 0 */
			if (downloadTasksList.size() == completedTaskID[0] + 1 && totalBtsDownloaded[0] == length){
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
