package com.wesley.bloblib;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.pmw.tinylog.Logger;

import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.NameValidator;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudAppendBlob;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.CloudPageBlob;
import com.microsoft.azure.storage.blob.CopyState;
import com.microsoft.azure.storage.blob.CopyStatus;
import com.microsoft.azure.storage.blob.DeleteSnapshotsOption;
import com.microsoft.azure.storage.blob.LeaseStatus;
import com.microsoft.azure.storage.blob.ListBlobItem;
import com.microsoft.azure.storage.blob.PageRange;
import com.wesley.bloblib.threads.ThreadPuddle;
import com.wesley.bloblib.utils.BfsUtility;


@SuppressWarnings("static-access")
public final class BlobService {
    private static BlobService instance = new BlobService();
	private static CloudBlobContainer container;
	private static ThreadPuddle puddle = ThreadPuddle.getInstance();
	//static CachedFilesInDbManager cachedFilesInDbManager = CachedFilesInDbManager.getInstance();
	static CachedFilesInMemManager cachedFilesInMemManager = CachedFilesInMemManager.getInstance();

	private static Object locker = new Object();
	private BlobService(){};
	private static CloudBlob blob;
	

	/* Get the block and append blob reference */
	/**
	 * @param reqParams: container, blobType, blob
	 * @return
	 * @throws BfsException
	 */
	public final static CloudBlob getBlobReference (BlobReqParams reqParams) throws BfsException{
		instance.blob = null;
		String containerName = reqParams.getContainer();
		String blobType = (null != reqParams.getBfsBlobType()) ? reqParams.getBfsBlobType().toString() : "";
		String blobName = reqParams.getBlob();
		try {
			container = ContainerService.getPrivateContainer(containerName);
			switch (blobType){
				case "BLOCKBLOB":
				case "VIRTUALDIRECTORY":
					instance.blob = (CloudBlockBlob)container.getBlockBlobReference(blobName);
					break;	
				case "APPENDBLOB":
					instance.blob = (CloudAppendBlob)container.getAppendBlobReference(blobName);
					break;
				case "PAGEBLOB":
					instance.blob = (CloudPageBlob)container.getPageBlobReference(blobName);
					break;
				default:
					instance.blob = (CloudBlob)container.getBlobReferenceFromServer(blobName);
					break;	
			}
		} catch (Exception ex) {
			String errMessage = "Exception occurred when geting the blob reference: " + blobName + ". " + ex.getMessage();
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
		return instance.blob;		
	}
	
	/* this function will create a blob object with zero size*/
	/**
	 * @param reqParams:container, blob
	 * @throws BfsException
	 */
	public final static boolean createVirtualDirectory (BlobReqParams reqParams) throws BfsException{
		boolean result = false;
		reqParams.setBfsBlobType(BfsBlobType.BLOCKBLOB);
		String vdir = reqParams.getBlob() + Constants.PATH_DELIMITER + Constants.VIRTUAL_DIRECTORY_NODE_NAME;
		reqParams.setBlob(vdir);
		try {
			NameValidator.validateBlobName(vdir);
			CloudBlockBlob blob = (CloudBlockBlob) getBlobReference(reqParams);
			if (blob.exists()){
				String errMessage = "The specified directory: " + reqParams.getBlobFullPath()+ " already exists. ";
				throw new BfsException(errMessage);
			}
			blob.uploadText("","UTF-8",null, null, null);
			result = true;
			Logger.trace("The specified directory:{} has been created.", reqParams.getBlobFullPath());
		} catch (Exception ex) {
			String errMessage = "Exception occurred when creating the directory: " + reqParams.getBlobFullPath() + ". " + ex.getMessage();
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
		return result;
	}
	
	/**
	 * @param reqParams
	 * @return
	 */
	public final static String getVirtualDirectoryPath(BlobReqParams reqParams){
		String sourceContainer = reqParams.getContainer();
		/* we should put "" or add "/" at the end of blob name */
	    String sourceBlob = (null == reqParams.getBlob() 
	    					|| "".equals(reqParams.getBlob())) ? "" : reqParams.getBlob() + "/";
	    return "/" + sourceContainer + "/" + sourceBlob;
	}
	
	/* get the blobs within the virtual directory */
	/**
	 * @param reqParams：virtualDirOptMode
	 * @return
	 * @throws BfsException
	 */
	public final static List<BfsBlobModel> getBlobsWithinVirtualDirectory (BlobReqParams reqParams) throws BfsException{
		List<BfsBlobModel> bfsBlobsList = new ArrayList<>();
		List<BfsBlobModel> bfsBlobsListRes = new ArrayList<>();
		String sourceContainer = reqParams.getContainer();
		/* we should put "" or add "/" at the end of blob name */
	    String sourceBlob = (null == reqParams.getBlob() 
	    					|| "".equals(reqParams.getBlob())) ? "" : reqParams.getBlob() + "/";
		boolean vDirFlatMode = false;
		boolean getBlobProps = false;
		/* use the flat mode here */
		if ("FBLOBPROPS".equals(reqParams.getVirtualDirOptMode().toString()) 
				|| "FLAT".equals(reqParams.getVirtualDirOptMode().toString())){
			vDirFlatMode = true;
		}
		/* get the blob properties or not */
		if ("FBLOBPROPS".equals(reqParams.getVirtualDirOptMode().toString()) 
				|| "HBLOBPROPS".equals(reqParams.getVirtualDirOptMode().toString())){
			getBlobProps = true;
		}	
	    try {
	    	/* does not check whether the destination directory exists or not */
			CloudBlobContainer container = ContainerService.getPrivateContainer(sourceContainer);
			/* list the blob in hierarchical mode, flat mode should set the 2nd parameter to true 
			 * the blobItem.getName() will return this format :"sourceBlob/itemname"
			 * */
			Iterable<ListBlobItem> collection = container.listBlobs(sourceBlob, vDirFlatMode);
			for (ListBlobItem blobItem : collection) {
				 /* the blob item is a blob */
				 BfsBlobType bfsBlobType = null;
				 String sourceBlobName = null;
				 String blobURI = null;
				 if (blobItem instanceof CloudBlob){					 
					 if (blobItem instanceof CloudBlockBlob) {
						sourceBlobName = ((CloudBlockBlob) blobItem).getName();
						blobURI = ((CloudBlockBlob) blobItem).getUri().toString();
						bfsBlobType = BfsBlobType.BLOCKBLOB;				 
					 }else if(blobItem instanceof CloudAppendBlob){
						sourceBlobName = ((CloudAppendBlob) blobItem).getName();
						blobURI = ((CloudAppendBlob) blobItem).getUri().toString();
						bfsBlobType = BfsBlobType.APPENDBLOB;
					 }else if(blobItem instanceof CloudPageBlob){
						 sourceBlobName = ((CloudPageBlob) blobItem).getName();
						 blobURI = ((CloudPageBlob) blobItem).getUri().toString();
						 bfsBlobType = BfsBlobType.PAGEBLOB;
					 }
					 /* don't support other type yet */
					 if (null == sourceBlobName) {continue;}
					 /* fill the BfsBlobModel */
					 BfsBlobModel bfsBlobModel = new BfsBlobModel();
					 bfsBlobModel.setBfsBlobType(bfsBlobType);
					 bfsBlobModel.setBlobName(sourceBlobName);
					 bfsBlobModel.setBlobURI(blobURI);
					 /* get the blob properties, can think lazy mode later */
					 BlobProperties blobProperties = new BlobProperties();
					 blobProperties.setName(bfsBlobModel.getBlobName());
					 blobProperties.setBfsBlobType(bfsBlobModel.getBfsBlobType());
					 bfsBlobModel.setBlobProperties(blobProperties);
					 /* add to the list */
					 bfsBlobsList.add(bfsBlobModel);
				
				 }/* end of if if (blobItem instanceof CloudBlob) */
				 /* the blob item is a virtual directory */
				 if (blobItem instanceof CloudBlobDirectory){
					 //sourceBlobName = ((CloudBlobDirectory) blobItem).getPrefix();
					 /* for fuse issue: the dir end with slash will cause the fuse error */
					 sourceBlobName = BfsUtility.removeLastSlash(((CloudBlobDirectory) blobItem).getPrefix());
					 blobURI = ((CloudBlobDirectory) blobItem).getUri().toString();
					 bfsBlobType = BfsBlobType.VIRTUALDIRECTORY;
					 /* fill the BfsBlobModel */
					 BfsBlobModel bfsBlobModel = new BfsBlobModel();
					 bfsBlobModel.setBfsBlobType(bfsBlobType);
					 bfsBlobModel.setBlobName(sourceBlobName);
					 bfsBlobModel.setBlobURI(blobURI);
					 BlobProperties blobProperties = new BlobProperties();
					 bfsBlobModel.setBlobProperties(blobProperties);
					 /* add to the list */
					 bfsBlobsList.add(bfsBlobModel);					 
				 }
				 //System.out.println(sourceBlobName);
	
		    }/* end of for primary loop */
			// if we should get the detailed properties of these blobs
			if (getBlobProps && bfsBlobsList.size() > 0){
				bfsBlobsListRes = getBlobPropertiesWithParallelThreads(sourceContainer, bfsBlobsList);
				bfsBlobsList = bfsBlobsListRes;
			}
	    } catch (Exception ex) {
			String errMessage = "Exception occurred when listing the directory: " + reqParams.getBlobFullPath() + ". " + ex.getMessage();
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
		return bfsBlobsList;
	}
	
	/**
	 * get blob properties with parallel threads pool
	 * @param Container
	 * @param BfsBlobModels
	 * @return
	 * @throws BfsException
	 */
	public final static List<BfsBlobModel> getBlobPropertiesWithParallelThreads(final String Container, List<BfsBlobModel> BfsBlobModels) throws BfsException{
		final List<BfsBlobModel> bfsBlobModelsRes = new ArrayList<>();
		final int totalTasks = BfsBlobModels.size();
		//System.out.println("Total tasks: " + totalTasks);
		try {
			for (final BfsBlobModel bfsBlobModel : BfsBlobModels) 
			{
				puddle.run(new Runnable() 
				{
					@Override
					public void run() 
					{
						BlobProperties blobProperties = null;
						try {
							String blobName = null;
							BlobReqParams getBlobReq = new BlobReqParams();
							getBlobReq.setContainer(Container);
							if (bfsBlobModel.getBfsBlobType() == BfsBlobType.VIRTUALDIRECTORY){
								blobName = bfsBlobModel.getBlobName() + "/" + Constants.VIRTUAL_DIRECTORY_NODE_NAME;
							}else{
								blobName = bfsBlobModel.getBlobName();
							}
							getBlobReq.setBlob(blobName);
							
							blobProperties = getBlobProperties(getBlobReq);
							
						} catch (BfsException ex) {
							// TODO Auto-generated catch block
							Logger.error(ex.getMessage());
						}finally {							
							if (null != blobProperties)
							{
								//System.out.println("inner" + blobProperties.getName());
								bfsBlobModel.setBlobProperties(blobProperties);
								synchronized (locker) {
									bfsBlobModelsRes.add(bfsBlobModel);
								}
							}else{
								synchronized (locker) {
									bfsBlobModelsRes.add(bfsBlobModel);
								}
							}

						}
					}
				});
			}
			// wait for all jobs have been done
			while(bfsBlobModelsRes.size() != totalTasks){
				Thread.sleep(100);
			}
			
		} catch (Exception ex) {
			String errMessage = "Exception occurred when geting blob properties with parallel threads. " + ex.getMessage();
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
		return bfsBlobModelsRes;
	}
	/**
	 * copy or move the blobs with parallel threads pool
	 * @param reqParams
	 * @param BfsBlobModels
	 * @return
	 * @throws BfsException
	 */
	public final static List<String> copyOrMoveBlobsWithParallelThreads(final BlobReqParams reqParams, List<BfsBlobModel> BfsBlobModels) throws BfsException{
		final List<String> failedBlobsList = new ArrayList<>();
		final Object cormLocker = new Object();
		/* move or copy the virtual directory, default is copy */
		final String sourceContainer = reqParams.getContainer();
		final String sourceBlobDir = reqParams.getBlob();
		final String destContainer = reqParams.getDestContainer();
		final String destBlobDir = reqParams.getDestBlob();
		final int retryTimes = Constants.DEFAULT_BLOB_OPRATION_RETRY_TIMES;;
		final int totalTasks = BfsBlobModels.size();
		final int failedTasks[] = new int[1];
		failedTasks[0] = 0;
		final int successedTasks[] = new int[1];
		successedTasks[0] = 0;
		try {
			for (final BfsBlobModel bfsBlobModel : BfsBlobModels) 
			{
				puddle.run(new Runnable() 
				{
					@Override
					public void run() 
					{
						try 
						{
							int retryCount = 1;
							String sourceBlobName = bfsBlobModel.getBlobName();
							BfsBlobType bfsBlobType = bfsBlobModel.getBfsBlobType();
							String destBlobName;
							if ("".equals(sourceBlobDir.trim())){
								destBlobName = destBlobDir + Constants.PATH_DELIMITER + sourceBlobName;
							} else {
								destBlobName = sourceBlobName.replace(sourceBlobDir, destBlobDir);
							}	
							BlobReqParams moveReq = new BlobReqParams();
							/* use copy mode for the directory copy/move */
							moveReq.setBlobOptMode(BlobOptMode.COPY);
							moveReq.setBfsBlobType(bfsBlobType);
							moveReq.setContainer(sourceContainer);
							moveReq.setBlob(sourceBlobName);
							moveReq.setDestContainer(destContainer);
							moveReq.setDestBlob(destBlobName);
							moveReq.setDoForoce(true);
							while(!CopyOrmoveBlobCrossContainer(moveReq)){
								if (retryCount == retryTimes) {
									synchronized (cormLocker) {
										failedBlobsList.add(bfsBlobModel.getBlobURI());
										Logger.trace("Exception occurred when renaming the directory from {} to {}."
												, reqParams.getBlobFullPath(), reqParams.getDestBlobFullPath());
									}
									break;
								 }
								 retryCount ++ ;
								 Thread.sleep(Constants.DEFAULT_THREAD_SLEEP_MILLS);
							 }/* end of while */				
							 /* rename the blob successfully */
							 if (retryCount < retryTimes){
								Logger.trace("Rename successed. from {} to {}.", moveReq.getBlobFullPath(), moveReq.getDestBlobFullPath() );
							 }
							 successedTasks[0] ++;

						} catch (BfsException ex) {
							failedTasks[0] ++;
							// TODO Auto-generated catch block
							Logger.error(ex.getMessage());
						} catch (InterruptedException e) {
							failedTasks[0] ++;
							// TODO Auto-generated catch block
							Logger.error(e.getMessage());
						}
					}
				});
				
			}
			// wait for all jobs have been done
			while(totalTasks != (successedTasks[0] + failedTasks[0])){
				Thread.sleep(100);
			}
			
		} catch (Exception ex) {
			String errMessage = "Exception occurred when copying or moving blobs with parallel threads. " + ex.getMessage();
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
		return failedBlobsList;
	}
	
	/* rename the virtual directory*/
	/**
	 * @param failedFilesList: store the failed file
	 * @param reqParams: blobOptMode, container, blob, destContainer, destBlob,
	 * @return
	 * @throws Exception
	 */
	public final static boolean copyOrMoveVirtualDirectory(List<String> failedFilesList, BlobReqParams reqParams) throws BfsException{
		boolean result = false;
		/* move or copy the virtual directory, default is copy */
		String sourceContainer = reqParams.getContainer();
		String sourceBlobDir = reqParams.getBlob();
		String destContainer = reqParams.getDestContainer();
		String destBlobDir = reqParams.getDestBlob();
		/* the source prefix and the destination prefix should be different, we don't check this */
		try {
			/* check whether the source directory exists or not */
			/* check whether the destination directory exists or not, this should be done in the caller */
			BlobReqParams checkReq = new BlobReqParams();
			checkReq.setContainer(destContainer);
			checkReq.setBlob(destBlobDir);
			if (virtualDirectoryExists(checkReq, false)){
				String errMessage = "Exception occurred when renaming the directory from " + reqParams.getBlobFullPath() + " to " +
						reqParams.getDestBlobFullPath() + ". The specified directory already exists.";
				throw new BfsException(errMessage);
			}
			/* get the blobs within the virtual directory with lazy mode */
			BlobReqParams getBlobsReq = new BlobReqParams();
			getBlobsReq.setContainer(sourceContainer);
			getBlobsReq.setBlob(sourceBlobDir);
			/* use flat mode here, there is no virtual directory in this mode 
			 * safe copy/move , delete the original file only if copy/move successed*/
			getBlobsReq.setVirtualDirOptMode(VirtualDirOptMode.FLAT);
			List<BfsBlobModel> bfsBlobModels = new BlobService().getBlobsWithinVirtualDirectory(getBlobsReq);
			/* copy or move the blob wit parallel threads pool */
			failedFilesList = copyOrMoveBlobsWithParallelThreads(reqParams, bfsBlobModels);
			/* copy/move successfully, we need delete the original file for move opt mode */
			if (failedFilesList.isEmpty()){
				if ("MOVE".equals(reqParams.getBlobOptMode().toString())){
					List<String> delFailedFiles = new ArrayList<String>();
					deleteVirtualDirectory(delFailedFiles, reqParams);
					if (delFailedFiles.isEmpty()){ result = true; }
				} else {
					result = true;
				}
			} 			
		} catch (Exception ex) {
			String errMessage = "Exception occurred when renaming the directory from " + reqParams.getBlobFullPath() + " to " +
					reqParams.getDestBlobFullPath() + ". " + ex.getMessage();
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
		
		return result;
	}
	/**
	 * delete blobs with parallel threads pool
	 * @param reqParams
	 * @param BfsBlobModels
	 * @return
	 * @throws BfsException
	 */
	public final static List<String> deleteBlobsWithParallelThreads(final BlobReqParams reqParams, List<BfsBlobModel> BfsBlobModels) throws BfsException{
		final List<String> failedBlobsList = new ArrayList<>();
		final Object delLocker = new Object();
		/* move or copy the virtual directory, default is copy */
		final String sourceContainer = reqParams.getContainer();
		final int retryTimes = Constants.DEFAULT_BLOB_OPRATION_RETRY_TIMES;;
		final int totalTasks = BfsBlobModels.size();
		final int failedTasks[] = new int[1];
		failedTasks[0] = 0;
		final int successedTasks[] = new int[1];
		successedTasks[0] = 0;
		try {
			for (final BfsBlobModel bfsBlobModel : BfsBlobModels) 
			{
				puddle.run(new Runnable() 
				{
					@Override
					public void run() 
					{
						try 
						{
							int retryCount = 1;
							 String sourceBlobName = bfsBlobModel.getBlobName();
							 BfsBlobType bfsBlobType = bfsBlobModel.getBfsBlobType();
							 BlobReqParams delReq = new BlobReqParams();
							 delReq.setBfsBlobType(bfsBlobType);
							 delReq.setContainer(sourceContainer);
							 delReq.setBlob(sourceBlobName);
							 delReq.setDoForoce(true);
							 while(!deleteBlob(delReq)){
								 if (retryCount == retryTimes) {
									 synchronized (delLocker) {
										 failedBlobsList.add(bfsBlobModel.getBlobURI());
										 Logger.trace("Exception occurred when deleting the directory: {}.", delReq.getBlobFullPath());
									 }
									 break;										
								 }
								 retryCount ++ ;
								 Thread.sleep(Constants.DEFAULT_THREAD_SLEEP_MILLS);
							 }/* end of while */				
							 /* delete the blob successfully */
							 if (retryCount < retryTimes){
								Logger.trace("Delete successed: {}.", delReq.getBlobFullPath());
							 }
							 successedTasks[0] ++;

						} catch (BfsException ex) {
							failedTasks[0] ++;
							// TODO Auto-generated catch block
							Logger.error(ex.getMessage());
						} catch (InterruptedException e) {
							failedTasks[0] ++;
							// TODO Auto-generated catch block
							Logger.error(e.getMessage());
						}
					}
				});
				
			}
			// wait for all jobs have been done
			while(totalTasks != (successedTasks[0] + failedTasks[0])){
				Thread.sleep(100);
			}
			
		} catch (Exception ex) {
			String errMessage = "Exception occurred when deleting blobs with parallel threads. " + ex.getMessage();
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
		return failedBlobsList;
	}
	/* delete the virtual directory*/
	/**
	 * @param failedFilesList
	 * @param reqParams: container, blob
	 * @return
	 * @throws Exception
	 */
	public final static boolean deleteVirtualDirectory(List<String> failedFilesList, BlobReqParams reqParams) throws BfsException{
		boolean result = false;
		String sourceContainer = reqParams.getContainer();
		String sourceBlobDir =  reqParams.getBlob();
		try {
			/* does not check whether the destination directory exists or not */
			/* get the blobs within the virtual directory with lazy mode */
			BlobReqParams getBlobsReq = new BlobReqParams();
			getBlobsReq.setContainer(sourceContainer);
			getBlobsReq.setBlob(sourceBlobDir);
			/* use flat mode here, there is no virtual directory in this mode */
			getBlobsReq.setVirtualDirOptMode(VirtualDirOptMode.FLAT);
			List<BfsBlobModel> bfsBlobModels = new BlobService().getBlobsWithinVirtualDirectory(getBlobsReq);
			/* delete blobs with parallel threads pool */
			failedFilesList = deleteBlobsWithParallelThreads(reqParams, bfsBlobModels);
			if (failedFilesList.size() == 0){
				result = true;
			}
		} catch (Exception ex) {
			String errMessage = "Exception occurred when deleting the directory from " + reqParams.getBlobFullPath() + " to " +
					reqParams.getDestBlobFullPath() + ". " + ex.getMessage();
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
		return result;
	}
	
	
	/* create the blob */	
	/**
	 * @param reqParams, container, blob, blobType, doForce
	 * 		  only support BLOCK, APPEND, PAGE blob
	 * @return
	 * @throws BfsException
	 */
	public final static boolean createBlob(BlobReqParams reqParams) throws BfsException {
		boolean result = false;
		AccessCondition accCondtion = new AccessCondition();
		CloudBlob blob = getBlobReference(reqParams);
		try {
			NameValidator.validateBlobName(reqParams.getBlob());
			if (blob.exists() && !reqParams.isDoForoce()){
				String errMessage = "The target blob: " + reqParams.getBlobFullPath() + " already exists.";
				throw new BfsException(errMessage);
			}
			/* set the lease ID */
			if (null != reqParams.getLeaseID() && reqParams.getLeaseID().length() > 0){
				accCondtion.setLeaseID(reqParams.getLeaseID());
			}
			/* create a empty temporary file */
	        File tempFile = File.createTempFile(Constants.BLOBFS_TEMP_FILE_PREFIX, "tmp");
	        FileInputStream fileInputStream = null;
			/* if it's page blob, we should initialize the blob */
			if ("PAGEBLOB".equals(reqParams.getBfsBlobType().toString())){
				/* fill the temp file with random data */
				byte[] zeroBytes = new byte[Constants.PAGEBLOB_MINIMUM_SIZE];
				FileOutputStream fileOutputStream = new FileOutputStream(tempFile);
				fileOutputStream.write(zeroBytes);
				fileOutputStream.close();
				/* initialize the page blob */
		        long blobSize = (null == reqParams.getBlobSize()) ? tempFile.length() : reqParams.getBlobSize();
				((CloudPageBlob) blob).create(blobSize);
				/* upload the data */
				fileInputStream = new FileInputStream(tempFile);
				((CloudPageBlob) blob).uploadPages(fileInputStream, 0, Constants.PAGEBLOB_MINIMUM_SIZE, accCondtion, null, null);

			}else{
				int length = 0;
				if (null != reqParams.getContent()){
					byte[] contentBytes = reqParams.getContent().getBytes(Constants.DEFAULT_CHARSET);
					length = contentBytes.length;
					FileOutputStream fileOutputStream = new FileOutputStream(tempFile);
					fileOutputStream.write(contentBytes);
					fileOutputStream.close();	
				}
				fileInputStream = new FileInputStream(tempFile);
				blob.upload(fileInputStream, length, accCondtion, null, null);
			}			
			/*  Delete tmp file when upload success. */
			if (null != fileInputStream) {fileInputStream.close();}
			tempFile.deleteOnExit();
			result = true;
			Logger.trace("the blob：{} has been created. ", reqParams.getBlob());

		} catch (Exception ex) {
			String errMessage = "Unexpected exception occurred when creating the blob: " + reqParams.getBlob()+ ". " + ex.getMessage();
			BfsUtility.throwBlobfsException(ex, errMessage);
		} 
		return result;
	}	
	/* delete the blob */	
	/**
	 * @param reqParams, container, blob, blobType, doForce
	 * @return
	 * @throws BfsException
	 */
	public final static boolean deleteBlob(BlobReqParams reqParams) throws BfsException {
		boolean result = false;
		AccessCondition accCondtion = new AccessCondition();
		CloudBlob blob = getBlobReference(reqParams);		
		try {
			/* set the lease ID */
			if (null != reqParams.getLeaseID() && reqParams.getLeaseID().length() > 0){
				accCondtion.setLeaseID(reqParams.getLeaseID());
			} else if (blob.getProperties().getLeaseStatus().equals(LeaseStatus.LOCKED)){
				reqParams.setBlobInstance(blob);
				String leaseID = getBlobMetadata(reqParams, Constants.BLOB_META_DATA_LEASE_ID_KEY);
				if (null != leaseID ) { accCondtion.setLeaseID(leaseID); }
			}
			
			blob.deleteIfExists(DeleteSnapshotsOption.INCLUDE_SNAPSHOTS, accCondtion, null, null);
			result = true;
			Logger.trace("the blob：{} has been deleted. ", reqParams.getBlob());

		} catch (Exception ex) {
			String errMessage = "Unexpected exception occurred when deleting the blob: " + reqParams.getBlob()+ ". " + ex.getMessage();
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
		return result;
	}
	/**
	 * check if the blob exists
	 * @param reqParams
	 * @return
	 * @throws BfsException
	 */
	public final static boolean blobExists(BlobReqParams reqParams, boolean checkCache) throws BfsException {
		boolean result = false;
		String key = cachedFilesInMemManager.getTheFormattedKey(reqParams.getContainer(), reqParams.getBlob());
		if(checkCache)
		{
			// process the cache logic, if found the blob in the cache ,return it
			BlobProperties tmpbPorperties = (BlobProperties) cachedFilesInMemManager.get(key);
			if (null != tmpbPorperties){return true;}
		}
		/* for checking whether the blob exists, the blob type can be used blindly */
		BfsBlobType bfsBlobType = (null == reqParams.getBfsBlobType()) 
									? BfsBlobType.BLOCKBLOB :reqParams.getBfsBlobType();
		reqParams.setBfsBlobType(bfsBlobType);
		CloudBlob blob = getBlobReference(reqParams);		
		try {
			if (blob.exists()){
				result = true;
			}
		} catch (Exception ex) {
			String errMessage = "Unexpected exception occurred when checking the blob: " + reqParams.getBlob()+ ". " + ex.getMessage();
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
		return result;
	}
	
	/* move or rename the append blob */
	/**
	 * @param reqParams: optMode, blobType, container, blob, destContainer, destBlob, doForce
	 * @return
	 * @throws BfsException
	 */
	public final static boolean CopyOrmoveBlobCrossContainer (BlobReqParams reqParams) throws BfsException {
		boolean result = false;
		try {
			/* move or copy the virtual directory, default is copy */
			BlobOptMode optMode = (null == reqParams.getBlobOptMode()) ? BlobOptMode.COPY : reqParams.getBlobOptMode();
			String sourceContainer = reqParams.getContainer();
			String sourePath = reqParams.getBlob();
			String destContainer = reqParams.getDestContainer();
			String destPath = reqParams.getDestBlob();
			boolean doForce = reqParams.isDoForoce();
			/* set the default type */
			BfsBlobType bfsBlobType;
			String leaseID = null;
			/* get the blob type, if the caller is CopyOrmoveVirtualDirectory, don't need the check type and existing  */
			if (null == (bfsBlobType = reqParams.getBfsBlobType())){
				if (null == (bfsBlobType = getBlobType(reqParams))){
					String errMessage = "The source blob: " + reqParams.getBlobFullPath() + " does not exist.";
					throw new BfsException(errMessage);
				}
			}
			/* the interval for checking the coping status */
			int getCSinterval = 0;
			/* leaseTimeInSeconds should be between 15 and 60 */
			int minLockedSec = Constants.BLOB_LOCKED_SECONDS;
			AccessCondition accCondtion = null;
			CloudBlob sourceBlob = null;
			CloudBlob destBlob = null;
			long startTime = 0;
			
			/* check the source path and dest path, if they are the same, azure will throw lease exception */
			if (sourceContainer.equals(destContainer) && sourePath.equals(destPath)){
				return true;
			}
			sourceBlob = getBlobReference(reqParams);
			BlobReqParams destReq = new BlobReqParams();
			destReq.setBfsBlobType(bfsBlobType);
			destReq.setContainer(destContainer);
			destReq.setBlob(destPath);
			destBlob = getBlobReference(destReq);

//			if (destBlob.exists() && !doForce){
//				String errMessage = "The target blob: " + reqParams.getDestBlobFullPath() + " already exists.";
//				throw new BlobfsException(errMessage);	
//			}
			/* lock the source blob */				 
			leaseID = sourceBlob.acquireLease(minLockedSec, null);
			/* start the move */
			String copyJobId = destBlob.startCopy(sourceBlob.getUri());
			startTime = System.currentTimeMillis();
			Logger.trace("Start copying, from {} to {} ,Copy ID is {}, The size is {}.", reqParams.getBlobFullPath(), 
						reqParams.getDestBlobFullPath() ,copyJobId, sourceBlob.getProperties().getLength());
			CopyState copyState = destBlob.getCopyState();
			accCondtion = new AccessCondition();
			accCondtion.setLeaseID(leaseID);
            while (copyState.getStatus().equals(CopyStatus.PENDING)) {
            	getCSinterval ++ ;
            	Thread.sleep(50);
            	/* keep locking the object */
            	if (getCSinterval >= (int) 20 * minLockedSec){
            		getCSinterval = 0;
            		sourceBlob.renewLease(accCondtion);
            	}
            }                
			/* free the source blob */
			//if (sourceBlob.getProperties().getLeaseStatus().equals(LeaseStatus.LOCKED)){
				sourceBlob.releaseLease(accCondtion);
			//}
			/* if the operation mode is move, we should delete the source file */
			if ("MOVE".equals(optMode.toString())){
				/* delete the source blob */
				sourceBlob.deleteIfExists(DeleteSnapshotsOption.INCLUDE_SNAPSHOTS, null, null, null);
			}
			result = true;
			long totalTime = System.currentTimeMillis() - startTime;
			/* refresh the properties */
			destBlob.downloadAttributes();
			Logger.trace("Copy completed, from {} to {} ,Copy ID is {}, The size is {}, The time spent is {} ms.",reqParams.getBlobFullPath(), 
					reqParams.getDestBlobFullPath() ,copyJobId, destBlob.getProperties().getLength(), totalTime);
		
		} catch (Exception ex) {
		    String errMessage = "Unexpected exception occurred when moving the blob from " 
		    					+reqParams.getBlobFullPath() + " to " + reqParams.getDestBlobFullPath() + ". " + ex.getMessage() ;
		    BfsUtility.throwBlobfsException(ex, errMessage);
		}
		return result;
	}
	
	/* set the meta data of the blob */
	/**
	 * @param reqParams: bfsBlobType, container, blob
	 * @param key
	 * @param value
	 * @throws BfsException
	 */
	public final static void setBlobMetadata (BlobReqParams reqParams, String key, String value) throws BfsException {
		CloudBlob blob = null;
		String leaseID = null;
		try {
			if (null == reqParams.getBlobInstance()){
				blob = getBlobReference(reqParams);
			} else {
				blob = reqParams.getBlobInstance();
			}
			leaseID = (null != reqParams.getLeaseID()) ? reqParams.getLeaseID() : null;
			AccessCondition accCondtion = new AccessCondition();
			if (null != leaseID){
				accCondtion.setLeaseID(leaseID);
			}
			blob.downloadAttributes();
			HashMap<String, String> metadata = blob.getMetadata();
		    if (null == metadata) {
		      metadata = new HashMap<String, String>();
		    }
		    metadata.put(key, value);
		    blob.setMetadata(metadata);
		    /* upload the meta data to blob service */
		    blob.uploadMetadata(accCondtion, null, null);
		} catch (Exception ex) {
			String errMessage = "Unexpected exception occurred when set metadata of the blob: " 
    				+ reqParams.getBlobFullPath() + ". key:" + key + " . value: " +value + ". " + ex.getMessage(); 
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
		
	}
	/* get the meta data of the blob */
	/**
	 * @param reqParams: bfsBlobType, container, blob
	 * @param keyAlternatives
	 * @return
	 * @throws BfsException
	 */
	public final static String getBlobMetadata (BlobReqParams reqParams, String... keyAlternatives) throws BfsException {
		CloudBlob blob = null;
		reqParams.setBfsBlobType(null);
		try {
			if (null == reqParams.getBlobInstance()){
				blob = getBlobReference(reqParams);
			} else {
				blob = reqParams.getBlobInstance();
			}
			/* down load the meta data firstly */
			blob.downloadAttributes();
			HashMap<String, String> metadata = blob.getMetadata();
		    if (null == metadata) {
		      return null;
		    }
		    for (String key : keyAlternatives) {
		      if (metadata.containsKey(key)) {
		        return metadata.get(key);
		      }
		    }
		} catch (StorageException ex) {
			String errMessage = "Unexpected exception occurred when set metadata of the blob: " 
    				+ reqParams.getBlobFullPath() + ". keys: " + keyAlternatives.toString() +  ". " + ex.getMessage(); 
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
	    return null;
	}
	/* remove the meta data of the blob */
	/**
	 * @param reqParams: bfsBlobType, container, blob
	 * @param keyAlternatives
	 * @return
	 * @throws BfsException
	 */
	public final static void removeBlobMetadata (BlobReqParams reqParams, String key) throws BfsException {
		CloudBlob blob = null;
		String leaseID = null;
		try {
			if (null == reqParams.getBlobInstance()){
				blob = getBlobReference(reqParams);
			} else {
				blob = reqParams.getBlobInstance();
			}
			leaseID = (null != reqParams.getLeaseID()) ? reqParams.getLeaseID() : null;
			AccessCondition accCondtion = new AccessCondition();
			if (null != leaseID && blob.getProperties().getLeaseStatus().equals(LeaseStatus.LOCKED)){
					accCondtion.setLeaseID(leaseID);
			}
			/* down load the meta data firstly */
			blob.downloadAttributes();
			HashMap<String, String> metadata = blob.getMetadata();
		    if (metadata != null) {
		    	 if (metadata.containsKey(key)) {
			        metadata.remove(key);
			     }
			     blob.setMetadata(metadata);
			     /* upload the meta data to blob service */
			     blob.uploadMetadata(accCondtion, null, null);
		    }		 
		} catch (StorageException ex) {
			String errMessage = "Unexpected exception occurred when remove metadata of the blob: " 
    				+ reqParams.getBlobFullPath() + ". key: " + key + ". " + ex.getMessage(); 
			BfsUtility.throwBlobfsException(ex, errMessage);
		}
	}
	/* Getting the properties of the blob */
	/**
	 * @param reqParams: blobType，container, blob
	 * @throws BfsException
	 */
	public final static BlobProperties getBlobProperties (BlobReqParams reqParams) throws BfsException {
		BlobProperties blobPorperties = new BlobProperties();
		CloudBlob blob = null;
		try {
			
			// process the cache logic, if found the blob in the cache ,return it
			String key = cachedFilesInMemManager.getTheFormattedKey(reqParams.getContainer(), reqParams.getBlob());
			BlobProperties tmpbPorperties = (BlobProperties) cachedFilesInMemManager.get(key);
			if (null != tmpbPorperties){return tmpbPorperties;}
			// process the normal logic
			if (null == reqParams.getBlobInstance()){
				blob = getBlobReference(reqParams);
			} else {
				blob = reqParams.getBlobInstance();
			}
			blob.downloadAttributes();
			blobPorperties.setName(blob.getName());
			blobPorperties.setBfsBlobType(blob.getProperties().getBlobType());
			blobPorperties.setContentMD5(blob.getProperties().getContentMD5());
			blobPorperties.setEtag(blob.getProperties().getEtag());
			blobPorperties.setCreated(blob.getProperties().getLastModified());
			blobPorperties.setLastModified(blob.getProperties().getLastModified());
			blobPorperties.setLength(blob.getProperties().getLength());	
			blobPorperties.setActualLength(blob.getProperties().getLength());
			/* fill the page blob */
			if ("PAGEBLOB".equals(blobPorperties.getBfsBlobType().toString())){
				blobPorperties.setActualLength(getPageBlobActualLength(reqParams));
			}
			
			// process the cache logic, cache it into db
			cachedFilesInMemManager.put(key, blobPorperties);

		} catch (Exception ex) {
		    String errMessage = "Unexpected exception occurred when getting the blob: " + reqParams.getBlobFullPath() + " properties. " + ex.getMessage(); 
		    throw new BfsException(errMessage);
		}		
		return blobPorperties;	
	}
	/* check the directory exists or not */
	/**
	 * @param reqParams: container, blob
	 * @return
	 * @throws BfsException
	 */
	public final static boolean virtualDirectoryExists (BlobReqParams reqParams, boolean checkCache) throws BfsException {
		boolean result  = false;
		try {
			
			String virtualDir = (reqParams.getBlob().endsWith("/")) ? reqParams.getBlob() : reqParams.getBlob() + "/";

			// process the cache logic, if found the blob in the cache ,return it
			String key = cachedFilesInMemManager.getTheFormattedKey(reqParams.getContainer(), virtualDir);
			if(checkCache)
			{
				BlobProperties tmpbPorperties = (BlobProperties) cachedFilesInMemManager.get(key);
				if (null != tmpbPorperties){return true;}
			}
			
			CloudBlobContainer container = ContainerService.getPrivateContainer(reqParams.getContainer());
			/* count() is not available up to this 5.0.0 */
			/* should check the last char, should add slash */
			Iterable<ListBlobItem> collection = container.listBlobs(virtualDir, true);	
			if (null != collection){
				for (ListBlobItem blobItem : collection) {
					if (blobItem instanceof CloudBlob) {
						/* make sure at least one blob within the directory */
						result = true;
						/* cache it */
						BlobProperties bProperties = new BlobProperties();
						bProperties.setBfsBlobType(BfsBlobType.VIRTUALDIRECTORY);
						cachedFilesInMemManager.put(key, bProperties);
						break;
					}
				}
			}
		} catch (Exception ex) {
		    String errMessage = "Unexpected exception occurred when checking the virtual directory : " + reqParams.getBlobFullPath() + ". " + ex.getMessage(); 
		    BfsUtility.throwBlobfsException(ex, errMessage);

		}		
		return result;
	}
	
	/* get the blob type */
	/**
	 * @param reqParams: container, blob
	 * @return
	 * @throws BfsException
	 */
	public final static BfsBlobType getBlobType (BlobReqParams reqParams) throws BfsException {
		BfsBlobType bfsBlobType = null;
		try {
			bfsBlobType = getBlobProperties(reqParams).getBfsBlobType();
		} catch (Exception ex) {
		    String errMessage = "Unexpected exception occurred when getting the blob type : " + reqParams.getBlobFullPath() + ". " + ex.getMessage(); 
		    BfsUtility.throwBlobfsException(ex, errMessage);

		}		
		return bfsBlobType;
	}
	/* change block blob to append blob */
	/**
	 * @param reqParams:blobType, container, blob, localTmpDir
	 * @return
	 * @throws BfsException
	 */
	public final static boolean changeBlocBlobToAppendBlob (BlobReqParams reqParams) throws BfsException {
		String sourceBlob = reqParams.getBlob();
		String leaseID = null;
		int minLockedSec = Constants.BLOB_LOCKED_SECONDS;
		AccessCondition accCondtion = new AccessCondition();
        String randomString = Long.toHexString(Double.doubleToLongBits(Math.random()));
		String appendBlobTempName = sourceBlob + "-" + randomString + ".tmp";
		boolean result = false;		
		/* check the blob type */
		if ("APPENDBLOB".equals(getBlobType(reqParams).toString())){
			Logger.trace("The blob: {} is the append blob already!", sourceBlob);
			return true;
		}
		try {
			Logger.trace("Start changing the type of the blob: {}. from block to append", sourceBlob);
			CloudBlob blob = getBlobReference(reqParams);
			blob.downloadAttributes();
			long srcBlobSize = blob.getProperties().getLength();
			/* create the temporary blob */
			BlobReqParams outsReq = new BlobReqParams();
			outsReq.setContainer(reqParams.getContainer());
			outsReq.setBlob(appendBlobTempName);
			outsReq.setBfsBlobType(BfsBlobType.APPENDBLOB);
			outsReq.setDoForoce(true);
			createBlob(outsReq);
			/* start to transfer the data */
			if (srcBlobSize > 0){				
				/* create the output stream*
				 * read data from source blob */
				BlobReqParams insReq = new BlobReqParams();
				insReq.setContainer(reqParams.getContainer());
				insReq.setBlob(reqParams.getBlob());
				insReq.setBfsBlobType(BfsBlobType.BLOCKBLOB);
				BlobBufferedIns bbIns = new BlobBufferedIns(insReq);
				/* create the output stream
				 * write data the to temporary blob */
				BlobBufferedOus bbOus =  new BlobBufferedOus(outsReq);
				/* lock the blob */
				leaseID = blob.acquireLease(minLockedSec, null);
				accCondtion.setLeaseID(leaseID);
				/* set counters */
				int blockSize = Constants.UPLOAD_MULTIPARTS_SPLIT_CHUNK_SIZE;
				long bytesLeft = srcBlobSize;
				/* loop transfer the date from the source blob to target blob */
				while( bytesLeft > 0 ) {
					/* how much to read (only last chunk may be smaller) */
					int bytesToRead = 0;
					if ( bytesLeft >= (long)blockSize ) {
						bytesToRead = blockSize;
					} else {
						bytesToRead = (int)bytesLeft;
					}
					byte[] bytesReaded = new byte[bytesToRead];
					if (bbIns.read(bytesReaded, (int)bbIns.readOffset, bytesToRead) != -1){
						bbOus.write(bytesReaded, 0, bytesToRead);
					}   
					/* increment/decrement counters */         
					bytesLeft -= bytesToRead;
					/* renew the lease */
					blob.renewLease(accCondtion);
				}/* end of while*/
				bbIns.close();           
				/* close the output stream */
				bbOus.close();			
			}
			
			/* due the new blob and original blob are different type, so should delete the original blob first */
			/* if the delete operation failed, we should keep the temporary append blob as backup and return false */
			BlobReqParams delReq = new BlobReqParams();
			delReq.setBlob(sourceBlob);
			delReq.setContainer(reqParams.getContainer());
			delReq.setBfsBlobType(BfsBlobType.BLOCKBLOB);
			if (srcBlobSize > 0) { delReq.setLeaseID(leaseID);}
			deleteBlob(delReq);

			/* rename temporary file name to the original blob name */
			BlobReqParams renameReq = new BlobReqParams();
			renameReq.setBlobOptMode(BlobOptMode.MOVE);
			renameReq.setContainer(reqParams.getContainer());
			renameReq.setBlob(appendBlobTempName);
			renameReq.setDestBlob(sourceBlob);
			renameReq.setDestContainer(reqParams.getContainer());
			renameReq.setDoForoce(true);
			renameReq.setBfsBlobType(BfsBlobType.APPENDBLOB);
			CopyOrmoveBlobCrossContainer(renameReq);
			result = true;
			Logger.trace("The block blob: {} is changed to the append blob successfully.", sourceBlob);
		} catch (Exception ex) {
			String errMessage = "Unexpected exception occurred when changing the blob type : " + reqParams.getBlobFullPath() + ". " + ex.getMessage(); 
			BfsUtility.throwBlobfsException(ex, errMessage);
		} finally {
			/* clean the local temporary file  */			
			System.gc();
		}		
		return result;
	}
	/* change the blob size */
	/**
	 * @param reqParams:blobType, container, blob, localTmpDir
	 * @return
	 * @throws BfsException
	 */
	public final static boolean changeBlobSize (BlobReqParams reqParams) throws BfsException {
		String sourceBlob = reqParams.getBlob();
		long tgtBlobSize = reqParams.getBlobSize();
		int minLockedSec = Constants.BLOB_LOCKED_SECONDS;
		String leaseID;
		BfsBlobType blobType = null;
        String randomString = Long.toHexString(Double.doubleToLongBits(Math.random()));
		String blobTempName = sourceBlob + "-" + randomString + ".tmp";
		boolean result = false;
		try {
			CloudBlob blob = getBlobReference(reqParams);
			blob.downloadAttributes();
			long srcBlobSize = blob.getProperties().getLength();
			/* the size of source blob is zero */
			if (srcBlobSize == 0){ return true; }
			/* get the blob type */
			if (null == reqParams.getBfsBlobType()){
				blobType = getBlobType(reqParams);
				reqParams.setBfsBlobType(blobType);
			}
			Logger.trace("Start resizing the the blob: {} from {} to {}.", sourceBlob, srcBlobSize, tgtBlobSize);
			/* get the lease ID */
			if (null != reqParams.getLeaseID() && reqParams.getLeaseID().length() > 0){
				leaseID = getBlobMetadata(reqParams, Constants.BLOB_META_DATA_LEASE_ID_KEY);
			} else {				
				leaseID = blob.acquireLease(minLockedSec, null);
			}
			/* resize to zero, overwrite the original blob with with zero length */
			if (tgtBlobSize == 0){
				reqParams.setDoForoce(true);
				reqParams.setLeaseID(leaseID);
				reqParams.setBfsBlobType(blobType);
				createBlob(reqParams);
				Logger.trace("The size of the blob: {} is changed to {} from {} successfully.", sourceBlob, srcBlobSize, tgtBlobSize );
				return true;
			}
			/* resize the page blob */
			if ("PAGEBLOB".equals(reqParams.getBfsBlobType().toString())){
				return changePageBlobSize(reqParams);
			}
			/* read data from source blob */
			BlobReqParams insReq = new BlobReqParams();
			insReq.setContainer(reqParams.getContainer());
			insReq.setBlob(reqParams.getBlob());
			insReq.setBfsBlobType(reqParams.getBfsBlobType());
			BlobBufferedIns bbIns = new BlobBufferedIns(insReq);
			
			/* write data to the temporary blob */
			BlobReqParams outsReq = new BlobReqParams();
			outsReq.setContainer(reqParams.getContainer());
			outsReq.setBlob(blobTempName);
			outsReq.setBfsBlobType(reqParams.getBfsBlobType());
			outsReq.setDoForoce(true);
			createBlob(outsReq);
			BlobBufferedOus bbOus =  new BlobBufferedOus(outsReq);
			/* we need fill the target blob with zero bytes when extending source blob */
			long bytesToFillWithZero = (int) Math.max(0, tgtBlobSize - srcBlobSize);
			/* start to transfer the data */
			/* set counters */
			long bytesFromSrcBlob = Math.min(tgtBlobSize, srcBlobSize);
            int blockSize = Constants.UPLOAD_MULTIPARTS_SPLIT_CHUNK_SIZE;
            long bytesLeft = bytesFromSrcBlob;
            /* loop transfer the date from the source blob to target blob */
            while( bytesLeft > 0 ) {
                 /* how much to read (only last chunk may be smaller) */
                int bytesToRead = 0;
                if ( bytesLeft >= (long)blockSize ) {
                	bytesToRead = blockSize;
                } else {
                	bytesToRead = (int)bytesLeft;
                }
                byte[] bytesReaded = new byte[bytesToRead];
              	if (bbIns.read(bytesReaded, (int)bbIns.readOffset, bytesToRead) != -1){
              		bbOus.write(bytesReaded, 0, bytesToRead);
                }   
                /* increment/decrement counters */         
                bytesLeft -= bytesToRead;
            }/* end of while*/
            bbIns.close();
            
            /* expand the blob with zero bytes */
            if (bytesToFillWithZero > 0){
                 bytesLeft = bytesToFillWithZero;
	             while( bytesLeft > 0 ) {	            	
	                 /* how much to read (only last chunk may be smaller) */
	                 int bytesToRead = 0;
	                 if ( bytesLeft >= (long)blockSize ) {
	                  	bytesToRead = blockSize;
	                 } else {
	                  	bytesToRead = (int)bytesLeft;
	                 }
	                 byte[] bytesReaded = new byte[bytesToRead];
	          		 bbOus.write(bytesReaded, 0, bytesToRead);  
	                 /* increment/decrement counters */         
	                 bytesLeft -= bytesToRead;
	            }
            }
            /* must close the output stream, this will commit the uploaded data to storage service */
            bbOus.close();
			/* in order to keep the file safe we should upload the temp file, and then renmae */
			/* if the delete operation failed, we should keep the temporary append blob as backup and return false */
			BlobReqParams delReq = new BlobReqParams();
			delReq.setBlob(sourceBlob);
			delReq.setContainer(reqParams.getContainer());
			delReq.setBfsBlobType(BfsBlobType.BLOCKBLOB);
			delReq.setLeaseID(leaseID);
			deleteBlob(delReq);

			/* rename temporary file name to the original blob name */
			BlobReqParams renameReq = new BlobReqParams();
			renameReq.setBlobOptMode(BlobOptMode.MOVE);
			renameReq.setContainer(reqParams.getContainer());
			renameReq.setBlob(blobTempName);
			renameReq.setDestBlob(sourceBlob);
			renameReq.setDestContainer(reqParams.getContainer());
			renameReq.setDoForoce(true);
			renameReq.setBfsBlobType(reqParams.getBfsBlobType());
			CopyOrmoveBlobCrossContainer(renameReq);

			result = true;
			Logger.trace("The size of the blob: {} is changed from {} to {} successfully.", sourceBlob, srcBlobSize, tgtBlobSize );
		} catch (Exception ex) {
			String errMessage = "Unexpected exception occurred when resizing the blob: " + reqParams.getBlobFullPath() + ". " + ex.getMessage(); 
			BfsUtility.throwBlobfsException(ex, errMessage);
		} finally {
			/* clean the temporary files */
			System.gc();
		}		
		return result;
	}
	
	/* change the blob size */
	public final static boolean changePageBlobSize (BlobReqParams reqParams) throws BfsException {
		boolean result = false;
		String sourceBlob = reqParams.getBlob();
		long tgtBlobSize = reqParams.getBlobSize();
		long finalBlobSize = Constants.PAGEBLOB_MINIMUM_SIZE;
		long srcBlobSize = -1;
		try {
			reqParams.setBfsBlobType(BfsBlobType.PAGEBLOB);
			CloudPageBlob blob = (CloudPageBlob)getBlobReference(reqParams);
			if (blob.exists()){
				BlobReqParams getSizeReq = new BlobReqParams();
				getSizeReq.setContainer(reqParams.getContainer());
				getSizeReq.setBlob(reqParams.getBlob());
				getSizeReq.setBfsBlobType(reqParams.getBfsBlobType());
				long actualPageBlobSize = getPageBlobActualLength(getSizeReq);
				if (actualPageBlobSize >= tgtBlobSize){ // shrink the blob
					finalBlobSize = actualPageBlobSize;
				}else{// expand the blob
					finalBlobSize = tgtBlobSize;
					if (tgtBlobSize >= Constants.PAGEBLOB_SIZE_LIMIT){
						finalBlobSize = Constants.PAGEBLOB_SIZE_LIMIT;
					}
				}
				srcBlobSize = blob.getProperties().getLength();
				blob.resize(finalBlobSize, null, null, null);
				result = true;
				Logger.trace("The size of the blob: {} is changed from {} to {} successfully.", sourceBlob, srcBlobSize, tgtBlobSize );
			}
		} catch (Exception ex) {
		    String errMessage = "Unexpected exception occurred when getting the actual size of the blob: " + reqParams.getBlobFullPath() + " properties. " + ex.getMessage(); 
		    throw new BfsException(errMessage);
		}
		return result;
	}
	
	/* get the page blob actual size */
	public final static long getPageBlobActualLength (BlobReqParams reqParams) throws BfsException {
		long pageBlobActualSize = -1;
		try {
			reqParams.setBfsBlobType(BfsBlobType.PAGEBLOB);
			CloudPageBlob blob = (CloudPageBlob)getBlobReference(reqParams);
			if (blob.exists()){
				ArrayList<PageRange> pageRanges = blob.downloadPageRanges();
			    if (pageRanges.size() == 0) {
			      return pageBlobActualSize = 0;
			    }
			    pageBlobActualSize =  pageRanges.get(0).getEndOffset() - pageRanges.get(0).getStartOffset() + 1;
			}
		} catch (Exception ex) {
		    String errMessage = "Unexpected exception occurred when getting the actual size of the blob: " + reqParams.getBlobFullPath() + " properties. " + ex.getMessage(); 
		    throw new BfsException(errMessage);
		}
		return pageBlobActualSize;
	}
}
