package com.wesley.bloblib;


import java.util.Objects;

import org.pmw.tinylog.Logger;

import com.wesley.bloblib.utils.BfsUtility;


@SuppressWarnings("static-access")
public class BfsPath{
	String bfsPrefix = Configuration.DEFAULT_BLOB_PREFIX.trim();
	String path;
	String bfsFullPath;
	String container;
	String virtualDirectoy;
	String blob;
	
	public BfsPath(String path) {
		bfsFullPath = getFullPath(path);	
	}
	
	private final String getFullPath(String path){
		
		if (bfsPrefix.length() >= 1 && !bfsPrefix.endsWith("/")){
			bfsPrefix = bfsPrefix + "/";
		}
		bfsFullPath = (bfsPrefix + path).replaceAll("/+", "/");
		return bfsFullPath;		
	}
	
	public String getBfsPrefix() {
		return bfsPrefix;
	}

	public String getBfsFullPath() {
		return bfsFullPath;
	}

	public void setBfsFullPath(String bfsFullPath) {
		this.bfsFullPath = bfsFullPath;
	}
	
	/* get the container name for the string :/container/folder/blob.txt */
	public String getContainer () {
		/* avoid the leading empty string */
		// bfsFullPath.replaceFirst("^/", "").split("/");
		String containerName = bfsFullPath.substring(1).split("/")[0];
		return containerName;
	}
	/* get the blob name for the string :/container/folder/blob.txt */
	public String getBlob () {
		String blobName = "";
		if (bfsFullPath.contains("/") && bfsFullPath.indexOf("/") != bfsFullPath.lastIndexOf("/")) {
			/* plus 2, due to substring, and do not need "/" */
			int beginIndex = bfsFullPath.substring(1).indexOf("/") + 2;
			blobName = bfsFullPath.substring(beginIndex);
		} 
		return blobName;
	}
	
	/**
	 * get the file properties
	 * @return
	 */
	public PathProperties getBfsPathProperties(){
		PathProperties pathProperties = new PathProperties();
		pathProperties.setBfsPathType(BfsPathType.INVALID);
		try {
			if (Objects.equals(bfsFullPath, "/")) {
				/*  root directory : /:*/
				pathProperties.setBfsPathType(BfsPathType.ROOT);
			} else if (!getContainer().equals("")  && getBlob().equals("")) {
				/* container: /container */
				if (ContainerService.containerExists(getContainer(), true)) {
					/* TODO: put into cache here */
					ContainerProperties containerProperties = ContainerService.getContainerProperties(getContainer());
					pathProperties.setName(containerProperties.getName());
					pathProperties.setBfsPathType(BfsPathType.CONTAINER);
					pathProperties.setCtime(containerProperties.getCreated());
					pathProperties.setMtime(containerProperties.getLastModified());
				} 
			} else if (!getContainer().equals("")  && !getBlob().equals("")) {
				BlobReqParams checkReq = new BlobReqParams();
				checkReq.setContainer(getContainer());
				checkReq.setBlob(getBlob());
				if (BlobService.blobExists(checkReq, true)){
					/* blob: /container/folder/file1 */
					/* get the blob type */
					/* over writte the blob type, due to set the block_blob when checking if exists*/
					checkReq.setBfsBlobType(BfsBlobType.INVALID);
					if (!BfsUtility.isWindows()){
						if (null != BlobService.getBlobMetadata(checkReq, Constants.BLOB_META_DATA_ISlINK_KEY)){
							pathProperties.setBfsPathType(BfsPathType.LINK);
						} else {
							pathProperties.setBfsPathType(BfsPathType.BLOB);
						}
					}else{
						pathProperties.setBfsPathType(BfsPathType.BLOB);
					}
					BlobProperties blobPorperties = BlobService.getBlobProperties(checkReq);
					pathProperties.setName(blobPorperties.getName());
					pathProperties.setBfsBlobType(blobPorperties.getBfsBlobType());
					pathProperties.setCtime(blobPorperties.getCreated());
					pathProperties.setMtime(blobPorperties.getLastModified());
					pathProperties.setSize(blobPorperties.getActualLength());
					
				} else if (BlobService.virtualDirectoryExists(checkReq, true)){
					/* virtual directory : /container/folder/ */
					/* TODO: put into cache here */
					pathProperties.setBfsPathType(BfsPathType.SUBDIR);
					String blobName = getBlob() + "/" + Constants.VIRTUAL_DIRECTORY_NODE_NAME;
					BlobReqParams vdParams = new BlobReqParams();
					vdParams.setContainer(getContainer());
					vdParams.setBlob(blobName);
					vdParams.setBfsBlobType(BfsBlobType.BLOCKBLOB);
					if (!BlobService.blobExists(vdParams, true)){
						BlobService.createBlob(vdParams);
						/* set the uid and gid */
//						BlobService.setBlobMetadata(vdParams, Constants.BLOB_META_DATE_UID_KEY, Integer.toString(uid));
//						BlobService.setBlobMetadata(vdParams, Constants.BLOB_META_DATE_GID_KEY, Integer.toString(gid));
					}
					BlobProperties blobPorperties = BlobService.getBlobProperties(vdParams);
					pathProperties.setName(blobPorperties.getName());					
					pathProperties.setCtime(blobPorperties.getCreated());
					pathProperties.setMtime(blobPorperties.getLastModified());
				} 
			}
		} catch (Exception ex) {
			Logger.error(ex.getMessage());
		}
		return pathProperties;
	}

	public String getParent(){
		/* over write the Paht.getParent */
		if (Objects.equals(bfsFullPath, "/")) {
			return null;
		}		
		boolean endsWithSlash = bfsFullPath.endsWith("/");
		int startIndex = endsWithSlash ? bfsFullPath.length() - 2 : bfsFullPath.length() - 1;
		String parentDir = bfsFullPath.substring(0, bfsFullPath.lastIndexOf("/",startIndex));
		return parentDir;
	}
	
	public String getCurrentPath(){
		String file = bfsFullPath.substring(getParent().length() + 1, bfsFullPath.length());
		/* remove last slash */
		file  = BfsUtility.removeLastSlash(file);
		return file;
		
	}
	
}
