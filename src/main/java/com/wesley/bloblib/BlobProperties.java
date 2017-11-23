package com.wesley.bloblib;

import java.util.Date;

import com.microsoft.azure.storage.blob.BlobType;
import com.microsoft.azure.storage.blob.LeaseDuration;
import com.microsoft.azure.storage.blob.LeaseState;
import com.microsoft.azure.storage.blob.LeaseStatus;

public class BlobProperties {
	String name;
	BfsBlobType bfsBlobType;
	String contentMD5;
	String etag;
	Long length;
	Long actualLength; // for page blob
	Date created;
	Date lastModified;
	
	public BlobProperties() {
		this.name = "";
		this.bfsBlobType = BfsBlobType.BLOCKBLOB;
		this.contentMD5 = "";
		this.etag = "";
		this.length = 0L;
		this.actualLength = 0L;
		this.created = new Date(0);
		this.lastModified = new Date(0);
		
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}



	public BfsBlobType getBfsBlobType() {
		
		return bfsBlobType;
	}

	public void setBfsBlobType(BfsBlobType bfsBlobType) {
		this.bfsBlobType = bfsBlobType;
	}
	
	public void setBfsBlobType(BlobType blobType) {
		BfsBlobType bfsBlobType;
		switch (blobType.toString()){
			case "APPEND_BLOB":
				bfsBlobType = BfsBlobType.APPENDBLOB;
				break;
			case "PAGE_BLOB":
				bfsBlobType = BfsBlobType.PAGEBLOB;
				break;
			default:
				bfsBlobType = BfsBlobType.BLOCKBLOB;
				break;	
		}
		this.bfsBlobType = bfsBlobType;
	}

	public String getContentMD5() {
		return contentMD5;
	}
	public void setContentMD5(String contentMD5) {
		this.contentMD5 = contentMD5;
	}
	public String getEtag() {
		return etag;
	}
	public void setEtag(String etag) {
		this.etag = etag;
	}
	public Long getLength() {
		return length;
	}
	public void setLength(Long length) {
		this.length = length;
	}
	public Long getActualLength() {
		return actualLength;
	}
	public void setActualLength(Long actualLength) {
		this.actualLength = actualLength;
	}
	
	public Date getCreated() {
		return created;
	}

	public void setCreated(Date created) {
		this.created = created;
	}

	public Date getLastModified() {
		return lastModified;
	}
	public void setLastModified(Date lastModified) {
		this.lastModified = lastModified;
	}

	@Override
	public String toString() {
		return "BlobProperties [name=" + name + ", bfsBlobType=" + bfsBlobType + ", contentMD5=" + contentMD5
				+ ", etag=" + etag + ", length=" + length + ", actualLength=" + actualLength + ", created=" + created
				+ ", lastModified=" + lastModified + "]";
	}
	
}
