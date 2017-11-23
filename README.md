BlobLib
=====
BlobLib is the core libraries of the blobfs and blobfs-win, blobfs is a distributed [FUSE](http://fuse.sourceforge.net) based file system backed by [Microsoft azure blob storage service](https://azure.microsoft.com/en-us/services/storage/blobs/). It allows you to mount the containers/blobs in the storage account as a the local folder/driver. , no matter it is a Linux system or a Windows system. It support the cluster mode. you can mount the blob container (or part of it) across multiple linux and windows nodes. BlobLob can be used separately.

## Important Notes:
* You can find the linux/Mac version of the [blobfs](https://github.com/wesley1975/blobfs).
* You can find the windows version of the [blobfs-win](https://github.com/wesley1975/blobfs-win).
* If you are interested in contributing, please contact me via jie1975.wu@gmail.com

## Project Goals
Packaged the core operations of the azure blob storage as a separated library, this will make it easier for supporting different OS platform. This library can also be used alone with various projects.


## Features:
* Support man operations of the container,virtual directory and blob. such as create, delete, list, read, write etc.
* New extension class of the InputStream and OutputStream. add a new cache layer and is optimized for the blob read and write.
* Use blob leases as the distributed locking mechanism across multiple nodes. The blob will be locked exclusively when it is written. 
* The contents are pre-cached by chunks when there is read operation. This will eliminate the times of http request and increase the performance greatly. 
* Multi-part uploads are used for the write operation. Data is buffered firstly and then be uploaded if the buffer size exceed the threshold. This also can eliminate the times of http request and increase the performance greatly. 
* Append mode is supported, you can append the new line to the existing blob directly. this is more friendly for logging operation. And it can change the block blob to append blob automatically.
* Use server-side copy for move, rename operations, more efficient for big files and folders.

## How To use
### 1.include the bloblib in your project

### 2.read from the blob
	BlobReqParams insParams = new BlobReqParams();
	insParams.setContainer("orders");
	insParams.setBfsBlobType(BfsBlobType.BLOCKBLOB);
	insParams.setBlob("neworders.log");
	BlobBufferedIns bbIns = new BlobBufferedIns(insParams);
	String line;
	while ((line = bbIns.readLine()) != null)
	{
		System.out.println((line));
		Thread.sleep(100);
	}
### 3.Write to the blob
	BlobReqParams ousParams = new BlobReqParams();
	ousParams.setContainer("orders");
	ousParams.setBfsBlobType(BfsBlobType.BLOCKBLOB);   // use BfsBlobType.APPENDBLOB to create a append blob
	ousParams.setBlob("neworders.log");
	BlobBufferedOus bbOus = new BlobBufferedOus(ousParams);
	String line = "insert new line here";
	bbOus.writeLine(line);
	bbOus.close();

### 4.please find other functions from the source code.
	
	
It is highly recommended that you should config it as a windows services.

## Performance Test
* The performance depends on the machine and the network. 

## Dependency
* [azure-storage](https://github.com/Azure/azure-storage-java): Microsoft Azure Storage Library for Java .

## Limitation and known issues:
* For the file copy, the blobfs will use read out - then write in to new blob mode. this will spent more time for large files/folders.
* For the page blob, currently, should be, but it is not well tested yet. it may casue file interruption. 

## Supported platforms
-Linux
-MacOS
-windows

## License
	Copyright (C) 2017 Wesley Wu jie1975.wu@gmail.com
	This code is licensed under The General Public License version 3
	
## FeedBack
	Your feedbacks are highly appreciated! :)
