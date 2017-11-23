package com.wesley.bloblib;

import java.io.IOException;

public class BfsException extends IOException {
	
	  private static final long serialVersionUID = 1L;
	
	  public BfsException(String message) {
	    super(message);
	  }
	
	  public BfsException(String message, Throwable cause) {
	    super(message, cause);
	  }
	
	  public BfsException(Throwable t) {
	    super(t);
	  }
}
