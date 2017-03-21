package contacts;



import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.*;

import com.google.gdata.client.batch.BatchInterruptedException;
import com.google.gdata.client.contacts.ContactsService;
import com.google.gdata.data.contacts.ContactEntry;
import com.google.gdata.data.contacts.ContactFeed;
import com.google.gdata.data.extensions.*;

import com.google.gdata.data.batch.*;
import com.google.gdata.util.AuthenticationException;
import com.google.gdata.util.ServiceException;



/**
 * A  class that uploads 5000 contacts from the input file new_contacts.csv to the Google Account of
 * test.exercise5@cloud8labs.com and prints out the time it finished to the console. 
 * 
 * @author Elissa
 *
 */
public class BuildContacts {

	//The ContactEntry data structure that is indexed by an Integer from 0 to 4999
	static final ConcurrentHashMap<Integer,ContactEntry> contacts = new ConcurrentHashMap<Integer,ContactEntry>();
	
	//The number of threads to instantiate
	static final int CONCURRENCY_LEVEL = 50;
	
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		//Print out starting time in ms 
		long startTime = new Date().getTime();
		
		BuildContacts bc = new BuildContacts();
		
		System.out.println("Start time: " + startTime);
		
		//Read the input from the csv file into the data structure
		try {
			readInputFile();
		} catch (FileNotFoundException e) {
			System.err.println("File Not Found Exception in Read Input File: " + e.getMessage());
		}
		
		//Print out the parsed time in ms
		long parsedTime = new Date().getTime();
		System.out.println("Input file has been parsed: " + parsedTime);
		
		//Batch upload the contacts to the server in threads, 100 at a time
		bc.runBatchUploadContacts();

		//Print out the finish time in ms
		long finishTime = new Date().getTime();
		System.out.println("Contacts uploaded: " + finishTime);
	}

/**
 * A method that reads the csv input file of contacts and parses them into the data structure contacts
 * 
 * @throws FileNotFoundException
 */
	
	public static void readInputFile() throws FileNotFoundException{
		String fileName = "new_contacts.csv";
		CsvReader csvReader;
	
		
		csvReader = new CsvReader(fileName);

		
		for(int i= 0; i <= 5000; i++){
			try{
			
				//Read in a row from the input file
				csvReader.readRecord();
			
			}catch(IOException e){
				System.err.println("IO Exception caught while getting contact " + i +" : "+ e.getMessage());
			}
			
			String[] data = new String[3];
			try{
				
				for (int j=0; j < 3; j++){
			
					//Read in each column into a String
					data[j] = csvReader.get(j);
					
				}
			
			}catch(IOException e){
				System.err.println("IO Exception caught while reading columns: " + e.getMessage());	
			}
			
			if (i != 0) {
				ContactEntry contact = new ContactEntry();
				
				
				Name name = new Name();
				final String NO_YOMI = null;
				
				  //Set contact's name
				name.setFullName(new FullName(data[0], NO_YOMI));
				name.setGivenName(new GivenName(data[0].substring(0, data[0].indexOf(' ')), NO_YOMI));
				name.setFamilyName(new FamilyName(data[0].substring(data[0].indexOf(' ')+1,data[0].length()), NO_YOMI));
				contact.setName(name);
				  
				// Set contact's e-mail address
				Email primaryMail = new Email();
				primaryMail.setAddress(data[1]);
				primaryMail.setRel("http://schemas.google.com/g/2005#work");
				primaryMail.setPrimary(true);
				contact.addEmailAddress(primaryMail);
				  
				// Set contact's phone number
				PhoneNumber primaryPhoneNumber = new PhoneNumber();
				primaryPhoneNumber.setPhoneNumber(data[2]);
				primaryPhoneNumber.setRel("http://schemas.google.com/g/2005#work");
				primaryPhoneNumber.setPrimary(true);
				contact.addPhoneNumber(primaryPhoneNumber);
				  
				String s = new String () + new Integer(i-1).toString();
				contact.setId(s);			    
			
				//Put contact into data structure 
				contacts.put(new Integer(i-1), contact);
			
			    
			}
		}
	
    }


/**
 * A method that creates CONCURRENCY_LEVEL of threads and uploads 100 contacts for each thread from the 
 * user's credentials given (test.exercise5@cloud8labs.com, 3x3rc1s3). It then waits for the threads to 
 * complete via join.
 */
	
	public void runBatchUploadContacts()
	{    
		//List of the threads the batch upload tasks will be run in so we can join them
		List<Thread> threads = new ArrayList<Thread>();

		//Make the number of threads
	    for(int i = 0; i < CONCURRENCY_LEVEL; i++) {
	        
	        //Inner class that the Thread will run
			class BatchUploadTask implements Runnable {
	            int batchNumber;
	                	 
	            //The batchNumber is the number of the i - times 100 plus j is the batch id
	            BatchUploadTask(int batchNumber){ 
	                this.batchNumber = batchNumber;
	            }
	                	 
	            public void run() {
	            	ContactFeed requestFeed = new ContactFeed();
	                ContactsService service = new ContactsService("ContactsExample" + batchNumber);
	                ContactFeed responseFeed;
	                		 
	                try{
	                	//set the credentials of the service
	                	service.setUserCredentials("test.exercise5@test.cloud8labs.com","3x3rc1s3");
	                }
	                catch(AuthenticationException e){
	                	System.err.println("Authentication Exception caught: " +e.getMessage());
	                }
	                		 
	               	for (int j= 0; j < 100; j++) {
	                	// System.out.printf("%d\n",(batchNumber *100)+ j );
	                    		
	                    //Get the entry from the data structure by the batch id number
	                    ContactEntry entry = contacts.get(new Integer(j + (batchNumber * 100)));
	                    		 
	                    //add entry to batch create
	                    BatchUtils.setBatchId(entry, "create");
	                    BatchUtils.setBatchOperationType(entry, BatchOperationType.INSERT);
	                    	
	                    //add entry to request Feed
	                   	requestFeed.getEntries().add(entry);
	                    		  
	                }
							
	                try {
							
	                	while(true){	
	                    	//send the batch requests to the server
	                        responseFeed = service.batch(new URL("https://www.google.com/m8/feeds/contacts/default/full/batch"), requestFeed);
								
	                        boolean retry = false; 
	                        	
	                        //For each entry in the responseFeed
	                        for (ContactEntry entry : responseFeed.getEntries()) {
	                        	BatchStatus status = BatchUtils.getBatchStatus(entry);
			                    	
	                            //If there is a temporary problem, retry
	                        		if (status.getCode() == 503)
	                        			retry = true;
	                        			
	                        }
		                    	
	                        //If retry, let the loop go again, otherwise exit loop
	                        if (retry)
	                        	continue;
	                        else
	                        	return;
	                	}
		                    	
		                    	
					} catch (BatchInterruptedException e) {
						System.err.println("Batch Interrupted Exception Caught in BatchUploadTask: " + e.getMessage());
					} catch (MalformedURLException e) {
						System.err.println("Malformed URL Exception caught in BatchUploadTask: " +e.getMessage() );
					} catch (IOException e) {
						System.err.println("IOException caught in BatchUploadTask: " + e.getMessage());
					} catch (ServiceException e) {
						System.err.println("ServiceException caught in BatchUploadTask: " + e.getMessage());
					}


				}
			}
	            
			Thread t = new Thread(new BatchUploadTask(i));
			t.start();
			threads.add(t);

		}

	         
	    for (int i = 0; i < threads.size(); i++) {
	    	try {
				((Thread)threads.get(i)).join(1000000);
			} catch (InterruptedException e) {
				System.err.println("Caught Interrupted Exception while joining threads: " + e.getMessage());
			}
	    }
		
	}
	

	
}
