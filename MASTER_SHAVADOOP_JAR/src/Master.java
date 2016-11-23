
// Modules to import.
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


// Main class.
public class Master {


//  Entry point of the program. This method will be executed when you launch Master.jar.
	public static void main(String[] args) {
		
//		As we are in a static context, we need to instantiate a Master object to be able to access 
//		its fields and methods.
		Master master = new Master();

//		Now we launch the method which will run our WordCount algorithm.
		master.startWordCount(args);
	}


//	Fields of the class. They are private so they can't be accessed from outside the class instance.
	private String workingDirectory; // Directory that contains the jars and files.
	private long initialTime; // For timing the whole program.
	private List<String> successAdresses; // List of the available machines (after testing SSH).
	private List<JobLauncher> sxUmxJobLaunchers; // List of Sx -> UMx processes.
	private List<JobLauncher> umxRMxJobLaunchers; // List UMx -> RMx processes.
	private Map<String, List<String>> keysAndTheirUmx; // Lists of UMx files containing each word.
	private Map<String, Integer> keysAndCounts; // Total occurences by word.

	
//	Constructor that the main method calls to instantiate a Master object.
	private Master() {
		
//		We initialize the fields here so they can be used later in the program.
		this.workingDirectory = System.getProperty("user.dir") + "/";
		this.initialTime = System.currentTimeMillis();
		this.successAdresses = new ArrayList<String>();
		this.sxUmxJobLaunchers = new ArrayList<JobLauncher>();
		this.umxRMxJobLaunchers = new ArrayList<JobLauncher>();
		this.keysAndTheirUmx = new HashMap<String, List<String>>();
		this.keysAndCounts = new HashMap<String, Integer>();
	}

	
//	Called by the main method. It runs all the WordCount algorithm.
	private void startWordCount(String[] args) {
		
//		We want to decompose the arguments given by the user.
		File inputFile = null;
		File adressesFile = null;
		int connectionTestTimeout = 0;
		
//		We first need to check if the number of arguments given is correct. If they are not, we
//		throw an error.
		if (args.length != 3) {
			System.err.println("Must add <input file, adresses file, timeout in ms> as argument");
			System.exit(1);
//		Otherwise we extract the arguments.
		} else {
			inputFile = new File(args[0]);
			adressesFile = new File(args[1]);
			connectionTestTimeout = Integer.parseInt(args[2]);
		}
		
//		We create the necessary folders.
		new File(this.workingDirectory + "Sx").mkdirs();
		new File(this.workingDirectory + "UMx").mkdirs();
		new File(this.workingDirectory + "Keys").mkdirs();
		new File(this.workingDirectory + "RMx").mkdirs();
		
//		Now we have to test the SSH connection with each adress, and store the successful adresses.
		this.testSSHConnections(adressesFile, connectionTestTimeout);
		
//		We now create subparts Sx of the input file for each successful adress.
		this.splitInputFile(inputFile);
		
//		Then we launch one thread for each Sx file, via the JobLauncher class which implements
//		the Runnable interface. Each one of these threads will connect to a successful adress 
//		via SSH and tell it to run Slave.jar with the Sx -> UMx mode and the corresponding Sx file,
//		which will make them compute the unsorted maps and write them to UMx files.
		this.launchSxUMxJobs();
		
//		After having launched the jobs, we need to retrieve the keys sent by the different slaves
//		via SSH.
		this.retrieveSxUMxKeys();
		
//		Now we have to shuffle the retrieved keys, by splitting groups of keys for each slave that
//		will run a UMx -> RMx process, and then launch the threads.
		this.shuffleKeysAndLaunchUMxRMxJobs();
		
//		Same as the Sx -> UMx phase, we need to retrieve the couples (key, count) sent by the 
//		different slaves via SSH (they are sent in sorted order).
		this.retrieveUMxRMxKeysCounts();
		
//		Finally we just have to sort the all the keys by their count, and prints them to a file.
		this.sortAndPrintKeys();
		
	}
	
	
	private void testSSHConnections(File adressesFile, int connectionTestTimeout) {
		
//		Keeps track of time.
		long startTime = System.currentTimeMillis();
		System.out.println("Checking SSH connection with given adresses:");

//		Object that will read lines from the adresses file.
		BufferedReader bufferedAdressReader = null;
		
//		Try-catch bloc to catch exceptions when trying to read files.
		try {
			
//			Initializes the reader.
			bufferedAdressReader = new BufferedReader(new FileReader(adressesFile));
			
//			List of connection tester, one for each adress that we will find on the given file.
			List<SSHConnectivityTester> testers = new ArrayList<SSHConnectivityTester>();
			
//			Loops for each line of the adresses file (each line contains an adress), and adds a new
//			connection tester to the list.
			String adress;
			while ((adress = bufferedAdressReader.readLine()) != null) {
				testers.add(new SSHConnectivityTester(adress));
			}
			
//			Loops for each connection tester, and launches the threads.
			for (SSHConnectivityTester tester : testers) {
				tester.setThread(new Thread(tester));
				tester.getThread().start();
			}
			
//			Waits for timeout, then interrupt all the threads
			Thread.sleep(connectionTestTimeout);
			for (SSHConnectivityTester tester : testers) {
				tester.getThread().interrupt();
			}
			
//			Checks the successful connections, and adds the successful adresses to a list.
			for (SSHConnectivityTester tester : testers) {
				if (tester.isConnectionSuccess()) {
					this.successAdresses.add(tester.getAdress());
					System.out.println("Connection with " + tester.getAdress() + " : Success");
				} else {
					System.out.println("Connection with " + tester.getAdress() + " : Failed");
				}
			}
			
//			If no successful connections, returns an error.
			if (this.successAdresses.isEmpty()) {
				System.out.println("Error : no successful SSH connection");
				System.exit(0);
			}
			
//			Prints the elapsed time.
			System.out.println("Connectivity checking duration = " + (
					System.currentTimeMillis() - startTime) + " ms.");
			System.out.println("-----------------------------------------------------");
		
//		Catches any exception that could occur when trying to read files.
		} catch (Exception e) {
			e.printStackTrace();
			
//		After the try-catch bloc has ended, we close the opened streams to prevent memory leak.
		} finally {
			try { 
				bufferedAdressReader.close(); 
			} catch (IOException e) { 
				e.printStackTrace(); 
			}
		}	
		
	}


	private void splitInputFile(File inputFile) {
		
//		Keeps track of time.
		long startTime = System.currentTimeMillis();
		System.out.println("Starting splitting phase:");
		
//		Object that will read bytes from the input file.
		FileInputStream inputReader = null;
		
//		Object that will write bytes to the Sx files.
		FileOutputStream sxWriter = null;
		
//		Try-catch bloc to catch exceptions when trying to read or write files.
		try {		
			
//			Initializes the reader.
			inputReader = new FileInputStream(inputFile);

//			We want to split the file into a number of parts equal to the number of successful
//			adress we have, so that each slave executes one thread. So we need to compute the size
//			of each part in bytes.
			int sizePerSplitFile = (int) (inputFile.length() / this.successAdresses.size() + 1);
			
//			Buffer of the right size, in which we will store the bytes read from the input file
//			for each part.
			byte[] buffer = new byte[sizePerSplitFile];
			
//			We actually won't take exactly the same number of bytes for each part because we don't
//			want to cut words while splitting the file. We will cut only at spaces, so we need to
//			keep track of the real number of bytes read for each part.
			int readSize = 0;
			
//			We now loop for each split we need to create.
			int splitNumber = 0;			
			while (splitNumber < this.successAdresses.size()
//					For each loop we read "sizePerSplit" amount of bytes from the input file and
//					we store them into the buffer, unless we had already reached the end of the
//					file (which shouldn't happen if we have split the file correctly, but
//					better be careful !) in which case we break out of the loop.
//					In practice, "readSize" will here always be equal to "sizePerSplit" unless
//					we are in the last part of the loop, in which case it will be equal to the
//					number of remaining bytes of the input file.
					&& (readSize = inputReader.read(buffer, 0, sizePerSplitFile)) != -1) {
								
//				We create a new output file with an object which we will write to it.
				sxWriter = new FileOutputStream("Sx/S_" + splitNumber + ".txt");
			
//				We write the content of the buffer to the output file. We limit the content to
//				the same amount "readSize" we have read from the input file.
				sxWriter.write(buffer, 0, readSize);

//				We now need to add additional bytes until we reach a space in the input file,
//				so we don't cut words.
//				First we try to read one additional byte into the start of the buffer, to see
//				if we have reached the end of file. As we have already written the buffer to the
//				output size, we can overwrite its values.
				if (inputReader.read(buffer, 0, 1) != -1) {
//					If it's a success, we now loop for each following byte of the input file
//					and add them to the buffer keeping track of the number of additional bytes
//					read. We break the loop	when we have found a space (which byte representation
//					is "32"), or if we have reached the end of the input file.
					int additionalBytes = 1;
					while (inputReader.read(buffer, additionalBytes, 1) != -1 
							&& buffer[additionalBytes] != 32) {
						additionalBytes++;
					}
					
//					Now we just need to write the additional bytes from the buffer. Note that the
//					space at which we split the file hasn't been added to the buffer, so it won't
//					be present in the output file.
					sxWriter.write(buffer, 0, additionalBytes);
				}
				
//				We have now finished to write this output file.
				System.out.println("-> wrote file S_" + splitNumber + ".txt");

//				Increments the counter for next loop.
				splitNumber++;
							
			}
			
//			Prints the elapsed time.
			System.out.println("Splitting phase duration = " + (
					System.currentTimeMillis() - startTime) + " ms.");
			System.out.println("-----------------------------------------------------");

//		Catches any exception that could occur when trying to read files or write files.
		} catch (Exception e) {
			e.printStackTrace();
			
//		After the try-catch bloc has ended, we close the opened streams to prevent memory leak.
		} finally {
			try {
				inputReader.close();
				sxWriter.close();
			} catch (IOException e) { 
				e.printStackTrace(); 
			}
		}
		
	}


	private void launchSxUMxJobs() {
		
//		We create a list "sxUmxJobLaunchers" containing one new JobLauncher for each
//		Sx file we have.
		for (int i = 0; i < this.successAdresses.size(); i++) {
			
//			Sets the current adress.
			String adress = this.successAdresses.get(i);
			
//			A new JobLauncher is initialized with the "SXUMX" mode, the adress of the
//			machine (slave) associated with it, the expected path of the output UMx file,
//			and the path of the split file Sx on which the slave will operate, as well as 
//			the directory containing the jars.
			this.sxUmxJobLaunchers.add(new JobLauncher("SXUMX", adress, 
					this.workingDirectory + "UMx/UM_" + adress + ".txt",
					this.workingDirectory + "Sx/S_" + i + ".txt", this.workingDirectory));

//			We set the JobLauncher field "thread" to be the new Thread based upon this JobLauncher
//			(JobLauncher implements the Runnable interface).
			this.sxUmxJobLaunchers.get(this.sxUmxJobLaunchers.size() - 1).setThread(
					new Thread(this.sxUmxJobLaunchers.get(this.sxUmxJobLaunchers.size() - 1)));
			
//			Now we start the thread, which will launch the Sx -> UMx process via SSH.
			this.sxUmxJobLaunchers.get(this.sxUmxJobLaunchers.size() - 1).getThread().start();
		}
						
	}


	private void retrieveSxUMxKeys() {
		
//		Keeps track of time.
		long startTime = System.currentTimeMillis();
		System.out.println("Starting mapping phase:");
		
//		We will keep checking for new keys to retrieve until all slaves have ended their job
//		(which means no more keys will be sent by the slaves). Keys are sent in a BlockingQueue
//		which is an array that manages concurrent writes and reads. So the slave can write keys at
//		at its tail while the master can read and remove keys from its head.
//		We add this keys to a Map and associate each one with the list of corresponding UMx
//		(files that contains the key).
		int numberOfEndedThreads = 0;
		while (numberOfEndedThreads != this.successAdresses.size()) {
			
//			We loop for each slave and check if a new key has been sent by it.
			for (JobLauncher jobLauncher : this.sxUmxJobLaunchers) {
				
				String word;
//				If the slave is still active, and the BlockingQueue contains a value, we check
//				it, otherwise we do nothing and go to the next slave.
				if (jobLauncher.isJobEnded() == false && 
						(word = jobLauncher.getOutputArray().poll()) != null) {
					
//					If the value is "END OF PROCESS SXUMX", then it means that the slave has ended
//					its job, so we set the appropriate boolean to "true", and increment the number
//					of finished jobs.
					if (word.equals("END OF PROCESS SXUMX")) {
						jobLauncher.setJobEnded(true);
						numberOfEndedThreads++;
						
//					Otherwise it is a new key.
					} else if (!word.equals("")) {					
//						If this key wasn't already present in the key/list_of_UMx Map, then we
//						add it and initialize its associated UMx list.
						if (!this.keysAndTheirUmx.containsKey(word)) {
							this.keysAndTheirUmx.put(word, new ArrayList<String>());
						}						
//						We add the UMx to the list of UMx associated with this key.
						this.keysAndTheirUmx.get(word).add(jobLauncher.getOutputUmxFile());
					}				
				}
			}				
		}
		
//		We now wait for every thread to die, which is actually quite useless because at this point,
//		each slave has finished its job. The "join" method blocks until the corresponding thread
//		dies, or does nothing if it is already dead.
		for (JobLauncher jobLauncher : this.sxUmxJobLaunchers) {
			try {
				jobLauncher.getThread().join();
			} catch (InterruptedException e) { 
				e.printStackTrace();
			}
		}
		
//		Prints the elapsed time
		System.out.println("Mapping phase duration = " + (
				System.currentTimeMillis() - startTime) + " ms.");
		System.out.println("-----------------------------------------------------");
		
	}
	

	private void shuffleKeysAndLaunchUMxRMxJobs() {
		
//		Keeps track of time.
		long startTime = System.currentTimeMillis();
		System.out.println("Starting shuffling phase:");
		
//		Sets the number of UMx -> RMx processes to the number of available adresses.
		int numberOfUMxRMxProcesses = this.successAdresses.size();

//		We set the number of keys in each group such that the keys are fairly split between each
//		one of the machines. So if P is the number of processes, K the number of keys, then the
//		p first processes will have k+1 keys each, and the remaining processes will have k keys
//		each, where K/P = k*P + p.
		int k = this.keysAndTheirUmx.size() / numberOfUMxRMxProcesses;
		int p = this.keysAndTheirUmx.size() % numberOfUMxRMxProcesses;
		
//		Then we fill the array describing the repartition of the number of keys between each
//		process.
		int[] keysRepartitionByProcess = new int[numberOfUMxRMxProcesses];
		for (int i = 0; i < keysRepartitionByProcess.length ; i++) {
			if (i < p) {
				keysRepartitionByProcess[i] = k + 1;
			} else {
				keysRepartitionByProcess[i] = k;
			}
		}
		
//		Creates an iterator over the retrieved keys (which will avoid a useless copy of the data).
		Iterator<String> keyIterator = this.keysAndTheirUmx.keySet().iterator();
		
//		We loop for each process needed.
		for (int i = 0; i < this.successAdresses.size(); i++) {
			
//			Sets the current adress.
			String adress = this.successAdresses.get(i);	
			
//			Array of keys which will be sent to the slave with the appropriate values.
			String[] keysToSend = new String[keysRepartitionByProcess[i]];
			
//			We now fill the array of keys using the iterator previously created. Note that because
//			of last "keysRepartitionByProcess", we don't need to check if the key iterator still 
//			has elements.
			for (int j = 0; j < keysToSend.length; j++) {
				keysToSend[j] = keyIterator.next();
			}
			
//			We then set the values of the input files UMx to be sent, which is the set of all the
//			UMx associated with the keys to send.
			Set<String> inputUmxFilesSet = new HashSet<String>();
			for (String key : keysToSend) {
				inputUmxFilesSet.addAll(this.keysAndTheirUmx.get(key));
			}
			
//			A new JobLauncher is initialized with the "UMXRMX" mode, the adress of the
//			machine (slave) associated with it, the array of keys to send, the expected 
//			path of the output UMx file, and the paths of the files UMx on which the slave 
//			will operate, as well as the project directory.
			this.umxRMxJobLaunchers.add(new JobLauncher("UMXRMX", adress, 
					Arrays.copyOf(keysToSend, keysToSend.length),
					this.workingDirectory + "RMx/RM_" + adress + ".txt", inputUmxFilesSet,
					this.workingDirectory));

//			We set the JobLauncher field "thread" to be the new Thread based upon this JobLauncher
//			(JobLauncher implements the Runnable interface).
			this.umxRMxJobLaunchers.get(this.umxRMxJobLaunchers.size() - 1).setThread(
					new Thread(this.umxRMxJobLaunchers.get(this.umxRMxJobLaunchers.size() - 1)));
			
//			Now we start the thread, which will launch the UMx -> RMx process via SSH.
			this.umxRMxJobLaunchers.get(this.umxRMxJobLaunchers.size() - 1).getThread().start();			
		}
		
//		Prints the number of keys and the elapsed time
		System.out.println(this.keysAndTheirUmx.size() + " different words were found");
		System.out.println("Shuffling phase duration = " + (
				System.currentTimeMillis() - startTime) + " ms.");
		System.out.println("-----------------------------------------------------");
		
	}


	private void retrieveUMxRMxKeysCounts() {
		
//		Keeps track of time.
		long startTime = System.currentTimeMillis();
		System.out.println("Starting reducing phase:");
	
//		We will keep checking for new couples to retrieve until all slaves have ended their job
//		(which means no more couples will be sent by the slaves). Couples are sent in a 
//		BlockingQueue, like in the Sx -> UMx phase.
//		We add this couples to a Map, and we also add the keys to sorted lists (each 
//		JobLauncher has one as a field).
		int numberOfEndedThreads = 0;
		while (numberOfEndedThreads != this.successAdresses.size()) {
			
//			We loop for each slave and check if a new couple has been sent by it.
			for (JobLauncher jobLauncher : this.umxRMxJobLaunchers) {
				
				String couple;
//				If the slave is still active, and the BlockingQueue contains a value, we check
//				it, otherwise we do nothing and go to the next slave.
				if (jobLauncher.isJobEnded() == false 
						&& (couple = jobLauncher.getOutputArray().poll()) != null) {
					
//					If the value is "END OF PROCESS UMXRMX", then it means that the slave has ended
//					its job, so we set the appropriate boolean to "true", and increment the number
//					of finished jobs.
					if (couple.equals("END OF PROCESS UMXRMX")) {
						jobLauncher.setJobEnded(true);
						numberOfEndedThreads++;
						
//					Otherwise it is a new couple.
					} else {	
						
//						Splits the couple into a key and a count.
						String[] keyAndCount = couple.split(" ");
						
//						Adds them to the key_count Map.
						this.keysAndCounts.put(keyAndCount[0], Integer.parseInt(keyAndCount[1]));		
					}								
				}			
			}				
		}
		
//		We now wait for every thread to die. This time it is useful because while we are retrieving
//		the couples, the slaves are writing these couples to RMx files.
		for (JobLauncher jobLauncher : this.umxRMxJobLaunchers) {
			try {
				jobLauncher.getThread().join();
			} catch (InterruptedException e) { 
				e.printStackTrace(); 
			}
		}
		
//		Prints the elapsed time
		System.out.println("Reducing phase duration = " + (
				System.currentTimeMillis() - startTime) + " ms.");
		System.out.println("-----------------------------------------------------");
	}
	

	private void sortAndPrintKeys() {
		
//		Keeps track of time.
		long startTime = System.currentTimeMillis();
		System.out.println("Starting assembling phase:");
		
//		We need to sort the list of keys by their count, using our custom Comprator.
		List<String> sortedKeys = new ArrayList<String>(this.keysAndCounts.keySet());
		Collections.sort(sortedKeys, new KeyComparator(this.keysAndCounts));
		
//		Object for writing the result file.
		PrintWriter resultWriter = null;
		
//		Try-catch bloc to prevent error while writing the file.
		try {
			
//			Initializes the writer.
			resultWriter = new PrintWriter("wordcount.txt");
			
//			We just have to write the pairs to the output file.
			for (String key : sortedKeys) {
				resultWriter.write(key + " " + this.keysAndCounts.get(key) + "\n");
			}
			
//		Catches errors that can occur while writing to the file.
		} catch (Exception e) {
			e.printStackTrace();

//		Closes the opened stream to prevent memory leak.
		} finally {
			resultWriter.close();
		}
		
//		Prints the elapsed time.
		System.out.println("Assembling phase duration = " + (
				System.currentTimeMillis() - startTime) + " ms.");
		System.out.println("-----------------------------------------------------");
		
//		Prints the total elapsed time of the program.
		System.out.println("WordCount ended with success ! Duration was " + (
				System.currentTimeMillis() - this.initialTime) + " ms.");
	}
	
}
