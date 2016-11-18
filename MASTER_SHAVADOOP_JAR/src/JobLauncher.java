
// Modules to import
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


// Class that sends a job (map or reduce) to a slave.
public class JobLauncher implements Runnable {

	private String masterProjectDirectory; // Directory that contains the jars of the project.
	private Thread thread; // Thread that will be associated to this instance.
	private String mode; // Sx -> UMx or UMx -> RMx.
	private String adress; // Adress of the slave who will execute the job.
	private BlockingQueue<String> outputArray; // For storing the outputs of the slave.
	private boolean isJobEnded; // For knowing when the slave has sent all the expected outputs.
	private String inputSxFile; // Input file for Sx -> UMx mode.
	private String outputUmxFile; // Output file for Sx -> UMx mode.
	private String[] rmxKeys; // Input keys for UMx -> RMx mode.
	private Set<String> inputUmxFiles; // Input files for UMx -> RMx mode.
	private String outputRmxFile; // Output file for UMx -> RMx mode.
	private List<String> sortedFinalKeys; // List of keys sorted by occurence.
	
	
//	Constructor for Sx -> UMx mode.
	public JobLauncher(String mode, String adress, String outputUmxFile, String inputSxFile,
			String masterProjectDirectory) {
		
//		We check that the given mode corresponds to this constructor, and throw an error if not.
		if (!mode.equals("SXUMX")) {
			System.err.println("This constructor requires a 'SXUMX' mode");
			System.exit(1);
//		Otherwise we initialize needed fields.
		} else {
			this.masterProjectDirectory = masterProjectDirectory;
			this.mode = mode;
			this.adress = adress;
			this.outputArray = new ArrayBlockingQueue<String>(15000);
			this.inputSxFile = inputSxFile;
			this.outputUmxFile = outputUmxFile;
			this.isJobEnded = false;
		}
	}
	
//	Constructor for UMx -> RMx mode.
	public JobLauncher(String mode, String adress, String[] rmxKeys, String outputRmxFile, 
			Set<String> inputUmxFiles, String masterProjectDirectory) {

//		We check that the given mode corresponds to this constructor, and throw an error if not.
		if (!mode.equals("UMXRMX")) {
			System.err.println("This constructor requires a 'UMXRMX' mode");
			System.exit(1);
//		Otherwise we initialize needed fields.
		} else {
			this.masterProjectDirectory = masterProjectDirectory;
			this.mode = mode;
			this.adress = adress;
			this.outputArray = new ArrayBlockingQueue<String>(15000);
			this.rmxKeys = rmxKeys;
			this.inputUmxFiles = inputUmxFiles;
			this.outputRmxFile = outputRmxFile;
			this.sortedFinalKeys = new ArrayList<String>();
			this.isJobEnded = false;
		}
	}
	
	
//	Getters and setters.
	public Thread getThread() {
		return this.thread;
	}	
	public void setThread(Thread thread) {
		this.thread = thread;
	}
	public String getAdress() {
		return this.adress;
	}
	public BlockingQueue<String> getOutputArray() {
		return this.outputArray;
	}
	public boolean isJobEnded() {
		return this.isJobEnded;
	}
	public void setJobEnded(boolean jobEnded) {
		this.isJobEnded = jobEnded;
	}
	public String getOutputUmxFile() {
		return this.outputUmxFile;
	}
	public List<String> getSortedFinalKeys() {
		return this.sortedFinalKeys;
	}
	
	
//	Method executed in a new thread when Thread.start() is called by Master.
	@Override
	public void run() {
		
//		Launches method corresponding to mode.	
		if (this.mode.equals("SXUMX")) {
			sxUmx();
		} else {
			umxRmx();
		}
	}

	
//	Sends a Sx -> UMx process to the slave.
	private void sxUmx() {
		
//		Object for reading the outputs (keys) of the process.
		BufferedReader outputReader = null;
		
//		Try-catch bloc to prevent errors while trying to read the process response.
		try {
			
//			Sends new process to slave via SSH.
			Process process = new ProcessBuilder("ssh", this.adress,
					"java -jar " + this.masterProjectDirectory + "Slave.jar "
					+ "SXUMX " + " " + this.masterProjectDirectory + this.outputUmxFile 
					+ " " + this.masterProjectDirectory + this.inputSxFile)
				.start();
			
//			Initializes the output reader.			
			outputReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
			
//			Stores the outputs at the tail of the array. The "readline" method returns null only
//			when the process has ended and all the outputs have been read.
			String word;
			while ((word = outputReader.readLine()) != null) {
				this.outputArray.put(word);
			}
			
//			Process has ended.			
			System.out.println("-> wrote file " + this.outputUmxFile);
		
//		Catches exceptions while trying to read the outputs.
		} catch (IOException | InterruptedException e) { 
			e.printStackTrace(); 
			
//		Closes the stream to prevent memory leak.
		} finally {
			try {
				outputReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	
//	Sends a UMx -> RMx process to a slave.
	private void umxRmx() {
		
//		Assemble the list of input files into a single String, with triple underscore as separator.
		String inputFilesAsString = " ";
		for (String inputFile : this.inputUmxFiles) {
			inputFilesAsString = inputFilesAsString.concat(
					this.masterProjectDirectory + inputFile + "___");
		}

//		Assemble the list of input keys into a single String, with triple underscore as separator.
		String keysAsString = " ";
		for (String key : this.rmxKeys) {
			keysAsString = keysAsString.concat(key + "___");
		}

//		Object for reading the outputs (key + count) of the process.		
		BufferedReader outputReader = null;
	
//		Try-catch bloc to prevent errors while trying to read the process response.
		try {
			
//			Sends new process to slave via SSH.
			Process process = new ProcessBuilder("ssh", this.adress,
					"java -jar " + this.masterProjectDirectory + "Slave.jar " 
					+ "UMXRMX" + keysAsString + " " +  this.masterProjectDirectory 
					+ this.outputRmxFile + inputFilesAsString)
				.start();
			
//			Initializes the output reader.	
			outputReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
			
//			Stores the outputs at the tail of the array. The "readline" method returns null only
//			when the process has ended and all the outputs have been read.
			String keyAndCount;
			while ((keyAndCount = outputReader.readLine()) != null) {
				this.outputArray.put(keyAndCount);
			}
						
//			Process has ended.		
			System.out.println("-> wrote file " + this.outputRmxFile);
			
//		Catches exceptions while trying to read the outputs.			
		} catch (IOException | InterruptedException e) {
			e.printStackTrace(); 
			
//		Closes the stream to prevent memory leak.
		} finally {
			try {
				outputReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}	
	
}
