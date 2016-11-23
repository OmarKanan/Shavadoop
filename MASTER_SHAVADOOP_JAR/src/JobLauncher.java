
// Modules to import
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


// Class that sends a job (map or reduce) to a slave.
public class JobLauncher implements Runnable {

//	Fields of the class.
	private Thread thread; // Thread that will be associated to this instance.
	private String mode; // Sx -> UMx or UMx -> RMx.
	private String adress; // Adress of the slave who will execute the job.
	private String slaveJarDirectory; // Directory that contains Slave.jar.
	private BlockingQueue<String> outputArray; // For storing the outputs of the slave.
	private boolean isJobEnded; // For knowing when the slave has sent all the expected outputs.
	private String inputSxFile; // Input file for Sx -> UMx mode.
	private String outputUmxFile; // Output file for Sx -> UMx mode.
	private String[] rmxKeys; // Input keys for UMx -> RMx mode.
	private Set<String> inputUmxFiles; // Input files for UMx -> RMx mode.
	private String outputRmxFile; // Output file for UMx -> RMx mode.
	
	
//	Constructor for Sx -> UMx mode.
	public JobLauncher(String mode, String adress, String outputUmxFile, String inputSxFile,
			String slaveJarDirectory) {
		
//		We check that the given mode corresponds to this constructor, and throw an error if not.
		if (!mode.equals("SXUMX")) {
			System.err.println("This constructor requires a 'SXUMX' mode");
			System.exit(1);
//		Otherwise we initialize needed fields.
		} else {
			this.mode = mode;
			this.adress = adress;
			this.slaveJarDirectory = slaveJarDirectory;
			this.outputArray = new ArrayBlockingQueue<String>(15000);
			this.inputSxFile = inputSxFile;
			this.outputUmxFile = outputUmxFile;
			this.isJobEnded = false;
		}
	}
	
//	Constructor for UMx -> RMx mode.
	public JobLauncher(String mode, String adress, String[] rmxKeys, String outputRmxFile, 
			Set<String> inputUmxFiles, String slaveJarDirectory) {

//		We check that the given mode corresponds to this constructor, and throw an error if not.
		if (!mode.equals("UMXRMX")) {
			System.err.println("This constructor requires a 'UMXRMX' mode");
			System.exit(1);
//		Otherwise we initialize needed fields.
		} else {
			this.mode = mode;
			this.adress = adress;
			this.slaveJarDirectory = slaveJarDirectory;
			this.outputArray = new ArrayBlockingQueue<String>(15000);
			this.rmxKeys = rmxKeys;
			this.inputUmxFiles = inputUmxFiles;
			this.outputRmxFile = outputRmxFile;
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
			Process process = new ProcessBuilder("ssh", this.adress, "java -jar " 
					+ this.slaveJarDirectory + "Slave.jar "
					+ "SXUMX " + this.outputUmxFile + " " + this.inputSxFile)
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
			process.waitFor();
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
		StringBuilder stringBuilder = new StringBuilder();
		for (String inputFile : this.inputUmxFiles) {
			stringBuilder.append(inputFile + "___");
		}
		inputFilesAsString = stringBuilder.toString();

//		Object for writing the input keys to a file.
		PrintWriter keysWriter = null;
		
//		Object for reading the outputs (key + count) of the process.		
		BufferedReader outputReader = null;
	
//		Try-catch bloc to prevent errors while trying to read or write streams.
		try {
			
//			Writes the keys to a file that we will send to the slave.
			keysWriter = new PrintWriter(this.slaveJarDirectory + "Keys/Keys_" + this.adress 
					+ ".txt");
			for (int i = 0; i < this.rmxKeys.length; i++) {
				if (i < this.rmxKeys.length - 1) {
					keysWriter.write(this.rmxKeys[i] + "\n");
				} else {
					keysWriter.write(this.rmxKeys[i]);
				}
			}
			
//			Closes the stream so that the slave can access the keys file.
			keysWriter.close();
			
//			Sends new process to slave via SSH.
			Process process = new ProcessBuilder("ssh", this.adress, "java -jar " 
					+ this.slaveJarDirectory + "Slave.jar " 
					+ "UMXRMX " + this.outputRmxFile + " " + inputFilesAsString + " "
					+ this.slaveJarDirectory + "Keys/Keys_" + this.adress + ".txt")
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
			process.waitFor();
			System.out.println("-> wrote file " + this.outputRmxFile);
			
//		Catches exceptions while trying to read or write streams.			
		} catch (IOException | InterruptedException e) {
			e.printStackTrace(); 
			
//		Closes the streams to prevent memory leak.
		} finally {
			try {
				keysWriter.close();
				outputReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}	
	
}
