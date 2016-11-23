
// Modules to import.
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;


// Class that will execute a map job or a reduce job on a worker machine.
public class Slave {

	
//  Entry point. This method will be executed when Slave.jar is launched by a JobLauncher.
	public static void main(String[] args) {
				
//		As we are in a static context, we need to instantiate a Slave object to be able to access 
//		its fields and methods.
		Slave slave = new Slave(args);
		
//		First we need to check whether the job is a map or a reduce.		
//		If no arguments are passed, we throw an error.
		if (args.length < 1) {
			System.err.println("Must have <mode> as first argument");
			System.exit(1);
			
//		If "SXUMX" is passed, then we launch a map job.
		} else if (args[0].equals("SXUMX")){
			slave.startMap();
			
//		If "UMXRMX" is passed, we launch a reduce job.
		} else if (args[0].equals("UMXRMX")) {
			slave.startReduce();
			
//		Otherwise we throw an error.
		} else {
			System.err.println("Mode must be in ['SXUMX', 'UMXRMX']");
			System.exit(1);			
		}
		
	}
	
	
//	Fields.
	private String[] args; // Arguments given by the master.


//	Constructor. We initialize the fields here.
	private Slave(String[] args) {
		this.args = args;
	}
	
	
//	Called by the main method to run a map job.
	private void startMap() {
			
//		First we need to extract the arguments given by the user.
		File outputFile = null;
		File inputFile = null;
		
//		We first need to check if the number of arguments given is correct. If they are not, we
//		throw an error.
		if (this.args.length != 3) {
			System.err.println("For SXUMX mode, must add <output file, input file> as arguments");
			System.exit(1);
//		Otherwise we extract the arguments.
		} else {
			outputFile = new File(this.args[1]);
			inputFile = new File(this.args[2]);
		}
				
//		We want to split the job using the number of available processors. So we build an array
//		of MapLaunchers.
		int numProcessors = Runtime.getRuntime().availableProcessors();
		MapLauncher[] mapLaunchers = new MapLauncher[numProcessors];
		
//		Array of the input file decomposition.
		int[] fileIndexes = new int[numProcessors + 1];
		for (int i = 0; i < fileIndexes.length; i++) {
			fileIndexes[i] = (int) Math.ceil(i * inputFile.length() / numProcessors);
		}
		
//		For reading the input file.
		FileInputStream inputReader = null;
		
//		For writing the output UMx file. Each thread will write its part with synchronization.
		PrintWriter outputWriter = null;
		
		try {
			
			inputReader = new FileInputStream(inputFile);
			outputWriter = new PrintWriter(outputFile);
			
//			We then initialize the MapLaunchers and start the threads associated with each one.
			for (int i = 0; i < mapLaunchers.length; i++) {
				mapLaunchers[i] = new MapLauncher(fileIndexes[i], fileIndexes[i + 1], 
						inputReader, outputWriter);
				mapLaunchers[i].setThread(new Thread(mapLaunchers[i]));
				mapLaunchers[i].getThread().start();
			}	

//			Now we wait for each thread to end.
			for (MapLauncher mapLauncher : mapLaunchers) {
				try {
					mapLauncher.getThread().join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
//			The map job has ended, we now need to send the end signal to the master.
			System.out.println("END OF PROCESS SXUMX");
			
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} finally {
			try {
				inputReader.close();
				outputWriter.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
			
	}
	
	
//	Called by the main method to run a reduce job.
	private void startReduce() {
		
//		First we need to extract the arguments given by the user.
		File outputFile = null;
		File[] inputFiles = null;
		File keysFile = null;
		
//		We first need to check if the number of arguments given is correct. If they are not, we
//		throw an error.
		if (this.args.length != 4) {
			System.err.println("For UMXRMX mode, must add <output file, input files, keys file> "
					+ "as arguments");
			System.exit(1);
//		Otherwise we extract the arguments. The input files argument is send by the master
//		with triple underscore as separator.
		} else {
			outputFile = new File(this.args[1]);
			String[] inputFilesAsStrings = this.args[2].split("___");
			inputFiles = new File[inputFilesAsStrings.length];
			for (int i = 0; i < inputFilesAsStrings.length; i++) {
				inputFiles[i] = new File(inputFilesAsStrings[i]);
			}
			keysFile = new File(this.args[3]);
		}
		
//		For reading the keys file.
		BufferedInputStream keysReader = null;
		String[] keys = null;

//		For writing the output RMx file.
		PrintWriter outputWriter = null;
		
		try {
			
//			Initializes the reader
			keysReader = new BufferedInputStream(new FileInputStream(keysFile));
			
//			Reads the keys.
			byte[] buffer = new byte[(int) keysFile.length()];
			keysReader.read(buffer);
			keys = (new String(buffer)).split("\\n");
			System.err.println(keysFile.length());
			
//			We want to split the job using one thread for each input file. So we create an array
//			of ReduceLaunchers and then launch the corresponding threads.
			ReduceLauncher[] reduceLaunchers = new ReduceLauncher[inputFiles.length];
			for (int inputFileNum = 0; inputFileNum < reduceLaunchers.length; inputFileNum++) {
				reduceLaunchers[inputFileNum] = new ReduceLauncher(keys, inputFiles[inputFileNum]);
				reduceLaunchers[inputFileNum].setThread(new Thread(reduceLaunchers[inputFileNum]));
				reduceLaunchers[inputFileNum].getThread().start();
			}
			
//			Now we wait for each thread to end.
			for (ReduceLauncher reduceLauncher : reduceLaunchers) {
				try {
					reduceLauncher.getThread().join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

//			Initializes the writer.
			outputWriter = new PrintWriter(outputFile);
			
//			For each key we sum the counts of each partial reduce, send it to the master via SSH 
//			and also write it to the output file.		
			for (String key : keys) {
				int keyCount = 0;
				for (ReduceLauncher reduceLauncher : reduceLaunchers) {
					keyCount = keyCount + reduceLauncher.getKeysCounts().get(key);
				}
				System.out.println(key + " " + keyCount);
				outputWriter.write(key + " " + keyCount + "\n");
			}
			
//			The reduce job has ended, we now need to send the end signal to the master.
			System.out.println("END OF PROCESS UMXRMX");
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				keysReader.close();
				outputWriter.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}

}

