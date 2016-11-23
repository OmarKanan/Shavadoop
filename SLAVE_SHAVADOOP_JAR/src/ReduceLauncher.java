
// Modules to import.
import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;


// Class that executes a partial reduce job in a new thread on the same machine.
public class ReduceLauncher implements Runnable {
	
	
//	Fields.
	private String[] keys; // Words to consider for the reduce job.
	private File inputFile; // The input file to do the reducing job on.
	private Map<String, Integer> keysCounts; // For counting the occurences of each key.
	private Thread thread; // Thread associated with this class instance.
	

//	Constructor.
	public ReduceLauncher(String[] keys, File inputFile) {
		
		this.keys = keys;
		this.inputFile = inputFile;
		this.keysCounts = new HashMap<String, Integer>();
	}


//	Getters and setters.
	public Thread getThread() {
		return this.thread;
	}
	public void setThread(Thread thread) {
		this.thread = thread;
	}
	public Map<String, Integer> getKeysCounts() {
		return this.keysCounts;
	}


	//	Partial reduce job executed in a new thread when Thread.start() is called by Master.
	@Override
	public void run() {
		
//		For counting the occurences of each key.
		for (String key : this.keys) {
			this.keysCounts.put(key, 0);
		}
		
//		For reading the input file.
		Scanner inputReader = null;
		
		try {
			
//			Initializes the reader.
			inputReader = new Scanner(new FileInputStream(this.inputFile));
			
//			We loop for each line of the input file (each line contains a word).
			String wordRead = null;
			Integer matchingCount = 0;
			while (inputReader.hasNextLine()) {	
				
//				We check if the word is a member of the keys set, and if so we increment its 
//				current count in the Map.
				if ((matchingCount = this.keysCounts.get(
						wordRead = inputReader.nextLine().split(" ")[0])) != null) {
					this.keysCounts.put(wordRead, matchingCount + 1);
				}
			}
			
//		Now the partial job is done, the main thread will do the rest of the reducing.
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			inputReader.close();
		}
		
		
	}
	
	
	

}
