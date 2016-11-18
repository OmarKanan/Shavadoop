import java.io.FileInputStream;
import java.io.PrintWriter;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;


public class Slave {

	
	public static void main(String[] args) {
		
//	TODO Split process with "Runtime.getRuntime().availableProcessors();"
//	TODO Create instance of Slave class and put code outside of main method.
//	TODO Comment code.
		
		// Gets mode

		String mode = null;
		
		if (args.length < 1) {
			System.err.println("Must have <mode> as first argument");
			System.exit(1);
		} else {
			mode = args[0];
		}
		
		
		// Gets other arguments and launches method corresponding to mode
		
		if (mode.equals("SXUMX")) {

			String sxFile = null;
			String umxFile = null;
			
			if (args.length != 3) {
				System.err.println("For SXUMX mode, must add <output file, input file> as arguments");
				System.exit(1);
			} else {
				umxFile = args[1];
				sxFile = args[2];
				sxUmx(umxFile, sxFile);
			}
			
		} else if (mode.equals("UMXRMX")) {

			String[] keys = null;
			String rmxFile = null;
			String[] umxFiles = null;
			
			if (args.length != 4) {
				System.err.println("For UMXRMX mode, must add <keys, output file, input files> "
						+ "as arguments");
				System.exit(1);
			} else {
				keys = args[1].split("___");
				rmxFile = args[2];
				umxFiles = args[3].split("___");
				umxRmx(keys, rmxFile, umxFiles);
			}
			
		} else {
			System.err.println("Mode must be in [SXUMX, UMXRMX]");
			System.exit(1);
		}			
	}


	private static void sxUmx(String outputFile, String inputFile) {
		
		// Creates file with (word, 1) pairs and add keys to 'wordSet'
		
		Scanner inputScanner = null;
		String[] words;
		PrintWriter outputWriter = null;
		
		HashSet<String> wordSet = new HashSet<String>();
				
		try {
			
			inputScanner = new Scanner(new FileInputStream(inputFile));		
			outputWriter = new PrintWriter(outputFile);
			
			while (inputScanner.hasNextLine()) {
				String line = inputScanner.nextLine();
				line = line.toLowerCase();
				line = Normalizer.normalize(line, Normalizer.Form.NFD);
				line = line.replaceAll("[^\\p{ASCII}]", "");
				line = line.replaceAll("[^a-z]", " ");
				words = line.split("\\s+");
				for (String word : words) {
					if (!word.equals("") && !(word == null)) {
						outputWriter.write(word + " " + 1 + "\n");
						if (wordSet.add(word)) {
							System.out.println(word);
						}
					}
				}
			}
			
			System.out.println("END OF PROCESS SXUMX");
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			inputScanner.close();
			outputWriter.close();
		}
	}
	
	
	private static void umxRmx(String[] keys, String outputFile, String[] inputFiles) {
		
		// Creates file with (key, count) for each keys given
		Scanner inputScanner = null;
		PrintWriter outputWriter = null;
		HashMap<String, Integer> keysCounts = new HashMap<String, Integer>();
		
		for (String key : keys) {
			keysCounts.put(key, 0);
		}
			
		try {
			
			outputWriter = new PrintWriter(outputFile);
			
			for (String inputFile : inputFiles) {
				inputScanner = new Scanner(new FileInputStream(inputFile));		
				String wordRead = null;
				Integer matchingCount = 0;
				while (inputScanner.hasNextLine()) {			
					if ((matchingCount = keysCounts.get(wordRead = inputScanner.nextLine().split(" ")[0])) != null) {
						keysCounts.put(wordRead, matchingCount + 1);
					}
				}
			}
			
			Collections.sort(Arrays.asList(keys), new KeyComparator(keysCounts));
			
			for (String key : keys) {
				System.out.println(key + " " + keysCounts.get(key));
			}
			for (String key : keys) {
				outputWriter.write(key + " " + keysCounts.get(key) + "\n");
			}
			
			System.out.println("END OF PROCESS UMXRMX");
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			inputScanner.close();
			outputWriter.close();
		}
		
	}

}
