
// Modules to import.
import java.io.FileInputStream;
import java.io.PrintWriter;
import java.text.Normalizer;
import java.util.HashSet;


// Class that executes a partial map job in a new thread on the same machine.
public class MapLauncher implements Runnable {
	
	
//	Fields.
	private int startPosition; // Start of the input part that this thread will do the mapping on.
	private int endPosition; // End (excluded) of the input part.
	private FileInputStream inputReader; // For reading the input file.
	private PrintWriter outputWriter; // For writing to the output file.
	private Thread thread; // Thread associated to this class instance.

	
//	Constructor. We initialize the fields here.
	public MapLauncher(int startPosition, int endPosition, FileInputStream inputReader, 
			PrintWriter outputWriter) {
		
		this.startPosition = startPosition;
		this.endPosition = endPosition;
		this.inputReader = inputReader;
		this.outputWriter = outputWriter;
	}

	
//	Getters and setters.
	public Thread getThread() {
		return this.thread;
	}
	public void setThread(Thread thread) {
		this.thread = thread;
	}


//	Partial map job executed in a new thread when Thread.start() is called by Master.
	@Override
	public void run() {
		
		try {
		
//			Buffer to store the read bytes.
			byte[] buffer = new byte[this.endPosition - Math.max(this.startPosition - 1, 0)];
			
//			Text of the input file part.
			String inputText;
			
//			For additional bytes due to not wanting to cut words.
			int additionalBytes = 0;
			
//			Now we read our input.
			synchronized (this.inputReader) {
				
//				We first skip the part of the file that doesn't interest us, plus one byte at the
//				start to check if it is a space or not.
				this.inputReader.getChannel().position(Math.max(this.startPosition - 1, 0));
		
//				Reads the part of the file.
				this.inputReader.read(buffer);
			
//				We want to skip the first bytes until we reach a space (we don't want to cut words).
				int offset = 0;
				if (this.startPosition > 0) {
					while (true) {
						if (buffer[offset] == 32) {
							break;
						}
						offset++;
					}
				}
				inputText = new String(buffer, offset, buffer.length - offset);			
			
//				Reads bytes until we reach a space or the end of text (we don't want to cut words).
				if (buffer[buffer.length - 1] != 32) {
					while (true) {
						if (this.inputReader.read(buffer, additionalBytes, 1) == -1) {
							break;
						}						
						additionalBytes++;
						if (buffer[additionalBytes - 1] == 32) {
							break;
						}
					}
				}
			}
		
//			Adds the additional bytes to the text.
			inputText = inputText.concat(new String(buffer, 0, additionalBytes));
		
//			Puts the charachters in lower case.
			inputText = inputText.toLowerCase();
	
//			Removes accentuation (thank you Google for the code).
			inputText = Normalizer.normalize(inputText, Normalizer.Form.NFD);
			inputText = inputText.replaceAll("[^\\p{ASCII}]", "");
		
//			Removes special characters and replaces them by spaces.
			inputText = inputText.replaceAll("[^a-z]", " ");
			
//			List of common words.
			String[] commonWords = new String[] {
					"je", "tu", "il", "elle", "nous", "vous", "ils", "elles", "le", "la", "lui",
					"les", "nous", "vous", "leur", "eux", "celui", "celle", "celui ci", "celui la",
					"celle ci", "celle la", "ceci", "cela", "ca", "ceux", "ceux ci", "ceux la",
					"celles ci", "celles la", "le mien", "le tien", "le sien", "le notre",
					"le votre", "le leur", "la mienne", "la tienne", "la sienne", "la notre",
					"la votre", "la leur", "les miens", "les tiens", "les siens", "les notres",
					"les votres", "les leurs", "les miennes", "les tiennes", "les siennes",
					"les notres", "les votres", "les leurs", "on", "rien", "aucun", "aucune",
					"nul", "nulle", "autre", "ni", "tout", "quelqu", "quelque", "certain",
					"certaine", "certains", "certaines", "plusieurs", "tous", "autres", "qui",
					"que", "quoi", "dont", "ou", "lequel", "laquelle", "duquel", "auquel",
					"lesquels", "desquels", "auxquels", "lesquelles", "desquelles", "auxquelles",
					"apres", "avant", "avec", "chez", "concernant", "contre", "dans", "de",
					"depuis", "derriere", "des", "devant", "durant", "en", "entre", "envers",
					"hormis", "hors", "jusque", "malgre", "moyennant", "outre", "par", "parmi",
					"pendant", "pour", "pres", "sans", "sauf", "selon", "sous", "suivant", "sur",
					"touchant", "vers", "via", "a bas de", "a cause de", "a cote de", "a defaut de",
					"afin de", "a force de", "a la merci", "a la faveur de", "a l egard de",
					"a l encontre de", "a l entour de",	"a l exception de", "a l instar de",
					"a l insu de", "a meme", "a moins de", "a partir de", "a raison de",
					"a seule fin de", "a travers", "au dedans de", "au defaut de", "au dehors",
					"au dessous de", "au dessus de", "au lieu de", "au moyen de", "aupres de",
					"aux environs de", "au prix de", "autour de", "aux alentours de",
					"au depens de", "avant de", "d apres", "d avec", "de façon a", "de la part de",
					"de maniere a", "d entre", "de par", "de peur de", "du cote de", "en bas de",
					"en deca de", "en dedans de", "en dehors de", "en depit de", "en face de",
					"en faveur de", "en guise de", "en outre de", "en plus de", "grace a",
					"hors de", "loin de", "lors de", "par rapport a", "par suite de", "pres de",
					"proche de", "quant a", "quitte a", "sauf a", "sous couleur de", "vis a vie de",
					"ainsi", "car", "cependant", "comme", "donc", "si", "et", "quand", "ni", "ou",
					"or", "puis", "que", "pourtant", "lorsque", "neanmoins", "toutefois", "sinon",
					"mais", "soit", "enfin", "puisque", "au reste", "au surplus", "ainsi que",
					"a moins que", "bien que", "tandis", "aussitot", "de peur", "par consequent",
					"c est a dire", "d ailleurs", "vu que", "en outre", "au contraire", "de plus",
					"de maniere", "de sorte", "parce", "alors", "ci", "ma", "ta", "sa", "mon",
					"ton", "son", "mes", "tes", "ses", "nos", "vos", "leurs", "au", "aux", "en",
					"ou", "soi", "et", "par", "etre", "pas", "sur", "plus", "te", "tu", "toi",
					"pour", "je", "me", "moi", "ce", "cet", "cette", "ces", "un", "une", "uns",
					"unes", "comme", "le", "la", "les", "qu", "que", "de", "du", "des", "dans",
					"lui", "elle", "se", "mais", "sans", "ne", "avoir", "faire", "peu", "meme", 
					"non", "fois", "vers", "chez", "jusque", "tres", "quel", "quelle", "quels",
					"quelles", "devant", "ici", "oui", "trop", "chaque", "deja", "tant", "avant",
					"enfin", "ah", "voila", "tel", "fait", "est", "oh", "eh", "cas", "sont", 
					"suis", "es", "etes", "ete", "sommes", "ont", "eu", "eus", "avait", "avaient",
					"etait", "etaient", "lorsqu", "peut", "peux", "peuvent", "ayant"
			};
			
//			Removes common words.
			for (String word : commonWords) {
				inputText = inputText.replaceAll(" " + word + " ", " ");		
			}
	
//			Splits the text into words.
			String[] words = inputText.split("\\s+");
			
//			Set of keys.
			HashSet<String> uniqueWords = new HashSet<String>();

//			For each word, we write it to the output file with synchronization. Then if it is a 
//			new word (not present in the word set), we add it to the set of words and send it 
//			to the Master via SSH.
			for (String word : words) {
				if (!(word == null) && !(word.length() < 2)) {
					synchronized (this.outputWriter) {
						this.outputWriter.write(word + " 1\n");
					}					
					if (uniqueWords.add(word)) {
						System.out.println(word);
					}
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}	

}
