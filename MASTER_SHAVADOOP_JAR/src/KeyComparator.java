
// Modules to import.
import java.util.Comparator;
import java.util.Map;


// Class for comparing two keys (words) by their occurences.
public class KeyComparator implements Comparator<String> {

	
//	Dictionnary of occurences by word, sent as parameter when instantiating the class.
	private Map<String, Integer> keyCounts;
	
	
//	Constructor.
	public KeyComparator(Map<String, Integer> keyCounts) {
		this.keyCounts = keyCounts;
	}

	
//	Method that compares two keys.
	@Override
	public int compare(String key1, String key2) {
		
//		Returns -1 if first key has more occurences than second key.
		if (this.keyCounts.get(key1) > this.keyCounts.get(key2)) {
			return -1;
			
//		Returns 0 if both keys have as many occurences.
		} else if (this.keyCounts.get(key1) == this.keyCounts.get(key2)) {
			return 0;
			
//		Returns 1 if first key has less occurences than second key.
		} else {
			return 1;
		}
	}

}
