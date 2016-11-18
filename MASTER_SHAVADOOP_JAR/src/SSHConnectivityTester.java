
// Modules to import.
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


// Class that tests the SSH connection with a given adress.
public class SSHConnectivityTester implements Runnable {

	
//	Fields.
	private String adress; // Adress of the machine on which we test the SSH connection.
	private Thread thread; // Thread that will be associated to this instance.
	private ProcessBuilder processBuilder; // For launching the SSH process.
	private Process process; // SSH process.
	private BufferedReader bufferedProcessReader; // For reading the output of the process.
	private boolean connectionSuccess; // State of the SSH connection.
	
	
//	Constructor.
	public SSHConnectivityTester(String adress) {
		
		this.adress = adress;
		this.thread = null;
		this.processBuilder = new ProcessBuilder();
		this.process = null;
		this.bufferedProcessReader = null;
		this.connectionSuccess = false;
	}

	
//	Getters and setters.
	public String getAdress() {
		return this.adress;
	}
	public boolean isConnectionSuccess() {
		return this.connectionSuccess;
	}
	public Thread getThread() {
		return this.thread;
	}
	public void setThread(Thread thread) {
		this.thread = thread;
	}


//	Method executed in a new thread when Thread.start() is called by Master.
	@Override
	public void run() {
		
//		Try-catch bloc to prevent errors while trying to read the process response.
		try {
			
//			Tries an SSH connection with the adress.
			this.process = this.processBuilder.command("ssh", this.adress).start();
			
//			Object that will read the response of the process launched.
			this.bufferedProcessReader = new BufferedReader(new InputStreamReader(
					this.process.getInputStream()));
			
//			If there is a response, the connection is successful, so we set the boolean to true.
			if (this.bufferedProcessReader.readLine() != null) {
				this.connectionSuccess = true;
//			Otherwise, the connection failed, so we set the boolean to false.
			} else {
				this.connectionSuccess = false;			
			}
			
//		Catches eventual errors while trying to read the stream.
		} catch (IOException e) {
			e.printStackTrace();
			
//		Closes the stream to prevent memory leak.
		} finally {
			try {
				this.bufferedProcessReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		
	}

	
}
