package net.pdynet.adresysk;

public class Main {
	
	public static void main(String[] args) {
		try {
			AddressImportSK app = new AddressImportSK();
			app.run(args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
