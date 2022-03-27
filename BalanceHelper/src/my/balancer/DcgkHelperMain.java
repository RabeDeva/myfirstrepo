package my.balancer;

public class DcgkHelperMain {
	
	public static void main(String[] args) {
		String dcgkTest = "2->RMI,9991,|9->6";
		
		DcgkHelper helper = new DcgkHelper(dcgkTest);
		System.out.println(helper.getDcgkDomain());

	}

}
