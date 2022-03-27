package my.balancer;

/*
 * Little parsing helper class for DataChunkGroupKeys implementing the 
 * currently given semantics of the key structure with simple scope.
 * To be extended on further needs for DCGK token information ...
 */
public final class DcgkHelper {
		private final String dcgkRaw;
		private final String dcgkDomain;
		
		public DcgkHelper(String datachunkgroupkey) {
			this.dcgkRaw = datachunkgroupkey;
			
			String[] dcgkParts = datachunkgroupkey.split(",");
			String domain = "TCD";
			if (dcgkParts.length > 0) {
				String[] dcgkTokenParts = dcgkParts[0].split("->");
				if (dcgkTokenParts.length > 0) {
					domain = dcgkTokenParts[1];
				}
			}
			
			this.dcgkDomain = domain;
		}
		
		public String getDcgkRaw() {
			return dcgkRaw;
		}
		public String getDcgkDomain() {
			return dcgkDomain;
		}
	}