package net.pdynet.adresysk;

public class BuildingRecord {
	
	private Integer objectId;
	private Integer municipalityIdentifier;
	private Integer districtIdentifier;
	private String propertyRegistrationNumber;
	
	public BuildingRecord(Integer objectId, Integer municipalityIdentifier, Integer districtIdentifier, String propertyRegistrationNumber) {
		this.objectId = objectId;
		this.municipalityIdentifier = municipalityIdentifier;
		this.districtIdentifier = districtIdentifier;
		this.propertyRegistrationNumber = propertyRegistrationNumber;
		/*
		if (StringUtils.isBlank(propertyRegistrationNumber))
			System.out.println("Blank propertyRegistrationNumber for objectId " + objectId);
		
		if (propertyRegistrationNumber.equals("0"))
			System.out.println("propertyRegistrationNumber == 0 for objectId " + objectId);
		*/
	}
	
	/**
	 * @return the objectId
	 */
	public Integer getObjectId() {
		return objectId;
	}
	
	/**
	 * @param objectId the objectId to set
	 */
	public void setObjectId(Integer objectId) {
		this.objectId = objectId;
	}
	
	/**
	 * @return the municipalityIdentifier
	 */
	public Integer getMunicipalityIdentifier() {
		return municipalityIdentifier;
	}
	
	/**
	 * @param municipalityIdentifier the municipalityIdentifier to set
	 */
	public void setMunicipalityIdentifier(Integer municipalityIdentifier) {
		this.municipalityIdentifier = municipalityIdentifier;
	}
	
	/**
	 * @return the districtIdentifier
	 */
	public Integer getDistrictIdentifier() {
		return districtIdentifier;
	}
	
	/**
	 * @param districtIdentifier the districtIdentifier to set
	 */
	public void setDistrictIdentifier(Integer districtIdentifier) {
		this.districtIdentifier = districtIdentifier;
	}
	
	/**
	 * @return the propertyRegistrationNumber
	 */
	public String getPropertyRegistrationNumber() {
		return propertyRegistrationNumber;
	}
	
	/**
	 * @param propertyRegistrationNumber the propertyRegistrationNumber to set
	 */
	public void setPropertyRegistrationNumber(String propertyRegistrationNumber) {
		this.propertyRegistrationNumber = propertyRegistrationNumber;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((districtIdentifier == null) ? 0 : districtIdentifier.hashCode());
		result = prime * result + ((municipalityIdentifier == null) ? 0 : municipalityIdentifier.hashCode());
		result = prime * result + ((objectId == null) ? 0 : objectId.hashCode());
		result = prime * result + ((propertyRegistrationNumber == null) ? 0 : propertyRegistrationNumber.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof BuildingRecord)) {
			return false;
		}
		BuildingRecord other = (BuildingRecord) obj;
		if (districtIdentifier == null) {
			if (other.districtIdentifier != null) {
				return false;
			}
		} else if (!districtIdentifier.equals(other.districtIdentifier)) {
			return false;
		}
		if (municipalityIdentifier == null) {
			if (other.municipalityIdentifier != null) {
				return false;
			}
		} else if (!municipalityIdentifier.equals(other.municipalityIdentifier)) {
			return false;
		}
		if (objectId == null) {
			if (other.objectId != null) {
				return false;
			}
		} else if (!objectId.equals(other.objectId)) {
			return false;
		}
		if (propertyRegistrationNumber == null) {
			if (other.propertyRegistrationNumber != null) {
				return false;
			}
		} else if (!propertyRegistrationNumber.equals(other.propertyRegistrationNumber)) {
			return false;
		}
		return true;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("BuildingRecord [objectId=").append(objectId)
				.append(", municipalityIdentifier=").append(municipalityIdentifier)
				.append(", districtIdentifier=").append(districtIdentifier)
				.append(", propertyRegistrationNumber=").append(propertyRegistrationNumber).append("]");
		return builder.toString();
	}
	
}
