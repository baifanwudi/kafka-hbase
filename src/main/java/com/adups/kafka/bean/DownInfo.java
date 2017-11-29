package com.adups.kafka.bean;

/**
 * @author allen
 */
public class DownInfo {

    private String mid;

    private String deviceId;

    private Long productId;

    private String originVersion;

    private String nowVersion;

    private String downStart;

    private String downEnd;

    private Integer deltaId;

    private Integer downloadStatus;

    private String createTime;

    private String ip;

    private String province;

    private String apnType;

    private String city;
    
    private String extStr;
    
    private Integer downSize;
    
    private String downIp;
    
    private String dataType;

	public String getMid() {
		return mid;
	}

	public void setMid(String mid) {
		this.mid = mid;
	}

	public String getDeviceId() {
		return deviceId;
	}

	public void setDeviceId(String deviceId) {
		this.deviceId = deviceId;
	}

	public Long getProductId() {
		return productId;
	}

	public void setProductId(Long productId) {
		this.productId = productId;
	}

	public String getOriginVersion() {
		return originVersion;
	}

	public void setOriginVersion(String originVersion) {
		this.originVersion = originVersion;
	}

	public String getNowVersion() {
		return nowVersion;
	}

	public void setNowVersion(String nowVersion) {
		this.nowVersion = nowVersion;
	}

	public String getDownStart() {
		return downStart;
	}

	public void setDownStart(String downStart) {
		this.downStart = downStart;
	}

	public String getDownEnd() {
		return downEnd;
	}

	public void setDownEnd(String downEnd) {
		this.downEnd = downEnd;
	}

	public Integer getDeltaId() {
		return deltaId;
	}

	public void setDeltaId(Integer deltaId) {
		this.deltaId = deltaId;
	}

	public Integer getDownloadStatus() {
		return downloadStatus;
	}

	public void setDownloadStatus(Integer downloadStatus) {
		this.downloadStatus = downloadStatus;
	}

	public String getCreateTime() {
		return createTime;
	}

	public void setCreateTime(String createTime) {
		this.createTime = createTime;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getProvince() {
		return province;
	}

	public void setProvince(String province) {
		this.province = province;
	}

	public String getApnType() {
		return apnType;
	}

	public void setApnType(String apnType) {
		this.apnType = apnType;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getExtStr() {
		return extStr;
	}

	public void setExtStr(String extStr) {
		this.extStr = extStr;
	}

	public Integer getDownSize() {
		return downSize;
	}

	public void setDownSize(Integer downSize) {
		this.downSize = downSize;
	}

	public String getDownIp() {
		return downIp;
	}

	public void setDownIp(String downIp) {
		this.downIp = downIp;
	}

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	@Override
	public String toString() {
		return "{\"mid\":\"" + mid + "\",\"deviceId\":\"" + deviceId + "\",\"productId\":\"" + productId
				+ "\",\"originVersion\":\"" + originVersion + "\",\"nowVersion\":\"" + nowVersion
				+ "\",\"downStart\":\"" + downStart + "\",\"downEnd\":\"" + downEnd + "\",\"deltaId\":\"" + deltaId
				+ "\",\"downloadStatus\":\"" + downloadStatus + "\",\"createTime\":\"" + createTime + "\",\"ip\":\""
				+ ip + "\",\"province\":\"" + province + "\",\"apnType\":\"" + apnType + "\",\"city\":\"" + city
				+ "\",\"extStr\":\"" + extStr + "\",\"downSize\":\"" + downSize + "\",\"downIp\":\"" + downIp + "\""
				+ ",\"dataType\":\"" + dataType + "\"}";
	}
}