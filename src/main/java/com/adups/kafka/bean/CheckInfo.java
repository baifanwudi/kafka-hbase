package com.adups.kafka.bean;

/**
 * @author  allen
 */
public class CheckInfo {

	private String mid;

	private Long productId;

	private Integer deltaId;

	private String originVersion;

	private String nowVersion;

	private String createTime;

	private String ip;

	private String province;

	private String city;

	private String networkType;

	private String deviceId;

	private String lac;

    private String cid;

    private String mcc;

    private String mnc;

    private String rxlev;

    private String dataType;

	public String getMid() {
		return mid;
	}

	public void setMid(String mid) {
		this.mid = mid;
	}

	public Long getProductId() {
		return productId;
	}

	public void setProductId(Long productId) {
		this.productId = productId;
	}

	public Integer getDeltaId() {
		return deltaId;
	}

	public void setDeltaId(Integer deltaId) {
		this.deltaId = deltaId;
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

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getNetworkType() {
		return networkType;
	}

	public void setNetworkType(String networkType) {
		this.networkType = networkType;
	}

	public String getDeviceId() {
		return deviceId;
	}

	public void setDeviceId(String deviceId) {
		this.deviceId = deviceId;
	}

	public String getLac() {
		return lac;
	}

	public void setLac(String lac) {
		this.lac = lac;
	}

	public String getCid() {
		return cid;
	}

	public void setCid(String cid) {
		this.cid = cid;
	}

	public String getMcc() {
		return mcc;
	}

	public void setMcc(String mcc) {
		this.mcc = mcc;
	}

	public String getMnc() {
		return mnc;
	}

	public void setMnc(String mnc) {
		this.mnc = mnc;
	}

	public String getRxlev() {
		return rxlev;
	}

	public void setRxlev(String rxlev) {
		this.rxlev = rxlev;
	}

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	@Override
	public String toString() {
		return "{\"mid\":\"" + mid + "\",\"productId\":\"" + productId + "\",\"deltaId\":\"" + deltaId
				+ "\",\"originVersion\":\"" + originVersion + "\",\"nowVersion\":\"" + nowVersion
				+ "\",\"createTime\":\"" + createTime + "\",\"ip\":\"" + ip + "\",\"province\":\"" + province
				+ "\",\"city\":\"" + city + "\",\"networkType\":\"" + networkType + "\",\"deviceId\":\"" + deviceId
				+ "\",\"lac\":\"" + lac + "\",\"cid\":\"" + cid + "\",\"mcc\":\"" + mcc + "\",\"mnc\":\"" + mnc
				+ "\",\"rxlev\":\"" + rxlev + "\",\"dataType\":\"" + dataType + "\"}";
	}
}
