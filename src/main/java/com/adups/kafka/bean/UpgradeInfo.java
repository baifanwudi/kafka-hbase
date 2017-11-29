package com.adups.kafka.bean;


/**
 * @author  allen
 */
public class UpgradeInfo {

    private String mid;

    private String deviceId;

    private Long productId;

    private String originVersion;

    private String nowVersion;

    private Integer updateStatus;

    private Integer deltaId;

    private String ip;

    private String province;

    private String apnType;

    private String city;

    private String createTime;
    
    private String extStr;
    
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

    public Integer getUpdateStatus() {
        return updateStatus;
    }

    public void setUpdateStatus(Integer updateStatus) {
        this.updateStatus = updateStatus;
    }

    public Integer getDeltaId() {
        return deltaId;
    }

    public void setDeltaId(Integer deltaId) {
        this.deltaId = deltaId;
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

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getExtStr() {
        return extStr;
    }

    public void setExtStr(String extStr) {
        this.extStr = extStr;
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
				+ "\",\"updateStatus\":\"" + updateStatus + "\",\"deltaId\":\"" + deltaId + "\",\"ip\":\"" + ip
				+ "\",\"province\":\"" + province + "\",\"apnType\":\"" + apnType + "\",\"city\":\"" + city
				+ "\",\"createTime\":\"" + createTime + "\",\"extStr\":\"" + extStr + "\",\"dataType\":\"" + dataType + "\"}";
	}
}