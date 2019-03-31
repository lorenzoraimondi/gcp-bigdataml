package gcp.cm.bigdata.adtech.domain;

import com.googlecode.objectify.annotation.Entity;
import com.googlecode.objectify.annotation.Id;
import com.googlecode.objectify.annotation.Index;

import java.util.Objects;

@Entity
public class Impression {
    @Id private String impressionId;
    private int clicked;
    @Index private int hour;
    private int bannerPos;
    private String siteId;
    private String siteDomain;
    @Index private String siteCategory;
    private String appId;
    private String appDomain;
    @Index private String appCategory;
    private String deviceId;
    private String deviceIp;
    private String deviceModel;
    @Index private int deviceType;
    private int deviceConnType;
    private int c1;
    private int c14;
    private int c15;
    private int c16;
    private int c17;
    private int c18;
    private int c19;
    private int c20;
    private int c21;

    public String getImpressionId() {
        return impressionId;
    }

    public void setImpressionId(String impressionId) {
        this.impressionId = impressionId;
    }

    public int getHour() {
        return hour;
    }

    public void setHour(int hour) {
        this.hour = hour;
    }

    public int getClicked() {
        return clicked;
    }

    public void setClicked(int clicked) {
        this.clicked = clicked;
    }

    public int getBannerPos() {
        return bannerPos;
    }

    public void setBannerPos(int bannerPos) {
        this.bannerPos = bannerPos;
    }

    public String getSiteId() {
        return siteId;
    }

    public void setSiteId(String siteId) {
        this.siteId = siteId;
    }

    public String getSiteDomain() {
        return siteDomain;
    }

    public void setSiteDomain(String siteDomain) {
        this.siteDomain = siteDomain;
    }

    public String getSiteCategory() {
        return siteCategory;
    }

    public void setSiteCategory(String siteCategory) {
        this.siteCategory = siteCategory;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getAppDomain() {
        return appDomain;
    }

    public void setAppDomain(String appDomain) {
        this.appDomain = appDomain;
    }

    public String getAppCategory() {
        return appCategory;
    }

    public void setAppCategory(String appCategory) {
        this.appCategory = appCategory;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getDeviceIp() {
        return deviceIp;
    }

    public void setDeviceIp(String deviceIp) {
        this.deviceIp = deviceIp;
    }

    public String getDeviceModel() {
        return deviceModel;
    }

    public void setDeviceModel(String deviceModel) {
        this.deviceModel = deviceModel;
    }

    public int getDeviceType() {
        return deviceType;
    }

    public void setDeviceType(int deviceType) {
        this.deviceType = deviceType;
    }

    public int getDeviceConnType() {
        return deviceConnType;
    }

    public void setDeviceConnType(int deviceConnType) {
        this.deviceConnType = deviceConnType;
    }

    public int getC1() {
        return c1;
    }

    public void setC1(int c1) {
        this.c1 = c1;
    }

    public int getC14() {
        return c14;
    }

    public void setC14(int c14) {
        this.c14 = c14;
    }

    public int getC15() {
        return c15;
    }

    public void setC15(int c15) {
        this.c15 = c15;
    }

    public int getC16() {
        return c16;
    }

    public void setC16(int c16) {
        this.c16 = c16;
    }

    public int getC17() {
        return c17;
    }

    public void setC17(int c17) {
        this.c17 = c17;
    }

    public int getC18() {
        return c18;
    }

    public void setC18(int c18) {
        this.c18 = c18;
    }

    public int getC19() {
        return c19;
    }

    public void setC19(int c19) {
        this.c19 = c19;
    }

    public int getC20() {
        return c20;
    }

    public void setC20(int c20) {
        this.c20 = c20;
    }

    public int getC21() {
        return c21;
    }

    public void setC21(int c21) {
        this.c21 = c21;
    }

    @Override
    public String toString() {
        return "Impression{" +
                "impressionId='" + impressionId + '\'' +
                ", hour=" + hour +
                ", siteId=" + siteId +
                ", appId=" + appId +
                ", deviceId=" + deviceId +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Impression that = (Impression) o;
        return impressionId.equals(that.impressionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(impressionId);
    }

}