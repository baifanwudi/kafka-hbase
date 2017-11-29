package com.adups.hbase.config;

/**
 * @author allen
 * @date 27/11/2017.
 */
public  final class BaseConfig {

	public static String TABLE_NAME="ota_pre_record";

	public  static String FAMILY_COLUMN="info";

	public  static  String STATUS_CHECK_SUCCESS="1";

	public  static  String STATUS_DOWN_FAIL="2";

	public  static  String STATUS_DOWN_SUCCESS="3";

	public  static  String STATUS_UPGRADE_FAIL="4";

	public  static  String STATUS_UPGRADE_SUCCESS="5";

}
