package com.pricedotcom.automation.test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import com.pricedotcom.mysql.MySqlOperation;
import com.pricedotcom.mysql.MysqlConnection;

public class TestAutomation {
	static MySqlOperation mysqlOpp = new MySqlOperation();
	static MysqlConnection mysql = new MysqlConnection();
	public static void main(String args[]) throws ClassNotFoundException, SQLException, IOException {
		// MySQL Connection
		Connection conn = mysql.connectMySQL();
		mysqlOpp.cjTargetTableCreation(conn, "PRICE_DOT_COM_STAGING.STG_CJ_MONSTER_W", "PRICE_DOT_COM_TARGET.CJ_MONSTER", "153", false, "");
	}
}
