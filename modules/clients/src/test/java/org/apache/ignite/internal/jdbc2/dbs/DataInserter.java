package org.apache.ignite.internal.jdbc2.dbs;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class DataInserter {
    /** Number of account IDs. */
    private static final int ACCT_ID_CNT = 5000;

    /** Number of records per account ID. */
    private static final int RECORDS_PER_ACCT_ID = 500;

    /** Account ID counter. */
    private static final AtomicLong ACCT_ID_CTR = new AtomicLong();

    /** Generated account IDs. */
    private static final ArrayList<String> ACCT_IDS = new ArrayList<>(ACCT_ID_CNT);

    /** */
    private static final String connectionString = System.getProperty("JDBC_CONN_STRING").split(";")[0];

    /**
     * Entry point.
     */
    @SuppressWarnings("unused")
    public static void main(String[] args) throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            executeUpdate(conn, "create table TRAN_HISTORY\n" +
                "(TRAN_ID  varchar(50) PRIMARY KEY,\n" +
                "ACCT_ID varchar(16) null,\n" +
                "ACCT_CURR varchar(3) null,\n" +
                "TRAN_CURR varchar(3) null,\n" +
                "LEDGER_BAL decimal(20, 4) null,\n" +
                "AVAIL_BAL decimal(20, 4) null,\n" +
                "VALUE_DATE date null,\n" +
                "POSTING_DATE date null,\n" +
                "TRAN_DATE date not null,\n" +
                "PART_TRAN_SERL_NUM varchar(4) not null,\n" +
                "TRAN_PARTICLR_CODE varchar(5) null,\n" +
                "TRAN_PARTICLRS varchar(50) null,\n" +
                "TRAN_PARTICLRS2 varchar(50) null,\n" +
                "TRAN_AMOUNT decimal(20, 4) null,\n" +
                "TXN_TYPE varchar(1) null,\n" +
                "PART_TXN_TYPE varchar(1) null,\n" +
                "TRAN_STATUS varchar(1) null,\n" +
                "TRAN_REF_NUM varchar(32) null,\n" +
                "TRAN_REMARKS varchar(50) null,\n" +
                "CHANNEL_ID varchar(10) null,\n" +
                "LST_TRAN_DATE date null,\n" +
                " SCHEME_CODE varchar(5) null,\n" +
                "REF_CURR_CODE varchar(3) null,\n" +
                "EXCHANGE_RATE_CODE varchar(5) null,\n" +
                " EXCHANGE_RATE decimal(21, 10) null,\n" +
                "DEAL_REF_NUM varchar(16) null,\n" +
                "LST_UPDT_SYS_ID varchar(255) null,\n" +
                "LST_UPDT_DTTM date null)");

            executeUpdate(conn, "CREATE INDEX accid_index ON TRAN_HISTORY(ACCT_ID)");

            generateData(conn);
        }
    }

    /**
     * Execute single update statment.
     *
     * @param conn Connection.
     * @param sql SQL statement.
     * @throws Exception If failed.
     */
    private static void executeUpdate(Connection conn, String sql) throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        }
    }

    /**
     * Generate data.
     *
     * @param conn Connection.
     * @throws Exception If failed.
     */
    private static void generateData(Connection conn) throws Exception {
        long start = System.currentTimeMillis();

        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("SET STREAMING 1");

            insertAll(stmt);

            stmt.executeUpdate("SET STREAMING 0");
        }
        finally {
            long dur = System.currentTimeMillis() - start;

            System.out.println(">>> GENERATED data in " + dur + " ms");
        }
    }

    /**
     * Insert all records.
     */
    private static void insertAll(Statement stmt) throws Exception {
        int ctr = 0;

        for (int i = 0; i < ACCT_ID_CNT; i++) {
            String acctId = generateAccountId();

            ACCT_IDS.add(acctId);
        }

        for (int j = 0; j < RECORDS_PER_ACCT_ID; j++) {
            for (String acctId : ACCT_IDS) {
                insertAccountId(stmt, acctId);

                ctr++;

                if (ctr % 50_000 == 0)
                    System.out.println(">>> INSERTED: " + ctr);
            }
        }
    }

    /**
     * Insert single record for the given account ID.
     *
     * @param stmt Statement.
     * @param acctId Account ID.
     * @throws Exception If failed.
     */
    private static void insertAccountId(Statement stmt, String acctId) throws Exception {
        String id = UUID.randomUUID().toString();

        stmt.executeUpdate("INSERT INTO TRAN_HISTORY VALUES ('" + id + "','" + acctId + "','TWD','TWD',0.0000,906.0000,'2018-04-19', '2018-04-19','2018-04-19','25','CHQ03','982668','',26500.0000, 'L','D','','50000000','Zone Serial [ 25]','','2018-09-09','RQODA','TWD','', 1.0000000000,'','','2018-09-09')");
    }

    /**
     * @return Generated account ID.
     */
    private static String generateAccountId() {
        return String.format("%016d", ACCT_ID_CTR.incrementAndGet());
    }
}
