package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.sql.catalog.SystemColumnImpl;
import com.splicemachine.db.shared.common.sanity.SanityManager;
import com.splicemachine.derby.iapi.catalog.TableStatisticsDescriptor;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Properties;

/**
 * @author Scott Fines
 *         Date: 2/25/15
 */
public class SYSTABLESTATISTICSRowFactory extends CatalogRowFactory {
    public static final String TABLENAME_STRING = "SYSTABLESTATS";
    private static final int SYSTABLESTATISTICS_COLUMN_COUNT = 14;
    private static final int CONGLOMID = 1;
    private static final int PARTITIONID = 2;
    private static final int TIMESTAMP = 3;
    private static final int STALENESS = 4;
    private static final int INPROGRESS = 5;
    private static final int ROWCOUNT = 6;
    private static final int PARTITION_SIZE = 7;
    private static final int MEANROWWIDTH= 8;
    private static final int QUERYCOUNT = 9;
    private static final int LOCALREADLATENCY = 10;
    private static final int REMOTEREADLATENCY = 11;
    private static final int WRITELATENCY = 12;
    private static final int OPENSCANNERLATENCY = 13;
    private static final int CLOSESCANNERLATENCY = 14;

    protected static final int		SYSTABLESTATISTICS_INDEX1_ID = 0;
    protected static final int		SYSTABLESTATISTICS_INDEX2_ID = 1;
    protected static final int		SYSTABLESTATISTICS_INDEX3_ID = 2;


    private String[] uuids = {
            "08264012-014b-c29b-a826-000003009390",
            "0826401a-014b-c29b-a826-000003009390",
            "08264014-014b-c29b-a826-000003009390",
            "08264016-014b-c29b-a826-000003009390",
            "08264018-014b-c29b-a826-000003009390"
    };

    private	static	final	boolean[]	uniqueness = {
            true,
            false,
            false
    };

    private static final int[][] indexColumnPositions =
            {
                    {CONGLOMID, PARTITIONID,TIMESTAMP},
                    {CONGLOMID, PARTITIONID},
                    {CONGLOMID},
            };

    public SYSTABLESTATISTICSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf) {
        super(uuidf, ef, dvf);
        initInfo(SYSTABLESTATISTICS_COLUMN_COUNT,TABLENAME_STRING,indexColumnPositions,uniqueness,uuids);
    }

    @Override
    public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent) throws StandardException {
        long conglomId = 0;
        String partitionId = null;
        long timestamp = 0;
        boolean staleness = false;
        boolean inProgress = false;
        long rowCount = 0;
        long partitionSize = 0;
        int meanRowWidth=0;
        long queryCount = 0;
        long localReadLatencyMicros = 0;
        long remoteReadLatencyMicros = 0;
        long writeLatencyMicros = 0;
        long openScannerMicros = 0l;
        long closeScannerMicros = 0l;

        if(td!=null){
            TableStatisticsDescriptor tsd = (TableStatisticsDescriptor)td;
            conglomId = tsd.getConglomerateId();
            partitionId = tsd.getPartitionId();
            timestamp  = tsd.getTimestamp();
            staleness = tsd.isStale();
            inProgress= tsd.isInProgress();
            rowCount = tsd.getRowCount();
            partitionSize = tsd.getPartitionSize();
            meanRowWidth = tsd.getMeanRowWidth();
            queryCount = tsd.getQueryCount();
            localReadLatencyMicros = tsd.getLocalReadLatency();
            remoteReadLatencyMicros = tsd.getRemoteReadLatency();
            writeLatencyMicros = tsd.getWriteLatency();
            openScannerMicros = tsd.getOpenScannerLatency();
            closeScannerMicros = tsd.getCloseScannerLatency();
        }

        ExecRow row = getExecutionFactory().getValueRow(SYSTABLESTATISTICS_COLUMN_COUNT);
        row.setColumn(CONGLOMID,new SQLLongint(conglomId));
        row.setColumn(PARTITIONID,new SQLVarchar(partitionId));
        row.setColumn(TIMESTAMP,new SQLTimestamp(new Timestamp(timestamp)));
        row.setColumn(STALENESS,new SQLBoolean(staleness));
        row.setColumn(INPROGRESS,new SQLBoolean(inProgress));
        row.setColumn(ROWCOUNT,new SQLLongint(rowCount));
        row.setColumn(PARTITION_SIZE,new SQLLongint(partitionSize));
        row.setColumn(MEANROWWIDTH,new SQLInteger(meanRowWidth));
        row.setColumn(QUERYCOUNT,new SQLLongint(queryCount));
        row.setColumn(LOCALREADLATENCY,new SQLLongint(localReadLatencyMicros));
        row.setColumn(REMOTEREADLATENCY,new SQLLongint(remoteReadLatencyMicros));
        row.setColumn(WRITELATENCY,new SQLLongint(writeLatencyMicros));
        row.setColumn(OPENSCANNERLATENCY,new SQLLongint(openScannerMicros));
        row.setColumn(CLOSESCANNERLATENCY,new SQLLongint(closeScannerMicros));
        return row;
    }

    @Override
    public TupleDescriptor buildDescriptor(ExecRow row, TupleDescriptor parentTuple, DataDictionary dataDictionary) throws StandardException {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT( row.nColumns() == SYSTABLESTATISTICS_COLUMN_COUNT,
                    "Wrong number of columns for a STATEMENTHISTORY row");
        }

        DataValueDescriptor col = row.getColumn(CONGLOMID);
        long conglomId = col.getLong();
        col = row.getColumn(PARTITIONID);
        String partitionId = col.getString();
        col = row.getColumn(TIMESTAMP);
        Timestamp timestamp = col.getTimestamp(null);
        col = row.getColumn(STALENESS);
        boolean isStale = col.getBoolean();
        col = row.getColumn(INPROGRESS);
        boolean inProgress = col.getBoolean();
        col = row.getColumn(ROWCOUNT);
        long rowCount = col.getLong();
        col = row.getColumn(PARTITION_SIZE);
        long partitionSize = col.getLong();
        col = row.getColumn(MEANROWWIDTH);
        int rowWidth = col.getInt();
        col = row.getColumn(QUERYCOUNT);
        long queryCount = col.getLong();
        col = row.getColumn(LOCALREADLATENCY);
        long localReadLatency = col.getLong();
        col = row.getColumn(REMOTEREADLATENCY);
        long remoteReadLatency = col.getLong();
        col = row.getColumn(WRITELATENCY);
        long writeLatency = col.getLong();
        col = row.getColumn(OPENSCANNERLATENCY);
        long openLatency = col.getLong();
        col = row.getColumn(CLOSESCANNERLATENCY);
        long closeLatency = col.getLong();

        return new TableStatisticsDescriptor(conglomId,
                partitionId,
                timestamp.getTime(),
                isStale,
                inProgress,
                rowCount,
                partitionSize,
                rowWidth,
                queryCount,
                localReadLatency,
                remoteReadLatency,
                writeLatency,
                openLatency,
                closeLatency);
    }

    @Override
    public SystemColumn[] buildColumnList() throws StandardException {
        return new SystemColumn[]{
                SystemColumnImpl.getColumn("CONGLOMERATEID", Types.BIGINT,false),
                SystemColumnImpl.getColumn("PARTITIONID",Types.VARCHAR,false),
                SystemColumnImpl.getColumn("LAST_UPDATED",Types.TIMESTAMP,false),
                SystemColumnImpl.getColumn("IS_STALE",Types.BOOLEAN,false),
                SystemColumnImpl.getColumn("IN_PROGRESS", Types.BOOLEAN, false),
                SystemColumnImpl.getColumn("ROWCOUNT",Types.BIGINT,true),
                SystemColumnImpl.getColumn("PARTITION_SIZE",Types.BIGINT,true),
                SystemColumnImpl.getColumn("MEANROWWIDTH",Types.INTEGER,true),
                SystemColumnImpl.getColumn("QUERYCOUNT",Types.BIGINT,true),
                SystemColumnImpl.getColumn("LOCALREADLATENCY",Types.BIGINT,true),
                SystemColumnImpl.getColumn("REMOTEREADLATENCY",Types.BIGINT,true),
                SystemColumnImpl.getColumn("WRITELATENCY",Types.BIGINT,true),
                SystemColumnImpl.getColumn("OPENSCANNERLATENCY",Types.BIGINT,true),
                SystemColumnImpl.getColumn("CLOSESCANNERLATENCY",Types.BIGINT,true)
        };
    }

    @Override
    public Properties getCreateHeapProperties() {
        return super.getCreateHeapProperties();
    }

    public static ColumnDescriptor[] getViewColumns(TableDescriptor view,UUID viewId) throws StandardException {
        DataTypeDescriptor varcharType = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR);
        DataTypeDescriptor longType = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT);
        return new ColumnDescriptor[]{
                new ColumnDescriptor("SCHEMANAME"               ,1,varcharType,null,null,view,viewId,0,0),
                new ColumnDescriptor("TABLENAME"                ,2,varcharType,null,null,view,viewId,0,0),
                new ColumnDescriptor("CONGLOMERATENAME"         ,3,varcharType,null,null,view,viewId,0,0),
                new ColumnDescriptor("TOTAL_ROW_COUNT"          ,4,longType,null,null,view,viewId,0,0),
                new ColumnDescriptor("AVG_ROW_COUNT"            ,5,longType,null,null,view,viewId,0,0),
                new ColumnDescriptor("TOTAL_SIZE"               ,6,longType,null,null,view,viewId,0,0),
                new ColumnDescriptor("NUM_PARTITIONS"           ,7,longType,null,null,view,viewId,0,0),
                new ColumnDescriptor("AVG_PARTITION_SIZE"       ,8,longType,null,null,view,viewId,0,0),
                new ColumnDescriptor("ROW_WIDTH"                ,9,longType,null,null,view,viewId,0,0),
                new ColumnDescriptor("TOTAL_QUERY_COUNT"        ,10,longType,null,null,view,viewId,0,0),
                new ColumnDescriptor("AVG_QUERY_COUNT"          ,11,longType,null,null,view,viewId,0,0),
                new ColumnDescriptor("AVG_LOCAL_READ_LATENCY"   ,12,longType,null,null,view,viewId,0,0),
                new ColumnDescriptor("AVG_REMOTE_READ_LATENCY"  ,13,longType,null,null,view,viewId,0,0),
                new ColumnDescriptor("AVG_WRITE_LATENCY"        ,14,longType,null,null,view,viewId,0,0),
        };
    }

    public static final String STATS_VIEW_SQL = "create view systablestatistics as select " +
            "s.schemaname" +
            ",t.tablename" + // 1
            ",c.conglomeratename" + //2
            ",sum(ts.rowCount) as TOTAL_ROW_COUNT" +  //3
            ",avg(ts.rowCount) as AVG_ROW_COUNT" +      //4
            ",sum(ts.partition_size) as TOTAL_SIZE" + //5
            ",count(ts.rowCount) as NUM_PARTITIONS" + //6
            ",avg(ts.partition_size) as AVG_PARTITION_SIZE" + //7
            ",max(ts.meanrowWidth) as ROW_WIDTH" + //8
            ",sum(ts.queryCount) as TOTAL_QUERY_COUNT" + //9
            ",avg(ts.queryCount) as AVG_QUERY_COUNT" + //10
            ",avg(CASE WHEN ts.rowCount>0 then ts.localReadLatency/ts.rowCount ELSE 0 END) as AVG_LOCAL_READ_LATENCY" + //11
            ",avg(CASE WHEN ts.rowCount>0 then ts.remoteReadLatency/ts.rowCount ELSE 0 END) as AVG_REMOTE_READ_LATENCY" + //12
            ",avg(CASE WHEN ts.rowCount>0 then ts.writeLatency/ts.rowCount ELSE 0 END) as AVG_WRITE_LATENCY" + //13
            " from " +
            "sys.systables t" +
            ",sys.sysschemas s" +
            ",sys.sysconglomerates c" +
            ",sys.systablestats ts" +
            " where " +
            "t.tableid = c.tableid " +
            "and c.conglomeratenumber = ts.conglomerateid " +
            "and t.schemaid = s.schemaid " +
            "and PARTITION_EXISTS(ts.conglomerateId,ts.partitionid)" +
            " group by " +
            "s.schemaname" +
            ",t.tablename"+
            ",c.conglomeratename";

    public static void main(String...args) {
        System.out.println(STATS_VIEW_SQL);
    }
}
