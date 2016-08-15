/*
 * ddlUtils is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.ddlutils.platform.splicemachine;

import java.io.IOException;
import java.util.Map;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Schema;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.platform.derby.DerbyBuilder;

/**
 * This is where splicemachine would implement splice-specific model SQL Builder methods.
 */
public class SpliceBuilder extends DerbyBuilder {
    /**
     * Creates a new builder instance.
     *
     * @param platform The platform this builder belongs to
     */
    public SpliceBuilder(Platform platform) {
        super(platform);
    }

    // EXPORT ( exportPath, compress, replicationCount, fileEncoding, fieldSeparator,  quoteCharacter )  select * from <SCHEMA>.<TABLE>;
    private static final String EXPORT_CMD = "EXPORT('<OUT>', <COMPRESS>, <REPLICATION_CNT>, <ENCODING>, <FIELD_SEP>, <QUOTE_CHAR>) select * from ";
    @Override
    public void createExportSQL(Database model, String exportRoot, Map<String, String> params) throws IOException {
        for (Schema schema : model.getSchemas()) {
            for (Table table : schema.getTables()) {
                table.setExportPath(exportRoot +"/"+schema.getSchemaName()+"/"+table.getName());
                print(EXPORT_CMD.replace("<OUT>", table.getExportPath())
                                .replace("<COMPRESS>", params.get(SplicePlatform.EXPORT_COMPRESS))
                                .replace("<REPLICATION_CNT>", params.get(SplicePlatform.EXPORT_REP_CNT))
                                .replace("<ENCODING>", params.get(SplicePlatform.EXPORT_IMPORT_FILE_ENCODING))
                                .replace("<FIELD_SEP>", params.get(SplicePlatform.EXPORT_IMPORT_FIELD_SEP))
                                .replace("<QUOTE_CHAR>", params.get(SplicePlatform.EXPORT_IMPORT_QUOTE_CHAR))
                );
                printIdentifier(getTableName(table));
                printEndOfStatement();
            }
        }
    }

    // call SYSCS_UTIL.IMPORT_DATA (schemaName,tableName,insertColumnList,fileOrDirectoryName,columnDelimiter,characterDelimiter,timestampFormat,dateFormat,timeFormat,badRecordsAllowed,badRecordDirectory,oneLineRecords,charset)
    // We default insertColumnList (all) and all time/date formats (already in internal form)
    private static final String IMPORT_CMD =
        "CALL SYSCS_UTIL.IMPORT_DATA('<SCHEMA>','<TABLE>', null,'<FILE_DIR>',<FIELD_SEP>,<QUOTE_CHAR>,'yyyy-MM-dd HH:mm:ss.SSSSSS',null,null,<FAIL_THRESHOLD>,'<FILE_DIR>', <ONE_LN_RECORDS> , <ENCODING>)";
    @Override
    public void createImportSQL(Database model, String exportRoot, Map<String, String> params) throws IOException {
        for (Schema schema : model.getSchemas()) {
            for (Table table : schema.getTables()) {
                String exportSubDir = table.getExportPath();
                if (exportSubDir != null && ! exportRoot.isEmpty()) {
                    print(IMPORT_CMD.replace("<SCHEMA>", schema.getSchemaName())
                                    .replace("<TABLE>", table.getName())
                                    .replace("<FILE_DIR>", exportSubDir)
                                    .replace("<FIELD_SEP>", params.get(SplicePlatform.EXPORT_IMPORT_FIELD_SEP))
                                    .replace("<QUOTE_CHAR>", params.get(SplicePlatform.EXPORT_IMPORT_QUOTE_CHAR))
                                    .replace("<FAIL_THRESHOLD>", params.get(SplicePlatform.IMPORT_FAIL_THRESHOLD))
                                    .replace("<ONE_LN_RECORDS>", params.get(SplicePlatform.IMPORT_ONE_LINE_RECORDS))
                                    .replace("<ENCODING>", params.get(SplicePlatform.EXPORT_IMPORT_FILE_ENCODING))
                    );
                    printEndOfStatement();
                }
            }
        }
    }
}
