package com.splicemachine.derby.utils.marshall;

import com.esotericsoftware.kryo.KryoException;
import com.splicemachine.hbase.KVPair;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.KeyValue;

/**
 * @author Scott Fines
 * Date: 11/15/13
 */
public class PairDecoder {
		private final KeyDecoder keyDecoder;
		private final KeyHashDecoder rowDecoder;
		private final ExecRow templateRow;

		public PairDecoder(KeyDecoder keyDecoder,
											 KeyHashDecoder rowDecoder,
											 ExecRow templateRow) {
				this.keyDecoder = keyDecoder;
				this.rowDecoder = rowDecoder;
				this.templateRow = templateRow;
		}

    public ExecRow decode(com.splicemachine.async.KeyValue data) throws StandardException{
        templateRow.resetRowArray();
        byte[] key = data.key();
        keyDecoder.decode(key,0,key.length,templateRow);
        byte[] row = data.value();
        rowDecoder.set(row,0,row.length);
        rowDecoder.decode(templateRow);
        return templateRow;
    }

		public ExecRow decode(KeyValue data) throws StandardException{
			try {
				templateRow.resetRowArray();
				keyDecoder.decode(data.getBuffer(),data.getRowOffset(),data.getRowLength(),templateRow);
				rowDecoder.set(data.getBuffer(),data.getValueOffset(),data.getValueLength());
				
				//System.out.println("templateRow " + templateRow);
				//int size = templateRow.nColumns();
				/*System.out.println("templateRow size " + size);
				for (int i=0;i<size;i++) {
					if (templateRow != null) {
					System.out.println(templateRow.getColumn(i+1).getTypeName());
//					System.out.println(templateRow.getColumn(i).getTraceString());
					} else {
						System.out.println("template: " + templateRow);						
					}
				}
				*/
				rowDecoder.decode(templateRow);
				return templateRow;
			} catch (StandardException se) {
				System.out.println("template Row " + templateRow);
				throw se;
			} catch (KryoException ke) {
				System.out.println("template Row " + templateRow);
				throw ke;
			}
		}

		public ExecRow decode(KVPair kvPair) throws StandardException{
				templateRow.resetRowArray();
				keyDecoder.decode(kvPair.getRow(),0,kvPair.getRow().length,templateRow);
				rowDecoder.set(kvPair.getValue(),0,kvPair.getValue().length);
				rowDecoder.decode(templateRow);
				return templateRow;
		}

		/*
		 *
		 *  < a | b |c >
		 *    1 | 2 | 3
		 *
		 *  sort (a) -->
		 *  Row Key: 1
		 *  Row Data: 2 | 3
		 *
		 *  group (a,b) ->
		 *  Row Key: a | b
		 *  Row Data: aggregate(c)
		 */

		public int getKeyPrefixOffset(){
				return keyDecoder.getPrefixOffset();
		}

		public ExecRow getTemplate() {
				return templateRow;
		}

		@Override
		public String toString() {
			return String.format("PairDecoder { keyDecoder=%s rowDecoder=%s, templateRow=%s}",keyDecoder,rowDecoder,templateRow);
		}		
}