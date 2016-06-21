package com.splicemachine.metrics;

/**
 * @author Scott Fines
 *         Date: 1/24/14
 */
public class MultiStatsView implements IOStats {
		private final MultiTimeView timeView;
		private long totalRows;
		private long totalBytes;

		public MultiStatsView(MultiTimeView timeView) {
				this.timeView = timeView;
		}

		@Override public TimeView getTime() { return timeView; }
		@Override public long elementsSeen() { return totalRows; }
		@Override public long bytesSeen() { return totalBytes; }

		public void update(TimeView time, long totalRows,long totalBytes){
				this.timeView.update(time);
				this.totalRows+= totalRows;
				this.totalBytes+=totalBytes;
		}

		public void merge(IOStats other){
				this.timeView.update(other.getTime());
				this.totalBytes+=other.bytesSeen();
				this.totalRows+=other.elementsSeen();
		}
}