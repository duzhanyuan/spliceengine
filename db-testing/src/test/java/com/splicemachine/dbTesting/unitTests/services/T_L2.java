/*
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
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
 *
 */

package com.splicemachine.dbTesting.unitTests.services;

import com.splicemachine.db.iapi.services.sanity.SanityManager;
import java.util.Hashtable;
import com.splicemachine.db.iapi.services.locks.*;

/**
	A semaphore that implements Lockable for unit testing.
*/
class T_L2 implements Lockable {

	private int allowed;
	private Object[]	lockers;
	private int[]		counts;

	T_L2(int allowed) {
		this.allowed = allowed;
		lockers = new Object[allowed];
		counts = new int[allowed];
	}

	/*
	** Lockable methods (Simple, qualifier assumed to be null), allows
	** up to 'allowed' lockers in at the same time.
	*/

	public void lockEvent(Latch lockInfo) {

		int empty = -1;
		for (int i = 0; i < allowed; i++) {
			if (lockers[i] == lockInfo.getCompatabilitySpace()) {
				counts[i]++;
				return;
			}

			if (lockers[i] == null)
				empty = i;
		}

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(empty != -1);
		lockers[empty] = lockInfo.getCompatabilitySpace();
		counts[empty] = 1;

	}

	public boolean requestCompatible(Object requestedQualifier, Object grantedQualifier) {
		return false;
	}

	public boolean lockerAlwaysCompatible() {
		return true;
	}

	public void unlockEvent(Latch lockInfo) {

		for (int i = 0; i < allowed; i++) {

			if (lockers[i] == lockInfo.getCompatabilitySpace()) {
				counts[i]--;
                if (SanityManager.DEBUG)
                    SanityManager.ASSERT(counts[i] >= 0);
				if (counts[i] == 0) {
					lockers[i] = null;
					return;
				}

				return;
			}
		}

        if (SanityManager.DEBUG)
            SanityManager.THROWASSERT("unlocked by a compatability space that does not exist");
	}

	public boolean lockAttributes(int flag, Hashtable t)
	{
		return false;
	}
	
}
