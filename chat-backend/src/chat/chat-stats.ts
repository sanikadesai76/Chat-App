
export interface ChatStats {
  // Legacy BullMQ metrics.
  jobs: number;
  slowQueueWaits: number;
  maxQueueWaitMs: number;
  maxAddMs: number;
  maxWaitMs: number;

  // Active group-commit writer metrics.
  inserts: number;
  batches: number;
  dbQueries: number;
  maxBatchSize: number;
  slowInserts: number;

  totalQueueWaitMs: number;
  maxQueueWaitMsActive: number;

  totalDbInsertMs: number;
  maxInsertMs: number;

  totalSaveMs: number;
  maxSaveMs: number;
}

function empty(): ChatStats {
  return {
    jobs: 0,
    slowQueueWaits: 0,
    maxQueueWaitMs: 0,
    maxAddMs: 0,
    maxWaitMs: 0,

    inserts: 0,
    batches: 0,
    dbQueries: 0,
    maxBatchSize: 0,
    slowInserts: 0,

    totalQueueWaitMs: 0,
    maxQueueWaitMsActive: 0,

    totalDbInsertMs: 0,
    maxInsertMs: 0,

    totalSaveMs: 0,
    maxSaveMs: 0,
  };
}

export const chatStats: ChatStats = empty();

export function snapshotAndResetChatStats(): ChatStats {
  const snap = { ...chatStats };
  Object.assign(chatStats, empty());
  return snap;
}
