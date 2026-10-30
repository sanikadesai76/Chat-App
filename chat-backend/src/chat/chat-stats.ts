export interface ChatStats {
  // Legacy BullMQ-path counters (unused by the group-commit writer).
  jobs: number;
  slowQueueWaits: number;
  maxQueueWaitMs: number;
  maxAddMs: number;
  maxWaitMs: number;
  // Group-commit writer counters.
  inserts: number;
  batches: number;
  maxBatchSize: number;
  slowInserts: number;
  maxInsertMs: number;
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
    maxBatchSize: 0,
    slowInserts: 0,
    maxInsertMs: 0,
    maxSaveMs: 0,
  };
}

export const chatStats: ChatStats = empty();

export function snapshotAndResetChatStats(): ChatStats {
  const snap = { ...chatStats };
  Object.assign(chatStats, empty());
  return snap;
}