#include "sync_utils.h"

int rwm_init(rwlock_monitor_t *rw) {
  /* TODO: initialize all fields
   * - mutex + condition variables
   * - counters AR/WR/AW/WW
   * - writer_batch_count
   * - phase (RWM_PHASE_WRITER or RWM_PHASE_READER)
   */
  if (!rw) return -1;
  pthread_mutex_init(&rw->m, NULL);
  pthread_cond_init(&rw->ok_to_read, NULL);
  pthread_cond_init(&rw->ok_to_write, NULL);
  rw->AR = 0;
  rw->WR = 0;
  rw->AW = 0;
  rw->WW = 0;
  rw->writer_batch_count = 0;
  rw->phase = RWM_PHASE_WRITER; /* 初始处于写者优先阶段 */
  return 0;
}

void rwm_destroy(rwlock_monitor_t *rw) {
  // TODO: destroy mutex + condition variables
  if (!rw) return;
  pthread_mutex_destroy(&rw->m);
  pthread_cond_destroy(&rw->ok_to_read);
  pthread_cond_destroy(&rw->ok_to_write);
}

void rwm_rlock(rwlock_monitor_t *rw) {
  /* TODO: reader lock (writer-priority + batch fairness)
   * - wait while writer active OR (writers waiting AND phase is writer)
   * - update WR/AR
   * - use while around pthread_cond_wait (Mesa semantics)
   */
  pthread_mutex_lock(&rw->m);

  rw->WR++;

  while (rw->AW > 0 || (rw->WW > 0 && rw->phase == RWM_PHASE_WRITER)) {
    pthread_cond_wait(&rw->ok_to_read, &rw->m);
  }

  rw->WR--;
  rw->AR++;

  pthread_mutex_unlock(&rw->m);
}

void rwm_runlock(rwlock_monitor_t *rw) {
  /* TODO: reader unlock
   * - update AR
   * - if last reader, decide who to wake (writer vs reader batch)
   */
  pthread_mutex_lock(&rw->m);

  rw->AR--;

  if (rw->AR == 0) {
    if (rw->WW >0) {
      rw->phase = RWM_PHASE_WRITER;
      rw->writer_batch_count = 0;
      pthread_cond_signal(&rw->ok_to_write);
    } else if (rw->WR > 0) {
      rw->phase = RWM_PHASE_READER;
      pthread_cond_broadcast(&rw->ok_to_read);
    }
  }

  pthread_mutex_unlock(&rw->m);
}

void rwm_wlock(rwlock_monitor_t *rw) {
  /* TODO: writer lock (writer-priority + batch fairness)
   * - wait while readers/writers active OR reader-phase with waiting readers
   * - update WW/AW
   * - increment writer_batch_count when readers are waiting
   */
  pthread_mutex_lock(&rw->m);
  
  rw->WW++; /* 登记：增加一个等待写者 */
  
  /* 写者等待的充要条件：
   * 1. 当前有活跃写者 (AW > 0)；
   * 2. 当前有活跃读者 (AR > 0)；
   * 3. 当前处于读者优先阶段且有读者在排队 (phase == RWM_PHASE_READER && WR > 0)
   */
  while (rw->AW > 0 || rw->AR > 0 || (rw->phase == RWM_PHASE_READER && rw->WR > 0)) {
    pthread_cond_wait(&rw->ok_to_write, &rw->m);
  }
  
  rw->WW--; /* 离开等待队列 */
  rw->AW++; /* 成为活跃写者 */

  if (rw->WR > 0) {
    rw->writer_batch_count++;
  } else {
    rw->writer_batch_count = 0;
  }

  pthread_mutex_unlock(&rw->m);
}

void rwm_wunlock(rwlock_monitor_t *rw) {
  /* TODO: writer unlock
   * - update AW
   * - if WR>0 and writer_batch_count reached RWM_WRITER_BATCH, switch to reader phase
   * - otherwise continue writers or release readers as needed
   */
  pthread_mutex_lock(&rw->m);

  rw->AW--;

  if (rw->WR > 0 && rw->writer_batch_count >= RWM_WRITER_BATCH) {
    rw->phase = RWM_PHASE_READER;
    rw->writer_batch_count = 0;
    pthread_cond_broadcast(&rw->ok_to_read);
  } else if (rw->WW > 0) {
    rw->phase = RWM_PHASE_WRITER;
    pthread_cond_signal(&rw->ok_to_write);
  } else if (rw->WR > 0) {
    rw->phase = RWM_PHASE_READER;
    rw->writer_batch_count = 0;
    pthread_cond_broadcast(&rw->ok_to_read);
  }

  pthread_mutex_unlock(&rw->m);
}
