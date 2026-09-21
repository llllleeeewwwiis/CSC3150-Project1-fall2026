#include "sync_utils.h"
#include <pthread.h>
#include <stdio.h>
#include <string.h>

/*
 * Deterministic tests for the Part 3 monitor lock (phase-fair / batch policy).
 *
 * This file contains two groups of tests:
 *
 *  (A) Sequence tests (deterministic ordering):
 *      Reuses the same step-by-step pattern as Part 1 tests: threads are all
 *      created at once, but a "turn" variable enforces a specific sequence
 *      of lock acquisitions.
 *
 *  (B) Batch-fairness tests:
 *      Validates the new requirement:
 *        - writer-priority still holds (a writer should run first), AND
 *        - if readers are waiting, the number of consecutive writers admitted
 *          before a reader gets in is bounded by RWM_WRITER_BATCH (K).
 *
 * Note: pthread condition variables are Mesa style; the lock implementation
 * must use while-loops around waits. These tests are designed to fail if
 * the lock violates exclusivity or the batch bound.
 */

int usleep(unsigned int usec);

static rwlock_monitor_t test_board;
static int test_schedule = 0;
static int test_passed = 1;

typedef enum {
  ACTION_READ,
  ACTION_WRITE
} action_type_t;

typedef struct {
  action_type_t type;
  int thread_id;
  int action_index;
  int expected_value;
} action_t;

static int get_readers_count(rwlock_monitor_t *lock) {
  return lock->AR;
}

/* Writers should never overlap with readers (exclusive access). */
static void check_writer_exclusive(const char *label) {
  int actual_readers = get_readers_count(&test_board);
  if (actual_readers != 0) {
    LOG("FAIL [%s]: Writer should have exclusive access, but readers=%d", label, actual_readers);
    test_passed = 0;
  } else {
    LOG("PASS [%s]: Writer has exclusive access (readers=0)", label);
  }
}

static int current_action = 0;
static pthread_mutex_t seq_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t seq_cond = PTHREAD_COND_INITIALIZER;

/* Turn-based gate: makes N threads run in a chosen order (for deterministic testing). */
static void wait_for_my_turn(int my_index) {
  pthread_mutex_lock(&seq_mutex);
  while (current_action < my_index) {
    pthread_cond_wait(&seq_cond, &seq_mutex);
  }
  pthread_mutex_unlock(&seq_mutex);
}

static void signal_next_thread(void) {
  pthread_mutex_lock(&seq_mutex);
  current_action++;
  pthread_cond_broadcast(&seq_cond);
  pthread_mutex_unlock(&seq_mutex);
}

static void* thread_func(void* arg) {
  action_t *action = (action_t*)arg;
  char label[100];

  /* Enforce the deterministic ordering for this test run. */
  wait_for_my_turn(action->action_index);

  if (action->type == ACTION_READ) {
    snprintf(label, sizeof(label), "R%d reading", action->thread_id);
    LOG("Reader%d: Starting (step %d)", action->thread_id, action->action_index);
    rwm_rlock(&test_board);
    int value = test_schedule;
    LOG("Reader%d: Acquired read lock, schedule=%d", action->thread_id, value);

    /* Validate that readers observe the expected schedule value. */
    if (value != action->expected_value) {
      LOG("FAIL [R%d value]: Expected schedule=%d, got %d",
          action->thread_id, action->expected_value, value);
      test_passed = 0;
    } else {
      LOG("PASS [R%d value]: Read correct schedule value (%d)",
          action->thread_id, action->expected_value);
    }

    /* Hold the lock briefly so overlaps would be observable if incorrect. */
    usleep(20000);
    rwm_runlock(&test_board);
    LOG("Reader%d: Released read lock", action->thread_id);
  } else {
    snprintf(label, sizeof(label), "W%d writing", action->thread_id);
    LOG("Writer%d: Starting (step %d)", action->thread_id, action->action_index);
    rwm_wlock(&test_board);
    int old_value = test_schedule;
    test_schedule += action->expected_value;
    LOG("Writer%d: Acquired write lock, updating schedule %d -> %d",
        action->thread_id, old_value, test_schedule);

    /* Writer must be exclusive: no active readers during the write section. */
    check_writer_exclusive(label);

    /* Hold the lock briefly to amplify any exclusivity bugs. */
    usleep(20000);
    rwm_wunlock(&test_board);
    LOG("Writer%d: Released write lock, schedule=%d", action->thread_id, test_schedule);
  }

  /* Allow the next action in the deterministic sequence to proceed. */
  signal_next_thread();
  return NULL;
}

/* Run a deterministic sequence of actions and check final state. */
static int run_sequence(const char *name, action_t *actions, int num_actions, int expected_final) {
  LOG("\n=== Test: %s ===", name);

  /* Reset shared data for this test case. */
  test_schedule = 0;
  test_passed = 1;

  /* Reset deterministic sequencing state. */
  pthread_mutex_lock(&seq_mutex);
  current_action = 0;
  pthread_mutex_unlock(&seq_mutex);

  /* Initialize lock under test. */
  rwm_init(&test_board);
  pthread_t threads[num_actions];

  /* Create all threads; each will block on wait_for_my_turn(). */
  for (int action_index = 0; action_index < num_actions; action_index++) {
    pthread_create(&threads[action_index], NULL, thread_func, &actions[action_index]);
  }

  /* Wait for all actions to complete. */
  for (int action_index = 0; action_index < num_actions; action_index++) {
    pthread_join(threads[action_index], NULL);
  }

  rwm_destroy(&test_board);

  /* Final value check. */
  if (test_schedule != expected_final) {
    LOG("FAIL [Final value]: Expected final schedule=%d, got %d", expected_final, test_schedule);
    test_passed = 0;
  } else {
    LOG("PASS [Final value]: Final schedule value correct (%d)", expected_final);
  }
  if (test_passed) {
    LOG("=== %s: PASSED ===\n", name);
    return 0;
  } else {
    LOG("=== %s: FAILED ===\n", name);
    return 1;
  }
}

#ifndef RWM_WRITER_BATCH
#define RWM_WRITER_BATCH 2
#endif

#define MAX_BATCH_THREADS 32
static rwlock_monitor_t batch_board;
static char order[MAX_BATCH_THREADS];
static int order_count = 0;
static pthread_mutex_t order_mutex = PTHREAD_MUTEX_INITIALIZER;

/*
 * "Start barrier" for batch tests:
 * We want all competitor threads to begin contending at the same time, so we:
 *  - have each thread increment ready_count,
 *  - wait until all are ready,
 *  - broadcast start_cond to release them together.
 */
static pthread_mutex_t start_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t start_cond = PTHREAD_COND_INITIALIZER;
static pthread_cond_t ready_cond = PTHREAD_COND_INITIALIZER;
static int start_flag = 0;
static int ready_count = 0;
static int expected_ready = 0;

/*
 * Blocking writer:
 * We first acquire the write lock and hold it until the test harness says
 * "release". This ensures all other reader/writer threads are waiting at the
 * same time, so the batch-fairness logic is exercised deterministically.
 */
static pthread_mutex_t block_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t block_cond = PTHREAD_COND_INITIALIZER;
static pthread_cond_t block_ready = PTHREAD_COND_INITIALIZER;
static int block_release = 0;
static int block_acquired = 0;

static void wait_for_start(void) {
  pthread_mutex_lock(&start_mutex);
  ready_count++;
  pthread_cond_broadcast(&ready_cond);
  while (!start_flag) {
    pthread_cond_wait(&start_cond, &start_mutex);
  }
  pthread_mutex_unlock(&start_mutex);
}

/* Record the observed acquisition order (W or R). */
static void record_order(char type) {
  pthread_mutex_lock(&order_mutex);
  if (order_count < MAX_BATCH_THREADS) {
    order[order_count++] = type;
  }
  pthread_mutex_unlock(&order_mutex);
}

static void* blocking_writer(void* arg) {
  (void)arg;
  rwm_wlock(&batch_board);
  pthread_mutex_lock(&block_mutex);
  block_acquired = 1;
  pthread_cond_broadcast(&block_ready);
  while (!block_release) {
    pthread_cond_wait(&block_cond, &block_mutex);
  }
  pthread_mutex_unlock(&block_mutex);
  rwm_wunlock(&batch_board);
  return NULL;
}

/* Competing writer in the batch test. */
static void* batch_writer(void* arg) {
  (void)arg;
  wait_for_start();
  rwm_wlock(&batch_board);
  record_order('W');
  usleep(1000);
  rwm_wunlock(&batch_board);
  return NULL;
}

/* Competing reader in the batch test. */
static void* batch_reader(void* arg) {
  (void)arg;
  wait_for_start();
  rwm_rlock(&batch_board);
  record_order('R');
  usleep(1000);
  rwm_runlock(&batch_board);
  return NULL;
}

/*
 * Batch fairness test:
 *  - Start 1 blocking writer that holds the write lock.
 *  - Create N readers and M writers that all wait on the write lock.
 *  - Release the blocking writer and observe the order of acquisitions.
 *
 * Checks:
 *  - First acquisition must be a writer (writer priority).
 *  - Number of writers before the first reader must be <= K (batch bound).
 */
static int run_batch_fair_test(const char *name, int reader_count, int writer_count) {
  LOG("\n=== Test: %s ===", name);

  /* Reset shared bookkeeping for a clean run. */
  test_passed = 1;
  memset(order, 0, sizeof(order));
  pthread_mutex_lock(&order_mutex);
  order_count = 0;
  pthread_mutex_unlock(&order_mutex);
  pthread_mutex_lock(&start_mutex);
  start_flag = 0;
  ready_count = 0;
  expected_ready = writer_count + reader_count;
  pthread_mutex_unlock(&start_mutex);
  pthread_mutex_lock(&block_mutex);
  block_release = 0;
  block_acquired = 0;
  pthread_mutex_unlock(&block_mutex);

  /* Initialize the lock under test. */
  rwm_init(&batch_board);

  /* Start the blocking writer and wait until it holds the lock. */
  pthread_t block_thread;
  pthread_create(&block_thread, NULL, blocking_writer, NULL);

  pthread_mutex_lock(&block_mutex);
  while (!block_acquired) {
    pthread_cond_wait(&block_ready, &block_mutex);
  }
  pthread_mutex_unlock(&block_mutex);

  /* Create all competing readers and writers (they will wait at the start barrier). */
  pthread_t reader_threads[reader_count];
  for (int i = 0; i < reader_count; i++) {
    pthread_create(&reader_threads[i], NULL, batch_reader, NULL);
  }
  pthread_t writers[writer_count];
  for (int i = 0; i < writer_count; i++) {
    pthread_create(&writers[i], NULL, batch_writer, NULL);
  }

  /* Wait until every competitor reached the barrier, then release them together. */
  pthread_mutex_lock(&start_mutex);
  while (ready_count < expected_ready) {
    pthread_cond_wait(&ready_cond, &start_mutex);
  }
  start_flag = 1;
  pthread_cond_broadcast(&start_cond);
  pthread_mutex_unlock(&start_mutex);

  /* Give competitors time to block on the held lock so the queue is populated. */
  usleep(20000);

  /* Release the blocking writer so contention starts for real. */
  pthread_mutex_lock(&block_mutex);
  block_release = 1;
  pthread_cond_broadcast(&block_cond);
  pthread_mutex_unlock(&block_mutex);

  /* Join all threads (the test runner will enforce an overall timeout). */
  for (int i = 0; i < reader_count; i++) {
    pthread_join(reader_threads[i], NULL);
  }
  for (int i = 0; i < writer_count; i++) {
    pthread_join(writers[i], NULL);
  }
  pthread_join(block_thread, NULL);

  /* Analyze observed acquisition order. */
  if (order_count != writer_count + reader_count) {
    LOG("FAIL [%s]: expected %d acquisitions, got %d", name, writer_count + reader_count, order_count);
    test_passed = 0;
  }
  int first_reader = -1;
  int writers_before = 0;
  for (int i = 0; i < order_count; i++) {
    if (order[i] == 'R') {
      first_reader = i;
      break;
    }
    if (order[i] == 'W') writers_before++;
  }
  if (first_reader < 0) {
    LOG("FAIL [%s]: reader never acquired the lock", name);
    test_passed = 0;
  } else {
    if (order[0] != 'W') {
      LOG("FAIL [%s]: writer priority violated (first acquisition was reader)", name);
      test_passed = 0;
    }
    if (writers_before > RWM_WRITER_BATCH) {
      LOG("FAIL [%s]: exceeded batch limit (writers before reader = %d, K=%d)",
          name, writers_before, RWM_WRITER_BATCH);
      test_passed = 0;
    } else {
      LOG("PASS [%s]: writers before reader = %d (K=%d)", name, writers_before, RWM_WRITER_BATCH);
    }
  }

  rwm_destroy(&batch_board);

  if (test_passed) {
    LOG("=== %s: PASSED ===\n", name);
    return 0;
  } else {
    LOG("=== %s: FAILED ===\n", name);
    return 1;
  }
}

int main(void) {
  int total_passed = 0;
  int total_tests = 0;
  {
    action_t sequence[] = {
      {ACTION_READ,  1, 0, 0},
      {ACTION_WRITE, 1, 1, 1},
      {ACTION_READ,  2, 2, 1},
      {ACTION_WRITE, 2, 3, 1},
      {ACTION_READ,  3, 4, 2}
    };
    total_tests++;
    if (run_sequence("R1->W1->R2->W2->R3", sequence, 5, 2) == 0) total_passed++;
  }
  {
    action_t sequence[] = {
      {ACTION_READ,  1, 0, 0},
      {ACTION_READ,  2, 1, 0},
      {ACTION_READ,  3, 2, 0},
      {ACTION_WRITE, 1, 3, 1},
      {ACTION_WRITE, 2, 4, 1}
    };
    total_tests++;
    if (run_sequence("R1->R2->R3->W1->W2", sequence, 5, 2) == 0) total_passed++;
  }
  {
    action_t sequence[] = {
      {ACTION_READ,  1, 0, 0},
      {ACTION_WRITE, 1, 1, 1},
      {ACTION_READ,  2, 2, 1},
      {ACTION_WRITE, 2, 3, 1},
      {ACTION_READ,  3, 4, 2},
      {ACTION_WRITE, 3, 5, 1}
    };
    total_tests++;
    if (run_sequence("R1->W1->R2->W2->R3->W3", sequence, 6, 3) == 0) total_passed++;
  }
  {
    action_t sequence[] = {
      {ACTION_WRITE, 1, 0, 1},
      {ACTION_WRITE, 2, 1, 1},
      {ACTION_WRITE, 3, 2, 1},
      {ACTION_READ,  1, 3, 3},
      {ACTION_READ,  2, 4, 3}
    };
    total_tests++;
    if (run_sequence("W1->W2->W3->R1->R2", sequence, 5, 3) == 0) total_passed++;
  }
  {
    action_t sequence[] = {
      {ACTION_READ,  1, 0, 0},
      {ACTION_WRITE, 1, 1, 1},
      {ACTION_WRITE, 2, 2, 1},
      {ACTION_WRITE, 3, 3, 1},
      {ACTION_READ,  2, 4, 3}
    };
    total_tests++;
    if (run_sequence("R1->W1->W2->W3->R2", sequence, 5, 3) == 0) total_passed++;
  }
  total_tests++;
  if (run_batch_fair_test("BATCH-K2", 1, RWM_WRITER_BATCH + 1) == 0) total_passed++;
  total_tests++;
  if (run_batch_fair_test("BATCH-K2-2R", 2, RWM_WRITER_BATCH + 2) == 0) total_passed++;
  total_tests++;
  if (run_batch_fair_test("BATCH-K2-3R", 3, RWM_WRITER_BATCH + 2) == 0) total_passed++;
  LOG("\n======================================");
  LOG("SUMMARY: %d/%d tests passed", total_passed, total_tests);
  LOG("======================================\n");
  return (total_passed == total_tests) ? 0 : 1;
}
