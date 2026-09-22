#define _POSIX_C_SOURCE 200809L

#include "sync_utils.h"

#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

/*
 * Concurrent test for the Part 2 bb_count() requirement.
 *
 * The supervisor uses only the public bb_count() API.  It does not inspect
 * q->count, head, or tail, so the test does not introduce a second, racy
 * implementation of the queue's bookkeeping.
 */

int usleep(unsigned int usec);

#define BUFFER_CAPACITY 1

#define CHURN_PRODUCERS 2
#define CHURN_CONSUMERS 2
#define CHURN_ITEMS_PER_PRODUCER 40
#define CHURN_TOTAL_ITEMS (CHURN_PRODUCERS * CHURN_ITEMS_PER_PRODUCER)
#define CHURN_BASE_ID 10000

static bb_t test_queue;

/* Test-coordination state.  This mutex protects only the test state. */
static pthread_mutex_t state_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t state_cond = PTHREAD_COND_INITIALIZER;
static int test_failed = 0;

static int supervisor_started = 0;
static int supervisor_stop = 0;
static int supervisor_polls = 0;
static int supervisor_active_polls = 0;

static int blocked_take_started = 0;
static int blocked_take_done = 0;
static int blocked_put_started = 0;
static int blocked_put_done = 0;

static int workers_ready = 0;
static int workers_go = 0;
static int workers_active = 0; /* Number of churn workers after the go signal. */
static int supervisor_seen_active = 0;

static pthread_mutex_t result_mutex = PTHREAD_MUTEX_INITIALIZER;
static int seen_ids[CHURN_TOTAL_ITEMS];
static int churn_failed = 0;

static void note_failure(const char *message) {
  pthread_mutex_lock(&state_mutex);
  test_failed = 1;
  pthread_mutex_unlock(&state_mutex);
  LOG("FAIL: %s", message);
}

static void wait_for_flag(int *flag) {
  pthread_mutex_lock(&state_mutex);
  while (!*flag) {
    pthread_cond_wait(&state_cond, &state_mutex);
  }
  pthread_mutex_unlock(&state_mutex);
}

/* Check an exact value from the public API while the supervisor is also
 * polling bb_count().  The queue is quiescent at each call site below. */
static int check_count(int expected, const char *label) {
  int actual = bb_count(&test_queue);
  if (actual != expected) {
    LOG("FAIL: count at %s expected %d, got %d", label, expected, actual);
    note_failure("bb_count() returned an unexpected value");
    return 1;
  }
  return 0;
}

static void *supervisor_thread(void *arg) {
  (void)arg;

  pthread_mutex_lock(&state_mutex);
  supervisor_started = 1;
  pthread_cond_broadcast(&state_cond);

  for (;;) {
    pthread_mutex_unlock(&state_mutex);
    int count = bb_count(&test_queue);
    usleep(100);
    pthread_mutex_lock(&state_mutex);

    supervisor_polls++;
    if (workers_active > 0) {
      supervisor_active_polls++;
      supervisor_seen_active = 1;
      pthread_cond_broadcast(&state_cond);
    }
    if (count < 0 || count > test_queue.cap) {
      test_failed = 1;
      LOG("FAIL: supervisor observed invalid count=%d (capacity=%d)",
          count, test_queue.cap);
    }
    if (supervisor_stop) {
      pthread_mutex_unlock(&state_mutex);
      return NULL;
    }
  }
}

static void *blocked_consumer_thread(void *arg) {
  (void)arg;

  pthread_mutex_lock(&state_mutex);
  blocked_take_started = 1;
  pthread_cond_broadcast(&state_cond);
  pthread_mutex_unlock(&state_mutex);

  food_tray_t *tray = bb_take(&test_queue);

  pthread_mutex_lock(&state_mutex);
  blocked_take_done = 1;
  if (!tray || tray->tray_id != 1) {
    test_failed = 1;
    LOG("FAIL: blocked consumer expected tray 1");
  }
  pthread_cond_broadcast(&state_cond);
  pthread_mutex_unlock(&state_mutex);
  free_food_tray(tray);
  return NULL;
}

static void *blocked_producer_thread(void *arg) {
  (void)arg;

  pthread_mutex_lock(&state_mutex);
  blocked_put_started = 1;
  pthread_cond_broadcast(&state_cond);
  pthread_mutex_unlock(&state_mutex);

  food_tray_t *tray = create_food_tray(2, "Full-queue tray", 2);
  bb_put(&test_queue, tray);

  pthread_mutex_lock(&state_mutex);
  blocked_put_done = 1;
  pthread_cond_broadcast(&state_cond);
  pthread_mutex_unlock(&state_mutex);
  return NULL;
}

static void wait_for_workers_start(void) {
  pthread_mutex_lock(&state_mutex);
  workers_ready++;
  pthread_cond_broadcast(&state_cond);
  while (!workers_go) {
    pthread_cond_wait(&state_cond, &state_mutex);
  }
  workers_active++;
  pthread_cond_broadcast(&state_cond);
  /* Do not start the churn until the supervisor has actually observed at
   * least one active worker.  This makes the concurrency portion a required
   * event instead of a timing-dependent hope. */
  while (!supervisor_seen_active) {
    pthread_cond_wait(&state_cond, &state_mutex);
  }
  pthread_mutex_unlock(&state_mutex);
}

static void *churn_producer_thread(void *arg) {
  int producer_id = *(int *)arg;
  wait_for_workers_start();

  for (int i = 0; i < CHURN_ITEMS_PER_PRODUCER; i++) {
    int tray_id = CHURN_BASE_ID + producer_id * CHURN_ITEMS_PER_PRODUCER + i;
    food_tray_t *tray = create_food_tray(tray_id, "Concurrent tray", producer_id);
    bb_put(&test_queue, tray);
    usleep(500);
  }

  pthread_mutex_lock(&state_mutex);
  workers_active--;
  pthread_cond_broadcast(&state_cond);
  pthread_mutex_unlock(&state_mutex);
  return NULL;
}

static void *churn_consumer_thread(void *arg) {
  (void)arg;
  wait_for_workers_start();

  for (int i = 0; i < CHURN_TOTAL_ITEMS / CHURN_CONSUMERS; i++) {
    food_tray_t *tray = bb_take(&test_queue);
    if (!tray) {
      pthread_mutex_lock(&result_mutex);
      churn_failed = 1;
      pthread_mutex_unlock(&result_mutex);
      continue;
    }

    int index = tray->tray_id - CHURN_BASE_ID;
    pthread_mutex_lock(&result_mutex);
    if (index < 0 || index >= CHURN_TOTAL_ITEMS || seen_ids[index] != 0) {
      churn_failed = 1;
    } else {
      seen_ids[index] = 1;
    }
    pthread_mutex_unlock(&result_mutex);
    free_food_tray(tray);
    usleep(700);
  }

  pthread_mutex_lock(&state_mutex);
  workers_active--;
  pthread_cond_broadcast(&state_cond);
  pthread_mutex_unlock(&state_mutex);
  return NULL;
}

static int run_blocking_checks(void) {
  pthread_t consumer;
  pthread_t producer;
  int failed = 0;

  failed |= check_count(0, "initial empty queue");

  blocked_take_started = 0;
  blocked_take_done = 0;
  pthread_create(&consumer, NULL, blocked_consumer_thread, NULL);
  wait_for_flag(&blocked_take_started);

  pthread_mutex_lock(&state_mutex);
  int consumer_finished_early = blocked_take_done;
  pthread_mutex_unlock(&state_mutex);
  if (consumer_finished_early) {
    note_failure("consumer returned while the queue was empty");
    failed = 1;
  }
  failed |= check_count(0, "consumer blocked on empty queue");

  food_tray_t *first = create_food_tray(1, "Empty-queue tray", 1);
  bb_put(&test_queue, first);
  pthread_join(consumer, NULL);
  failed |= check_count(0, "after blocked consumer is released");

  food_tray_t *full_item = create_food_tray(1, "Full-queue item", 1);
  bb_put(&test_queue, full_item);
  failed |= check_count(1, "full queue before blocked producer");

  blocked_put_started = 0;
  blocked_put_done = 0;
  pthread_create(&producer, NULL, blocked_producer_thread, NULL);
  wait_for_flag(&blocked_put_started);

  pthread_mutex_lock(&state_mutex);
  int producer_finished_early = blocked_put_done;
  pthread_mutex_unlock(&state_mutex);
  if (producer_finished_early) {
    note_failure("producer returned while the queue was full");
    failed = 1;
  }
  failed |= check_count(1, "producer blocked on full queue");

  food_tray_t *taken = bb_take(&test_queue);
  if (!taken || taken->tray_id != 1) {
    note_failure("FIFO order was broken while releasing blocked producer");
    failed = 1;
  }
  free_food_tray(taken);
  pthread_join(producer, NULL);

  failed |= check_count(1, "blocked producer inserted its tray");
  taken = bb_take(&test_queue);
  if (!taken || taken->tray_id != 2) {
    note_failure("blocked producer's tray was not delivered");
    failed = 1;
  }
  free_food_tray(taken);
  failed |= check_count(0, "after full-queue check");
  return failed;
}

static int run_churn_checks(void) {
  pthread_t producers[CHURN_PRODUCERS];
  pthread_t consumers[CHURN_CONSUMERS];
  int producer_ids[CHURN_PRODUCERS];

  memset(seen_ids, 0, sizeof(seen_ids));
  pthread_mutex_lock(&state_mutex);
  workers_ready = 0;
  workers_go = 0;
  workers_active = 0;
  supervisor_seen_active = 0;
  pthread_mutex_unlock(&state_mutex);

  for (int i = 0; i < CHURN_CONSUMERS; i++) {
    pthread_create(&consumers[i], NULL, churn_consumer_thread, NULL);
  }
  for (int i = 0; i < CHURN_PRODUCERS; i++) {
    producer_ids[i] = i;
    pthread_create(&producers[i], NULL, churn_producer_thread, &producer_ids[i]);
  }

  pthread_mutex_lock(&state_mutex);
  while (workers_ready < CHURN_PRODUCERS + CHURN_CONSUMERS) {
    pthread_cond_wait(&state_cond, &state_mutex);
  }
  workers_go = 1;
  pthread_cond_broadcast(&state_cond);
  pthread_mutex_unlock(&state_mutex);

  for (int i = 0; i < CHURN_CONSUMERS; i++) {
    pthread_join(consumers[i], NULL);
  }
  for (int i = 0; i < CHURN_PRODUCERS; i++) {
    pthread_join(producers[i], NULL);
  }

  pthread_mutex_lock(&result_mutex);
  if (churn_failed) {
    note_failure("concurrent producer/consumer integrity check failed");
  }
  for (int i = 0; i < CHURN_TOTAL_ITEMS; i++) {
    if (seen_ids[i] != 1) {
      churn_failed = 1;
      break;
    }
  }
  pthread_mutex_unlock(&result_mutex);
  if (churn_failed) {
    note_failure("a produced tray was lost or consumed more than once");
  }

  return check_count(0, "after concurrent churn") != 0;
}

int main(void) {
  LOG("=== Test: Concurrent bb_count() supervisor ===");

  if (bb_init(&test_queue, BUFFER_CAPACITY) != 0) {
    LOG("FAIL: bb_init failed");
    return 1;
  }

  pthread_t supervisor;
  pthread_create(&supervisor, NULL, supervisor_thread, NULL);
  wait_for_flag(&supervisor_started);
  LOG("Supervisor thread started");

  int failed = 0;
  failed |= run_blocking_checks();
  failed |= run_churn_checks();

  pthread_mutex_lock(&state_mutex);
  supervisor_stop = 1;
  pthread_cond_broadcast(&state_cond);
  pthread_mutex_unlock(&state_mutex);
  pthread_join(supervisor, NULL);

  pthread_mutex_lock(&state_mutex);
  int polls = supervisor_polls;
  int active_polls = supervisor_active_polls;
  int state_failed = test_failed;
  pthread_mutex_unlock(&state_mutex);

  if (polls == 0 || active_polls == 0) {
    LOG("FAIL: supervisor did not perform concurrent polling (polls=%d, active=%d)",
        polls, active_polls);
    failed = 1;
  }
  if (state_failed) failed = 1;

  bb_destroy(&test_queue);

  if (failed) {
    LOG("=== CONCURRENT bb_count TEST: FAILED ===");
    return 1;
  }

  LOG("PASS: supervisor performed %d bb_count() calls (%d during worker activity)",
      polls, active_polls);
  LOG("=== CONCURRENT bb_count TEST: PASSED ===");
  return 0;
}
