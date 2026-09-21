#include "sync_utils.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

/*
 * Stress test for the Part 3 monitor lock (phase-fair / batch policy).
 *
 * Focus: safety under contention (not a deterministic fairness schedule).
 *  - No deadlock: all threads should finish within the runner timeout.
 *  - Readers should never observe the shared counter changing while holding
 *    a read lock (writers must be excluded).
 *  - Writers increments should not be lost.
 *
 * This test intentionally introduces many different interleavings via random
 * sleeps to increase the chance of exposing race conditions.
 */

int usleep(unsigned int usec);

static rwlock_monitor_t test_board;
static int shared_counter = 0;
static int test_failed = 0;
static int total_reads = 0;
static int total_writes = 0;

/* Kept for symmetry with other stress tests; we mainly detect corruption via value checks. */
static int race_condition_detected = 0;
static int data_corruption_detected = 0;
static pthread_mutex_t violation_mutex = PTHREAD_MUTEX_INITIALIZER;

#define NUM_READERS 100
#define NUM_WRITERS 40
#define ITERATIONS_PER_THREAD 10
#define MAX_SLEEP_US 5000

/* Reader thread:
 * - Take a read lock
 * - Read shared_counter twice (with a small delay)
 * - Values must match if the lock is correct
 */
static void* reader_thread(void* arg) {
  int reader_id = *(int*)arg;
  for (int iteration = 0; iteration < ITERATIONS_PER_THREAD; iteration++) {
    usleep(rand() % MAX_SLEEP_US);
    rwm_rlock(&test_board);
    int value_one = shared_counter;
    usleep(rand() % 100);
    int value_two = shared_counter;
    if (value_one != value_two) {
      pthread_mutex_lock(&violation_mutex);
      LOG("DATA CORRUPTION: Reader%d saw counter change %d -> %d during read",
          reader_id, value_one, value_two);
      data_corruption_detected = 1;
      test_failed = 1;
      pthread_mutex_unlock(&violation_mutex);
    }
    usleep(rand() % 1000);
    rwm_runlock(&test_board);
    pthread_mutex_lock(&violation_mutex);
    total_reads++;
    pthread_mutex_unlock(&violation_mutex);
  }
  return NULL;
}

/* Writer thread:
 * - Take a write lock
 * - Increment the shared counter
 * - Validate the increment is visible
 */
static void* writer_thread(void* arg) {
  int writer_id = *(int*)arg;
  for (int iteration = 0; iteration < ITERATIONS_PER_THREAD; iteration++) {
    usleep(rand() % MAX_SLEEP_US);
    rwm_wlock(&test_board);
    int old_value = shared_counter;
    usleep(rand() % 100);
    shared_counter = old_value + 1;
    if (shared_counter != old_value + 1) {
      pthread_mutex_lock(&violation_mutex);
      LOG("DATA CORRUPTION: Writer%d increment failed: %d -> %d (expected %d)",
          writer_id, old_value, shared_counter, old_value + 1);
      data_corruption_detected = 1;
      test_failed = 1;
      pthread_mutex_unlock(&violation_mutex);
    }
    usleep(rand() % 1000);
    rwm_wunlock(&test_board);
    pthread_mutex_lock(&violation_mutex);
    total_writes++;
    pthread_mutex_unlock(&violation_mutex);
  }
  return NULL;
}

int main(void) {
  LOG("=== Monitor Reader-Writer Stress Test ===");
  LOG("Configuration:");
  LOG("  Readers: %d (each performs %d reads)", NUM_READERS, ITERATIONS_PER_THREAD);
  LOG("  Writers: %d (each performs %d writes)", NUM_WRITERS, ITERATIONS_PER_THREAD);
  LOG("  Expected final counter value: %d", NUM_WRITERS * ITERATIONS_PER_THREAD);
  LOG("");
  /* Seed random delays to create lots of different schedules. */
  srand(time(NULL));

  /* Initialize the lock and shared state. */
  rwm_init(&test_board);
  shared_counter = 0;
  test_failed = 0;
  race_condition_detected = 0;
  data_corruption_detected = 0;
  total_reads = 0;
  total_writes = 0;
  pthread_t readers[NUM_READERS];
  pthread_t writers[NUM_WRITERS];
  int reader_ids[NUM_READERS];
  int writer_ids[NUM_WRITERS];
  LOG("Starting threads...");
  for (int reader_index = 0; reader_index < NUM_READERS; reader_index++) {
    reader_ids[reader_index] = reader_index + 1;
    pthread_create(&readers[reader_index], NULL, reader_thread, &reader_ids[reader_index]);
  }
  for (int writer_index = 0; writer_index < NUM_WRITERS; writer_index++) {
    writer_ids[writer_index] = writer_index + 1;
    pthread_create(&writers[writer_index], NULL, writer_thread, &writer_ids[writer_index]);
  }
  LOG("All threads created. Waiting for completion...");
  for (int reader_index = 0; reader_index < NUM_READERS; reader_index++) {
    pthread_join(readers[reader_index], NULL);
  }
  for (int writer_index = 0; writer_index < NUM_WRITERS; writer_index++) {
    pthread_join(writers[writer_index], NULL);
  }
  LOG("All threads completed.");
  LOG("");
  int expected_final = NUM_WRITERS * ITERATIONS_PER_THREAD;
  LOG("=== Results ===");
  LOG("Total reads completed: %d (expected %d)", total_reads, NUM_READERS * ITERATIONS_PER_THREAD);
  LOG("Total writes completed: %d (expected %d)", total_writes, expected_final);
  LOG("Final counter value: %d (expected %d)", shared_counter, expected_final);
  LOG("");
  if (race_condition_detected) {
    LOG("FAIL: Race conditions detected!");
  } else {
    LOG("PASS: No race conditions detected");
  }
  if (data_corruption_detected) {
    LOG("FAIL: Data corruption detected!");
  } else {
    LOG("PASS: No data corruption detected");
  }
  if (shared_counter != expected_final) {
    LOG("FAIL: Final counter value incorrect (%d != %d)", shared_counter, expected_final);
    test_failed = 1;
  } else {
    LOG("PASS: Final counter value correct (%d)", shared_counter);
  }
  if (total_reads != NUM_READERS * ITERATIONS_PER_THREAD) {
    LOG("FAIL: Not all reads completed");
    test_failed = 1;
  } else {
    LOG("PASS: All reads completed");
  }
  if (total_writes != expected_final) {
    LOG("FAIL: Not all writes completed");
    test_failed = 1;
  } else {
    LOG("PASS: All writes completed");
  }
  rwm_destroy(&test_board);
  LOG("");
  if (test_failed) {
    LOG("=== STRESS TEST: FAILED ===");
    return 1;
  } else {
    LOG("=== STRESS TEST: PASSED ===");
    return 0;
  }
}
