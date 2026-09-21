#ifndef SYNC_UTILS_H
#define SYNC_UTILS_H

#include <pthread.h>
#include <semaphore.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#define DIE(msg) do { perror(msg); exit(EXIT_FAILURE); } while (0)

/* Logging (timestamped) */
uint64_t now_ms(void);
#define LOG(fmt, ...) \
  do { fprintf(stdout, "[%10llu ms] " fmt "\n", (unsigned long long)now_ms(), ##__VA_ARGS__); fflush(stdout); } while (0)

/* Thread helpers */
typedef void* (*thread_fn)(void*);
pthread_t spawn(thread_fn fn, void *arg, const char *name);
void join(pthread_t t);

/* Random jitter for schedule perturbation */
void jitter_us(int min_us, int max_us);

/* ---------- Reader-Writer Lock for Conference Schedule ---------- */

/* Reader-Writer lock (students implement in readers_writers.c) */
typedef struct {
  /* TODO: add semaphores/mutexes and counters */
  // You need to add the required fields here to implement reader-priority reader-writer lock
  pthread_mutex_t m;
  sem_t wlock;
  int readers;
  int readers_waiting;
  int writers_waiting;
  bool writer_active;
  pthread_cond_t ok_to_read;
} rwlock_t;

int  rw_init(rwlock_t *rw);
void rw_destroy(rwlock_t *rw);
void rw_rlock(rwlock_t *rw);
void rw_runlock(rwlock_t *rw);
void rw_wlock(rwlock_t *rw);
void rw_wunlock(rwlock_t *rw);

/* Monitor-Style Reader-Writer lock (students implement in rwlock_monitor.c) */
#define RWM_WRITER_BATCH 2
#define RWM_PHASE_WRITER 0
#define RWM_PHASE_READER 1

typedef struct {
  /* TODO: add mutex, condition variables, counters, and batch/phase state */
  pthread_mutex_t m;          /* 管程互斥锁 */
  pthread_cond_t ok_to_read;  /* 读者等待条件变量 */
  pthread_cond_t ok_to_write; /* 写者等待条件变量 */
  int AR;                     /* Active Readers: 正在临界区内的活跃读者数 */
  int WR;                     /* Waiting Readers: 正在排队的等待读者数 */
  int AW;                     /* Active Writers: 正在写入的活跃写者数 (0 或 1) */
  int WW;                     /* Waiting Writers: 正在排队的等待写者数 */
  int writer_batch_count;     /* 当前批次中已连续放行的写者计数器 */
  int phase;                  /* 当前阶段: RWM_PHASE_WRITER 或 RWM_PHASE_READER */
} rwlock_monitor_t;

int  rwm_init(rwlock_monitor_t *rw);
void rwm_destroy(rwlock_monitor_t *rw);
void rwm_rlock(rwlock_monitor_t *rw);
void rwm_runlock(rwlock_monitor_t *rw);
void rwm_wlock(rwlock_monitor_t *rw);
void rwm_wunlock(rwlock_monitor_t *rw);

/* ---------- Bounded Buffer for Producer-Consumer (Snacks) ---------- */

/* Food tray structure */
typedef struct {
  int tray_id;           // Unique tray identifier
  char *food_name;       // Name of food on tray (dynamically allocated)
  int prepared_by;       // Cook who prepared it
} food_tray_t;

/* Bounded buffer (students implement) */
typedef struct {
  /* TODO: add buffer array, semaphores, mutex, and indices */
  // You need to add the required fields here to implement a bounded buffer
  food_tray_t **buf;
  int cap;
  int head;
  int tail;
  int count;             // Current number of items in the buffer
  sem_t empty;
  sem_t full;
  pthread_mutex_t m;
} bb_t;

int  bb_init(bb_t *q, int capacity);
void bb_destroy(bb_t *q);
void bb_put(bb_t *q, food_tray_t *tray);   /* blocks if full */
food_tray_t* bb_take(bb_t *q);             /* blocks if empty, returns tray to consume */
int  bb_count(bb_t *q);                    /* returns current number of items safely */

/* Helper functions for food trays */
food_tray_t* create_food_tray(int tray_id, const char *food_name, int cook_id);
void free_food_tray(food_tray_t *tray);

#endif
