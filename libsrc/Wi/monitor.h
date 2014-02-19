#ifndef _MONITOR_H
#define _MONITOR_H

#include <stdio.h>

typedef struct monitor_s
{
  uint64 mon_time_now;		/* sample time, msecs */
  uint64 mon_time_elapsed;	/* elapsed time, msecs */
  long mon_cpu_time;		/* rusage cpu time */
  double mon_cpu_pct;		/* cpu % */
  char mon_high_cpu;		/* high cpu flag */
  long mon_pageflts;
  long mon_disk_reads;
  int mon_thr_run;
  int mon_thr;
  int mon_lw_thr;
  long mon_read_block_usec;
  long mon_write_block_usec;
  long mon_read_cum_time;
  double mon_read_pct;
  long mon_tc_no_thread_kill_idle;
  long mon_tc_no_thread_kill_vdb;
  long mon_tc_no_thread_kill_running;
  long mon_tws_accept_queued;
  long mon_tc_read_wait;
  long mon_tc_write_wait;
  long mon_tc_cl_keep_alive_timeouts;
  long mon_tc_cl_deadlocks;
  long mon_lock_deadlocks;
  long mon_lock_2r1w_deadlocks;
  long mon_lock_waits;
  long mon_lock_wait_msec;
  long mon_tc_no_mem_for_longer_batch;
  size_t mon_mp_large_in_use;
  int64 mon_mp_mmap_clocks;
  long mon_tc_part_hash_join;
  long mon_tc_slow_temp_insert;
  long mon_tc_slow_temp_lookup;
} monitor_t;


#ifndef WIN32
#include <sys/time.h>
#include <sys/resource.h>
#endif
void mon_init ();
int mon_get_next (int n_threads, int n_vdb_threads, int n_lw_threads, const monitor_t * previous, monitor_t * next);
void mon_update (int n_threads, int n_vdb_threads, int n_lw_threads);
void mon_check ();

#endif
