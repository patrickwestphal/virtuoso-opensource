

typedef void *iext_txn_t;
typedef void *iext_cursor_t;
typedef void *iext_index_t;


typedef struct index_type_s
{
  caddr_t iext_name;
  int (*iext_open) (char *cfg, slice_id_t slid, iext_index_t * ret, caddr_t * err_ret);
  int (*iext_close) (iext_index_t inx, caddr_t * err_ret);
  int (*iext_checkpoint) (iext_index_t inx, caddr_t * err_ret);
  int (*iext_insert) (iext_index_t inx, iext_txn_t txn, int n_ins, int64 * ids, caddr_t * data, caddr_t * err_ret);
  int (*iext_delete) (iext_index_t inx, iext_txn_t txn, int n_del, int64 * ids, caddr_t * data, caddr_t * err_ret);
   iext_txn_t (*iext_start_txn) ();
  int (*iext_transact) (iext_txn_t * txn, int op, caddr_t * err_ret);
  void (*iext_cursor_free) (iext_cursor_t cr);
  int (*iext_exec) (iext_index_t inx, iext_txn_t txn, iext_cursor_t * cr_ret, struct client_connection_s * cli, char *query,
      void *params, caddr_t * err_ret);
  int (*iext_next) (iext_cursor_t cr, iext_txn_t * txn, int max, int *n_ret, int64 * ret, int64 * ret2, caddr_t * err_ret);
  int (*iext_is_match) (iext_index_t * iext, iext_txn_t * txn, char *query, void *params, int n_matches, int *n_match_ret,
      int64 * ids, int64 * ret, int64 * score_ret, caddr_t * err_ret);
  int (*iext_sample) (iext_index_t * iext, char *search_text, int64 * matches_est, float *usec_est);
  int (*iext_cost) (iext_index_t iext, float *usec_per_hit, float *usec_to_check);
} index_type_t;
