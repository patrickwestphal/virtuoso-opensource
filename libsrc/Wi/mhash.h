/*
 *  mhash.h
 *
 *  $Id$
 *
 *  This file is part of the OpenLink Software Virtuoso Open-Source (VOS)
 *  project.
 *
 *  Copyright (C) 1998-2014 OpenLink Software
 *
 *  This project is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the
 *  Free Software Foundation; only version 2 of the License, dated June 1991.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *  General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with this program; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 *
 */

#ifndef MHASH_M
#define MHASH_M  ((uint64) 0xc6a4a7935bd1e995)
#define MHASH_R 47
#endif


#ifdef SSE42
#define MHASH_STEP_1(h, data) \
do { \
  uint64 __k = data; \
  __k = __builtin_ia32_crc32di (1, __k); \
      __k *= MHASH_M;  \
      h = __k; \
 } while (0)

#else
#define MHASH_STEP_1(h, data) \
do { \
  uint64 __k = data; \
      __k *= MHASH_M;  \
      h = __k; \
 } while (0)
#endif

#ifndef MHASH_STEP
#define MHASH_STEP(h, data) \
{ \
  uint64 __k = data; \
      __k *= MHASH_M;  \
      __k ^= __k >> MHASH_R;  \
      __k *= MHASH_M;  \
      h ^= __k; \
      h *= MHASH_M; \
    }


#define MHASH_ID_STEP(h, data) \
{ \
  uint64 __k = data; \
  uint32 __k1 = __k; \
      __k *= MHASH_M;  \
      __k ^= __k >> MHASH_R;  \
      __k *= MHASH_M;  \
      h ^= __k; \
      h *= MHASH_M; \
      h = (h & 0xffffffff00000000) | __k1;	\
    }




#ifdef VALGRIND
#define MHASH_VAR(init, ptr, len) BYTE_BUFFER_HASH (init, ptr, len)
#else
#define MHASH_VAR(init, ptr, len)		\
{ \
    uint64 __h = init; \
  uint64 * data = (uint64*)ptr; \
  uint64 * end = (uint64*)(((ptrlong)data) + (len & ~7));	\
  while (data < end) \
    { \
      uint64 k  = *(data++); \
      k *= MHASH_M;  \
      k ^= k >> MHASH_R;  \
      k *= MHASH_M;  \
      __h ^= k; \
      __h *= MHASH_M; \
    } \
  if (len & 7) \
    { \
      uint64 k = *data; \
      k &= ((int64)1 << ((len & 7) << 3)) - 1;	\
      k *= MHASH_M;  \
      k ^= k >> MHASH_R;  \
      k *= MHASH_M;  \
      __h ^= k; \
      __h *= MHASH_M; \
    }\
  init = __h; \
}
#endif
#endif


#define BF_BIT_1(h) (63 & (h))
#define BF_BIT_2(h) (63 & ((h) >> 8))
#define BF_BIT_3(h) (63 & ((h) >> 14))
#define BF_BIT_4(h) (63 & ((h) >> 21))
#define BF_WORD(h, size) ((uint64)h % size)
#define BF_MASK(h) ((1L << BF_BIT_1 (h)) | (1L << BF_BIT_2 (h)) | (1L << BF_BIT_3 (h)) | (1L << BF_BIT_4 (h)))

void asc_row_nos (row_no_t * matches, int start, int n_values);
/* H_PART is extracts the part of the hash no that is used for hash join partitioning */
#define H_PART(h) (((uint64)(h)) >> 32)

#define CHA_PARTITION(cha, h) (cha->cha_n_partitions ? &cha->cha_partitions[h % cha->cha_n_partitions] : cha)
#define CHA_POS_1(cha, hno) ((((uint32)hno)) % cha->cha_size)
#define CHA_POS_2(cha, hno) ((((uint64)hno) >> 32) % cha->cha_size)
#define CHA_LIN_POS(cha, hno) ((((uint32)hno)) % chash_part_size)

extern int chash_part_size;

#define HIGH_16(w) ((uint64)(w) & 0xffff000000000000)
#define LOW_48(w) ((uint64)(w) & 0x0000ffffffffffff)
#define LOW_3(w) ((w) & 7)
#define CLR_LOW_3(w) ((w) & (uint32)0xfffffff8)
#define INC_LOW_3(n) (n = (CLR_LOW_3 (n) + LOW_3 (n + 1)))

#define CHA_ARRAY(cha, cha_p, h) cha_p->cha_array
#define L_BIT_SET(flags, n, bool) flags |= (bool) << n

#define L_BIT_CLR(flags, n) (flags &= ~(1 << bit)
#define L_IS_SET(flags, n) (flags & (1 << n))

int ce_hash_range_filter (col_pos_t * cpo, db_buf_t ce_first, int n_values, int n_bytes);
int ce_hash_sets_filter (col_pos_t * cpo, db_buf_t ce_first, int n_values, int n_bytes);
void cha_inline_result (hash_source_t * hs, chash_t * cha, caddr_t * inst, int64 * row, int rl);
