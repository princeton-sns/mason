#ifndef SEQUENCER_BITMAP_H
#define SEQUENCER_BITMAP_H

#include "common.h"
#include <rte_memcpy.h>

// For tracking seqnums already used
class Bitmap {
 public:
  int thread_id = -1;
  size_t head_block = 0;
  size_t tail_block = INIT_N_BLOCKS - 1;
  size_t nblocks = INIT_N_BLOCKS;
  uint64_t base_seqnum = 0;
  uint64_t sequence_space;
  char f_name[255];

  // necessary and complicates serialization
  uint8_t pending_truncates;

  uint64_t *counts;
  uint8_t *bitmap;

  std::mutex mutex;

  Bitmap() {
    bitmap = new uint8_t[INIT_N_BLOCKS * BYTES_PER_BLOCK]();
    counts = new uint64_t[INIT_N_BLOCKS]();
    f_name[0] = '\0';
  }

  // boost ::serialize requires a default constructor, so this needs
  // to be done separately from the contructor
  void assign_sequence_space_and_f_name(uint64_t _sequence_space) {
    sequence_space = _sequence_space;
    int r = rand();
    sprintf(f_name, "/usr/local/snapshot_space_%zu_rand_%d", sequence_space, r);
  }

  ~Bitmap() {
    delete[] bitmap;
    delete[] counts;
  }

  // Grow the bitmap to accommodate this seqnum
  void grow(uint64_t seqnum) {
    while (seqnum >= (base_seqnum + nblocks * SEQNUMS_PER_BLOCK)) {

      LOG_BITMAP("[%d] growing from base %zu largest %zu to %zu "
                 "for sn %zu cap %zu\n",
                 thread_id, base_seqnum,
                 base_seqnum + nblocks * SEQNUMS_PER_BLOCK,
                 base_seqnum + 2 * nblocks * SEQNUMS_PER_BLOCK, seqnum,
                 2 * nblocks * SEQNUMS_PER_BLOCK);

      size_t new_size = 2 * nblocks;

      auto *tmp_bitmap = new uint8_t[new_size * BYTES_PER_BLOCK]();
      auto *tmp_counts = new uint64_t[new_size]();

      rte_memcpy(tmp_bitmap, bitmap + head_block * BYTES_PER_BLOCK,
                 (nblocks - head_block) * BYTES_PER_BLOCK);
      rte_memcpy(tmp_bitmap + (nblocks - head_block) * BYTES_PER_BLOCK,
                 bitmap, head_block * BYTES_PER_BLOCK);

      rte_memcpy(tmp_counts, counts + head_block,
                 (nblocks - head_block) * sizeof(uint64_t));
      rte_memcpy(tmp_counts + nblocks - head_block, counts,
                 head_block * sizeof(uint64_t));

      delete bitmap;
      bitmap = tmp_bitmap;

      delete counts;
      counts = tmp_counts;

      // Update bookkeeping variables
      nblocks = new_size;
      head_block = 0;
      tail_block = nblocks - 1;
    }
    print();
  }

  long unsigned int capacity() {
    return nblocks * SEQNUMS_PER_BLOCK;
  }

  // Truncate first block
  void truncate() {
    memset(bitmap + head_block * BYTES_PER_BLOCK, 0, BYTES_PER_BLOCK);
    counts[head_block] = 0;
    head_block = (head_block + 1) % nblocks;
    tail_block = (tail_block + 1) % nblocks;
    LOG_BITMAP("[%d] Truncated block in space %zu at seqnum %lu; "
               "head %zu, tail %zu, new base %zu, new max %zu\n",
               thread_id, sequence_space, base_seqnum, head_block, tail_block,
               base_seqnum + SEQNUMS_PER_BLOCK,
               base_seqnum + nblocks * SEQNUMS_PER_BLOCK);
    base_seqnum += SEQNUMS_PER_BLOCK;
  }

  uint64_t get_seqnum_index(uint64_t seqnum) {
    erpc::rt_assert(seqnum >= base_seqnum,
                    "get_seqnum_index: Seqnum is lower than base of bitmap!\n");
    return (head_block * BYTES_PER_BLOCK + ((seqnum - base_seqnum) / 8)) % (
        nblocks * BYTES_PER_BLOCK);
  }

  uint8_t get_seqnum_bitmask(uint64_t seqnum) {
    erpc::rt_assert(seqnum >= base_seqnum,
                    "get_seqnum_bitmask: Seqnum is lower than base of bitmap!\n");
    return static_cast<uint8_t>(1 << ((seqnum - base_seqnum) % 8));
  }

  uint64_t get_seqnum_block(uint64_t seqnum) {
    erpc::rt_assert(seqnum >= base_seqnum,
                    "get_seqnum_block: Seqnum is lower than base of bitmap!\n");
    return (((seqnum - base_seqnum) / SEQNUMS_PER_BLOCK) + head_block) %
        nblocks;
  }

  uint64_t get_seqnum_from_loc(uint64_t idx, size_t bit) {
    uint64_t start_idx = head_block * BYTES_PER_BLOCK;
    if (idx >= start_idx) {
      return (idx * 8 - head_block * SEQNUMS_PER_BLOCK +
          base_seqnum + bit);
    } else {
      return (base_seqnum + (nblocks - head_block) * SEQNUMS_PER_BLOCK +
          idx * 8 + bit);
    }
  }

  void insert_seqnum(uint64_t seqnum) {
    fmt_rt_assert(seqnum >= base_seqnum,
                  "Seqnum %lu is smaller than base_seqnum %lu!",
                  seqnum, base_seqnum);

    // Check if bitmap size needs to increase
    if (seqnum >= (base_seqnum + nblocks * SEQNUMS_PER_BLOCK)) {
      grow(seqnum);
    }

    // Calculate bit position based on base_seqnum
    uint64_t byte_idx = get_seqnum_index(seqnum);
    uint8_t bitmask = get_seqnum_bitmask(seqnum);

    // Flip bit (if not already flipped, it shouldn't be!) and increment counts
    fmt_rt_assert(!(bitmap[byte_idx] & bitmask),
                  "Trying to record a seqnum twice!: %lu at byte_idx %zu, "
                  "bitmask %zu, byte value %zu", seqnum, byte_idx, bitmask,
                  bitmap[byte_idx]);
    bitmap[byte_idx] |= bitmask;
    counts[get_seqnum_block(seqnum)]++;
  }

  void print() {
    LOG_BITMAP("[%d] BITMAP INFODUMP, address %p!\n"
               "\thead_block: %zu\n"
               "\ttail_block: %zu\n"
               "\tnblocks: %zu\n"
               "\tbase_seqnum: %lu\n"
               "\tmax recordable seqnum: %zu\n",
               thread_id, static_cast<void *>(this),
               head_block, tail_block, nblocks, base_seqnum,
               base_seqnum + SEQNUMS_PER_BLOCK * nblocks - 1);
  }

  void write_to_file() {
    FILE *file = fopen(f_name, "w");
    erpc::rt_assert(file != NULL, "Could not open file for bitmap\n");
    size_t n = 0;
    while ((n += fwrite(bitmap + n, 1, (nblocks * BYTES_PER_BLOCK) - n, file))
        < nblocks * BYTES_PER_BLOCK);
    erpc::rt_assert(n == nblocks * BYTES_PER_BLOCK,
                    "n not equal to size of bitmap?\n");
    fclose(file);
  }

  // does this need to free bitmap!?
  // assumes metadata is already set up
  void read_from_file(const char *file_name) {
    size_t size = nblocks * BYTES_PER_BLOCK;
    erpc::rt_assert(
        nblocks * BYTES_PER_BLOCK == size, // todo don't need this anymore
        "read_from_file size arg is wrong\n");
    FILE *file = fopen(file_name, "rb");
    bitmap = reinterpret_cast<uint8_t *>(malloc(size));
    erpc::rt_assert(fread(bitmap, 1, size, file),
                    "Could not read in bitmap from file.\n");
    fclose(file);
  }

  template<class Archive>
  void serialize(Archive &ar, const unsigned int) {
    ar & thread_id;
    ar & head_block;
    ar & tail_block;
    ar & nblocks;
    ar & base_seqnum;

    ar & pending_truncates;

    // read_from_file must be called outside
    if (Archive::is_loading::value) {
      counts = new uint64_t[nblocks]();
      bitmap = new uint8_t[nblocks * BYTES_PER_BLOCK]();
    }

    for (size_t i = 0; i < nblocks; i++) {
      ar & counts[i];
    }

    if (Archive::is_saving::value) {
      write_to_file();
    }
  }
};

#endif //SEQUENCER_BITMAP_H
