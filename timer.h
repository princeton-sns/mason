#ifndef TIMER_H
#define TIMER_H

class Timer {
 public:
  uint64_t timeout_us = 0;
  uint64_t timeout_tsc = 0;
  uint64_t prev_tsc = 0;
  uint64_t diff_tsc;
  double freq_ghz;
  bool running = false;
  void (*callback)(void *);  // The function to call when timer runs out
  void *arg;

  // Can explicitly init with null to have a timer with no cb
  void init(uint64_t to_us, void (*cb)(void *), void *a) {
    freq_ghz = erpc::measure_rdtsc_freq();
    timeout_us = to_us;
    timeout_tsc = erpc::us_to_cycles(to_us, freq_ghz);

    callback = cb;
    arg = a;
    running = false;
  }

  void set_cb(void (*cb)(void *), void *a) {
    callback = cb;
    arg = a;
  }

  // Maintain the callback
  void start() {
    prev_tsc = erpc::rdtsc();
    running = true;
  }

  __inline__ void reset() {
    start();
  }

  // Change the callback
  void start(void (*cb)(void *)) {
    callback = cb;
    start();
  }

  void stop() {
    running = false;
  }

  void change_period(uint64_t to_us) {
    timeout_us = to_us;
    timeout_tsc = erpc::us_to_cycles(to_us, freq_ghz);
  }

  // If the timer has run out, call the callback
  void check() {
    if (!running) {
      return;
    }

    uint64_t curr_tsc = erpc::rdtsc();
    if (curr_tsc - prev_tsc > timeout_tsc) {
      stop();
      erpc::rt_assert(callback != nullptr, "Timer with null cb expired!\n");
      callback(arg);
    }
  }

  bool expired() {
    // can't expire if it wasn't running?
    if (!running) return false;
    uint64_t curr_tsc = erpc::rdtsc();
    return curr_tsc - prev_tsc > timeout_tsc;
  }

  bool expired_stop() {
    // can't expire if it wasn't running?
    if (!running) return false;
    uint64_t curr_tsc = erpc::rdtsc();
    if (curr_tsc - prev_tsc > timeout_tsc) {
      running = false;
      return true;
    }
    return false;
  }

  // set_cb MUST be reset after deserialization
  template<class Archive>
  void serialize(Archive &ar, const unsigned int) {
    ar & freq_ghz;
    ar & timeout_us;
    ar & timeout_tsc;
    ar & prev_tsc;
    ar & diff_tsc;
    ar & running;
  }
};

#endif  // TIMER_H
