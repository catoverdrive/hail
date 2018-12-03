#ifndef HAIL_TABLEFILTERROWS_H
#define HAIL_TABLEFILTERROWS_H 1

#include "hail/table/TableEmit.h"
#include "hail/NativeStatus.h"

namespace hail {

template<typename Prev, typename Filter>
class TableFilterRows {
  template<typename> friend class TablePartitionRange;
  private:
    typename Prev::Iterator it_;
    typename Prev::Iterator end_;
    PartitionContext * ctx_;
    char const * value_ = nullptr;
    Filter filter_{};
    const char * map() {
      while (it_ != end_ && !filter_(ctx_->st_, ctx_->region_.get(), ctx_->globals_, *it_)) {
        ++it_;
      }
      return *it_;
    }
    bool advance() {
      ++it_;
      value_ = map();
      return (value_ != nullptr);
    }
  public:
    TableFilterRows(PartitionContext * ctx, Prev & prev) :
    it_(prev.begin()),
    end_(prev.end()),
    ctx_(ctx) { value_ = map(); }
};

}

#endif