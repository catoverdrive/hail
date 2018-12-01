#ifndef HAIL_TABLEFILTERROWS_H
#define HAIL_TABLEFILTERROWS_H 1

#include "hail/table/TableEmit.h"
#include "hail/table/NativeStatus.h"

namespace hail {

template<typename Prev, typename Filter>
class TableFilterRows {
  template<typename> friend class TablePartitionRange;
  private:
    Prev:TablePartitionIterator it_;
    Prev:TablePartitionIterator end_;
    PartitionContext * ctx_;
    char * value_ = nullptr;
    Filter filter_;
    char * map() {
      while (it_ != end_ && !filter(ctx_->st_, ctx_->region, *it_)) {
        ++it_;
      }
      return *it_;
    }
    bool advance() {
      ++it_;
      value = map();
      return (value_ != nullptr);
    }
  public:
    TableFilterRows(PartitionContext ctx, Prev & prev, Filter &&filter) :
    it_(prev.begin()),
    end_(prev.end()),
    ctx_(ctx_),
    filter_(filter) { value_ = map(); }
}

}

#endif