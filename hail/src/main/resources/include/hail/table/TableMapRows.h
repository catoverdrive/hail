#ifndef HAIL_TABLEMAPROWS_H
#define HAIL_TABLEMAPROWS_H 1

#include "hail/table/TableEmit.h"
#include "hail/table/NativeStatus.h"

namespace hail {

template<typename Prev, typename Mapper>
class TableMapRows {
  template<typename> friend class TablePartitionRange;
  private:
    Prev::TablePartitionIterator it_;
    Prev::TablePartitionIterator end_;
    PartitionContext * ctx_;
    char * value_ = nullptr;
    Mapper mapper_;

    bool advance() {
      ++it_;
      if (prev_ != end_) {
        value_ = mapper_(ctx_->st_, ctx_->region, *it_);
      } else {
        value_ = nullptr;
      }
      return (value_ != nullptr);
    }

  public:
    TableMapRows(PartitionContext ctx, Prev & prev, Mapper &&mapper) :
    it_(prev.begin()),
    end_(prev.end()),
    ctx_(ctx),
    mapper_(mapper) { value_ = mapper_(ctx_->st_, ctx_->region, *it_); }
}

}

#endif