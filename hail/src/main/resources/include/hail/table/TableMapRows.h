#ifndef HAIL_TABLEMAPROWS_H
#define HAIL_TABLEMAPROWS_H 1

#include "hail/table/TableEmit.h"
#include "hail/NativeStatus.h"

namespace hail {

template<typename Prev, typename Mapper>
class TableMapRows {
  template<typename> friend class TablePartitionRange;
  private:
    typename Prev::Iterator it_;
    typename Prev::Iterator end_;
    PartitionContext * ctx_;
    char const * value_ = nullptr;
    Mapper mapper_{};

    bool advance() {
      ++it_;
      if (it_ != end_) {
        value_ = mapper_(ctx_->st_, ctx_->region_.get(), ctx_->globals_, *it_);
      } else {
        value_ = nullptr;
      }
      return (value_ != nullptr);
    }

  public:
    TableMapRows(PartitionContext * ctx, Prev & prev) :
    it_(prev.begin()),
    end_(prev.end()),
    ctx_(ctx) { value_ = mapper_(ctx_->st_, ctx_->region_.get(), ctx_->globals_, *it_); }
};

}

#endif