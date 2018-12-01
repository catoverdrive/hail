#ifndef HAIL_TABLEEXPLODE_H
#define HAIL_TABLEEXPLODE_H 1

#include "hail/table/TableEmit.h"
#include "hail/table/NativeStatus.h"

namespace hail {

template<typename Prev, typename ExplodeF>
class TableExplodeRows {
  template<typename> friend class TablePartitionRange;
  private:
    Prev:TablePartitionIterator it_;
    Prev:TablePartitionIterator end_;
    PartitionContext * ctx_;
    char * value_ = nullptr;
    char * exploded_ = nullptr;
    Exploder exploder_;
    //exploder_.at_end;
    //exploder_.create_iterator(st, &&oldregion, it_)
    //exploder_.next_row(st, newregion, it_)

    bool advance() {
      while (exploder_.at_end()) {
        ++it_;
        if (it_ == end_) {
          value_ = nullptr;
          return false;
        }
        exploder_.create_iterator(ctx_->st, std::move(ctx_->region_), *it_);
      }
      ctx_->new_region();
      value = exploder_.next_row(ctx_->st, ctx_->region_, *it_);
    }

  public:
    TableExplodeRows(PartitionContext ctx, Prev * prev, Explode && explode) :
    it_(prev.begin()),
    end_(prev.end()),
    ctx_(ctx),
    explode_(explode) {
      if (it_ != end_) {
        exploder_.create_iterator(ctx_->st, std::move(ctx_->region_, *it_));
        advance();
      }
    }
}

}

#endif