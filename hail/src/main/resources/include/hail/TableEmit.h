#ifndef HAIL_TABLEEMIT_H
#define HAIL_TABLEEMIT_H 1

namespace hail {

struct PartitionContext {
  NativeStatus * st_;
  char * globals_;
  RegionPool pool_{};
  RegionPtr region_;
  void new_region() { region = pool_->get_region(); }

  PartitionContext(NativeStatus * st, char * globals) : st_(st), globals_(globals), region_(pool_->get_region()) { }
  PartitionContext(NativeStatus * st) : PartitionContext(st, nullptr) { }
}

template <typename TableIRPartition>
class TablePartitionRange : public TableIRPartition {
  private:
//    char * value_;
//    bool advance();
    char * get() const { return value_; }
  public:
    class TablePartitionIterator {
      private:
        TablePartitionRange * range_;
        explicit TablePartitionIterator(Range * range) : range_(range) { }
      public:
        TablePartitionIterator& operator++() {
          if (range_ != nullptr && !(range_->advance())) {
            range_ = nullptr;
          }
          return *this;
        }
        char const* operator*() const { return range_->get(); }
        friend bool operator==(TablePartitionIterator const& lhs, TablePartitionIterator const& rhs) {
          return (lhs.range_ == rhs.range_);
        }
        friend bool operator!=(TablePartitionIterator const& lhs, TablePartitionIterator const& rhs) {
          return !(lhs == rhs);
        }
    };
    TablePartitionIterator begin() { return TablePartitionIterator(this); }
    TablePartitionIterator end() { return Iterator(nullptr); }
}

template<typename Decoder>
class TableNativeRead {
  template<typename> friend class TablePartitionRange;
  private:
    Decoder dec_;
    PartitionContext * ctx_;
    char * value_ = nullptr;
    bool advance() {
      if (dec_.decode_byte(ctx_->st_)) {
        ctx_.new_region();
        value_ = dec_.decode_row(ctx_->, ctx_->region);
      } else {
        value_ = nullptr;
      }
      return (value_ != nullptr);
    }

  public:
    Reader(Decoder dec, NativeStatus* st) : dec_(dec), ctx_(st) {
      if (dec_.decode_byte(ctx_->st_)) {
        value_ = dec_.decode_row(ctx_->, ctx_->region);
      }
    }
};


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

template<typename Prev, typename Explode>
class TableExplodeRows {
  template<typename> friend class TablePartitionRange;
  private:
    Prev:TablePartitionIterator it_;
    Prev:TablePartitionIterator end_;
    PartitionContext * ctx_;
    char * value_ = nullptr;
    Explode explode_;

    char * map() {

    }

    bool advance() {


    }

  public:
    TableExplodeRows(PartitionContext ctx, Prev * prev, Explode && explode) :
    it_(prev.begin()),
    end_(prev.end()),
    ctx_(ctx),
    explode_(explode) {


    }
}

}

#endif