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


}

#endif