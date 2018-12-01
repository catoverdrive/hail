#ifndef HAIL_TABLEREAD_H
#define HAIL_TABLEREAD_H 1

#include "hail/table/TableEmit.h"
#include "hail/table/NativeStatus.h"

namespace hail {

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
    TableNativeRead(Decoder dec, NativeStatus* st) : dec_(dec), ctx_(st) {
      if (dec_.decode_byte(ctx_->st_)) {
        value_ = dec_.decode_row(ctx_->, ctx_->region);
      }
    }
};



}

#endif