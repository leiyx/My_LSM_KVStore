#include <cassert>
#include "crc32c/crc32c.h"

#include "db/log/log_writer.h"
#include "util/file.h"
#include "util/coding.h"
#include <iostream>

namespace lsmkv {
namespace log {
    static void InitTypeCrc(uint32_t* type_crc) {
        // Encode the type into the crc.
        for (int i = 0; i <= KMaxType; i++) {
            char t = static_cast<char>(i);
            type_crc[i] = crc32c::Crc32c(&t,1);
        }
    }
    Writer::Writer(WritableFile* dest) 
        : dest_(dest),block_offset_(0) {
            InitTypeCrc(type_crc_);
        }
    
    Writer::Writer(WritableFile* dest, uint64_t dest_length) 
        :dest_(dest), block_offset_(dest_length % kBlockSize) {
            InitTypeCrc(type_crc_);
        }
    Status Writer::AddRecord(std::string_view sv) {
        const char* ptr = sv.data();
        size_t remain = sv.size();

        Status s;
        bool begin = true;

        do {
            size_t block_left = kBlockSize - block_offset_;
            assert(block_left >= 0);
            if (block_left < KLogHeadSize) {
                if (block_left > 0) {
                    dest_->Append(std::string_view("\x00\x00\x00\x00\x00\x00\x00", block_left));
                }
                block_offset_ = 0;
            }
            size_t fragment_avail = kBlockSize - block_offset_ - KLogHeadSize;
            size_t fragment_length = (remain < fragment_avail) ? remain : fragment_avail;
            bool end = (remain == fragment_length);

            LogRecodeType type;
            if (begin && end) {
                type = KFullType;
            } else if(begin) {
                type = KFirstType;
            } else if (end) {
                type = KLastType;
            } else {
                type = KMiddleType;
            }
            
            s = WritePhysicalRecord(type, ptr, fragment_length);
            remain -= fragment_length;
            ptr += fragment_length;
            begin = false;
        } while(s.ok() && remain > 0);
        return s;
    }
    Status Writer::WritePhysicalRecord(LogRecodeType type, const char* p, size_t len) {
        char header[KLogHeadSize];
        uint32_t crc = crc32c::Extend(type_crc_[type], reinterpret_cast<const uint8_t*>(p), len);
        crc = CrcMask(crc);
        EncodeFixed32(header, crc);

        header[4] = static_cast<char>(len & 0xff);
        header[5] = static_cast<char>(len >> 8);
        header[6] = static_cast<char>(type);

        Status s = dest_->Append(std::string_view(header, KLogHeadSize));
        if (s.ok()) {
            s = dest_->Append(std::string_view(p, len));
            if(s.ok()) {
                s = dest_->Flush();
            }
        }
        block_offset_ += KLogHeadSize + len;
        return s;
    }
}

}