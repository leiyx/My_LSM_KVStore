#include "util/coding.h"

namespace lsmkv {

size_t VarintLength(uint64_t v) {
    size_t ret = 1;
    while (v >= 128) {
        ret++;
        v >>= 7;
    }
    return ret;
}

char* EncodeVarint32(char* dst,uint32_t val) {
    uint8_t* ptr = reinterpret_cast<uint8_t*>(dst);
    static const uint8_t B = (1 << 7);

    while(val >= B) {
        *(ptr++) = static_cast<uint8_t>(val | B);
        val >>= 7;
    }
    *(ptr++) = static_cast<uint8_t>(val);
    return reinterpret_cast<char*>(ptr);
}

char* EncodeVarint64(char* dst,uint64_t val) {
    uint8_t* ptr = reinterpret_cast<uint8_t*>(dst);
    static const uint8_t B = (1 << 7);

    while(val >= B) {
        *(ptr++) = static_cast<uint8_t>(val | B);
        val >>= 7;
    }
    *(ptr++) = static_cast<uint8_t>(val);
    return reinterpret_cast<char*>(ptr);
}

void PutVarint32(std::string* dst,uint32_t val) {
    char buf[5];
    char* p = EncodeVarint32(buf, val);
    dst->append(buf, p - buf);
}

void PutVarint64(std::string* dst,uint64_t val) {
    char buf[10];
    char* p = EncodeVarint64(buf, val);
    dst->append(buf, p - buf);
}

const char* DecodeVarint32(const char* p,const char* limit, uint32_t* result) {
    uint32_t buf = 0;
    for(uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
        uint32_t byte = *(reinterpret_cast<const uint8_t*>(p));
        p++;
        if (byte & 128) {
            buf |= ((byte & 127) << shift);
        } else {
            buf |= (byte  << shift);
            *result = buf;
            return reinterpret_cast<const char*>(p);
        }
    }
    return nullptr;
}

const char* DecodeVarint64(const char* p,const char* limit, uint64_t* result) {  
    uint32_t buf = 0;
    for(uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
        uint32_t byte = *(reinterpret_cast<const uint8_t*>(p));
        p++;
        if (byte & 128) {
            buf |= ((byte & 127) << shift);
        } else {
            buf |= (byte  << shift);
            *result = buf;
            return reinterpret_cast<const char*>(p);
        }
    }
    return nullptr;
}

uint32_t DecodeFixed32(const char* dst) {
    const uint8_t* const p = reinterpret_cast<const uint8_t*>(dst);

    return static_cast<uint32_t>(p[0]) |
           (static_cast<uint32_t>(p[1]) << 8) |
           (static_cast<uint32_t>(p[2]) << 16) |
           (static_cast<uint32_t>(p[3]) << 24);
}

uint64_t DecodeFixed64(const char* dst) {
    const uint8_t* const p = reinterpret_cast<const uint8_t*>(dst);

    return static_cast<uint64_t>(p[0]) |
           (static_cast<uint64_t>(p[1]) << 8) |
           (static_cast<uint64_t>(p[2]) << 16) |
           (static_cast<uint64_t>(p[3]) << 24) |
           (static_cast<uint64_t>(p[4]) << 32) |
           (static_cast<uint64_t>(p[5]) << 40) |
           (static_cast<uint64_t>(p[6]) << 48) |
           (static_cast<uint64_t>(p[7]) << 56);
}
bool GetVarint32(std::string_view* input, uint32_t* value) {
    const char* p = input->data();
    const char* limit = input->data() + input->size();
    const char* p_end = DecodeVarint32(p, limit, value);
    if(p_end == nullptr) {
        return false;
    } else {
        *input = std::string_view(p_end, limit - p_end);
        return true;
    }
}

bool GetVarint64(std::string_view* input, uint64_t* value) {
    const char* p = input->data();
    const char* limit = input->data() + input->size();
    const char* p_end = DecodeVarint64(p, limit, value);
    if(p_end == nullptr) {
        return false;
    } else {
        *input = std::string_view(p_end, limit - p_end);
        return true;
    }
}

bool GetLengthPrefixedSlice(std::string_view* input, std::string_view* result) {
    uint32_t len;
    if(GetVarint32(input,&len) && input->size() >= len) {
        *result = std::string_view(input->data(),len);
        input->remove_prefix(len);
        return true;
    } else {
        return false;
    }
}

void PutLengthPrefixedSlice(std::string* dst, std::string_view input) {
    PutVarint32(dst,input.size());
    dst->append(input.data(),input.size());
}

uint32_t CrcMask(uint32_t crc) {
    return ((crc >> 17) | (crc << 15)) + KMaskValue;
}

uint32_t CrcUnMask(uint32_t crc) {
    crc -= KMaskValue;
    return (crc << 17) | (crc >> 15);
}

}