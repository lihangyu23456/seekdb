/**
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ob_parquet_reader.h"

#include <stdarg.h>
#include <string.h>

#include <string>

#include "lib/allocator/page_arena.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace storage
{

namespace
{

// the types of the fields of the thrift compact protocol
enum ObThriftType
{
  OB_THRIFT_STOP = 0,
  OB_THRIFT_TRUE = 1,
  OB_THRIFT_FALSE = 2,
  OB_THRIFT_BYTE = 3,
  OB_THRIFT_I16 = 4,
  OB_THRIFT_I32 = 5,
  OB_THRIFT_I64 = 6,
  OB_THRIFT_DOUBLE = 7,
  OB_THRIFT_BINARY = 8,
  OB_THRIFT_LIST = 9,
  OB_THRIFT_SET = 10,
  OB_THRIFT_MAP = 11,
  OB_THRIFT_STRUCT = 12
};

// the types of the values of a parquet file
enum ObParquetType
{
  OB_PARQUET_BOOLEAN = 0,
  OB_PARQUET_INT32 = 1,
  OB_PARQUET_INT64 = 2,
  OB_PARQUET_INT96 = 3,
  OB_PARQUET_FLOAT = 4,
  OB_PARQUET_DOUBLE = 5,
  OB_PARQUET_BYTE_ARRAY = 6,
  OB_PARQUET_FIXED_LEN_BYTE_ARRAY = 7
};

// the types of the pages of a parquet file
enum ObParquetPageType
{
  OB_PARQUET_DATA_PAGE = 0,
  OB_PARQUET_INDEX_PAGE = 1,
  OB_PARQUET_DICTIONARY_PAGE = 2,
  OB_PARQUET_DATA_PAGE_V2 = 3
};

// the compression of the pages of a parquet file
enum ObParquetCodec
{
  OB_PARQUET_UNCOMPRESSED = 0,
  OB_PARQUET_SNAPPY = 1,
  OB_PARQUET_GZIP = 2
};

// the encoding of the values of a page
enum ObParquetEncoding
{
  OB_PARQUET_PLAIN = 0,
  OB_PARQUET_RLE = 3,
  OB_PARQUET_BIT_PACKED = 4
};

// the repetition of a column of a parquet file
enum ObParquetRepetition
{
  OB_PARQUET_REQUIRED = 0,
  OB_PARQUET_OPTIONAL = 1,
  OB_PARQUET_REPEATED = 2
};

// the types of the values of a parquet file that have a text of their own
enum ObParquetConvertedType
{
  OB_PARQUET_CONVERTED_DATE = 6,
  OB_PARQUET_CONVERTED_TIMESTAMP_MILLIS = 9,
  OB_PARQUET_CONVERTED_TIMESTAMP_MICROS = 10
};

/**
 * @brief A reader of the thrift compact protocol.
 *
 * The metadata of a parquet file is written with the thrift compact
 * protocol, so the reader of the metadata reads the fields of a struct one
 * after the other.
 */
class ObThriftReader
{
public:
  ObThriftReader(const char *buf, int64_t length)
      : buf_(buf), length_(length), pos_(0), last_field_id_(0)
  {
  }

  int read_byte(uint8_t &value)
  {
    int ret = OB_SUCCESS;
    if (pos_ >= length_) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      value = static_cast<uint8_t>(buf_[pos_++]);
    }
    return ret;
  }

  int read_varint(uint64_t &value)
  {
    int ret = OB_SUCCESS;
    value = 0;
    int64_t shift = 0;
    uint8_t byte = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(read_byte(byte))) {
        break;
      }
      value |= (static_cast<uint64_t>(byte & 0x7F) << shift);
      if (0 == (byte & 0x80)) {
        break;
      }
      shift += 7;
      if (shift > 63) {
        ret = OB_ERR_UNEXPECTED;
      }
    }
    return ret;
  }

  int read_zigzag(int64_t &value)
  {
    int ret = OB_SUCCESS;
    uint64_t raw = 0;
    if (OB_FAIL(read_varint(raw))) {
    } else {
      value = static_cast<int64_t>(raw >> 1) ^ -static_cast<int64_t>(raw & 1);
    }
    return ret;
  }

  int read_i32(int32_t &value)
  {
    int ret = OB_SUCCESS;
    int64_t wide = 0;
    if (OB_FAIL(read_zigzag(wide))) {
    } else {
      value = static_cast<int32_t>(wide);
    }
    return ret;
  }

  int read_i64(int64_t &value) { return read_zigzag(value); }

  int read_binary(common::ObString &value)
  {
    int ret = OB_SUCCESS;
    uint64_t length = 0;
    if (OB_FAIL(read_varint(length))) {
    } else if (length > static_cast<uint64_t>(length_ - pos_)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      value.assign_ptr(buf_ + pos_, static_cast<int64_t>(length));
      pos_ += static_cast<int64_t>(length);
    }
    return ret;
  }

  int read_list_header(int64_t &size, uint8_t &element_type)
  {
    int ret = OB_SUCCESS;
    uint8_t header = 0;
    if (OB_FAIL(read_byte(header))) {
    } else {
      element_type = header & 0x0F;
      size = (header >> 4) & 0x0F;
      if (15 == size) {
        uint64_t wide = 0;
        if (OB_FAIL(read_varint(wide))) {
        } else {
          size = static_cast<int64_t>(wide);
        }
      }
    }
    return ret;
  }

  /// Reads the header of the next field of a struct.
  int read_field(int16_t &field_id, uint8_t &field_type)
  {
    int ret = OB_SUCCESS;
    uint8_t header = 0;
    if (OB_FAIL(read_byte(header))) {
    } else {
      field_type = header & 0x0F;
      if (OB_THRIFT_STOP == field_type) {
        // the end of the struct, the field has no number
        return ret;
      }
      const uint8_t delta = (header >> 4) & 0x0F;
      if (0 == delta) {
        int64_t id = 0;
        if (OB_FAIL(read_zigzag(id))) {
        } else {
          field_id = static_cast<int16_t>(id);
        }
      } else {
        field_id = static_cast<int16_t>(last_field_id_ + delta);
      }
      last_field_id_ = field_id;
    }
    return ret;
  }

  /**
   * @brief Starts the reading of a struct.
   *
   * The number of a field of a struct is the difference to the field before
   * it, so the reader has to know the field before the struct.
   */
  int begin_struct(int16_t &saved_field_id)
  {
    saved_field_id = last_field_id_;
    last_field_id_ = 0;
    return OB_SUCCESS;
  }

  /// Ends the reading of a struct.
  int end_struct(int16_t saved_field_id)
  {
    last_field_id_ = saved_field_id;
    return OB_SUCCESS;
  }

  /// Reads a value of a field that the reader does not need.
  int skip(uint8_t type)
  {
    int ret = OB_SUCCESS;
    switch (type) {
      case OB_THRIFT_TRUE:
      case OB_THRIFT_FALSE:
        break;
      case OB_THRIFT_BYTE: {
        uint8_t value = 0;
        ret = read_byte(value);
      } break;
      case OB_THRIFT_I16:
      case OB_THRIFT_I32:
      case OB_THRIFT_I64: {
        int64_t value = 0;
        ret = read_zigzag(value);
      } break;
      case OB_THRIFT_DOUBLE: {
        if (length_ - pos_ < 8) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          pos_ += 8;
        }
      } break;
      case OB_THRIFT_BINARY: {
        common::ObString value;
        ret = read_binary(value);
      } break;
      case OB_THRIFT_LIST:
      case OB_THRIFT_SET: {
        int64_t size = 0;
        uint8_t element_type = 0;
        if (OB_SUCC(ret) && OB_FAIL(read_list_header(size, element_type))) {
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
            ret = skip(element_type);
          }
        }
      } break;
      case OB_THRIFT_MAP: {
        uint64_t size = 0;
        if (OB_SUCC(ret) && OB_FAIL(read_varint(size))) {
        } else if (size > 0) {
          uint8_t types = 0;
          if (OB_SUCC(ret) && OB_FAIL(read_byte(types))) {
          } else {
            const uint8_t key_type = (types >> 4) & 0x0F;
            const uint8_t value_type = types & 0x0F;
            for (uint64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
              if (OB_SUCC(ret) && OB_FAIL(skip(key_type))) {
              } else if (OB_FAIL(skip(value_type))) {
              }
            }
          }
        }
      } break;
      case OB_THRIFT_STRUCT: {
        int16_t field_id = 0;
        uint8_t field_type = 0;
        int16_t saved_field_id = 0;
        begin_struct(saved_field_id);
        while (OB_SUCC(ret)) {
          if (OB_FAIL(read_field(field_id, field_type))) {
            break;
          }
          if (OB_THRIFT_STOP == field_type) {
            break;
          }
          if (OB_FAIL(skip(field_type))) {
            break;
          }
        }
        end_struct(saved_field_id);
      } break;
      default:
        ret = OB_ERR_UNEXPECTED;
        break;
    }
    return ret;
  }

  int64_t get_pos() const { return pos_; }
  int64_t get_remaining() const { return length_ - pos_; }

private:
  const char *buf_;
  int64_t length_;
  int64_t pos_;
  int16_t last_field_id_;
};

/**
 * @brief Reads the text of a page that is written with snappy.
 *
 * The text of snappy is a copy of the text that was written and the text
 * that was not changed, so the reader copies the parts that were not changed
 * and repeats the parts that were.
 */
int snappy_uncompress(const char *src,
                      int64_t src_length,
                      common::ObIAllocator &allocator,
                      common::ObString &result)
{
  int ret = OB_SUCCESS;
  uint64_t length = 0;
  int64_t shift = 0;
  int64_t pos = 0;
  while (pos < src_length && shift <= 63) {
    const uint8_t byte = static_cast<uint8_t>(src[pos++]);
    length |= (static_cast<uint64_t>(byte & 0x7F) << shift);
    if (0 == (byte & 0x80)) {
      break;
    }
    shift += 7;
  }
  char *buf = static_cast<char *>(allocator.alloc(static_cast<int64_t>(length) + 1));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    int64_t out_pos = 0;
    while (OB_SUCC(ret) && pos < src_length) {
      const uint8_t tag = static_cast<uint8_t>(src[pos++]);
      if (0 == (tag & 3)) {
        // the text that was not changed
        int64_t literal_length = (tag >> 2) + 1;
        if (literal_length > 60) {
          const int64_t extra = literal_length - 60;
          literal_length = 0;
          for (int64_t i = 0; OB_SUCC(ret) && i < extra; ++i) {
            literal_length |= (static_cast<int64_t>(static_cast<uint8_t>(src[pos + i])) << (8 * i));
          }
          pos += extra;
          ++literal_length;
        }
        if (pos + literal_length > src_length || out_pos + literal_length > static_cast<int64_t>(length)) {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        MEMCPY(buf + out_pos, src + pos, literal_length);
        pos += literal_length;
        out_pos += literal_length;
      } else {
        // the text that was copied
        int64_t copy_length = 0;
        int64_t offset = 0;
        if (1 == (tag & 3)) {
          copy_length = 4 + ((tag >> 2) & 7);
          offset = (static_cast<int64_t>(tag >> 5) << 8) | static_cast<int64_t>(static_cast<uint8_t>(src[pos]));
          pos += 1;
        } else if (2 == (tag & 3)) {
          copy_length = (tag >> 2) + 1;
          offset = static_cast<int64_t>(static_cast<uint8_t>(src[pos]))
                   | (static_cast<int64_t>(static_cast<uint8_t>(src[pos + 1])) << 8);
          pos += 2;
        } else {
          copy_length = (tag >> 2) + 1;
          offset = static_cast<int64_t>(static_cast<uint8_t>(src[pos]))
                   | (static_cast<int64_t>(static_cast<uint8_t>(src[pos + 1])) << 8)
                   | (static_cast<int64_t>(static_cast<uint8_t>(src[pos + 2])) << 16)
                   | (static_cast<int64_t>(static_cast<uint8_t>(src[pos + 3])) << 24);
          pos += 4;
        }
        if (offset <= 0 || offset > out_pos || out_pos + copy_length > static_cast<int64_t>(length)) {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        // the copy may write over the text that was written already
        for (int64_t i = 0; i < copy_length; ++i) {
          buf[out_pos] = buf[out_pos - offset];
          ++out_pos;
        }
      }
    }
    if (OB_SUCC(ret)) {
      buf[out_pos] = '\0';
      result.assign_ptr(buf, out_pos);
    }
  }
  return ret;
}

/**
 * @brief Reads the levels of a page that are written with the run length
 * encoding.
 *
 * The levels are a run of the same value or a group of the values that are
 * written with a fixed number of bits.
 */
int decode_levels(const char *data,
                  int64_t length,
                  int32_t bit_width,
                  int64_t num_levels,
                  common::ObArray<int32_t> &levels)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t count = 0;
  if (0 == bit_width) {
    // every value is there
    for (int64_t i = 0; OB_SUCC(ret) && i < num_levels; ++i) {
      if (OB_FAIL(levels.push_back(0))) {
        break;
      }
    }
    return ret;
  }
  while (OB_SUCC(ret) && count < num_levels && pos < length) {
    uint64_t header = 0;
    int64_t shift = 0;
    while (pos < length && shift <= 63) {
      const uint8_t byte = static_cast<uint8_t>(data[pos++]);
      header |= (static_cast<uint64_t>(byte & 0x7F) << shift);
      if (0 == (byte & 0x80)) {
        break;
      }
      ++shift;
    }
    if (0 == (header & 1)) {
      // a run of the same value
      const int64_t run = static_cast<int64_t>(header >> 1);
      const int64_t bytes = (bit_width + 7) / 8;
      if (pos + bytes > length) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      uint64_t value = 0;
      for (int64_t i = 0; i < bytes; ++i) {
        value |= (static_cast<uint64_t>(static_cast<uint8_t>(data[pos + i])) << (8 * i));
      }
      pos += bytes;
      for (int64_t i = 0; OB_SUCC(ret) && i < run && count < num_levels; ++i) {
        if (OB_FAIL(levels.push_back(static_cast<int32_t>(value)))) {
          break;
        }
        ++count;
      }
    } else {
      // a group of the values
      const int64_t groups = static_cast<int64_t>(header >> 1);
      const int64_t values = groups * 8;
      const int64_t bytes = (values * bit_width + 7) / 8;
      if (pos + bytes > length) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      uint64_t buffer = 0;
      int64_t bits = 0;
      int64_t index = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < values && count < num_levels; ++i) {
        while (bits < bit_width) {
          buffer |= (static_cast<uint64_t>(static_cast<uint8_t>(data[pos + index])) << bits);
          bits += 8;
          ++index;
        }
        const uint64_t value = buffer & ((1ULL << bit_width) - 1);
        buffer >>= bit_width;
        bits -= bit_width;
        if (OB_FAIL(levels.push_back(static_cast<int32_t>(value)))) {
          break;
        }
        ++count;
      }
      pos += bytes;
    }
  }
  return ret;
}

/// The number of the days of a month, the month of a leap year has one more.
int parquet_days_of_month(int year, int month)
{
  static const int days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  if (month < 1 || month > 12) {
    return 0;
  }
  if (2 == month) {
    const bool is_leap = (0 == year % 4 && 0 != year % 100) || 0 == year % 400;
    return is_leap ? 29 : 28;
  }
  return days[month - 1];
}

/// Writes the date of the number of the days since the first of January 1970.
void write_date(char *buf, int32_t days)
{
  int32_t year = 1970;
  int32_t month = 1;
  int32_t left = days;
  while (left >= 0) {
    const bool is_leap = (0 == year % 4 && 0 != year % 100) || 0 == year % 400;
    const int32_t length = parquet_days_of_month(year, month);
    if (left < length) {
      break;
    }
    left -= length;
    ++month;
    if (month > 12) {
      month = 1;
      ++year;
    }
    if (year > 9999) {
      break;
    }
  }
  snprintf(buf, 16, "%04d-%02d-%02d", year, month, left + 1);
}

/// Writes the time of the number of the seconds since the first of January 1970.
void write_datetime(char *buf, int64_t seconds)
{
  int32_t days = static_cast<int32_t>(seconds / 86400);
  int32_t rest = static_cast<int32_t>(seconds % 86400);
  if (rest < 0) {
    rest += 86400;
    --days;
  }
  char date[16];
  write_date(date, days);
  snprintf(buf, 32, "%s %02d:%02d:%02d", date, rest / 3600, (rest % 3600) / 60, rest % 60);
}

/// The type of the column of a parquet file.
ObFileColumnType parquet_type_to_column_type(int32_t type, int32_t converted_type)
{
  ObFileColumnType column_type = ObFileColumnType::VARCHAR;
  switch (type) {
    case OB_PARQUET_BOOLEAN:
      column_type = ObFileColumnType::BOOLEAN;
      break;
    case OB_PARQUET_INT32:
    case OB_PARQUET_INT64:
      column_type = ObFileColumnType::BIGINT;
      break;
    case OB_PARQUET_FLOAT:
    case OB_PARQUET_DOUBLE:
      column_type = ObFileColumnType::DOUBLE;
      break;
    default:
      column_type = ObFileColumnType::VARCHAR;
      break;
  }
  if (OB_PARQUET_CONVERTED_DATE == converted_type) {
    column_type = ObFileColumnType::DATE;
  } else if (OB_PARQUET_CONVERTED_TIMESTAMP_MILLIS == converted_type
             || OB_PARQUET_CONVERTED_TIMESTAMP_MICROS == converted_type) {
    column_type = ObFileColumnType::DATETIME;
  }
  return column_type;
}

} // namespace

ObParquetReader::ObParquetReader()
    : file_(nullptr), allocator_(nullptr), footer_(nullptr), footer_length_(0), columns_(), column_readers_(),
      row_groups_(), row_group_index_(0), row_group_rows_read_(0)
{
}

ObParquetReader::~ObParquetReader() { close(); }

void ObParquetReader::close()
{
  if (nullptr != file_) {
    fclose(file_);
    file_ = nullptr;
  }
  footer_ = nullptr;
  footer_length_ = 0;
  columns_.reset();
  column_readers_.reset();
  row_groups_.reset();
  row_group_index_ = 0;
  row_group_rows_read_ = 0;
  allocator_ = nullptr;
}

int ObParquetReader::open(const common::ObString &path, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  close();
  allocator_ = &allocator;
  char *path_buf = nullptr;
  if (OB_ISNULL(path_buf = static_cast<char *>(allocator.alloc(path.length() + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMCPY(path_buf, path.ptr(), path.length());
    path_buf[path.length()] = '\0';
    file_ = fopen(path_buf, "rb");
    if (OB_ISNULL(file_)) {
      ret = OB_IO_ERROR;
      LOG_WARN("cannot open the parquet file", K(ret), K(path));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(read_footer())) {
    LOG_USER_ERROR(OB_ERR_UNEXPECTED, "the footer of the parquet file is not read");
  } else if (OB_SUCC(ret) && OB_FAIL(read_schema(allocator))) {
    LOG_USER_ERROR(OB_ERR_UNEXPECTED, "the schema of the parquet file is not read");
  } else if (OB_SUCC(ret) && OB_FAIL(read_row_groups(allocator))) {
    LOG_USER_ERROR(OB_ERR_UNEXPECTED, "the row groups of the parquet file are not read");
  } else {
    LOG_WARN("the parquet file is read", K(columns_.count()), K(row_groups_.count()));
  }
  if (OB_FAIL(ret)) {
    close();
  }
  return ret;
}

int ObParquetReader::read_footer()
{
  int ret = OB_SUCCESS;
  // the last eight bytes of the file hold the length of the footer and the
  // text that ends every file of parquet
  int64_t file_length = 0;
  if (0 != fseek(file_, 0, SEEK_END)) {
    ret = OB_IO_ERROR;
  } else {
    file_length = ftell(file_);
  }
  int64_t footer_length = 0;
  if (OB_SUCC(ret) && file_length < 12) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the parquet file is too short", K(ret), K(file_length));
  }
  char tail[8];
  if (OB_SUCC(ret) && 0 != fseek(file_, file_length - 8, SEEK_SET)) {
    ret = OB_IO_ERROR;
  }
  if (OB_SUCC(ret) && 8 != fread(tail, 1, 8, file_)) {
    ret = OB_IO_ERROR;
  }
  if (OB_SUCC(ret) && 0 != memcmp(tail + 4, "PAR1", 4)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the file does not end with the text of a parquet file", K(ret));
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 3; i >= 0; --i) {
      footer_length = (footer_length << 8) | static_cast<uint8_t>(tail[i]);
    }
    footer_length_ = footer_length;
  }
  if (OB_SUCC(ret) && (footer_length <= 0 || footer_length + 8 > file_length)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the length of the footer of the parquet file is wrong", K(ret), K(footer_length));
  }
  if (OB_SUCC(ret) && OB_ISNULL(footer_ = static_cast<char *>(allocator_->alloc(footer_length + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  if (OB_SUCC(ret) && 0 != fseek(file_, file_length - 8 - footer_length, SEEK_SET)) {
    ret = OB_IO_ERROR;
  }
  if (OB_SUCC(ret) && static_cast<int64_t>(fread(footer_, 1, footer_length, file_)) != footer_length) {
    ret = OB_IO_ERROR;
  }
  return ret;
}

int ObParquetReader::read_schema(common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObThriftReader reader(footer_, footer_length_);
  int16_t field_id = 0;
  uint8_t field_type = 0;
  LOG_WARN("the footer of the parquet file is read", K(footer_length_));
  // the schema of the file is a tree, the leaves of the tree are the columns
  common::ObArray<common::ObString> names;
  common::ObArray<int32_t> types;
  common::ObArray<int32_t> lengths;
  common::ObArray<int32_t> repetitions;
  common::ObArray<int32_t> converted_types;
  common::ObArray<int64_t> children;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(reader.read_field(field_id, field_type))) {
      break;
    }
    if (OB_THRIFT_STOP == field_type) {
      break;
    }
    if (2 != field_id) {
      // the schema is the second field of the metadata of the file
      if (OB_FAIL(reader.skip(field_type))) {
        break;
      }
      continue;
    }
    int64_t size = 0;
    uint8_t element_type = 0;
    if (OB_FAIL(reader.read_list_header(size, element_type))) {
      break;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
      common::ObString name;
      int32_t type = 0;
      int32_t type_length = 0;
      int32_t repetition = 0;
      int32_t converted_type = 0;
      int32_t num_children = 0;
      int16_t element_field_id = 0;
      uint8_t element_field_type = 0;
      int16_t saved_field_id = 0;
      reader.begin_struct(saved_field_id);
      while (OB_SUCC(ret)) {
        if (OB_FAIL(reader.read_field(element_field_id, element_field_type))) {
          break;
        }
        if (OB_THRIFT_STOP == element_field_type) {
          break;
        }
        if (1 == element_field_id && OB_THRIFT_I32 == element_field_type) {
          ret = reader.read_i32(type);
        } else if (2 == element_field_id && OB_THRIFT_I32 == element_field_type) {
          ret = reader.read_i32(type_length);
        } else if (3 == element_field_id && OB_THRIFT_I32 == element_field_type) {
          ret = reader.read_i32(repetition);
        } else if (4 == element_field_id && OB_THRIFT_BINARY == element_field_type) {
          ret = reader.read_binary(name);
        } else if (5 == element_field_id && OB_THRIFT_I32 == element_field_type) {
          ret = reader.read_i32(num_children);
        } else if (6 == element_field_id && OB_THRIFT_I32 == element_field_type) {
          ret = reader.read_i32(converted_type);
        } else {
          ret = reader.skip(element_field_type);
        }
      }
      reader.end_struct(saved_field_id);
      if (OB_SUCC(ret) && OB_FAIL(names.push_back(name))) {
        break;
      }
      if (OB_SUCC(ret) && OB_FAIL(types.push_back(type))) {
        break;
      }
      if (OB_SUCC(ret) && OB_FAIL(lengths.push_back(type_length))) {
        break;
      }
      if (OB_SUCC(ret) && OB_FAIL(repetitions.push_back(repetition))) {
        break;
      }
      if (OB_SUCC(ret) && OB_FAIL(converted_types.push_back(converted_type))) {
        break;
      }
      if (OB_SUCC(ret) && OB_FAIL(children.push_back(num_children))) {
        break;
      }
    }
    break;
  }
  // the leaves of the tree are the columns of the file
  if (OB_SUCC(ret)) {
    LOG_WARN("the schema of the parquet file is read", K(names.count()));
    for (int64_t i = 0; OB_SUCC(ret) && i < names.count(); ++i) {
      if (children.at(i) > 0) {
        continue;
      }
      ObFileColumnMeta meta;
      meta.name_ = names.at(i);
      meta.nullable_ = (OB_PARQUET_OPTIONAL == repetitions.at(i));
      meta.type_ = parquet_type_to_column_type(types.at(i), converted_types.at(i));
      if (OB_SUCC(ret) && OB_FAIL(columns_.push_back(meta))) {
        break;
      }
      ObParquetColumn column;
      column.type_ = types.at(i);
      column.type_length_ = lengths.at(i);
      column.converted_type_ = converted_types.at(i);
      column.max_def_level_ = meta.nullable_ ? 1 : 0;
      if (OB_SUCC(ret) && OB_FAIL(column_readers_.push_back(column))) {
        break;
      }
    }
  }
  return ret;
}

int ObParquetReader::read_row_groups(common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObThriftReader reader(footer_, footer_length_);
  int16_t field_id = 0;
  uint8_t field_type = 0;
  LOG_WARN("the row groups of the parquet file are read");
  while (OB_SUCC(ret)) {
    if (OB_FAIL(reader.read_field(field_id, field_type))) {
      break;
    }
    if (OB_THRIFT_STOP == field_type) {
      break;
    }
    if (4 != field_id) {
      if (OB_FAIL(reader.skip(field_type))) {
        break;
      }
      continue;
    }
    int64_t size = 0;
    uint8_t element_type = 0;
    if (OB_FAIL(reader.read_list_header(size, element_type))) {
      break;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
      ObParquetRowGroup group;
      int64_t num_rows = 0;
      int16_t group_field_id = 0;
      uint8_t group_field_type = 0;
      int16_t saved_field_id = 0;
      reader.begin_struct(saved_field_id);
      while (OB_SUCC(ret)) {
        if (OB_FAIL(reader.read_field(group_field_id, group_field_type))) {
          break;
        }
        if (OB_THRIFT_STOP == group_field_type) {
          break;
        }
        if (1 == group_field_id && OB_THRIFT_LIST == group_field_type) {
          int64_t chunk_count = 0;
          uint8_t chunk_type = 0;
          if (OB_FAIL(reader.read_list_header(chunk_count, chunk_type))) {
            break;
          }
          for (int64_t j = 0; OB_SUCC(ret) && j < chunk_count; ++j) {
            ObParquetChunk chunk;
            int64_t total_compressed = 0;
            int16_t chunk_field_id = 0;
            uint8_t chunk_field_type = 0;
            int16_t saved_field_id = 0;
            reader.begin_struct(saved_field_id);
            while (OB_SUCC(ret)) {
              if (OB_FAIL(reader.read_field(chunk_field_id, chunk_field_type))) {
                break;
              }
              if (OB_THRIFT_STOP == chunk_field_type) {
                break;
              }
              if (3 != chunk_field_id) {
                if (OB_FAIL(reader.skip(chunk_field_type))) {
                  break;
                }
                continue;
              }
              // the metadata of the chunk
              int16_t meta_field_id = 0;
              uint8_t meta_field_type = 0;
              int16_t saved_meta_field_id = 0;
              reader.begin_struct(saved_meta_field_id);
              while (OB_SUCC(ret)) {
                if (OB_FAIL(reader.read_field(meta_field_id, meta_field_type))) {
                  break;
                }
                if (OB_THRIFT_STOP == meta_field_type) {
                  break;
                }
                if (2 == meta_field_id && OB_THRIFT_LIST == meta_field_type) {
                  int64_t encoding_count = 0;
                  uint8_t encoding_type = 0;
                  if (OB_FAIL(reader.read_list_header(encoding_count, encoding_type))) {
                    break;
                  }
                  for (int64_t k = 0; OB_SUCC(ret) && k < encoding_count; ++k) {
                    int32_t encoding = 0;
                    if (OB_SUCC(ret) && OB_THRIFT_I32 == encoding_type) {
                      ret = reader.read_i32(encoding);
                    } else {
                      ret = reader.skip(encoding_type);
                    }
                    if (OB_SUCC(ret) && OB_PARQUET_PLAIN == encoding) {
                      chunk.encoding_ = encoding;
                    }
                  }
                } else if (4 == meta_field_id && OB_THRIFT_I32 == meta_field_type) {
                  ret = reader.read_i32(chunk.codec_);
                } else if (7 == meta_field_id && OB_THRIFT_I64 == meta_field_type) {
                  ret = reader.read_i64(total_compressed);
                } else if (9 == meta_field_id && OB_THRIFT_I64 == meta_field_type) {
                  ret = reader.read_i64(chunk.page_offset_);
                } else {
                  ret = reader.skip(meta_field_type);
                }
              }
              reader.end_struct(saved_meta_field_id);
              chunk.end_offset_ = chunk.page_offset_ + total_compressed;
            }
            reader.end_struct(saved_field_id);
            if (OB_SUCC(ret) && OB_FAIL(group.chunks_.push_back(chunk))) {
              break;
            }
          }
        } else if (3 == group_field_id && OB_THRIFT_I64 == group_field_type) {
          ret = reader.read_i64(num_rows);
        } else {
          ret = reader.skip(group_field_type);
        }
      }
      reader.end_struct(saved_field_id);
      group.num_rows_ = num_rows;
      if (OB_SUCC(ret) && OB_FAIL(row_groups_.push_back(group))) {
        break;
      }
    }
    break;
  }
  return ret;
}

int ObParquetReader::next_row(common::ObIArray<ObFileCell> &cells)
{
  int ret = OB_SUCCESS;
  if (row_group_index_ >= row_groups_.count()) {
    ret = OB_ITER_END;
  } else {
    const ObParquetRowGroup &group = row_groups_.at(row_group_index_);
    // the page of every column is read when the row that needs it is read
    for (int64_t i = 0; OB_SUCC(ret) && i < column_readers_.count(); ++i) {
      ObParquetColumn &column = column_readers_.at(i);
      while (OB_SUCC(ret) && column.value_index_ >= column.values_.count()) {
        if (i >= group.chunks_.count()) {
          break;
        }
        if (OB_FAIL(read_next_page(column, group.chunks_.at(i)))) {
          break;
        }
      }
    }
    if (OB_SUCC(ret)) {
      cells.reset();
      for (int64_t i = 0; OB_SUCC(ret) && i < column_readers_.count(); ++i) {
        ObParquetColumn &column = column_readers_.at(i);
        ObFileCell cell;
        if (column.value_index_ < column.values_.count()) {
          cell = column.values_.at(column.value_index_);
          ++column.value_index_;
        }
        if (OB_FAIL(cells.push_back(cell))) {
          break;
        }
      }
    }
    if (OB_SUCC(ret)) {
      ++row_group_rows_read_;
      if (row_group_rows_read_ >= group.num_rows_) {
        ++row_group_index_;
        if (row_group_index_ < row_groups_.count()) {
          const ObParquetRowGroup &next = row_groups_.at(row_group_index_);
          row_group_rows_read_ = 0;
          for (int64_t i = 0; OB_SUCC(ret) && i < column_readers_.count(); ++i) {
            ObParquetColumn &column = column_readers_.at(i);
            column.values_.reset();
            column.value_index_ = 0;
            if (i < next.chunks_.count()) {
              column.page_offset_ = next.chunks_.at(i).page_offset_;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObParquetReader::read_next_page(ObParquetColumn &column, const ObParquetChunk &chunk)
{
  int ret = OB_SUCCESS;
  int64_t offset = (column.page_offset_ > 0) ? column.page_offset_ : chunk.page_offset_;
  const int64_t end = chunk.end_offset_;
  while (OB_SUCC(ret) && offset < end) {
    int32_t page_type = 0;
    int64_t uncompressed_size = 0;
    int64_t compressed_size = 0;
    int64_t num_values = 0;
    int32_t encoding = OB_PARQUET_PLAIN;
    int32_t def_levels_length_header = 0;
    int32_t rep_levels_length_header = 0;
    bool is_compressed = true;
    int64_t data_offset = 0;
    if (OB_FAIL(read_page_header(offset,
                                 page_type,
                                 uncompressed_size,
                                 compressed_size,
                                 num_values,
                                 encoding,
                                 def_levels_length_header,
                                 rep_levels_length_header,
                                 is_compressed,
                                 data_offset))) {
      break;
    }
    offset = data_offset + compressed_size;
    if (OB_PARQUET_DICTIONARY_PAGE == page_type) {
      // the dictionary of the values is not used
      continue;
    }
    if (OB_PARQUET_DATA_PAGE != page_type && OB_PARQUET_DATA_PAGE_V2 != page_type) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("the page of the parquet file is not supported", K(ret), K(page_type));
      break;
    }
    // the data of the page
    char *page_data = nullptr;
    if (OB_SUCC(ret) && OB_ISNULL(page_data = static_cast<char *>(allocator_->alloc(compressed_size + 1)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    if (OB_SUCC(ret) && 0 != fseek(file_, data_offset, SEEK_SET)) {
      ret = OB_IO_ERROR;
    }
    if (OB_SUCC(ret) && static_cast<int64_t>(fread(page_data, 1, compressed_size, file_)) != compressed_size) {
      ret = OB_IO_ERROR;
    }
    // The levels of a page are not compressed, the values of the page are.
    const char *values_begin = page_data;
    int64_t values_length = compressed_size;
    common::ObArray<int32_t> def_levels;
    if (OB_SUCC(ret) && column.max_def_level_ > 0) {
      int64_t rep_levels_length = 0;
      int64_t def_levels_length = 0;
      const char *levels = page_data;
      if (OB_PARQUET_DATA_PAGE_V2 == page_type) {
        rep_levels_length = rep_levels_length_header;
        def_levels_length = def_levels_length_header;
      } else {
        // the levels of a page of the first version have their length
        if (compressed_size < 4) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          int64_t length = 0;
          for (int64_t i = 0; i < 4; ++i) {
            length = (length << 8) | static_cast<uint8_t>(levels[i]);
          }
          rep_levels_length = length;
          levels += 4;
          if (compressed_size - 4 < 4) {
            ret = OB_ERR_UNEXPECTED;
          } else {
            length = 0;
            for (int64_t i = 0; i < 4; ++i) {
              length = (length << 8) | static_cast<uint8_t>(levels[i]);
            }
            def_levels_length = length;
            levels += 4;
          }
        }
      }
      if (OB_SUCC(ret)
          && (rep_levels_length + def_levels_length > compressed_size
              || levels + rep_levels_length + def_levels_length > page_data + compressed_size)) {
        ret = OB_ERR_UNEXPECTED;
      }
      if (OB_SUCC(ret)) {
        const char *def_levels_begin = levels + rep_levels_length;
        if (OB_FAIL(decode_levels(def_levels_begin,
                                 def_levels_length,
                                 column.max_def_level_,
                                 num_values,
                                 def_levels))) {
        } else {
          values_begin = def_levels_begin + def_levels_length;
          values_length = compressed_size - (values_begin - page_data);
        }
      }
    }
    common::ObString page_text;
    page_text.assign_ptr(values_begin, values_length);
    if (OB_SUCC(ret) && OB_PARQUET_SNAPPY == chunk.codec_) {
      // the text of the values is compressed
      common::ObString plain;
      if (OB_FAIL(snappy_uncompress(page_text.ptr(),
                                   page_text.length(),
                                   *allocator_,
                                   plain))) {
      } else {
        page_text = plain;
      }
    } else if (OB_SUCC(ret) && OB_PARQUET_GZIP == chunk.codec_) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("the compression of the parquet file is not supported", K(ret));
    }
    if (OB_SUCC(ret)
        && OB_FAIL(decode_values(column, page_text.ptr(), page_text.length(), num_values, def_levels))) {
    }
    column.page_offset_ = offset;
  }
  return ret;
}

int ObParquetReader::read_page_header(int64_t offset,
                                      int32_t &page_type,
                                      int64_t &uncompressed_size,
                                      int64_t &compressed_size,
                                      int64_t &num_values,
                                      int32_t &encoding,
                                      int32_t &def_levels_length,
                                      int32_t &rep_levels_length,
                                      bool &is_compressed,
                                      int64_t &data_offset)
{
  int ret = OB_SUCCESS;
  // the header of a page is small, so a part of the file is read and the
  // header is read from it
  char buffer[4096];
  if (0 != fseek(file_, offset, SEEK_SET)) {
    ret = OB_IO_ERROR;
  } else {
    const int64_t read_length = fread(buffer, 1, sizeof(buffer), file_);
    ObThriftReader reader(buffer, read_length);
    int16_t field_id = 0;
    uint8_t field_type = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(reader.read_field(field_id, field_type))) {
        break;
      }
      if (OB_THRIFT_STOP == field_type) {
        break;
      }
      if (1 == field_id && OB_THRIFT_I32 == field_type) {
        ret = reader.read_i32(page_type);
      } else if (2 == field_id && OB_THRIFT_I32 == field_type) {
        int32_t value = 0;
        ret = reader.read_i32(value);
        uncompressed_size = value;
      } else if (3 == field_id && OB_THRIFT_I32 == field_type) {
        int32_t value = 0;
        ret = reader.read_i32(value);
        compressed_size = value;
      } else if (5 == field_id && OB_THRIFT_STRUCT == field_type) {
        // the header of a page of the first version
        int16_t data_field_id = 0;
        uint8_t data_field_type = 0;
        int16_t saved_field_id = 0;
        reader.begin_struct(saved_field_id);
        while (OB_SUCC(ret)) {
          if (OB_FAIL(reader.read_field(data_field_id, data_field_type))) {
            break;
          }
          if (OB_THRIFT_STOP == data_field_type) {
            break;
          }
          if (1 == data_field_id && OB_THRIFT_I32 == data_field_type) {
            int32_t value = 0;
            ret = reader.read_i32(value);
            num_values = value;
          } else if (2 == data_field_id && OB_THRIFT_I32 == data_field_type) {
            ret = reader.read_i32(encoding);
          } else {
            ret = reader.skip(data_field_type);
          }
        }
        reader.end_struct(saved_field_id);
      } else if (8 == field_id && OB_THRIFT_STRUCT == field_type) {
        // the header of a page of the second version
        int16_t data_field_id = 0;
        uint8_t data_field_type = 0;
        int16_t saved_field_id = 0;
        reader.begin_struct(saved_field_id);
        while (OB_SUCC(ret)) {
          if (OB_FAIL(reader.read_field(data_field_id, data_field_type))) {
            break;
          }
          if (OB_THRIFT_STOP == data_field_type) {
            break;
          }
          if (1 == data_field_id && OB_THRIFT_I32 == data_field_type) {
            int32_t value = 0;
            ret = reader.read_i32(value);
            num_values = value;
          } else if (4 == data_field_id && OB_THRIFT_I32 == data_field_type) {
            ret = reader.read_i32(encoding);
          } else if (5 == data_field_id && OB_THRIFT_I32 == data_field_type) {
            ret = reader.read_i32(def_levels_length);
          } else if (6 == data_field_id && OB_THRIFT_I32 == data_field_type) {
            ret = reader.read_i32(rep_levels_length);
          } else if (7 == data_field_id) {
            // the text of the field tells whether the values are compressed
            is_compressed = (OB_THRIFT_TRUE == data_field_type);
          } else {
            ret = reader.skip(data_field_type);
          }
        }
        reader.end_struct(saved_field_id);
      } else {
        ret = reader.skip(field_type);
      }
    }
    if (OB_SUCC(ret)) {
      data_offset = offset + reader.get_pos();
    }
  }
  return ret;
}

int ObParquetReader::decode_values(ObParquetColumn &column,
                                   const char *data,
                                   int64_t length,
                                   int64_t num_values,
                                   const common::ObArray<int32_t> &def_levels)
{
  int ret = OB_SUCCESS;
  column.values_.reset();
  column.value_index_ = 0;
  // the text of the values that had to be written again is written into the
  // memory of the column
  column.text_.clear();
  column.text_.reserve(static_cast<size_t>(num_values) * 32 + 64);
  int64_t pos = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < num_values; ++i) {
    ObFileCell cell;
    const bool is_null = (column.max_def_level_ > 0)
                         && (i >= def_levels.count() || def_levels.at(i) < column.max_def_level_);
    if (is_null) {
      cell = ObFileCell();
      if (OB_FAIL(column.values_.push_back(cell))) {
        break;
      }
      continue;
    }
    char text[64];
    int64_t text_length = 0;
    if (OB_PARQUET_BOOLEAN == column.type_) {
      if (pos >= length) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      const bool value = 0 != (static_cast<uint8_t>(data[pos]) & 1);
      text_length = snprintf(text, sizeof(text), "%s", value ? "true" : "false");
      ++pos;
      column.text_.append(text, text_length);
      cell = ObFileCell(column.text_.data() + column.text_.size() - text_length, text_length, false);
    } else if (OB_PARQUET_INT32 == column.type_) {
      if (pos + 4 > length) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      int32_t value = 0;
      for (int64_t i = 0; i < 4; ++i) {
        value |= (static_cast<int32_t>(static_cast<uint8_t>(data[pos + i])) << (8 * i));
      }
      pos += 4;
      if (OB_PARQUET_CONVERTED_DATE == column.converted_type_) {
        text_length = snprintf(text, sizeof(text), "%s", "");
        char date[16];
        write_date(date, value);
        text_length = snprintf(text, sizeof(text), "%s", date);
      } else {
        text_length = snprintf(text, sizeof(text), "%d", value);
      }
      column.text_.append(text, text_length);
      cell = ObFileCell(column.text_.data() + column.text_.size() - text_length, text_length, false);
    } else if (OB_PARQUET_INT64 == column.type_) {
      if (pos + 8 > length) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      int64_t value = 0;
      for (int64_t i = 0; i < 8; ++i) {
        value |= (static_cast<int64_t>(static_cast<uint8_t>(data[pos + i])) << (8 * i));
      }
      pos += 8;
      if (OB_PARQUET_CONVERTED_TIMESTAMP_MILLIS == column.converted_type_) {
        char datetime[32];
        write_datetime(datetime, value / 1000);
        text_length = snprintf(text, sizeof(text), "%s", datetime);
      } else if (OB_PARQUET_CONVERTED_TIMESTAMP_MICROS == column.converted_type_) {
        char datetime[32];
        write_datetime(datetime, value / 1000000);
        text_length = snprintf(text, sizeof(text), "%s", datetime);
      } else {
        text_length = snprintf(text, sizeof(text), "%ld", value);
      }
      column.text_.append(text, text_length);
      cell = ObFileCell(column.text_.data() + column.text_.size() - text_length, text_length, false);
    } else if (OB_PARQUET_FLOAT == column.type_) {
      if (pos + 4 > length) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      float value = 0;
      memcpy(&value, data + pos, 4);
      pos += 4;
      text_length = snprintf(text, sizeof(text), "%g", static_cast<double>(value));
      column.text_.append(text, text_length);
      cell = ObFileCell(column.text_.data() + column.text_.size() - text_length, text_length, false);
    } else if (OB_PARQUET_DOUBLE == column.type_) {
      if (pos + 8 > length) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      double value = 0;
      memcpy(&value, data + pos, 8);
      pos += 8;
      text_length = snprintf(text, sizeof(text), "%g", value);
      column.text_.append(text, text_length);
      cell = ObFileCell(column.text_.data() + column.text_.size() - text_length, text_length, false);
    } else if (OB_PARQUET_BYTE_ARRAY == column.type_) {
      if (pos + 4 > length) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      int32_t value_length = 0;
      for (int64_t i = 0; i < 4; ++i) {
        value_length |= (static_cast<int32_t>(static_cast<uint8_t>(data[pos + i])) << (8 * i));
      }
      pos += 4;
      if (value_length < 0 || pos + value_length > length) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      cell = ObFileCell(data + pos, value_length, false);
      pos += value_length;
    } else if (OB_PARQUET_FIXED_LEN_BYTE_ARRAY == column.type_) {
      const int64_t value_length = column.type_length_;
      if (pos + value_length > length) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      cell = ObFileCell(data + pos, value_length, false);
      pos += value_length;
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("the type of the values of the parquet file is not supported", K(ret), K(column.type_));
      break;
    }
    if (OB_FAIL(column.values_.push_back(cell))) {
      break;
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
