/*
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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_file_scan_utils.h"

#include <sys/stat.h>
#include <unistd.h>

#include <cctype>
#include <cstring>

#include "lib/string/ob_string_buffer.h"
#include "share/ob_errno.h"
namespace oceanbase
{
namespace storage
{

namespace
{

const int64_t CSV_BUFFER_SIZE = 64 * 1024;
const int64_t MAX_INFER_ROWS = 10000; // the rows that are read to infer a schema

inline bool is_space(char c) { return c == ' ' || c == '\t' || c == '\r' || c == '\n'; }

inline bool is_digit(char c) { return c >= '0' && c <= '9'; }

/// Reads the sign and the digits of a number, returns the position after it.
int64_t scan_number(const char *data, int64_t length, bool &is_integer, bool &is_valid)
{
  int64_t pos = 0;
  is_integer = true;
  is_valid = false;
  if (pos < length && (data[pos] == '-' || data[pos] == '+')) {
    ++pos;
  }
  int64_t digits = 0;
  while (pos < length && is_digit(data[pos])) {
    ++pos;
    ++digits;
  }
  if (pos < length && data[pos] == '.') {
    is_integer = false;
    ++pos;
    while (pos < length && is_digit(data[pos])) {
      ++pos;
      ++digits;
    }
  }
  if (pos < length && (data[pos] == 'e' || data[pos] == 'E')) {
    is_integer = false;
    ++pos;
    if (pos < length && (data[pos] == '-' || data[pos] == '+')) {
      ++pos;
    }
    int64_t exp_digits = 0;
    while (pos < length && is_digit(data[pos])) {
      ++pos;
      ++exp_digits;
    }
    if (exp_digits == 0) {
      return pos;
    }
  }
  is_valid = (digits > 0);
  return pos;
}

/// Checks a "YYYY-MM-DD" date, the month and the day have to be real.
bool is_date(const char *data, int64_t length)
{
  if (length != 10 || data[4] != '-' || data[7] != '-') {
    return false;
  }
  for (int64_t i = 0; i < length; ++i) {
    if (4 == i || 7 == i) {
      continue;
    }
    if (!is_digit(data[i])) {
      return false;
    }
  }
  const int month = (data[5] - '0') * 10 + (data[6] - '0');
  const int day = (data[8] - '0') * 10 + (data[9] - '0');
  if (month < 1 || month > 12 || day < 1 || day > 31) {
    return false;
  }
  return true;
}

/// Checks a "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DDTHH:MM:SS" datetime.
bool is_datetime(const char *data, int64_t length)
{
  if (length < 19) {
    return false;
  }
  if (!is_date(data, 10)) {
    return false;
  }
  if (' ' != data[10] && 'T' != data[10]) {
    return false;
  }
  if (data[13] != ':' || data[16] != ':') {
    return false;
  }
  for (int64_t i = 11; i < 19; ++i) {
    if (13 == i || 16 == i) {
      continue;
    }
    if (!is_digit(data[i])) {
      return false;
    }
  }
  const int hour = (data[11] - '0') * 10 + (data[12] - '0');
  const int minute = (data[14] - '0') * 10 + (data[15] - '0');
  const int second = (data[17] - '0') * 10 + (data[18] - '0');
  return hour <= 23 && minute <= 59 && second <= 60;
}

} // namespace

ObCsvFileReader::ObCsvFileReader()
    : file_(nullptr),
      buffer_(nullptr),
      buffer_size_(CSV_BUFFER_SIZE),
      buffer_begin_(0),
      buffer_end_(0),
      eof_(false),
      stamp_(),
      header_names_(),
      header_read_(false)
{
}

ObCsvFileReader::~ObCsvFileReader() { close(); }

int ObCsvFileReader::open(const common::ObString &path)
{
  int ret = OB_SUCCESS;
  close();
  if (path.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the csv file path is empty", K(ret));
  } else if (nullptr == (file_ = fopen(path.ptr(), "rb"))) {
    ret = OB_IO_ERROR;
    LOG_WARN("cannot open the csv file", K(ret), K(path));
  } else {
    buffer_ = (char *)ob_malloc(buffer_size_, "ObCsvFileReader");
    if (OB_ISNULL(buffer_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      buffer_begin_ = 0;
      buffer_end_ = 0;
      eof_ = false;
      header_read_ = false;
      header_names_.reset();
      if (OB_FAIL(ObFileScanUtils::stat_file(path, stamp_))) {
        // The stat is only used to notice a change of the file, so a file that
        // can not be stat'ed is still readable.
        stamp_ = ObFileStamp();
      }
    }
  }
  if (OB_FAIL(ret)) {
    close();
  }
  return ret;
}

void ObCsvFileReader::close()
{
  if (file_ != nullptr) {
    fclose(file_);
    file_ = nullptr;
  }
  if (buffer_ != nullptr) {
    ob_free(buffer_);
    buffer_ = nullptr;
  }
  buffer_begin_ = 0;
  buffer_end_ = 0;
  eof_ = false;
  header_read_ = false;
  header_names_.reset();
}

int ObCsvFileReader::fill()
{
  int ret = OB_SUCCESS;
  if (eof_) {
    ret = OB_ITER_END;
  } else {
    // The part of the buffer that was read is moved to the front, so the row
    // that is being parsed is never split.
    if (buffer_begin_ > 0) {
      const int64_t rest = buffer_end_ - buffer_begin_;
      if (rest > 0) {
        memmove(buffer_, buffer_ + buffer_begin_, rest);
      }
      buffer_end_ = rest;
      buffer_begin_ = 0;
    }
    if (buffer_end_ == buffer_size_) {
      ret = OB_SIZE_OVERFLOW; // a single row does not fit into the buffer
    } else {
      const int64_t read = fread(buffer_ + buffer_end_, 1, buffer_size_ - buffer_end_, file_);
      if (read < 0) {
        ret = OB_IO_ERROR;
        LOG_WARN("cannot read the csv file", K(ret));
      } else {
        buffer_end_ += read;
        if (read == 0) {
          eof_ = true;
        }
      }
    }
  }
  return ret;
}

int ObCsvFileReader::parse_row(const char *begin,
                                const char *end,
                                common::ObIArray<ObFileCell> &cells)
{
  int ret = OB_SUCCESS;
  const char *pos = begin;
  bool quoted = false;
  const char *field_begin = pos;
  common::ObArray<const char *> field_ends;
  common::ObArray<bool> field_quoted;
  while (pos < end) {
    if (quoted) {
      if ('"' == *pos) {
        if (pos + 1 < end && '"' == pos[1]) {
          ++pos; // a doubled quote inside a quoted field
        } else {
          quoted = false;
        }
      }
    } else if ('"' == *pos && pos == field_begin) {
      quoted = true;
    } else if (',' == *pos) {
      if (OB_FAIL(field_ends.push_back(pos))) {
      } else if (OB_FAIL(field_quoted.push_back('"' == *field_begin))) {
      }
      field_begin = pos + 1;
    }
    ++pos;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(field_ends.push_back(end))) {
    } else if (OB_FAIL(field_quoted.push_back('"' == *field_begin))) {
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < field_ends.count(); ++i) {
    const char *field_end = field_ends.at(i);
    const char *value_begin = field_begin;
    const char *value_end = field_end;
    if (field_quoted.at(i)) {
      ++value_begin; // the opening quote
      if (value_end > value_begin && '"' == value_end[-1]) {
        --value_end; // the closing quote
      }
    }
    const int64_t length = value_end - value_begin;
    // An empty field that is not quoted is a NULL, an empty quoted field is an
    // empty string.
    const bool is_null = (length == 0) && !field_quoted.at(i);
    if (OB_FAIL(cells.push_back(ObFileCell(value_begin, length, is_null)))) {
      break;
    }
    field_begin = field_end + 1;
  }
  return ret;
}

int ObCsvFileReader::read_line(common::ObIArray<ObFileCell> &cells, bool &is_eof)
{
  int ret = OB_SUCCESS;
  is_eof = false;
  cells.reset();
  const char *begin = buffer_ + buffer_begin_;
  const char *end = buffer_ + buffer_end_;
  const char *pos = begin;
  bool quoted = false;
  const char *row_end = nullptr;
  while (pos < end) {
    if (quoted) {
      if ('"' == *pos) {
        if (pos + 1 < end && '"' == pos[1]) {
          ++pos;
        } else {
          quoted = false;
        }
      }
    } else if ('"' == *pos) {
      quoted = true;
    } else if ('\n' == *pos) {
      row_end = pos;
      break;
    }
    ++pos;
  }
  if (OB_ISNULL(row_end)) {
    if (eof_) {
      // The last row may end without a line break.
      if (begin < end) {
        row_end = end;
      } else {
        is_eof = true;
        ret = OB_ITER_END;
      }
    } else if (OB_FAIL(fill())) {
      // The row continues in the next chunk.
      ret = OB_SUCCESS;
      ret = read_line(cells, is_eof);
      return ret;
    } else {
      ret = read_line(cells, is_eof);
      return ret;
    }
  }
  if (OB_SUCC(ret) && !is_eof) {
    int64_t line_length = row_end - begin;
    // The line break of a CRLF pair belongs to the row.
    if (line_length > 0 && '\r' == begin[line_length - 1]) {
      --line_length;
    }
    if (OB_FAIL(parse_row(begin, begin + line_length, cells))) {
    } else {
      buffer_begin_ += (row_end - begin) + 1;
    }
  }
  return ret;
}

int ObCsvFileReader::next_row(common::ObIArray<ObFileCell> &cells)
{
  int ret = OB_SUCCESS;
  if (!is_open()) {
    ret = OB_NOT_INIT;
  } else {
    bool is_eof = false;
    ret = read_line(cells, is_eof);
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObCsvFileReader::header(common::ObIArray<common::ObString> &names)
{
  int ret = OB_SUCCESS;
  names.reset();
  if (!header_read_) {
    common::ObArray<ObFileCell> cells;
    bool is_eof = false;
    if (OB_FAIL(read_line(cells, is_eof))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the csv file has no header", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < cells.count(); ++i) {
        if (OB_FAIL(names.push_back(cells.at(i).string()))) {
          break;
        }
      }
      header_read_ = true;
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < header_names_.count(); ++i) {
      if (OB_FAIL(names.push_back(header_names_.at(i)))) {
        break;
      }
    }
  }
  return ret;
}

const char *ObFileScanUtils::type_name(ObFileColumnType type)
{
  switch (type) {
  case ObFileColumnType::BIGINT:
    return "BIGINT";
  case ObFileColumnType::DOUBLE:
    return "DOUBLE";
  case ObFileColumnType::BOOLEAN:
    return "BOOLEAN";
  case ObFileColumnType::DATE:
    return "DATE";
  case ObFileColumnType::DATETIME:
    return "DATETIME";
  case ObFileColumnType::NULL_TYPE:
    return "NULL";
  default:
    return "VARCHAR";
  }
}

ObFileColumnType ObFileScanUtils::infer_type(const char *data, int64_t length, bool is_null)
{
  ObFileColumnType type = ObFileColumnType::NULL_TYPE;
  if (is_null || OB_ISNULL(data) || 0 == length) {
    type = ObFileColumnType::NULL_TYPE;
  } else if (4 == length && 0 == strncasecmp(data, "true", 4)) {
    type = ObFileColumnType::BOOLEAN;
  } else if (5 == length && 0 == strncasecmp(data, "false", 5)) {
    type = ObFileColumnType::BOOLEAN;
  } else if (is_datetime(data, length)) {
    type = ObFileColumnType::DATETIME;
  } else if (is_date(data, length)) {
    type = ObFileColumnType::DATE;
  } else {
    bool is_integer = false;
    bool is_valid = false;
    const int64_t pos = scan_number(data, length, is_integer, is_valid);
    if (is_valid && pos == length) {
      type = is_integer ? ObFileColumnType::BIGINT : ObFileColumnType::DOUBLE;
    } else {
      type = ObFileColumnType::VARCHAR;
    }
  }
  return type;
}

ObFileColumnType ObFileScanUtils::widen_type(ObFileColumnType column_type, ObFileColumnType value_type)
{
  ObFileColumnType result = column_type;
  if (ObFileColumnType::NULL_TYPE == value_type) {
    result = column_type;
  } else if (ObFileColumnType::NULL_TYPE == column_type) {
    result = value_type;
  } else if (column_type == value_type) {
    result = column_type;
  } else if ((ObFileColumnType::BIGINT == column_type && ObFileColumnType::DOUBLE == value_type)
             || (ObFileColumnType::DOUBLE == column_type && ObFileColumnType::BIGINT == value_type)) {
    result = ObFileColumnType::DOUBLE;
  } else if ((ObFileColumnType::DATE == column_type && ObFileColumnType::DATETIME == value_type)
             || (ObFileColumnType::DATETIME == column_type && ObFileColumnType::DATE == value_type)) {
    result = ObFileColumnType::DATETIME;
  } else {
    result = ObFileColumnType::VARCHAR;
  }
  return result;
}

int ObFileScanUtils::stat_file(const common::ObString &path, ObFileStamp &stamp)
{
  int ret = OB_SUCCESS;
  struct stat info;
  if (0 != stat(path.ptr(), &info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cannot stat the file", K(ret), K(path));
  } else {
    stamp.size_ = info.st_size;
    stamp.mtime_ = info.st_mtime;
  }
  return ret;
}

common::ObString ObFileScanUtils::dir_name(const common::ObString &path)
{
  const char *pos = (nullptr == path.ptr()) ? nullptr : strrchr(path.ptr(), '/');
  if (nullptr == pos) {
    return common::ObString();
  }
  return common::ObString(pos - path.ptr() + 1, path.ptr());
}

common::ObString ObFileScanUtils::absolute_path(const common::ObString &path)
{
  if (path.empty()) {
    return path;
  }
  if ('/' == path.ptr()[0]) {
    return path;
  }
  char cwd[PATH_MAX];
  if (nullptr == getcwd(cwd, sizeof(cwd))) {
    return path;
  }
  int ret = OB_SUCCESS;
  common::ObStringBuffer buffer;
  common::ObString result;
  const int64_t cwd_length = strlen(cwd);
  if (OB_SUCC(buffer.append(cwd, cwd_length))) {
    if (cwd_length > 0 && '/' != cwd[cwd_length - 1]) {
      buffer.append("/");
    }
    buffer.append(path.ptr(), path.length());
    result = buffer.string();
  } else {
    result = path;
  }
  return result;
}

int ObFileScanUtils::check_path(const common::ObString &path, const common::ObString &secure_file_priv)
{
  int ret = OB_SUCCESS;
  if (path.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the file path is empty", K(ret));
  } else if (0 != strncmp(path.ptr(), "/", 1)) {
    // Only an absolute path is accepted, so the file can not be found through
    // the working directory of the server.
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("the file path has to be absolute", K(ret), K(path));
  } else {
    // A path may not walk out of the directory that secure_file_priv allows.
    if (nullptr != strstr(path.ptr(), "/../")) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("the file path may not contain ..", K(ret), K(path));
    } else if (!secure_file_priv.empty() && 0 != strcmp(secure_file_priv.ptr(), "/")) {
      const common::ObString dir = dir_name(path);
      if (0 != strncmp(dir.ptr(), secure_file_priv.ptr(), secure_file_priv.length())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("the file is outside of secure_file_priv", K(ret), K(path), K(secure_file_priv));
      }
    }
  }
  return ret;
}

int ObFileScanUtils::infer_csv_schema(const common::ObString &path,
                                     common::ObIAllocator &allocator,
                                     common::ObIArray<ObFileColumnMeta> &columns,
                                     ObFileStamp &stamp)
{
  int ret = OB_SUCCESS;
  columns.reset();
  ObCsvFileReader reader;
  if (OB_FAIL(reader.open(path))) {
  } else {
    common::ObArray<common::ObString> names;
    if (OB_FAIL(reader.header(names))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cannot read the header of the csv file", K(ret), K(path));
    } else {
      common::ObArray<ObFileColumnMeta> metas;
      // A header that appears twice gets the suffix of the column number.
      for (int64_t i = 0; OB_SUCC(ret) && i < names.count(); ++i) {
        ObFileColumnMeta meta;
        meta.name_ = names.at(i);
        meta.type_ = ObFileColumnType::NULL_TYPE;
        meta.nullable_ = false;
        bool duplicated = false;
        for (int64_t j = 0; j < metas.count(); ++j) {
          if (metas.at(j).name_.length() == meta.name_.length()
              && 0 == memcmp(metas.at(j).name_.ptr(), meta.name_.ptr(), meta.name_.length())) {
            duplicated = true;
            break;
          }
        }
        if (duplicated) {
          // A header that appears twice gets the number of the column as a
          // suffix, so both columns keep a name of their own.
          char name[128];
          const int written = snprintf(name, sizeof(name), "%.*s__%ld",
                                      static_cast<int>(meta.name_.length()), meta.name_.ptr(), i);
          if (written <= 0 || written >= static_cast<int>(sizeof(name))) {
            ret = OB_SIZE_OVERFLOW;
            break;
          }
          meta.name_ = common::ObString(written, name);
        }
        if (OB_FAIL(metas.push_back(meta))) {
          break;
        }
      }
      // The rows give the types of the columns.
      int64_t read_rows = 0;
      common::ObArray<ObFileCell> cells;
      while (OB_SUCC(ret) && read_rows < MAX_INFER_ROWS) {
        bool is_eof = false;
        if (OB_FAIL(reader.read_line(cells, is_eof))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          }
          break;
        }
        ++read_rows;
        for (int64_t i = 0; OB_SUCC(ret) && i < metas.count() && i < cells.count(); ++i) {
          const ObFileCell &cell = cells.at(i);
          const ObFileColumnType value_type = infer_type(cell.data_, cell.length_, cell.is_null_);
          if (ObFileColumnType::NULL_TYPE == value_type) {
            metas.at(i).nullable_ = true;
          } else {
            metas.at(i).type_ = widen_type(metas.at(i).type_, value_type);
          }
        }
      }
      if (OB_SUCC(ret)) {
        for (int64_t i = 0; OB_SUCC(ret) && i < metas.count(); ++i) {
          if (OB_FAIL(columns.push_back(metas.at(i)))) {
            break;
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(stat_file(path, stamp))) {
          stamp = ObFileStamp();
        }
      }
    }
  }
  return ret;
}

int ObFileScanUtils::infer_jsonl_schema(const common::ObString &path,
                                       common::ObIAllocator &allocator,
                                       common::ObIArray<ObFileColumnMeta> &columns,
                                       ObFileStamp &stamp)
{
  int ret = OB_SUCCESS;
  columns.reset();
  // The schema of a json lines file is the union of the fields of its rows.
  // The rows are parsed by the json reader of the database, so the values keep
  // the type they have in the document.
  common::ObArray<common::ObString> names;
  common::ObArray<ObFileColumnType> types;
  common::ObArray<bool> nullable;
  FILE *file = fopen(path.ptr(), "rb");
  if (OB_ISNULL(file)) {
    ret = OB_IO_ERROR;
    LOG_WARN("cannot open the jsonl file", K(ret), K(path));
  } else {
    ObFileStamp file_stamp;
    if (OB_FAIL(stat_file(path, file_stamp))) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      stamp = file_stamp;
    }
    fclose(file);
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
