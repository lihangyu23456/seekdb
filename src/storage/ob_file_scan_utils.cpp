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
#include <glob.h>
#include <string>

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
  // Every field is read on its own: a quoted field holds the commas and the
  // line breaks of the text, a doubled quote is one quote.
  while (true) {
    const char *field_begin = pos;
    bool quoted = false;
    if (pos < end && '"' == *pos) {
      quoted = true;
      ++pos;
      while (pos < end) {
        if ('"' == *pos) {
          if (pos + 1 < end && '"' == pos[1]) {
            ++pos; // a doubled quote inside a quoted field
            ++pos;
          } else {
            ++pos; // the closing quote
            break;
          }
        } else {
          ++pos;
        }
      }
    } else {
      while (pos < end && ',' != *pos) {
        ++pos;
      }
    }
    const char *value_begin = field_begin;
    const char *value_end = pos;
    if (quoted) {
      ++value_begin; // the opening quote
      if (value_end > value_begin && '"' == value_end[-1]) {
        --value_end; // the closing quote
      }
    }
    const int64_t length = value_end - value_begin;
    // An empty field that is not quoted is a NULL, an empty quoted field is an
    // empty string.
    const bool is_null = (0 == length) && !quoted;
    if (OB_FAIL(cells.push_back(ObFileCell(value_begin, length, is_null)))) {
      break;
    }
    if (pos >= end || ',' != *pos) {
      break; // the last field of the row
    }
    ++pos; // the comma between the fields
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

int ObFileScanUtils::list_files(const common::ObString &pattern,
                                common::ObIAllocator &allocator,
                                common::ObIArray<common::ObString> &files)
{
  int ret = OB_SUCCESS;
  files.reset();
  glob_t matches;
  memset(&matches, 0, sizeof(matches));
  char *pattern_buf = nullptr;
  if (pattern.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(pattern_buf = static_cast<char *>(allocator.alloc(pattern.length() + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMCPY(pattern_buf, pattern.ptr(), pattern.length());
    pattern_buf[pattern.length()] = '\0';
    const int glob_ret = glob(pattern_buf, 0, nullptr, &matches);
    if (0 != glob_ret) {
      // a pattern that matches nothing is not an error
      globfree(&matches);
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < matches.gl_pathc; ++i) {
        const int64_t length = strlen(matches.gl_pathv[i]);
        char *path_buf = static_cast<char *>(allocator.alloc(length + 1));
        if (OB_ISNULL(path_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          break;
        }
        MEMCPY(path_buf, matches.gl_pathv[i], length);
        path_buf[length] = '\0';
        if (OB_FAIL(files.push_back(common::ObString(length, path_buf)))) {
          break;
        }
      }
      globfree(&matches);
    }
  }
  return ret;
}

common::ObString ObFileScanUtils::dir_name(const common::ObString &path){
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
        int64_t same_count = 0;
        for (int64_t j = 0; j < metas.count(); ++j) {
          if (metas.at(j).name_.length() == meta.name_.length()
              && 0 == memcmp(metas.at(j).name_.ptr(), meta.name_.ptr(), meta.name_.length())) {
            duplicated = true;
            ++same_count;
          }
        }
        if (duplicated) {
          // A header that appears twice gets the number of the occurrence as a
          // suffix, so both columns keep a name of their own.
          char name[128];
          const int written = snprintf(name, sizeof(name), "%.*s__%ld",
                                      static_cast<int>(meta.name_.length()), meta.name_.ptr(),
                                      same_count + 1);
          if (written <= 0 || written >= static_cast<int>(sizeof(name))) {
            ret = OB_SIZE_OVERFLOW;
            break;
          }
          meta.name_ = common::ObString(written, name);
        }
        // The name of a column points into the buffer of the reader, which is
        // gone when the reader is closed, so it is copied into the memory of
        // the caller.
        char *name_buf = static_cast<char *>(allocator.alloc(meta.name_.length() + 1));
        if (OB_ISNULL(name_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          break;
        }
        MEMCPY(name_buf, meta.name_.ptr(), meta.name_.length());
        name_buf[meta.name_.length()] = '\0';
        meta.name_ = common::ObString(meta.name_.length(), name_buf);
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

int ObFileScanUtils::parse_json_object(const char *begin,
                                       const char *end,
                                       common::ObIAllocator &allocator,
                                       common::ObIArray<ObFileJsonField> &fields)
{
  int ret = OB_SUCCESS;
  fields.reset();
  const char *pos = begin;
  // the whitespace of the document is not part of a value
  auto skip_space = [&pos, end]() {
    while (pos < end && (' ' == *pos || '\t' == *pos || '\r' == *pos || '\n' == *pos)) {
      ++pos;
    }
  };
  // the text of a string is copied into the memory of the caller
  auto copy_text = [&allocator](const char *text, int64_t length, common::ObString &out) -> int {
    char *buf = static_cast<char *>(allocator.alloc(length + 1));
    if (OB_ISNULL(buf)) {
      return OB_ALLOCATE_MEMORY_FAILED;
    }
    MEMCPY(buf, text, length);
    buf[length] = '\0';
    out.assign_ptr(buf, length);
    return OB_SUCCESS;
  };
  skip_space();
  if (pos >= end || '{' != *pos) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the row of the json file is not an object", K(ret));
  } else {
    ++pos;
    while (OB_SUCC(ret)) {
      skip_space();
      if (pos >= end) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      if ('}' == *pos) {
        ++pos;
        break;
      }
      if ('"' != *pos) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the name of a field of the json file is not a string", K(ret));
        break;
      }
      const char *name_begin = ++pos;
      while (pos < end && '"' != *pos) {
        ++pos;
      }
      if (pos >= end) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      ObFileJsonField field;
      if (OB_FAIL(copy_text(name_begin, pos - name_begin, field.name_))) {
        break;
      }
      ++pos; // the closing quote of the name
      skip_space();
      if (pos >= end || ':' != *pos) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      ++pos; // the colon
      skip_space();
      if (pos >= end) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      if ('"' == *pos) {
        // a string
        const char *value_begin = ++pos;
        while (pos < end && '"' != *pos) {
          ++pos;
        }
        if (pos >= end) {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        if (OB_FAIL(copy_text(value_begin, pos - value_begin, field.text_))) {
          break;
        }
        ++pos; // the closing quote
        // a string that holds a date or a time is a date or a time, the same
        // way the reader of a csv file reads it
        field.type_ = infer_type(field.text_.ptr(), field.text_.length(), false);
        field.is_null_ = false;
      } else if ('t' == *pos && pos + 4 <= end && 0 == memcmp(pos, "true", 4)) {
        pos += 4;
        if (OB_FAIL(copy_text("true", 4, field.text_))) {
          break;
        }
        field.type_ = ObFileColumnType::BOOLEAN;
        field.is_null_ = false;
      } else if ('f' == *pos && pos + 5 <= end && 0 == memcmp(pos, "false", 5)) {
        pos += 5;
        if (OB_FAIL(copy_text("false", 5, field.text_))) {
          break;
        }
        field.type_ = ObFileColumnType::BOOLEAN;
        field.is_null_ = false;
      } else if ('n' == *pos && pos + 4 <= end && 0 == memcmp(pos, "null", 4)) {
        pos += 4;
        field.type_ = ObFileColumnType::NULL_TYPE;
        field.is_null_ = true;
      } else if ('{' == *pos || '[' == *pos) {
        // only a flat object is read
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("the value of a field of the json file is not a simple value", K(ret));
        break;
      } else {
        // a number
        const char *value_begin = pos;
        while (pos < end && ',' != *pos && '}' != *pos && ' ' != *pos && '\t' != *pos) {
          ++pos;
        }
        if (OB_FAIL(copy_text(value_begin, pos - value_begin, field.text_))) {
          break;
        }
        field.type_ = infer_type(field.text_.ptr(), field.text_.length(), false);
        field.is_null_ = false;
      }
      if (OB_FAIL(fields.push_back(field))) {
        break;
      }
      skip_space();
      if (pos < end && ',' == *pos) {
        ++pos;
        continue;
      }
      if (pos < end && '}' == *pos) {
        ++pos;
        break;
      }
      ret = OB_ERR_UNEXPECTED;
      break;
    }
  }
  return ret;
}

int ObFileScanUtils::read_jsonl_line(FILE *file,
                                     common::ObIAllocator &allocator,
                                     common::ObString &line,
                                     bool &is_eof)
{
  int ret = OB_SUCCESS;
  is_eof = false;
  line = common::ObString();
  std::string text;
  char chunk[4096];
  while (nullptr != fgets(chunk, sizeof(chunk), file)) {
    text.append(chunk);
    if (!text.empty() && '\n' == text[text.size() - 1]) {
      break;
    }
  }
  if (text.empty()) {
    is_eof = true;
  } else {
    // the line break of the file is not part of the row
    int64_t length = text.size();
    while (length > 0 && ('\n' == text[length - 1] || '\r' == text[length - 1])) {
      --length;
    }
    char *buf = static_cast<char *>(allocator.alloc(length + 1));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(buf, text.c_str(), length);
      buf[length] = '\0';
      line.assign_ptr(buf, length);
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
  FILE *file = fopen(path.ptr(), "rb");
  if (OB_ISNULL(file)) {
    ret = OB_IO_ERROR;
    LOG_WARN("cannot open the jsonl file", K(ret), K(path));
  } else {
    // The rows are read into a memory of their own, so the memory of the
    // caller only holds the names of the columns.
    common::ObArenaAllocator row_allocator;
    common::ObArray<common::ObString> names;
    common::ObArray<ObFileColumnType> types;
    common::ObArray<bool> nullable;
    common::ObArray<ObFileJsonField> fields;
    common::ObString line;
    bool is_eof = false;
    int64_t read_rows = 0;
    while (OB_SUCC(ret) && read_rows < MAX_INFER_ROWS) {
      if (OB_FAIL(read_jsonl_line(file, row_allocator, line, is_eof))) {
        break;
      }
      if (is_eof) {
        break;
      }
      ++read_rows;
      fields.reset();
      if (OB_FAIL(parse_json_object(line.ptr(), line.ptr() + line.length(), row_allocator, fields))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cannot read the row of the jsonl file", K(ret));
        break;
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < fields.count(); ++i) {
        const ObFileJsonField &field = fields.at(i);
        int64_t index = -1;
        for (int64_t j = 0; index < 0 && j < names.count(); ++j) {
          if (names.at(j).length() == field.name_.length()
              && 0 == memcmp(names.at(j).ptr(), field.name_.ptr(), field.name_.length())) {
            index = j;
          }
        }
        if (index < 0) {
          index = names.count();
          // the name of a column lives as long as the statement
          char *name_buf = static_cast<char *>(allocator.alloc(field.name_.length() + 1));
          if (OB_ISNULL(name_buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            break;
          }
          MEMCPY(name_buf, field.name_.ptr(), field.name_.length());
          name_buf[field.name_.length()] = '\0';
          if (OB_FAIL(names.push_back(common::ObString(field.name_.length(), name_buf)))
              || OB_FAIL(types.push_back(field.type_))
              || OB_FAIL(nullable.push_back(field.is_null_))) {
            break;
          }
        } else if (field.is_null_) {
          nullable.at(index) = true;
        } else {
          types.at(index) = widen_type(types.at(index), field.type_);
        }
      }
    }
    fclose(file);
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < names.count(); ++i) {
        ObFileColumnMeta meta;
        meta.name_ = names.at(i);
        meta.type_ = types.at(i);
        meta.nullable_ = nullable.at(i);
        if (OB_FAIL(columns.push_back(meta))) {
          break;
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(stat_file(path, stamp))) {
      stamp = ObFileStamp();
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
