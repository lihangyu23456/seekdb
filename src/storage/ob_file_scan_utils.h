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

#ifndef OB_FILE_SCAN_UTILS_H_
#define OB_FILE_SCAN_UTILS_H_

#include <stdint.h>
#include <stdio.h>

#include "lib/allocator/ob_malloc.h"
#include "lib/container/ob_array.h"
#include "lib/oblog/ob_log.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace storage
{

/**
 * @brief The type of a column that was read from a file.
 *
 * The names follow the SQL types of the database, so the planner can use them
 * without another mapping step.
 */
enum class ObFileColumnType : int8_t {
  NULL_TYPE = 0, // every value of the column was NULL
  BIGINT,
  DOUBLE,
  BOOLEAN,
  VARCHAR,
  DATE,
  DATETIME,
};

/**
 * @brief One column of an inferred file schema.
 */
struct ObFileColumnMeta {
  common::ObString name_;
  ObFileColumnType type_;
  bool nullable_;
  ObFileColumnMeta() : name_(), type_(ObFileColumnType::VARCHAR), nullable_(true) {}
};

/**
 * @brief The identity of a file, used to notice that the file changed.
 */
struct ObFileStamp {
  int64_t size_;
  int64_t mtime_;
  ObFileStamp() : size_(-1), mtime_(-1) {}
  bool operator==(const ObFileStamp &other) const
  {
    return size_ == other.size_ && mtime_ == other.mtime_;
  }
};

/**
 * @brief A value of one row of a file.
 *
 * The value keeps the text of the file: the conversion to the column type
 * happens when the value is read, so the reader and the type inference always
 * agree on what a cell means.
 */
struct ObFileCell {
  const char *data_;
  int64_t length_;
  bool is_null_;
  ObFileCell() : data_(nullptr), length_(0), is_null_(true) {}
  ObFileCell(const char *data, int64_t length, bool is_null)
      : data_(data), length_(length), is_null_(is_null)
  {
  }
  common::ObString string() const { return common::ObString(length_, data_); }
};

/**
 * @brief Reads a CSV file row by row.
 *
 * The reader understands the usual CSV rules: a header row, UTF-8, LF and CRLF
 * line endings, quoted fields, doubled quotes and line breaks inside a quoted
 * field. The file is read in chunks, so a large file is never held in memory.
 */
class ObCsvFileReader
{
public:
  ObCsvFileReader();
  ~ObCsvFileReader();
  int open(const common::ObString &path);
  void close();
  bool is_open() const { return file_ != nullptr; }
  /**
   * @brief Reads the next row of a file.
   *
   * The fields point into the reader's buffer, so they stay valid until the
   * next call.
   */
  int next_row(common::ObIArray<ObFileCell> &cells);

  /// Reads the next row, the end of the file is reported with OB_ITER_END.
  int read_line(common::ObIArray<ObFileCell> &cells, bool &is_eof);

  /**
   * @brief The header of the file, the names point into the reader's buffer.
   */
  int header(common::ObIArray<common::ObString> &names);
  const ObFileStamp &stamp() const { return stamp_; }

private:
  int fill();
  int parse_row(const char *begin, const char *end, common::ObIArray<ObFileCell> &cells);

private:
  FILE *file_;
  char *buffer_;
  int64_t buffer_size_;
  int64_t buffer_begin_;
  int64_t buffer_end_;
  bool eof_;
  ObFileStamp stamp_;
  common::ObArray<common::ObString> header_names_;
  bool header_read_;
};

/**
 * @brief Infers the schema of a file and reads its rows.
 *
 * The inference and the reader share the same parsing rules, so the columns
 * always describe the values that the scan returns.
 */
class ObFileScanUtils
{
public:
  /// The file formats the table functions understand.
  enum class ObFileFormat : int8_t {
    CSV = 0,
    JSONL,
    PARQUET,
    GLOB,
  };

  /**
   * @brief Infers the columns of a CSV file.
   *
   * The header gives the names, the rows give the types. A column that only
   * holds NULL becomes a NULL column, and a column that holds a wider value
   * later widens its type.
   */
  static int infer_csv_schema(const common::ObString &path,
                              common::ObIAllocator &allocator,
                              common::ObIArray<ObFileColumnMeta> &columns,
                              ObFileStamp &stamp);

  /**
   * @brief Infers the columns of a JSON lines file.
   *
   * The fields of all the rows form the schema, a field that a row does not
   * have is NULL in that row. The rows are parsed by the json reader of the
   * database, so the values keep the type they have in the document.
   */
  static int infer_jsonl_schema(const common::ObString &path,
                                common::ObIAllocator &allocator,
                                common::ObIArray<ObFileColumnMeta> &columns,
                                ObFileStamp &stamp);

  /**
   * @brief Infers the type of one value.
   *
   * The value is widened while the rows are read: an integer that does not fit
   * becomes a double, a value that is not a number becomes text.
   */
  static ObFileColumnType infer_type(const char *data, int64_t length, bool is_null);

  /// Merges the type of a value into the type of the column.
  static ObFileColumnType widen_type(ObFileColumnType column_type, ObFileColumnType value_type);

  /// The name of a type, used by DESCRIBE.
  static const char *type_name(ObFileColumnType type);

  /// The stat of a file, used to notice that the file changed.
  static int stat_file(const common::ObString &path, ObFileStamp &stamp);

  /**
   * @brief Checks that a path may be read.
   *
   * The path has to stay inside the directory that secure_file_priv allows, so
   * a query can not walk out of it with ../ or a symbolic link.
   */
  static int check_path(const common::ObString &path, const common::ObString &secure_file_priv);

  /**
   * @brief Lists the files that match a pattern.
   *
   * The pattern is the path of a directory followed by a wildcard, so the
   * files of the directory are found. The paths are returned in the order of
   * the pattern.
   */
  static int list_files(const common::ObString &pattern,
                         common::ObIAllocator &allocator,
                         common::ObIArray<common::ObString> &files);

  /// The directory part of a path.
  static common::ObString dir_name(const common::ObString &path);

  /// Resolves a path that may hold the glob pattern.
  static common::ObString absolute_path(const common::ObString &path);
};

} // namespace storage
} // namespace oceanbase

#endif // OB_FILE_SCAN_UTILS_H_
