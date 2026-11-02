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

#ifndef OCEANBASE_STORAGE_OB_PARQUET_READER_H_
#define OCEANBASE_STORAGE_OB_PARQUET_READER_H_

#include <stdio.h>

#include <string>

#include "lib/container/ob_array.h"
#include "lib/string/ob_string.h"
#include "storage/ob_file_scan_utils.h"

namespace oceanbase
{
namespace storage
{

/**
 * @brief A reader of the rows of a parquet file.
 *
 * The reader reads the schema of the file from the footer of the file and the
 * rows of the file from the pages of the file. The values are read as the
 * text of the value, so the reader of a parquet file and the reader of a csv
 * file agree on what a cell means.
 *
 * Only the values that are needed are read: the pages of a column are read
 * when the row that needs them is read, so the memory of the reader holds a
 * page of every column and not the whole file.
 */
class ObParquetReader
{
public:
  ObParquetReader();
  ~ObParquetReader();

  int open(const common::ObString &path, common::ObIAllocator &allocator);
  void close();
  bool is_open() const { return nullptr != file_; }

  /// The columns of the file.
  const common::ObArray<ObFileColumnMeta> &get_columns() const { return columns_; }

  /**
   * @brief Reads the next row of the file.
   *
   * The cells hold the text of the values, a cell of a value that is not
   * there is null. The reader returns OB_ITER_END when the file has no more
   * rows.
   */
  int next_row(common::ObIArray<ObFileCell> &cells);

private:
  /// The metadata of the chunk of one column of one row group.
  struct ObParquetChunk
  {
    // the offset of the first page of the chunk
    int64_t page_offset_;
    // the offset of the end of the chunk
    int64_t end_offset_;
    // the compression of the pages of the chunk
    int32_t codec_;
    // the encoding of the values of the pages of the chunk
    int32_t encoding_;

    ObParquetChunk() : page_offset_(0), end_offset_(0), codec_(0), encoding_(0) {}
  };

  /// The metadata of one row group of the file.
  struct ObParquetRowGroup
  {
    int64_t num_rows_;
    common::ObArray<ObParquetChunk> chunks_;

    ObParquetRowGroup() : num_rows_(0), chunks_() {}
  };

  /// The state of the reader of the values of one column of the file.
  struct ObParquetColumn
  {
    // the type of the values of the column
    int32_t type_;
    // the number of the bytes of a fixed length value
    int32_t type_length_;
    // the type of the values that has a text of its own
    int32_t converted_type_;
    // the level of the definition of a value that is not null
    int32_t max_def_level_;
    // the offset of the page that is read next
    int64_t page_offset_;
    // the values of the page that is read now
    common::ObArray<ObFileCell> values_;
    // the index of the value of the page that is read next
    int64_t value_index_;
    // the text of the values that had to be written again
    std::string text_;

    ObParquetColumn()
        : type_(0), type_length_(0), converted_type_(0), max_def_level_(0), page_offset_(0), values_(),
          value_index_(0), text_()
    {
    }
  };

  int read_footer();
  int read_schema(common::ObIAllocator &allocator);
  int read_row_groups(common::ObIAllocator &allocator);
  int read_page_header(int64_t offset,
                       int32_t &page_type,
                       int64_t &uncompressed_size,
                       int64_t &compressed_size,
                       int64_t &num_values,
                       int32_t &encoding,
                       int32_t &def_levels_length,
                       int32_t &rep_levels_length,
                       bool &is_compressed,
                       int64_t &data_offset);
  int read_next_page(ObParquetColumn &column, const ObParquetChunk &chunk);
  int decode_values(ObParquetColumn &column,
                    const char *data,
                    int64_t length,
                    int64_t num_values,
                    const common::ObArray<int32_t> &def_levels);

private:
  FILE *file_;
  // the memory of the metadata of the file
  common::ObIAllocator *allocator_;
  // the text of the footer of the file
  char *footer_;
  int64_t footer_length_;
  // the columns of the file
  common::ObArray<ObFileColumnMeta> columns_;
  // the reader of the values of every column
  common::ObArray<ObParquetColumn> column_readers_;
  // the row groups of the file
  common::ObArray<ObParquetRowGroup> row_groups_;
  // the index of the row group that is read now
  int64_t row_group_index_;
  // the number of the rows of the row group that is read now
  int64_t row_group_rows_read_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_PARQUET_READER_H_
