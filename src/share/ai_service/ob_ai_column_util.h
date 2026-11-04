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

#ifndef OCEANBASE_SHARE_OB_AI_COLUMN_UTIL_H_
#define OCEANBASE_SHARE_OB_AI_COLUMN_UTIL_H_

#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "lib/allocator/ob_allocator.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObColumnSchemaV2;
}
using schema::ObColumnSchemaV2;

// Persisted specification of one AI generated column.
// Serialized into the result column's extended_type_info_[0] with a marker
// prefix, and mirrored into oceanbase.__all_ai_column at DDL time.
struct ObAIColumnSpec
{
  ObAIColumnSpec() : is_embed_(false), max_retries_(3), dims_(0) {}
  bool is_embed_;            // true: AI_EMBED, false: AI_COMPLETE
  common::ObString model_;   // ai model name (first arg)
  common::ObString prompt_;  // prompt / content expression SQL text (second arg)
  common::ObString sys_prompt_;  // type-specific system prompt
  common::ObString deps_;    // comma separated dependency column names
  int64_t max_retries_;
  int64_t dims_;             // explicit dims for AI_EMBED, 0 = not specified
  common::ObString sync_mode_;  // 'async' (only async is acted upon)

  // field separator inside the packed value; control bytes never appear in SQL text
  static const char *packed_prefix() { return "\x01" "AICOL" "\x01"; }

  int pack(common::ObIAllocator &allocator, common::ObString &packed) const;
  int unpack(const common::ObString &packed);
  static bool is_ai_packed(const common::ObString &v)
  {
    const common::ObString prefix(common::ObString::make_string("\x01" "AICOL" "\x01"));
    return v.length() >= prefix.length()
           && 0 == memcmp(v.ptr(), prefix.ptr(), prefix.length());
  }
  // extract the packed spec from a column's extended_type_info, if present
  static int get_column_ai_spec(const ObColumnSchemaV2 &column, ObAIColumnSpec &spec);
  static bool column_is_ai_result(const ObColumnSchemaV2 &column);
};

class ObAIColumnUtil
{
public:
  // hidden companion column names of one AI column
  static int state_col_name(common::ObIAllocator &alloc, const common::ObString &col, common::ObString &name);
  static int request_col_name(common::ObIAllocator &alloc, const common::ObString &col, common::ObString &name);
  static int result_spec_col_name(common::ObIAllocator &alloc, const common::ObString &col, common::ObString &name);
  // system prompt by result type: json -> valid JSON value, bigint -> base-10 integer
  static void type_system_prompt(const bool is_json, const bool is_bigint, common::ObString &sys);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_AI_COLUMN_UTIL_H_
