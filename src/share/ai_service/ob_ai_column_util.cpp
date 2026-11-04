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

#define USING_LOG_PREFIX SHARE
#include "share/ai_service/ob_ai_column_util.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_errno.h"
#include "share/schema/ob_column_schema.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace share
{

// packed layout (fields separated by '\x01'):
//   "\x01AICOL\x01" kind '\x01' max_retries '\x01' dims '\x01' model '\x01' prompt '\x01' sys '\x01' deps '\x01' sync_mode
int ObAIColumnSpec::pack(ObIAllocator &allocator, ObString &packed) const
{
  int ret = OB_SUCCESS;
  ObSqlString str;
  if (OB_FAIL(str.append(ObAIColumnSpec::packed_prefix()))) {
  } else if (OB_FAIL(str.append(is_embed_ ? "E" : "C"))) {
  } else if (OB_FAIL(str.append_fmt("\x01%ld\x01%ld\x01", max_retries_, dims_))) {
  } else if (OB_FAIL(str.append(model_))) {
  } else if (OB_FAIL(str.append("\x01"))) {
  } else if (OB_FAIL(str.append(prompt_))) {
  } else if (OB_FAIL(str.append("\x01"))) {
  } else if (OB_FAIL(str.append(sys_prompt_))) {
  } else if (OB_FAIL(str.append("\x01"))) {
  } else if (OB_FAIL(str.append(deps_))) {
  } else if (OB_FAIL(str.append("\x01"))) {
  } else if (OB_FAIL(str.append(sync_mode_))) {
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ob_write_string(allocator, str.string(), packed))) {
    }
  }
  return ret;
}

int ObAIColumnSpec::unpack(const ObString &packed)
{
  int ret = OB_SUCCESS;
  static const ObString PREFIX = ObString::make_string("\x01" "AICOL" "\x01");
  if (!ObAIColumnSpec::is_ai_packed(packed)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const char *begin = packed.ptr() + PREFIX.length();
    const char *end = packed.ptr() + packed.length();
    const int64_t NFIELD = 8;
    const char *fields[8] = {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL};
    int64_t lens[8] = {0, 0, 0, 0, 0, 0, 0, 0};
    const char *cur = begin;
    for (int i = 0; OB_SUCC(ret) && i < NFIELD; ++i) {
      fields[i] = cur;
      const char *sep = (i < NFIELD - 1) ? static_cast<const char *>(memchr(cur, '\x01', end - cur)) : end;
      if (NULL == sep || sep > end) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        lens[i] = sep - cur;
        cur = sep + 1;
      }
    }
    if (OB_SUCC(ret)) {
      if (lens[0] != 1 || (fields[0][0] != 'C' && fields[0][0] != 'E')) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        is_embed_ = (fields[0][0] == 'E');
        char buf[32] = {0};
        int64_t n = lens[1] < 31 ? lens[1] : 31;
        MEMCPY(buf, fields[1], n);
        buf[n] = '\0';
        max_retries_ = atoi(buf);
        MEMSET(buf, 0, sizeof(buf));
        n = lens[2] < 31 ? lens[2] : 31;
        MEMCPY(buf, fields[2], n);
        buf[n] = '\0';
        dims_ = atoi(buf);
        model_.assign_ptr(fields[3], static_cast<ObString::obstr_size_t>(lens[3]));
        prompt_.assign_ptr(fields[4], static_cast<ObString::obstr_size_t>(lens[4]));
        sys_prompt_.assign_ptr(fields[5], static_cast<ObString::obstr_size_t>(lens[5]));
        deps_.assign_ptr(fields[6], static_cast<ObString::obstr_size_t>(lens[6]));
        sync_mode_.assign_ptr(fields[7], static_cast<ObString::obstr_size_t>(lens[7]));
      }
    }
  }
  return ret;
}

int ObAIColumnSpec::get_column_ai_spec(const ObColumnSchemaV2 &column, ObAIColumnSpec &spec)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<ObString> &infos = column.get_extended_type_info();
  bool found = false;
  for (int64_t i = 0; !found && i < infos.count(); ++i) {
    if (is_ai_packed(infos.at(i))) {
      found = true;
      if (OB_FAIL(spec.unpack(infos.at(i)))) {
      }
    }
  }
  if (OB_SUCC(ret) && !found) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

bool ObAIColumnSpec::column_is_ai_result(const ObColumnSchemaV2 &column)
{
  const common::ObIArray<ObString> &infos = column.get_extended_type_info();
  bool found = false;
  for (int64_t i = 0; !found && i < infos.count(); ++i) {
    if (is_ai_packed(infos.at(i))) {
      found = true;
    }
  }
  return found;
}

int ObAIColumnUtil::state_col_name(ObIAllocator &alloc, const ObString &col, ObString &name)
{
  int ret = OB_SUCCESS;
  char *buf = static_cast<char *>(alloc.alloc(col.length() + 16));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMCPY(buf, "__ai_", 5);
    MEMCPY(buf + 5, col.ptr(), col.length());
    MEMCPY(buf + 5 + col.length(), "_state", 6);
    name.assign_ptr(buf, static_cast<ObString::obstr_size_t>(5 + col.length() + 6));
  }
  return ret;
}

int ObAIColumnUtil::request_col_name(ObIAllocator &alloc, const ObString &col, ObString &name)
{
  int ret = OB_SUCCESS;
  char *buf = static_cast<char *>(alloc.alloc(col.length() + 18));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMCPY(buf, "__ai_", 5);
    MEMCPY(buf + 5, col.ptr(), col.length());
    MEMCPY(buf + 5 + col.length(), "_request", 8);
    name.assign_ptr(buf, static_cast<ObString::obstr_size_t>(5 + col.length() + 8));
  }
  return ret;
}

int ObAIColumnUtil::result_spec_col_name(ObIAllocator &alloc, const ObString &col, ObString &name)
{
  int ret = OB_SUCCESS;
  char *buf = static_cast<char *>(alloc.alloc(col.length() + 22));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMCPY(buf, "__ai_", 5);
    MEMCPY(buf + 5, col.ptr(), col.length());
    MEMCPY(buf + 5 + col.length(), "_result_spec", 12);
    name.assign_ptr(buf, static_cast<ObString::obstr_size_t>(5 + col.length() + 12));
  }
  return ret;
}

void ObAIColumnUtil::type_system_prompt(const bool is_json, const bool is_bigint, ObString &sys)
{
  if (is_json) {
    sys = ObString::make_string("Return exactly one valid JSON value. Do not output anything else.");
  } else if (is_bigint) {
    sys = ObString::make_string("Return exactly one base-10 integer. Do not output anything else.");
  } else {
    sys.reset();
  }
}

} // namespace share
} // namespace oceanbase
