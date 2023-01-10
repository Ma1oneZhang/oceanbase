/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_OB_DDL_CHECKSUM_H_
#define OCEANBASE_SHARE_OB_DDL_CHECKSUM_H_

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashmap.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/schema/ob_table_param.h"

namespace oceanbase
{
namespace share
{

struct ObDDLChecksumItem
{
  ObDDLChecksumItem()
    : execution_id_(-1), tenant_id_(common::OB_INVALID_ID),
      table_id_(common::OB_INVALID_ID), ddl_task_id_(0),
      column_id_(common::OB_INVALID_ID), task_id_(common::OB_INVALID_ID), checksum_(0)
  {}
  ~ObDDLChecksumItem() {};
  bool is_valid() const
  {
    return 0 <= execution_id_
        && common::OB_INVALID_ID != tenant_id_
        && common::OB_INVALID_ID != table_id_
        && 0 < ddl_task_id_
        && common::OB_INVALID_ID != column_id_;
  }
  TO_STRING_KV(K_(execution_id), K_(tenant_id), K_(table_id),
      K_(ddl_task_id), K_(column_id), K_(task_id), K_(checksum));
  int64_t execution_id_;
  uint64_t tenant_id_;
  uint64_t table_id_;
  int64_t ddl_task_id_;
  int64_t column_id_;
  uint64_t task_id_;
  int64_t checksum_;
};

class ObDDLChecksumOperator
{
public:
  static int update_checksum(
      const uint64_t tenant_id,
      const int64_t table_id,
      const int64_t ddl_task_id,
      const common::ObIArray<int64_t> &main_table_checksum,
      const common::ObIArray<int64_t> &col_ids,
      const int64_t schema_version,
      const int64_t task_idx,
      common::ObMySQLProxy &sql_proxy);
  static int update_checksum(const common::ObIArray<ObDDLChecksumItem> &checksum_items,
      common::ObMySQLProxy &sql_proxy);
  static int get_table_column_checksum(
      const uint64_t tenant_id,
      const int64_t execution_id,
      const uint64_t table_id,
      const int64_t ddl_task_id,
      common::hash::ObHashMap<int64_t, int64_t> &column_checksums, common::ObMySQLProxy &sql_proxy);
  static int get_tablet_checksum_record(
      const uint64_t tenant_id,
      const uint64_t execution_id,
      const uint64_t table_id,
      const int64_t ddl_task_id,
      ObIArray<ObTabletID> &tablet_ids,
      ObMySQLProxy &sql_proxy,
      common::hash::ObHashMap<uint64_t, bool> &tablet_checksum_map);
  static int check_column_checksum(
      const uint64_t tenant_id,
      const int64_t execution_id,
      const uint64_t data_table_id,
      const uint64_t index_table_id,
      const int64_t ddl_task_id,
      bool &is_equal,
      common::ObMySQLProxy &sql_proxy);
  static int delete_checksum(
      const uint64_t tenant_id,
      const int64_t execution_id,
      const uint64_t source_table_id,
      const uint64_t dest_table_id,
      const int64_t ddl_task_id,
      common::ObMySQLProxy &sql_proxy);
private:
  static int fill_one_item(const ObDDLChecksumItem &item,
      share::ObDMLSqlSplicer &dml);
  static int get_column_checksum(
      const common::ObSqlString &sql,
      const uint64_t tenant_id,
      common::hash::ObHashMap<int64_t, int64_t> &column_checksum_map,
      common::ObMySQLProxy &sql_proxy);
  static int get_tablet_checksum_status(
      const ObSqlString &sql,
      const uint64_t tenant_id,
      ObIArray<uint64_t> &batch_tablet_ids,
      ObIArray<ObColDesc> &column_ids,
      common::ObMySQLProxy &sql_proxy,
      common::hash::ObHashMap<uint64_t, ObArray<int64_t>> &tablet_columns_map,
      common::hash::ObHashMap<uint64_t, bool> &tablet_checksum_status_map);
  static int construct_tablet_column_map(
      const uint64_t tablet_id,
      const int64_t column_id,
      common::hash::ObHashMap<uint64_t, ObArray<int64_t>> &tablet_columns_map);
};

}  // end namespace share
}  // end namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_DDL_CHECKSUM_H_
