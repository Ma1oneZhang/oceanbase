/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#include "lib/ob_errno.h"
#include "lib/thread/ob_thread_name.h"
#include <atomic>
#include <future>
#define USING_LOG_PREFIX RS
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/utility/ob_tracepoint.h"
#include "ob_lob_meta_builder.h"
#include "ob_lob_piece_builder.h"
#include "rootserver/ob_balance_group_ls_stat_operator.h"
#include "rootserver/ob_ddl_help.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_index_builder.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/ob_schema2ddl_sql.h"
#include "rootserver/ob_tenant_thread_helper.h" //get_zone_priority
#include "rootserver/ob_unit_manager.h"
#include "rootserver/ob_vertical_partition_builder.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ls/ob_ls_life_manager.h" //ObLSLifeAgentManager
#include "share/ls/ob_ls_operator.h"
#include "share/ob_autoincrement_service.h"
#include "share/ob_freeze_info_proxy.h"
#include "share/ob_global_context_operator.h"
#include "share/ob_index_builder_util.h"
#include "share/ob_primary_zone_util.h"
#include "share/ob_service_epoch_proxy.h"
#include "share/schema/ob_context_ddl_proxy.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/schema/ob_schema_printer.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_table_schema.h"
#include "share/sequence/ob_sequence_ddl_proxy.h"
#include "sql/resolver/ob_stmt_type.h"
#ifdef OB_BUILD_ARBITRATION
#include "share/arbitration_service/ob_arbitration_service_table_operator.h"
#include "share/arbitration_service/ob_arbitration_service_utils.h" // ObArbitrationServiceUtils
#endif
#ifdef OB_BUILD_ORACLE_PL
#include "pl/sys_package/ob_dbms_audit_mgmt.h" // ObDbmsAuditMgmt
#endif

namespace oceanbase {
using namespace common;
using namespace share;
using namespace obrpc;
using namespace storage;
using namespace palf;
namespace rootserver {

class parallel_DDL {
public:
  parallel_DDL(ObDDLOperator &ddl_operator, ObMySQLTransaction &trans,
               common::ObIArray<ObTableSchema> &tables)
      : tables_(tables), ddl_operator_(ddl_operator), trans_(trans) {
    for (int i = 0; i < tables.count(); i++) {
      auto &table = tables.at(i);
      mapping[table.get_table_id()] = i;
    }
    for (int i = 0; i < tables.count(); i++) {
      const auto &table = tables.at(i);
      for (int j = 0; j < table.get_depend_table_ids().count(); j++) {
        add(table.get_table_id(), table.get_depend_table_ids().at(j));
      }
    }
  }
  void add(int a, int b) {
    a = mapping[a];
    b = mapping[b];
    mp[b]++;
    e[idx] = b;
    ne[idx] = h[a];
    h[a] = ++idx;
  };

  void reduce(int table_th) {
    int ret = OB_SUCCESS;
    for (int i = h[table_th]; i != 0; i = ne[i]) {
      mp[e[i]]--;
      if (mp[e[i]].load(std::memory_order_seq_cst) == 0) {
        auto b = e[i];
        LOG_WARN("table_th = ", K(b));
        OB_ASSERT(b < tables_.count());
        auto future = start_create(b);
        future.get();
        reduce(b);
      }
    }
  }

  std::future<void> start_create(int64_t table_th) {
    int ret = OB_SUCCESS;
    auto future = std::async([&, table_th]() {
      set_thread_name("DDLQueueTh");
      ObTableSchema &table = tables_.at(table_th);
      const int64_t table_id = table.get_table_id();
      const ObString &table_name = table.get_table_name();
      const ObString *ddl_stmt = NULL;
      bool need_sync_schema_version =
          !(ObSysTableChecker::is_sys_table_index_tid(table_id) ||
            is_sys_lob_table(table_id));
      if (OB_FAIL(ddl_operator_.create_table(table, trans_, ddl_stmt,
                                             need_sync_schema_version,
                                             false /*is_truncate_table*/))) {
        LOG_WARN("add table schema failed", KR(ret), K(table_id),
                 K(table_name));
      } else {
        LOG_INFO("add table schema succeed", K(table_th), K(table_id),
                 K(table_name));
      }
      reduce(table_th);
    });
    return future;
  }

  void start() {
    std::vector<std::future<void>> futures;
    int cnt = 0;
    for (int64_t i = 16; i < tables_.count(); i++) {
      const auto &table = tables_.at(i);
      auto depend_num = table.get_depend_table_ids().count();
      if (depend_num == 0) {
        futures.push_back(start_create(i));
        cnt++;
      }
    }
    for (auto &future : futures) {
      future.get();
    }
    LOG_INFO("[MYINFO] cnt = ", K(cnt));
  }

private:
  std::unordered_map<int, std::atomic<int>> mp;
  std::unordered_map<int, int> mapping;
  std::unordered_map<int, int> h, e, ne;
  int idx{0};
  common::ObIArray<ObTableSchema> &tables_;
  ObDDLOperator &ddl_operator_;
  ObMySQLTransaction &trans_;
};
int ObDDLService::create_sys_table_schemas(
    ObDDLOperator &ddl_operator, ObMySQLTransaction &trans,
    common::ObIArray<ObTableSchema> &tables) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init", KR(ret));
  } else if (OB_ISNULL(sql_proxy_) || OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP_(sql_proxy), KP_(schema_service));
  } else {
    // persist __all_core_table's schema in inner table, which is only used for
    // sys views.
    parallel_DDL pddl(ddl_operator, trans, tables);
    for (int64_t i = 0; i < 16; i++) {
      ObTableSchema &table = tables.at(i);
      const int64_t table_id = table.get_table_id();
      const ObString &table_name = table.get_table_name();
      const ObString *ddl_stmt = NULL;
      bool need_sync_schema_version =
          !(ObSysTableChecker::is_sys_table_index_tid(table_id) ||
            is_sys_lob_table(table_id));
      if (OB_FAIL(ddl_operator.create_table(table, trans, ddl_stmt,
                                            need_sync_schema_version,
                                            false /*is_truncate_table*/))) {
        LOG_WARN("add table schema failed", KR(ret), K(table_id),
                 K(table_name));
      } else {
        LOG_INFO("add table schema succeed", K(i), K(table_id), K(table_name));
      }
    }
    pddl.start();
  }
  // for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); i++) {
  //   ObTableSchema &table = tables.at(i);
  //   const int64_t table_id = table.get_table_id();
  //   const ObString &table_name = table.get_table_name();
  //   const ObString *ddl_stmt = NULL;
  //   bool need_sync_schema_version =
  //       !(ObSysTableChecker::is_sys_table_index_tid(table_id) ||
  //         is_sys_lob_table(table_id));
  //   if (OB_FAIL(ddl_operator.create_table(table, trans, ddl_stmt,
  //                                         need_sync_schema_version,
  //                                         false /*is_truncate_table*/))) {
  //     LOG_WARN("add table schema failed", KR(ret), K(table_id),
  //              K(table_name));
  //   } else {
  //     LOG_INFO("add table schema succeed", K(i), K(table_id),
  //     K(table_name));
  //   }
  // }

  return ret;
}
} // namespace rootserver
} // namespace oceanbase