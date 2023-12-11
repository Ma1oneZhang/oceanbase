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
#define USING_LOG_PREFIX RS
#include <future>
#include <vector>

#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_tracepoint.h"
#include "ob_lob_meta_builder.h"
#include "ob_lob_piece_builder.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "rootserver/freeze/ob_major_freeze_helper.h"
#include "rootserver/ob_balance_group_ls_stat_operator.h"
#include "rootserver/ob_ddl_help.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_ddl_sql_generator.h"
#include "rootserver/ob_index_builder.h"
#include "rootserver/ob_locality_util.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/ob_schema2ddl_sql.h"
#include "rootserver/ob_table_creator.h"
#include "rootserver/ob_tenant_thread_helper.h" //get_zone_priority
#include "rootserver/ob_unit_manager.h"
#include "rootserver/ob_vertical_partition_builder.h"
#include "share/config/ob_server_config.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ls/ob_ls_creator.h"
#include "share/ls/ob_ls_life_manager.h" //ObLSLifeAgentManager
#include "share/ls/ob_ls_operator.h"
#include "share/ob_autoincrement_service.h"
#include "share/ob_freeze_info_proxy.h"
#include "share/ob_global_context_operator.h"
#include "share/ob_global_stat_proxy.h"
#include "share/ob_index_builder_util.h"
#include "share/ob_leader_election_waiter.h"
#include "share/ob_primary_standby_service.h" // ObPrimaryStandbyService
#include "share/ob_primary_zone_util.h"
#include "share/ob_service_epoch_proxy.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/schema/ob_context_ddl_proxy.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/schema/ob_schema_printer.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_table_schema.h"
#include "share/scn.h"
#include "share/sequence/ob_sequence_ddl_proxy.h"
#include "sql/resolver/ob_stmt_type.h"
#include "storage/tx/ob_ts_mgr.h"
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

int ObDDLService::create_sys_table_schemas(
    ObDDLOperator &ddl_operator, ObMySQLTransaction &trans,
    common::ObIArray<ObTableSchema> &tables, int start, int end,
    bool is_parallel) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init", KR(ret));
  } else if (OB_ISNULL(sql_proxy_) || OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP_(sql_proxy), KP_(schema_service));
  } else {
    // persist __all_core_table's schema in inner table, which is only used for
    // sys views.
    for (int64_t i = start; OB_SUCC(ret) && i < end; i++) {
      ObTableSchema &table = tables.at(i);
      const int64_t table_id = table.get_table_id();
      const ObString &table_name = table.get_table_name();
      const ObString *ddl_stmt = NULL;
      // bool need_sync_schema_version =
      //     !(ObSysTableChecker::is_sys_table_index_tid(table_id) ||
      //       is_sys_lob_table(table_id));
      bool need_sync_schema_version = false;
      if (is_core_table(table.get_table_id())) {
        if (i != tables.count() - 1) {
          if (!is_core_table(tables.at(i + 1).get_table_id())) {
            table.refresh_schema_ = true;
          } else {
            table.refresh_schema_ = false;
          }
        } else {
          table.refresh_schema_ = true;
        }
      } else {
        table.refresh_schema_ = false;
      }
      if (is_parallel && table.get_depend_table_ids().count() == 0) {
        if (OB_FAIL(ddl_operator.create_table(table, trans, ddl_stmt,
                                              need_sync_schema_version,
                                              false /*is_truncate_table*/))) {
          LOG_WARN("add table schema failed", K(i), KR(ret), K(table_id),
                   K(table_name));
        } else {
          LOG_INFO("add table schema succeed", K(i), K(table_id),
                   K(table_name));
        }
      } else if (!is_parallel && table.get_depend_table_ids().count() != 0) {
        if (OB_FAIL(ddl_operator.create_table(table, trans, ddl_stmt,
                                              need_sync_schema_version,
                                              false /*is_truncate_table*/))) {
          LOG_WARN("add table schema failed", KR(ret), K(table_id),
                   K(table_name));
        } else {
          LOG_INFO("add table schema succeed", K(i), K(table_id),
                   K(table_name));
        }
      }
    }
  }
  return ret;
}

int ObDDLService::init_tenant_schema(
    const uint64_t tenant_id, const ObTenantSchema &tenant_schema,
    const share::ObTenantRole &tenant_role, const SCN &recovery_until_scn,
    common::ObIArray<ObTableSchema> &tables, ObSysVariableSchema &sys_variable,
    const common::ObIArray<common::ObConfigPairs> &init_configs,
    bool is_creating_standby, const common::ObString &log_restore_source) {
  const int64_t start_time = ObTimeUtility::fast_current_time();
  LOG_INFO("[CREATE_TENANT] STEP 2.4. start init tenant schemas", K(tenant_id));
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init", KR(ret));
  } else if (OB_UNLIKELY(!recovery_until_scn.is_valid_and_not_min() ||
                         (is_creating_standby && log_restore_source.empty()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(recovery_until_scn),
             K(is_creating_standby), K(log_restore_source));
  } else if (OB_ISNULL(sql_proxy_) || OB_ISNULL(schema_service_) ||
             OB_ISNULL(schema_service_->get_schema_service()) ||
             OB_ISNULL(GCTX.lst_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP_(sql_proxy), KP_(schema_service),
             KP(GCTX.lst_operator_));
  } else {
    ObSchemaService *schema_service_impl =
        schema_service_->get_schema_service();
    // 1. init tenant global stat
    if (OB_SUCC(ret)) {
      const int64_t core_schema_version = OB_CORE_SCHEMA_VERSION + 1;
      const int64_t baseline_schema_version = OB_INVALID_VERSION;
      const int64_t ddl_epoch = 0;
      const SCN snapshot_gc_scn = SCN::min_scn();
      // find compatible version
      uint64_t data_version = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < init_configs.count(); i++) {
        const ObConfigPairs &config = init_configs.at(i);
        if (tenant_id == config.get_tenant_id()) {
          for (int64_t j = 0; data_version == 0 && OB_SUCC(ret) &&
                              j < config.get_configs().count();
               j++) {
            const ObConfigPairs::ObConfigPair &pair =
                config.get_configs().at(j);
            if (0 != pair.key_.case_compare("compatible")) {
            } else if (OB_FAIL(ObClusterVersion::get_version(pair.value_.ptr(),
                                                             data_version))) {
              LOG_WARN("fail to get compatible version", KR(ret), K(tenant_id),
                       K(pair));
            }
          }
        }
      }

      common::ObMySQLTransaction trans;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(trans.start(sql_proxy_, tenant_id))) {
        LOG_WARN("failed to start trans", KR(ret), K(tenant_id));
      } else {
        ObGlobalStatProxy global_stat_proxy(trans, tenant_id);
        if (OB_FAIL(ret)) {
        } else if (0 == data_version) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("compatible version not defined", KR(ret), K(tenant_id),
                   K(init_configs));
        } else if (OB_FAIL(global_stat_proxy.set_tenant_init_global_stat(
                       core_schema_version, baseline_schema_version,
                       snapshot_gc_scn, ddl_epoch, data_version,
                       data_version))) {
          LOG_WARN("fail to set tenant init global stat", KR(ret), K(tenant_id),
                   K(core_schema_version), K(baseline_schema_version),
                   K(snapshot_gc_scn), K(ddl_epoch), K(data_version));
        } else if (is_user_tenant(tenant_id) &&
                   OB_FAIL(OB_PRIMARY_STANDBY_SERVICE.write_upgrade_barrier_log(
                       trans, tenant_id, data_version))) {
          LOG_WARN("fail to write_upgrade_barrier_log", KR(ret), K(tenant_id),
                   K(data_version));
        }
      }
      if (trans.is_started()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
          ret = OB_SUCC(ret) ? tmp_ret : ret;
        }
      }
    }

    constexpr int batch_size = 50;
    // 2. init tenant schema
    if (OB_SUCC(ret)) {
      ObDDLSQLTransaction trans(schema_service_, true, true, false, false);
      const int64_t init_schema_version = tenant_schema.get_schema_version();
      int64_t new_schema_version = OB_INVALID_VERSION;
      ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
      // FIXME:(yanmu.ztl) lock tenant's __all_core_table
      const int64_t refreshed_schema_version = 0;
      if (OB_FAIL(
              trans.start(sql_proxy_, tenant_id, refreshed_schema_version))) {
        LOG_WARN("fail to start trans", KR(ret), K(tenant_id));
      }
      constexpr int batch_size = 120;
      std::vector<std::future<void>> futures;
      for (int64_t i = 0; i < tables.count(); i += batch_size) {
        auto start = i, end = std::min(tables.count(), i + batch_size);
        futures.push_back(std::async([&, start, end]() {
          set_thread_name("DDLThQueue");
          ObDDLSQLTransaction trans(schema_service_, true, true, false, false);
          trans.start(sql_proxy_, tenant_id, refreshed_schema_version);
          create_sys_table_schemas(ddl_operator, trans, tables, start, end,
                                   true);
          if (ret != OB_SUCCESS) {
            LOG_WARN("fail to create sys tables", KR(ret), K(tenant_id));
          }
          trans.end(true);
        }));
      }
      for (auto &f : futures) {
        f.get();
      }
      create_sys_table_schemas(ddl_operator, trans, tables, 0, tables.count(),
                               false);
      if (ret != OB_SUCCESS) {
        LOG_WARN("fail to create sys tables", KR(ret), K(tenant_id));
      }
      if (is_user_tenant(tenant_id) && OB_FAIL(set_sys_ls_status(tenant_id))) {
        LOG_WARN("failed to set sys ls status", KR(ret), K(tenant_id));
      } else if (OB_FAIL(schema_service_impl->gen_new_schema_version(
                     tenant_id, init_schema_version, new_schema_version))) {
      } else if (OB_FAIL(ddl_operator.replace_sys_variable(
                     sys_variable, new_schema_version, trans,
                     OB_DDL_ALTER_SYS_VAR))) {
        LOG_WARN("fail to replace sys variable", KR(ret), K(sys_variable));
      } else if (OB_FAIL(ddl_operator.init_tenant_env(
                     tenant_schema, sys_variable, tenant_role,
                     recovery_until_scn, init_configs, trans))) {
        LOG_WARN("init tenant env failed", KR(ret), K(tenant_role),
                 K(recovery_until_scn), K(tenant_schema));
      } else if (OB_FAIL(ddl_operator.insert_tenant_merge_info(
                     OB_DDL_ADD_TENANT_START, tenant_schema, trans))) {
        LOG_WARN("fail to insert tenant merge info", KR(ret), K(tenant_schema));
      } else if (is_meta_tenant(tenant_id) &&
                 OB_FAIL(ObServiceEpochProxy::init_service_epoch(
                     trans, tenant_id, 0, /*freeze_service_epoch*/
                     0,                   /*arbitration_service_epoch*/
                     0,                   /*server_zone_op_service_epoch*/
                     0 /*heartbeat_service_epoch*/))) {
        LOG_WARN("fail to init service epoch", KR(ret));
      } else if (is_creating_standby &&
                 OB_FAIL(set_log_restore_source(gen_user_tenant_id(tenant_id),
                                                log_restore_source, trans))) {
        LOG_WARN("fail to set_log_restore_source", KR(ret), K(tenant_id),
                 K(log_restore_source));
      }

      if (trans.is_started()) {
        int temp_ret = OB_SUCCESS;
        bool commit = OB_SUCC(ret);
        if (OB_SUCCESS != (temp_ret = trans.end(commit))) {
          ret = (OB_SUCC(ret)) ? temp_ret : ret;
          LOG_WARN("trans end failed", K(commit), K(temp_ret));
        }
      }
    }
    if (OB_SUCC(ret) && is_meta_tenant(tenant_id)) {
      // If tenant config version in RS is valid first and ddl trans
      // doesn't commit, observer may read from empty
      // __tenant_parameter successfully and raise its tenant config
      // version, which makes some initial tenant configs are not
      // actually updated before related observer restarts. To fix
      // this problem, tenant config version in RS should be valid
      // after ddl trans commits.
      const int64_t config_version =
          omt::ObTenantConfig::INITIAL_TENANT_CONF_VERSION + 1;
      const uint64_t user_tenant_id = gen_user_tenant_id(tenant_id);
      if (OB_FAIL(
              OTC_MGR.set_tenant_config_version(tenant_id, config_version))) {
        LOG_WARN("failed to set tenant config version", KR(ret), K(tenant_id));
      } else if (OB_FAIL(OTC_MGR.set_tenant_config_version(user_tenant_id,
                                                           config_version))) {
        LOG_WARN("failed to set tenant config version", KR(ret),
                 K(user_tenant_id));
      }
    }

    ObLSInfo sys_ls_info;
    ObAddrArray addrs;
    this->stopped_ = true;
    if (FAILEDx(GCTX.lst_operator_->get(GCONF.cluster_id, tenant_id, SYS_LS,
                                        share::ObLSTable::DEFAULT_MODE,
                                        sys_ls_info))) {
      LOG_WARN("fail to get sys ls info by operator", KR(ret), K(tenant_id));
    } else if (OB_FAIL(sys_ls_info.get_paxos_member_addrs(addrs))) {
      LOG_WARN("fail to get paxos member addrs", K(ret), K(tenant_id),
               K(sys_ls_info));
    } else if (OB_FAIL(publish_schema(tenant_id, addrs))) {
      LOG_WARN("fail to publish schema", KR(ret), K(tenant_id), K(addrs));
    }
    // 3. set baseline schema version
    if (OB_SUCC(ret)) {
      ObGlobalStatProxy global_stat_proxy(*sql_proxy_, tenant_id);
      ObRefreshSchemaStatus schema_status;
      schema_status.tenant_id_ = tenant_id;
      int64_t baseline_schema_version = OB_INVALID_VERSION;
      if (OB_FAIL(schema_service_->get_schema_version_in_inner_table(
              *sql_proxy_, schema_status, baseline_schema_version))) {
        LOG_WARN("fail to gen new schema version", KR(ret), K(schema_status));
      } else if (OB_FAIL(global_stat_proxy.set_baseline_schema_version(
                     baseline_schema_version))) {
        LOG_WARN("fail to set baseline schema version", KR(ret), K(tenant_id),
                 K(baseline_schema_version));
      }
    }
  }

  LOG_INFO("[CREATE_TENANT] STEP 2.4. finish init tenant schemas", KR(ret),
           K(tenant_id), "cost",
           ObTimeUtility::fast_current_time() - start_time);
  this->stopped_ = false;
  return ret;
}

} // namespace rootserver
} // namespace oceanbase