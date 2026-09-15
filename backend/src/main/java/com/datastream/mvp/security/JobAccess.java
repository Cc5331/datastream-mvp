package com.datastream.mvp.security;

import com.datastream.mvp.model.JobDefinition;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

/**
 * 作业资源归属校验的唯一实现（RBAC）。
 *
 * 规则：
 * - ADMIN：全量可见；
 * - OPERATOR / VIEWER：只能访问自己创建的作业；
 * - ownerId 为空的作业（历史遗留、脚本直接入库、创建者已被删除等）**不再对所有人开放**，
 *   仅 ADMIN 可访问——此前「ownerId 非空才校验」等于把无归属作业暴露给任意登录用户。
 *
 * 之所以抽成静态工具：JobService / DependencyService / LineageService / MonitorService
 * 原本各自复制了一份判断，规则一度不一致（MonitorService 曾把空归属作业判为可见）。
 */
public final class JobAccess {

    private JobAccess() {
    }

    /** 校验当前用户能否访问该作业，不满足时抛 401/403 */
    public static void assertCanAccess(JobDefinition job, CurrentUser cu) {
        if (cu == null) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "未登录");
        }
        if (cu.isAdmin()) return;
        Long ownerId = job == null ? null : job.getOwnerId();
        if (ownerId == null) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "该作业无归属人，仅管理员可访问");
        }
        if (!ownerId.equals(cu.id())) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "无权访问该作业");
        }
    }

    /** 列表/统计过滤用：与 {@link #assertCanAccess} 同一规则，不抛异常 */
    public static boolean canAccess(JobDefinition job, CurrentUser cu) {
        if (cu == null) return false;
        if (cu.isAdmin()) return true;
        return job != null && job.getOwnerId() != null && job.getOwnerId().equals(cu.id());
    }
}
